"""
 Classes to manage job runs.
"""

from collections import deque
import logging
import itertools
import operator
from tron import node, command_context, event
from tron.core import actionrun
from tron.core.actionrun import ActionRun, ActionRunFactory
from tron.serialize import filehandler
from tron.utils import timeutils, proxy
from tron.utils.observer import Observable, Observer

log = logging.getLogger(__name__)

class Error(Exception):
    pass


class JobRunContext(object):

    def __init__(self, job_run):
        self.job_run = job_run

    @property
    def cleanup_job_status(self):
        """Provide 'SUCCESS' or 'FAILURE' to a cleanup action context based on
        the status of the other steps
        """
        if self.job_run.action_runs.is_failed:
            return 'FAILURE'
        elif self.job_run.action_runs.is_complete_without_cleanup:
            return 'SUCCESS'
        return 'UNKNOWN'


class JobRun(Observable, Observer):
    """A JobRun is an execution of a Job.  It has a list of ActionRuns and is
    responsible for starting ActionRuns in the correct order and managing their
    dependencies.
    """

    NOTIFY_DONE           = 'notify_done'
    NOTIFY_STATE_CHANGED  = 'notify_state_changed'

    EVENT_START           = event.EventType(event.LEVEL_INFO, "start")
    EVENT_STARTED         = event.EventType(event.LEVEL_INFO, "started")
    EVENT_FAILED          = event.EventType(event.LEVEL_CRITICAL, "failed")
    EVENT_SUCCEEDED       = event.EventType(event.LEVEL_OK, "succeeded")

    def __init__(self, job_name, run_num, run_time, node, output_path=None,
                base_context=None, action_runs=None,
                action_graph=None, manual=None):
        super(JobRun, self).__init__()
        self.job_name           = job_name
        self.run_num            = run_num
        self.run_time           = run_time
        self.node               = node
        self.output_path        = output_path or filehandler.OutputPath()
        self.output_path.append(self.id)
        self.action_runs_proxy  = None
        self._action_runs       = None
        self.action_graph       = action_graph
        # TODO: expose this through the api
        self.manual             = manual

        if action_runs:
            self.action_runs    = action_runs

        context = JobRunContext(self)
        self.context = command_context.CommandContext(context, base_context)

    @property
    def id(self):
        return '%s.%s' % (self.job_name, self.run_num)

    @classmethod
    def for_job(cls, job, run_num, run_time, node, manual):
        """Create a JobRun for a job."""
        run = cls(job.name, run_num, run_time, node, job.output_path.clone(),
                job.context, action_graph=job.action_graph, manual=manual)

        action_runs     = ActionRunFactory.build_action_run_collection(run)
        run.action_runs = action_runs
        return run

    @classmethod
    def from_state(cls, state_data, action_graph, output_path, context,
                run_node):
        """Restore a JobRun from a serialized state."""
        node_pools = node.NodePoolStore.get_instance()
        if state_data.get('node_name'):
            run_node = node_pools.get(state_data['node_name'])

        # TODO: remove in 0.6
        if 'job_name' not in state_data:
            # This is only to support old state files.
            job_name = state_data['id'].split('.')[0]
        else:
            job_name = state_data['job_name']

        job_run =  cls(
            job_name,
            state_data['run_num'],
            state_data['run_time'],
            run_node,
            action_graph=action_graph,
            manual=state_data.get('manual', False),
            output_path=output_path,
            base_context=context
        )
        action_runs = ActionRunFactory.action_run_collection_from_state(
                job_run, state_data['runs'], state_data['cleanup_run'])
        job_run.action_runs = action_runs
        return job_run

    @property
    def state_data(self):
        """This data is used to serialize the state of this job run."""
        return {
            'job_name':         self.job_name,
            'run_num':          self.run_num,
            'run_time':         self.run_time,
            'node_name':        self.node.name if self.node else None,
            'runs':             self.action_runs.state_data,
            'cleanup_run':      self.action_runs.cleanup_action_state_data,
            'manual':           self.manual,
        }

    def _get_action_runs(self):
        return self._action_runs

    def _set_action_runs(self, run_collection):
        """Store action runs and register callbacks."""
        if self._action_runs is not None:
            raise ValueError("ActionRunCollection already set on %s" % self)

        self._action_runs = run_collection
        for action_run in run_collection.action_runs_with_cleanup:
            self.watch(action_run)

        self.action_runs_proxy = proxy.AttributeProxy(
            self.action_runs,
            [
                'queue',
                'cancel',
                'success',
                'fail',
                'is_cancelled',
                'is_unknown',
                'is_failed',
                'is_succeeded',
                'is_running',
                'is_starting',
                'is_queued',
                'is_scheduled',
                'is_skipped',
                'is_starting',
                'start_time',
                'end_time'
            ])

    def _del_action_runs(self):
        del self._action_runs

    action_runs = property(_get_action_runs, _set_action_runs, _del_action_runs)

    def seconds_until_run_time(self):
        run_time = self.run_time
        now = timeutils.current_time()
        if run_time.tzinfo:
            now = run_time.tzinfo.localize(now)
        return max(0, timeutils.delta_total_seconds(run_time - now))

    def start(self):
        """Start this JobRun as a scheduled run (not a manual run)."""
        self.notify(self.EVENT_START)
        if self.action_runs.has_startable_action_runs and self._do_start():
            return True

    def _do_start(self):
        log.info("Starting JobRun %s", self.id)

        self.action_runs.ready()
        if any(self._start_action_runs()):
            self.notify(self.EVENT_STARTED)
            return True

    def _start_action_runs(self):
        """Start all startable action runs, and return any that were
        successfully started.
        """
        started_actions = []
        for action_run in self.action_runs.get_startable_action_runs():
            try:
                action_run.start()
                started_actions.append(action_run)
            except actionrun.Error, e:
                log.warning("Failed to start actions: %r", e)

        return started_actions

    def handle_action_run_state_change(self, action_run, _):
        """Handle events triggered by JobRuns."""
        # propagate all state changes (from action runs) up to state serializer
        self.notify(self.NOTIFY_STATE_CHANGED)

        if not action_run.is_done:
            return

        if not action_run.is_broken and any(self._start_action_runs()):
            log.info("Action runs started for %s." % self)
            return

        if self.action_runs.is_active or self.action_runs.is_scheduled:
            log.info("%s still has running or scheduled actions." % self)
            return

        # If we can't make any progress, we're done
        cleanup_run = self.action_runs.cleanup_action_run
        if not cleanup_run or cleanup_run.is_done:
            return self.finalize()

        # TODO: remove in (0.6), start() no longer raises an exception
        # When a job is being disabled, or the daemon is being shut down a bunch
        # of ActionRuns will be cancelled/failed. This would cause cleanup
        # action to be triggered more then once. Guard against that.
        if cleanup_run.check_state('start'):
            cleanup_run.start()
    handler = handle_action_run_state_change

    def finalize(self):
        """The last step of a JobRun. Called when the cleanup action
        completes or if the job has no cleanup action, called once all action
        runs have reached a 'done' state.

        Triggers an event to notifies the Job that is is done.
        """
        failure = self.action_runs.is_failed
        event = self.EVENT_FAILED if failure else self.EVENT_SUCCEEDED
        self.notify(event)

        # Notify Job that this JobRun is complete
        self.notify(self.NOTIFY_DONE)

    def cleanup(self):
        """Cleanup any resources used by this JobRun."""
        self.clear_observers()
        self.action_runs.cleanup()
        event.EventManager.get_instance().remove(self)
        self.node = None
        self.action_graph = None
        self._action_runs = None
        self.output_path.delete()

    @property
    def state(self):
        """The overall state of this job run. Based on the state of its actions.
        """
        if not self.action_runs:
            log.info("%s has no state" % self)
            return ActionRun.STATE_UNKNOWN

        if self.action_runs.is_complete:
            return ActionRun.STATE_SUCCEEDED
        if self.action_runs.is_cancelled:
            return ActionRun.STATE_CANCELLED
        if self.action_runs.is_running:
            return ActionRun.STATE_RUNNING
        if self.action_runs.is_starting:
            return ActionRun.STATE_STARTING
        if self.action_runs.is_failed:
            return ActionRun.STATE_FAILED
        if self.action_runs.is_scheduled:
            return ActionRun.STATE_SCHEDULED
        if self.action_runs.is_queued:
            return ActionRun.STATE_QUEUED

        log.info("%s in an unknown state: %s" % (self, self.action_runs))
        return ActionRun.STATE_UNKNOWN

    def __getattr__(self, name):
        if self.action_runs_proxy:
            return self.action_runs_proxy.perform(name)
        raise AttributeError(name)

    def __str__(self):
        return "JobRun:%s" % self.id


class JobRunCollection(object):
    """A JobRunCollection is a deque of JobRun objects. Responsible for
    ordering and logic related to a group of JobRuns which should all be runs
    for the same Job.

    A JobRunCollection is created in two stages. First it's populated from a
    configuration object, and second its state is loaded from a serialized
    state dict.

    Runs in a JobRunCollection should always remain sorted by their run_num.
    """

    def __init__(self, run_limit):
        self.run_limit = run_limit
        self.runs = deque()

    @classmethod
    def from_config(cls, job_config):
        """Factory method for creating a JobRunCollection from a config."""
        return cls(job_config.run_limit)

    def restore_state(self, state_data, action_graph, output_path, context,
            node_pool):
        """Apply state to all jobs from the state dict."""
        if self.runs:
            msg = "State can not be restored to a collection with runs."
            raise ValueError(msg)

        restored_runs = [
            JobRun.from_state(run_state, action_graph, output_path.clone(),
                context, node_pool.next())
            for run_state in state_data
        ]
        self.runs.extend(restored_runs)
        return restored_runs

    def build_new_run(self, job, run_time, node, manual=False):
        """Create a new run for the job, add it to the runs list,
        and return it.
        """
        run_num = self.next_run_num()
        log.info("Building JobRun %s for %s on %s at %s" %
             (run_num, job, node, run_time))

        run = JobRun.for_job(job, run_num, run_time, node, manual)
        self.runs.appendleft(run)
        self.remove_old_runs()
        return run

    def cancel_pending(self):
        """Find any queued or scheduled runs and cancel them."""
        for pending in self.get_pending():
            pending.cancel()

    def remove_pending(self):
        """Remove pending runs from the run list."""
        for pending in list(self.get_pending()):
            pending.cleanup()
            self.runs.remove(pending)

    def _get_runs_using(self, func, reverse=False):
        """Filter runs using func()."""
        job_runs = self.runs if not reverse else reversed(self.runs)
        return itertools.ifilter(func, job_runs)

    def _get_run_using(self, func, reverse=False):
        """Find the first run (from most recent to least recent), where func()
        returns true.  func() should be a callable which takes a single
        argument (a JobRun), and return True or False.
        """
        try:
            return self._get_runs_using(func, reverse).next()
        except StopIteration:
            return None

    def _filter_by_state(self, state):
        return lambda r: r.state == state

    def get_run_by_state(self, state):
        """Returns the most recent run which matches the state."""
        return self._get_run_using(self._filter_by_state(state))

    def get_run_by_num(self, num):
        """Return a the run with run number which matches num."""
        return self._get_run_using(lambda r: r.run_num == num)

    def get_run_by_state_short_name(self, short_name):
        """Returns the most recent run which matches the state short name."""
        return self._get_run_using(lambda r: r.state.short_name == short_name)

    def get_newest(self, include_manual=True):
        """Returns the most recently created JobRun."""
        func = lambda r: True if include_manual else not r.manual
        return self._get_run_using(func)

    def get_pending(self):
        """Return the job runs that are queued or scheduled."""
        return self._get_runs_using(lambda r: r.is_scheduled or r.is_queued)

    @property
    def has_pending(self):
        return any(self.get_pending())

    def get_active(self, node=None):
        if node:
            func = lambda r: (r.is_running or r.is_starting) and r.node == node
        else:
            func = lambda r: r.is_running or r.is_starting
        return self._get_runs_using(func)

    def get_first_queued(self, node=None):
        state = ActionRun.STATE_QUEUED
        if node:
            queued_func = lambda r: r.state == state and r.node == node
        else:
            queued_func = self._filter_by_state(state)
        return self._get_run_using(queued_func, reverse=True)

    def get_scheduled(self):
        state = ActionRun.STATE_SCHEDULED
        return self._get_runs_using(self._filter_by_state(state))

    def get_next_to_finish(self, node=None):
        """Return the most recent run which is either running or scheduled. If
        node is none None, then only looks for runs on that node.
        """
        def compare(run):
            if node and run.node != node:
                return False
            if run.is_running or run.is_scheduled:
                return run
        return self._get_run_using(compare)

    def next_run_num(self):
        """Return the next run number to use."""
        if not self.runs:
            return 0
        return max(r.run_num for r in self.runs) + 1

    def remove_old_runs(self):
        """Remove old runs to attempt to reduce the number of completed runs
        to within RUN_LIMIT.
        """
        if not self.last_success or not self.runs:
            return

        next_run = self.get_next_to_finish() or self.get_newest()
        keep_run = min(next_run, self.last_success,
                key=operator.attrgetter('run_num'))

        while (
            len(self.runs) > self.run_limit and
            keep_run.run_num > self.runs[-1].run_num
        ):
            run = self.runs.pop()
            run.cleanup()

    @property
    def state_data(self):
        """Return the state data to serialize."""
        return [r.state_data for r in self.runs]

    @property
    def last_success(self):
        return self.get_run_by_state(ActionRun.STATE_SUCCEEDED)

    def __iter__(self):
        return iter(self.runs)

    def __str__(self):
        return "%s[%s]" % (
                type(self).__name__,
                ', '.join("%s(%s)" % (r.run_num, r.state) for r in self.runs)
        )