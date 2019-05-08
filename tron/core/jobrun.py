"""
 Classes to manage job runs.
"""
import json
import logging
import time
from collections import deque

import tron.metrics as metrics
from tron import command_context
from tron import node
from tron.core.actionrun import ActionRun
from tron.core.actionrun import ActionRunFactory
from tron.serialize import filehandler
from tron.utils import maybe_decode
from tron.utils import next_or_none
from tron.utils import proxy
from tron.utils import timeutils
from tron.utils.observer import Observable
from tron.utils.observer import Observer

log = logging.getLogger(__name__)
state_logger = logging.getLogger(f'{__name__}.state_changes')


class Error(Exception):
    pass


class JobRun(Observable, Observer):
    """A JobRun is an execution of a Job.  It has a list of ActionRuns and is
    responsible for starting ActionRuns in the correct order and managing their
    dependencies.
    """

    NOTIFY_DONE = 'notify_done'
    NOTIFY_STATE_CHANGED = 'notify_state_changed'

    context_class = command_context.JobRunContext

    # TODO: use config object
    def __init__(
        self,
        job_name,
        run_num,
        run_time,
        node,
        output_path=None,
        base_context=None,
        action_runs=None,
        action_graph=None,
        manual=None,
    ):
        super(JobRun, self).__init__()
        self.job_name = maybe_decode(job_name)
        self.run_num = run_num
        self.run_time = run_time
        self.node = node
        self.output_path = output_path or filehandler.OutputPath()
        self.output_path.append(self.id)
        self.action_runs_proxy = None
        self._action_runs = None
        self.action_graph = action_graph
        self.manual = manual

        if action_runs:
            self.action_runs = action_runs

        self.context = command_context.build_context(self, base_context)

    @property
    def id(self):
        return '%s.%s' % (self.job_name, self.run_num)

    @classmethod
    def for_job(cls, job, run_num, run_time, node, manual):
        """Create a JobRun for a job."""
        run = cls(
            job.get_name(),
            run_num,
            run_time,
            node,
            output_path=job.output_path.clone(),
            base_context=job.context,
            action_graph=job.action_graph,
            manual=manual,
        )
        action_runs = ActionRunFactory.build_action_run_collection(
            run,
            job.action_runner,
        )
        run.action_runs = action_runs
        return run

    @classmethod
    def from_state(
        cls,
        state_data,
        action_graph,
        output_path,
        context,
        run_node,
    ):
        """Restore a JobRun from a serialized state."""
        pool_repo = node.NodePoolRepository.get_instance()
        run_node = pool_repo.get_node(state_data.get('node_name'), run_node)
        job_name = state_data['job_name']

        job_run = cls(
            job_name,
            state_data['run_num'],
            state_data['run_time'],
            run_node,
            action_graph=action_graph,
            manual=state_data.get('manual', False),
            output_path=output_path,
            base_context=context,
        )
        action_runs = ActionRunFactory.action_run_collection_from_state(
            job_run,
            state_data['runs'],
            state_data['cleanup_run'],
        )
        job_run.action_runs = action_runs
        return job_run

    @property
    def state_data(self):
        """This data is used to serialize the state of this job run."""
        return {
            'job_name': self.job_name,
            'run_num': self.run_num,
            'run_time': self.run_time,
            'node_name': self.node.get_name() if self.node else None,
            'runs': self.action_runs.state_data,
            'cleanup_run': self.action_runs.cleanup_action_state_data,
            'manual': self.manual,
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
            action_run.setup_subscriptions()

        self.action_runs_proxy = proxy.AttributeProxy(
            run_collection,
            [
                'queue',
                'cancel',
                'success',
                'fail',
                'start_time',
                'end_time',
            ],
        )

    def _del_action_runs(self):
        self._action_runs = None
        self.action_runs_proxy = None

    action_runs = property(
        _get_action_runs,
        _set_action_runs,
        _del_action_runs,
    )

    def seconds_until_run_time(self):
        run_time = self.run_time
        if run_time.tzinfo:
            now = timeutils.current_time(tz=run_time.tzinfo)
        else:
            now = timeutils.current_time()
        return max(0, timeutils.delta_total_seconds(run_time - now))

    def start(self):
        self.log_state_update(state='start')
        if self._do_start():
            return True

    def _do_start(self):
        log.info(f"{self} starting")
        self.action_runs.ready()
        if any(self._start_action_runs()):
            log.info(f'{self} started')
            return True

    def stop(self):
        if self.action_runs.is_done:
            return
        self.action_runs.stop()

    def _start_action_runs(self):
        """Start all startable action runs, and return any that were
        successfully started.
        """
        started_runs = [
            action_run
            for action_run in self.action_runs.get_startable_action_runs()
            if action_run.start()
        ]

        if not started_runs:
            for r in self.action_runs:
                if r.is_blocked_on_trigger:
                    log.debug(f"{r} is blocked on triggers: {r.remaining_triggers}")

        return started_runs

    def handle_action_run_state_change(self, action_run: ActionRun, event):
        """Handle events triggered by JobRuns."""
        log.info(f"{self} got an event: {event}")
        metrics.meter(f'tron.actionrun.{event}')

        if event == ActionRun.NOTIFY_TRIGGER_READY:
            if self.is_scheduled or self.is_queued:
                log.info(f"{self} triggers are satisfied but run not started yet")
                return

            started = self._start_action_runs()
            if any(started):
                log.info(
                    f"{self} action runs triggered: "
                    f"{', '.join(str(s) for s in started)}"
                )
            return

        # propagate all state changes (from action runs) up to state serializer
        self.notify(self.NOTIFY_STATE_CHANGED)
        self.log_state_update(
            state=action_run.state,
            action_name=action_run.name,
        )

        if not action_run.is_done:
            return

        if action_run.is_skipped and self.action_runs.is_scheduled:
            return

        if not action_run.is_broken:
            started = self._start_action_runs()
            if any(started):
                log.info(
                    f"{self} action runs started: "
                    f"{', '.join(str(s) for s in started)}"
                )
                return

        if not self.action_runs.is_done:
            log.info(f"{self} still has running or waiting actions")
            return

        # If we can't make any progress, we're done
        cleanup_run: ActionRun = self.action_runs.cleanup_action_run
        if not cleanup_run or cleanup_run.is_done:
            return self.finalize()

        cleanup_run.start()

    handler = handle_action_run_state_change

    def finalize(self):
        """The last step of a JobRun. Called when the cleanup action
        completes or if the job has no cleanup action, called once all action
        runs have reached a 'done' state.

        Triggers an event to notifies the Job that is is done.
        """
        if self.action_runs.is_failed:
            log.error(f'{self} failed')
        else:
            log.info(f'{self} succeeded')

        # Notify Job that this JobRun is complete
        self.notify(self.NOTIFY_DONE)
        self.log_state_update(state=self.state)

    def cleanup(self):
        """Cleanup any resources used by this JobRun."""
        log.info(f'{self} removed')
        self.clear_observers()
        self.action_runs.cleanup()
        self.node = None
        self.action_graph = None
        self._del_action_runs()
        self.output_path.delete()

    def get_action_run(self, action_name):
        return self.action_runs.get(action_name)

    def log_state_update(self, state, action_name=None):
        if action_name is None:
            state = f'job_{state}'
        else:
            action_name = str(action_name)

        data = {
            'job_name': str(self.job_name),
            'run_num': str(self.run_num),
            'action_name': action_name,
            'state': str(state),
            'timestamp': time.time(),
        }
        state_logger.info(json.dumps(data))

    @property
    def state(self):
        """The overall state of this job run. Based on the state of its actions.
        """
        if not self.action_runs:
            log.info(f"{self} has no action runs to determine state")
            return ActionRun.UNKNOWN

        if self.action_runs.is_complete:
            return ActionRun.SUCCEEDED
        if self.action_runs.is_cancelled:
            return ActionRun.CANCELLED
        if self.action_runs.is_running:
            return ActionRun.RUNNING
        if self.action_runs.is_starting:
            return ActionRun.STARTING
        if self.action_runs.is_failed:
            return ActionRun.FAILED
        if self.action_runs.is_blocked_on_trigger:
            return ActionRun.WAITING
        if self.action_runs.is_scheduled:
            return ActionRun.SCHEDULED
        if self.action_runs.is_queued:
            return ActionRun.QUEUED

        return ActionRun.UNKNOWN

    def __getattr__(self, name):
        if name.startswith('is_'):
            state_name = name[3:]
            if state_name not in ActionRun.STATE_MACHINE.states:
                raise RuntimeError(f"{state_name} not in ActionRun.VALID_STATES")
            return self.state == state_name
        elif self.action_runs_proxy:
            return self.action_runs_proxy.perform(name)
        else:
            raise AttributeError(name)

    def __str__(self):
        return f"JobRun:{self.id}"


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

    def build_new_run(self, job, run_time, node, manual=False):
        """Create a new run for the job, add it to the runs list,
        and return it.
        """
        run_num = self.next_run_num()
        run = JobRun.for_job(job, run_num, run_time, node, manual)
        log.info(f"{run} created on {node.name} at {run_time}")
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

    def get_run_by_state(self, state):
        """Returns the most recent run which matches the state."""
        return next_or_none(r for r in self.runs if r.state == state)

    def get_run_by_num(self, num):
        """Return a the run with run number which matches num."""
        return next_or_none(r for r in self.runs if r.run_num == num)

    def get_run_by_index(self, index):
        """Return the job run at index. Jobs are indexed from oldest to newest.
        """
        try:
            return self.runs[index * -1 - 1]
        except IndexError:
            return None

    def get_newest(self, include_manual=True):
        """Returns the most recently created JobRun."""
        return next_or_none(
            r for r in self.runs if include_manual or not r.manual
        )

    def get_pending(self):
        """Return the job runs that are queued or scheduled."""
        return [r for r in self.runs if r.state in (ActionRun.SCHEDULED, ActionRun.QUEUED)]

    @property
    def has_pending(self):
        return any(self.get_pending())

    def get_active(self, node=None):
        active_states = {ActionRun.RUNNING, ActionRun.STARTING, ActionRun.WAITING}
        return [
            r for r in self.runs if (r.state in active_states) and
            (not node or r.node == node)
        ]

    def get_first_queued(self, node=None):
        return next_or_none(
            r for r in reversed(self.runs)
            if (not node or r.node == node) and r.state == ActionRun.QUEUED
        )

    def get_scheduled(self):
        return [r for r in self.runs if r.state == ActionRun.SCHEDULED]

    def next_run_num(self):
        """Return the next run number to use."""
        if not self.runs:
            return 0
        return max(r.run_num for r in self.runs) + 1

    def remove_old_runs(self):
        """Remove old runs to reduce the number of completed runs
        to within RUN_LIMIT.
        """
        while len(self.runs) > self.run_limit:
            run = self.runs.pop()
            run.cleanup()

    def get_action_runs(self, action_name):
        return [job_run.get_action_run(action_name) for job_run in self.runs]

    @property
    def state_data(self):
        """Return the state data to serialize."""
        return [r.state_data for r in self.runs]

    @property
    def last_success(self):
        return self.get_run_by_state(ActionRun.SUCCEEDED)

    @property
    def next_run(self):
        return self.get_run_by_state(ActionRun.SCHEDULED)

    def __iter__(self):
        return iter(self.runs)

    def __str__(self):
        return "%s[%s]" % (
            type(self).__name__,
            ', '.join("%s(%s)" % (r.run_num, r.state) for r in self.runs),
        )


def job_runs_from_state(
    runs,
    action_graph,
    output_path,
    context,
    node_pool,
):
    return [
        JobRun.from_state(
            run,
            action_graph,
            output_path.clone(),
            context,
            node_pool.next(),
        ) for run in runs
    ]
