"""
 tron.core.jobrun
"""

from collections import deque
import logging
from tron.core.action import ActionRun
from tron.core.actiongraph import ActionRunFactory
from tron.utils.observer import Observable, Observer
from tron.utils.proxy import CollectionProxy

log = logging.getLogger('tron.core.jobrun')


class JobRunContext(object):
    pass
    # TODO

    # TODO: cleanup_job_status


class JobRun(Observable, Observer):
    """A JobRun is an execution of a Job.  It has a list of ActionRuns."""

    EVENT_DONE = 'done'

    def __init__(self, id, run_num, run_time, node, base_path, context=None,
                action_runs=None):
        super(JobRun, self).__init__()
        self.run_num = run_num
        self.id = id

        self.run_time = run_time
        self.base_path = base_path
        self.build_output_dir()
        self.start_time = None
        self.end_time = None
        self.node = node
        self.action_runs = action_runs or []
        self.cleanup_action_run = None
        self.context = command_context.CommandContext(self, context or {})
        # TODO: replace by events
        self.event_recorder = event.EventRecorder(
            self, parent=self.job.event_recorder)
        self.event_recorder.emit_info("created")

        # Setup proxies
        self.proxy_action_runs_with_cleanup = CollectionProxy(
            self.action_runs_with_cleanup, [
                ('is_failure',      any,    False),
                ('is_starting',     any,    False),
                ('is_running',      any,    False),
                ('is_scheduled',    any,    False),
                ('is_unknown',      any,    False),
                ('is_queued',       all,    False),
                ('is_cancelled',    all,    False),
                ('is_skipped',      all,    False),
                ('is_done',         all,    False),
                ('check_state',     all,    True),
                ('cancel',          all,    True),
                ('succeed',         all,    True),
                ('fail',            any,    True)
            ])

        self.proxy_action_runs = CollectionProxy(
            self.action_runs, [
                ('schedule',        all,    True),
                ('queue',           all,    True),
            ])
        super(JobRun, self).__init__()

    @classmethod
    def from_state(cls, state, actions):
        pass

    @classmethod
    def for_job(cls, job, run_num, run_time, node):
        """Create a JobRun for a job."""
        id = "%s.%s" % (job.name, run_num)
        run = cls(id, run_num, run_time, node,
            job.output_path, job.context,
        )

        action_run_graph = ActionRunFactory.build_action_run_graph(
                job.action_graph, run)
        run.register_cleanup_action(action_run_graph)
        cleanup_action_run = ActionRunFactory.build_run_for_action(
                job.cleanup_action, run)
        run.register_action_runs(cleanup_action_run)
        return run

    def register_action_runs(self, action_runs):
        """Store action runs and register callbacks."""
        self.action_runs = action_runs
        for action_run in action_runs:
            self.watch(action_run, True)

    def register_cleanup_action(self, cleanup_action):
        pass

    # TODO: propagate all state changes (from action runs) up to state serializer

#    def build_run(self, job_run, cleanup=False):
#        """Build an instance of ActionRun for this action."""
#        if cleanup:
#            callback = job_run.notify_cleanup_action_run_completed
#        else:
#            callback = job_run.notify_action_run_completed
#
#        action_run = ActionRun(
#            self,
#            context=job_run.context,
#            node=job_run.node,
#            id="%s.%s" % (job_run.id, self.name),
#            output_path=job_run.output_path,
#            run_time=job_run.run_time)
#
#        # TODO: these should now be setup using watch() on Jobrun.for_job
#        # but I need to add the watcher
#
#        # Notify on any state change so state can be serialized
#        action_run.machine.listen(True, job_run.job.notify_state_changed)
#        # Notify when we reach an end state so the next run can be scheduled
#        action_run.machine.listen(ActionRun.END_STATES, callback)
#        return action_run



    @property
    def output_path(self):
        return os.path.join(self.base_path, self.id)

    def build_output_dir(self):
        # TODO: It would be great if this were abstracted out a bit
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

    def set_run_time(self, run_time):
        self.run_time = run_time

        # TODO: why do actions even have a run time if they're all the same
        # as the job_run?
        for action_run in self.action_runs_with_cleanup:
            action_run.run_time = run_time

    def seconds_until_run_time(self):
        run_time = self.run_time
        tz = run_time.tz
        now = timeutils.current_time()
        if tz is not None:
            now = tz.localize(now)
        sleep = run_time - now
        seconds = (sleep.days * SECS_PER_DAY + sleep.seconds +
                   sleep.microseconds * MICRO_SEC)
        return max(0, seconds)

    def scheduled_start(self):
        """Called when this JobRun is triggered automatically because it was
        scheduled to run (and wasn't created for a manual run).
        """
        # TODO: a possible state is that this run was cancelled after being
        # scheduled
        self.event_recorder.emit_info("scheduled_start")
        if self.attempt_start():
            return True

        # If its still scheduled it failed to run
        if self.is_scheduled:
            # TODO: fix this reference, this should probably just be an event
            # triggered to inform the Job that it failed to start
            if self.job.queueing:
                self.event_recorder.emit_notice("queued")
                log.warning("A previous run for %s has not finished - placing"
                            " in queue", self.id)
                self.queue()
                return True

            self.event_recorder.emit_notice("cancelled")
            log.warning("A previous run for %s has not finished"
                        " - cancelling", self.id)
            self.cancel()
            return False

    @property
    def state(self):
        """The overall state of this job run. Based on the state of its actions.
        """
        if self.is_success:
            return ActionRun.STATE_SUCCEEDED
        if self.is_cancelled:
            return ActionRun.STATE_CANCELLED
        if self.is_running:
            return ActionRun.STATE_RUNNING
        if self.is_failure:
            return ActionRun.STATE_FAILED
        if self.is_scheduled:
            return ActionRun.STATE_SCHEDULED
        if self.is_queued:
            return ActionRun.STATE_QUEUED
        if self.is_skipped:
            return ActionRun.STATE_SKIPPED
        return ActionRun.STATE_UNKNOWN

    def start(self):
        if not self.proxy_action_runs_with_cleanup.perform('check_state', 'start'):
            raise InvalidStartStateError("Not scheduled")

        log.info("Starting JobRun %s", self.id)
        self.start_time = timeutils.current_time()

        try:
            for action_run in self.action_runs:
                action_run.attempt_start()
            if self.cleanup_action_run:
                # Don't call attempt_start or it will just go. The JobRun
                # object will kick this when it's actually time to run (any
                # failure or total success).
                self.cleanup_action_run.machine.transition('ready')
            self.event_recorder.emit_info("started")
        except action.Error, e:
            log.warning("Failed to start actions: %r", e)
            raise Error("Failed to start job run")

    def manual_start(self):
        self.event_recorder.emit_info("manual_start")
        self.attempt_start()

        # Similar to a scheduled start, if the attempt didn't take, that must
        # be because something else is running. We'll assume the user meant to
        # queue this job up without caring whether the job was configured for
        # that.
        if self.is_scheduled:
            self.event_recorder.emit_notice("queued")
            log.warning("A previous run for %s has not finished - placing in"
                        " queue", self.id)
            self.queue()

    def attempt_start(self):
        """Starts the JobRun if the JobRun is the next queued run."""
        if self.should_start:
            try:
                self.start()
                return True
            except Error, e:
                log.warning("Attempt to start failed: %r", e)

    def notify_cleanup_action_run_completed(self):
        """Called to notify this JobRun that it's cleanup action is done."""
        self.finalize()

    def notify_action_run_completed(self):
        """Called to notify this JobRun that one of its ActionRuns is done."""
        if self.all_but_cleanup_done:
            if self.cleanup_action_run is None:
                log.info('No cleanup action for %s exists' % self.id)
                self.finalize()
                return

            log.info('Running cleanup action for %s' % self.id)
            self.cleanup_action_run.attempt_start()

    def finalize(self):
        """This is the last step of a JobRun. Called when the cleanup action
        completes or if the job has no cleanup action, called once all action
        runs have reached a 'done' state.

        Sets end_time triggers an event and notifies the Job that is is done.
        """
        self.end_time = timeutils.current_time()

        if self.is_failure:
            self.event_recorder.emit_critical("failed")
        else:
            self.event_recorder.emit_ok("succeeded")

        # Notify Job that this JobRun is complete
        self.notify(self.EVENT_DONE)

    @property
    def data(self):
        """This data is used to serialize the state of this job run."""
        data = {
            'id':           self.id,
            'runs':         [a.data for a in self.action_runs],
            'cleanup_run':  None,
            'run_num':      self.run_num,
            'run_time':     self.run_time,
            'start_time':   self.start_time,
            'end_time':     self.end_time
        }
        if self.cleanup_action_run is not None:
            data['cleanup_run'] = self.cleanup_action_run.data
        return data

    def repr_data(self):
        """A dict that represents the externally visible state."""
        return {
            'id':           self.id,
            'state':        self.state.short_name,
            'node':         self.node.hostname if self.node else None,
            'run_num':      self.run_num,
            'run_time':     self.run_time,
            'start_time':   self.start_time,
            'end_time':     self.end_time,
            }

    def restore_state(self, data):
        self.start_time = data['start_time']
        self.end_time = data['end_time']
        self.set_run_time(data['run_time'])

        for r, state in zip(self.action_runs, data['runs']):
            r.restore_state(state)

        if self.cleanup_action_run is not None:
            self.cleanup_action_run.restore_state(data['cleanup_run'])

        self.event_recorder.emit_info("restored")

    #    def restore_state(self, data):
    #        action_names = []
    #        for action in data['runs']:
    #            action_names.append(action['id'].split('.')[-1])
    #
    #        def action_filter(topo_action):
    #            return topo_action.name in action_names
    #
    #        action_list = filter(action_filter, self.topo_actions)
    #
    #        ca = (self.cleanup_action
    #              if self.cleanup_action and action_filter(self.cleanup_action)
    #              else None)
    #        # TODO: this seems like it should be easier to restore a runs state
    #        run = self.build_run(None, run_num=data['run_num'], actions=action_list,
    #                             cleanup_action=ca)
    #        self.run_num = max([run.run_num + 1, self.run_num])
    #
    #        run.restore(data)
    #        self.runs.append(run)
    #        return run

    def __getattr__(self, name):
        # The order here is important.  We don't want to raise too many
        # exceptions, so proxies should be ordered by those most likely
        # to be used.
        for proxy in [
            self.proxy_action_runs_with_cleanup,
            self.proxy_action_runs
        ]:
            try:
                return proxy.perform(name)
            except AttributeError:
                pass

        # We want to re-raise this exception because the proxy code
        # will not be relevant in the stack trace
        raise AttributeError(name)

    @property
    def action_runs_with_cleanup(self):
        if self.cleanup_action_run is not None:
            return self.action_runs + [self.cleanup_action_run]
        else:
            return self.action_runs

    @property
    def should_start(self):
        """Returns True if the Job is enabled and this is the next JobRun."""
        node = self.node if self.job.all_nodes else None
        # TODO: remove this reference
        return self.job.next_to_finish(node) is self

    @property
    def all_but_cleanup_success(self):
        """Overloaded all_but_cleanup_success, because we can still succeed
        if some actions were skipped.
        """
        return all(r.is_success or r.is_skipped for r in self.action_runs)

    @property
    def is_success(self):
        """Overloaded is_success, because we can still succeed if some
        actions were skipped.
        """
        return all(
            r.is_success or r.is_skipped for r in self.action_runs_with_cleanup
        )

    @property
    def all_but_cleanup_done(self):
        """True when any ActionRun has failed, or when all ActionRuns are done.
        """
        return all(r.is_done for r in self.action_runs)

    @property
    def cleanup_job_status(self):
        """Provide 'SUCCESS' or 'FAILURE' to a cleanup action context based on
        the status of the other steps
        """
        # TODO: these strings from somewhere better
        if self.is_failure:
            return 'FAILED'
        elif self.all_but_cleanup_success:
            return 'SUCCESS'
        else:
            return 'UNKNOWN'

    def cleanup(self):
        """Called to have this JobRun cleanup any resources it has.  This will
         remove the reference to its job and remove its output directory.
         """
        # TODO: use OutputStreamSerializer
        if os.path.exists(self.output_path):
            shutil.rmtree(self.output_path)

        self.node = None
        self.event_recorder = None

    def __str__(self):
        return "JobRun:%s" % self.id


# TODO: tests
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

    def restore_state(self, job_run_states, action_graph):
        """Apply state to all jobs from the state dict."""
        restored_runs = [
            JobRun.from_state(run_state, action_graph)
            for run_state in job_run_states
        ]
        self.runs.appendleft(restored_runs)
        return self.runs.appendleft()

    def build_new_run(self, job, run_time, node):
        """Create a new run for the job, add it to the runs list,
        and return it.
        """
        run_num = self.next_run_num()
        log.info("Building JobRun %s for %s on %s at %s" %
             (run_num, job, node, run_time))

        run = JobRun.for_job(job, run_num, run_time, node)
        self.runs.appendleft(run)
        self.remove_old_runs()
        return run

    def cancel_pending(self):
        """Find any queued or scheduled runs and cancel them."""
        pending_func = lambda r: r.is_scheduled or r.is_queued
        pending = self._get_run_using(pending_func)
        while pending:
            pending.cancel()
            pending = self._get_run_using(pending_func)

    def _get_run_using(self, func):
        """Find the first run (from most recent to least recent), where func()
        returns true.  func() should be a callable which tables a single
        argument (a JobRun), and return True or False.
        """
        for run in self.runs:
            if func(run):
                return run
        return None

    def get_run_by_state(self, state):
        """Returns the most recent run which matches the state."""
        return self._get_run_using(lambda r: r.state == state)

    def get_run_by_num(self, num):
        """Return a the run with run number which matches num."""
        return self._get_run_using(lambda r: r.run_num == num)

    def get_run_by_state_short_name(self, short_name):
        """Returns the most recent run which matches the state short name."""
        return self._get_run_using(lambda r: r.state.short_name == short_name)

    def get_newest(self):
        """Returns the most recently created JobRun."""
        return self.runs[0] if self.runs else None

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
        return max(r.run_num for r in self.runs) + 1

    def remove_old_runs(self):
        """Remove old runs to attempt to reduce the number of completed runs
        to within RUN_LIMIT.
        """
        next = self.get_next_to_finish()
        next_num = next.run_num if next else self.runs[0].run_num
        last_success = self.last_success
        succ_num = last_success.run_num if last_success else 0
        keep_num = min(next_num, succ_num)

        while (
            len(self.runs) > self.run_limit and
            keep_num > self.runs[-1].run_num
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