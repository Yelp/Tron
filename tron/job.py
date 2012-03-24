import logging
import os
import shutil
from collections import deque
from twisted.internet import reactor

from tron import action, command_context, event
from tron.action import Action, ActionRun
from tron.scheduler import scheduler_from_config
from tron.utils import timeutils
from tron.utils.observer import Observable, Observer
from tron.utils.proxy import CollectionProxy


class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger('tron.job')

RUN_LIMIT = 50
SECS_PER_DAY = 86400
MICRO_SEC = .000001


class JobRun(Observable, Observer):
    """A JobRun is an execution of a Job.  It has a list of ActionRuns."""

    EVENT_DONE = 'done'

    def __init__(self, job, run_num=None, run_time=None):
        super(JobRun, self).__init__()
        # TODO: remove this reference
        self.job = job
        self.run_num = job.next_num() if run_num is None else run_num
        self.state_callback = job.state_callback
        self.id = "%s.%s" % (job.name, self.run_num)

        self.run_time = run_time
        self.start_time = None
        self.end_time = None
        self.node = None
        self.action_runs = []
        self.cleanup_action_run = None
        self.context = command_context.CommandContext(self, job.context)
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

    @property
    def output_path(self):
        return os.path.join(self.job.output_path, self.id)

    def set_run_time(self, run_time):
        self.run_time = run_time

        # TODO: why do actions even have a run time if they're all the same
        # as the job_run?
        for action_run in self.action_runs_with_cleanup:
            action_run.run_time = run_time

    def seconds_until_run_time(self):
        run_time = self.run_time
        tz = self.job.scheduler.time_zone
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
        self.event_recorder.emit_info("scheduled_start")
        if self.attempt_start():
            return True

        # If its still scheduled it failed to run
        if self.is_scheduled:
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

    def restore(self, data):
        self.start_time = data['start_time']
        self.end_time = data['end_time']
        self.set_run_time(data['run_time'])

        for r, state in zip(self.action_runs, data['runs']):
            r.restore_state(state)

        if self.cleanup_action_run is not None:
            self.cleanup_action_run.restore_state(data['cleanup_run'])

        self.event_recorder.emit_info("restored")

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
        if not self.job.enabled:
            return False

        node = self.node if self.job.all_nodes else None
        # TODO: shouldn't this check the state of self ?
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
        if self.is_failure:
            return True
        return all(r.is_done for r in self.action_runs)

    @property
    def cleanup_job_status(self):
        """Provide 'SUCCESS' or 'FAILURE' to a cleanup action context based on
        the status of the other steps
        """
        if self.is_failure:
            return 'FAILED'
        elif self.all_but_cleanup_success:
            return 'SUCCESS'
        else:
            return 'UNKNOWN'

    def __str__(self):
        return "JOB_RUN:%s" % self.id


class Job(Observable, Observer):
    """Job core object.

    Responsibilities:
        - JobRun collection
    """
    run_num = 0

    # Constants for Job status
    STATUS_DISABLED =   "DISABLED"
    STATUS_ENABLED =    "ENABLED"
    STATUS_UNKNOWN =    "UNKNOWN"
    STATUS_RUNNING =    "RUNNING"

    EVENT_STATE_CHANGE = 'state_change'

    def next_num(self):
        self.run_num += 1
        return self.run_num - 1

    def __init__(self,
        name=None,
        action=None,
        context=None,
        event_recorder=None,
        queueing=True,
        run_limit=RUN_LIMIT,
        all_nodes=False,
        scheduler=None,
        node_pool=None,
        enabled=True
    ):
        super(Job, self).__init__()
        self.name = name
        self.topo_actions = [action] if action else []
        self.cleanup_action = None
        self.scheduler = scheduler
        self.runs = deque()

        self.queueing = queueing
        self.all_nodes = all_nodes
        self.enabled = enabled

        self.run_limit = run_limit
        self.node_pool = node_pool
        self.output_path = None
        self.state_callback = lambda: None
        self.context = command_context.CommandContext(self, context)
        self.event_recorder = event.EventRecorder(self, parent=event_recorder)

    @classmethod
    def from_config(cls, job_config, node_pools, time_zone):
        """Build a job from a ConfigJob."""
        job = cls(
            name=job_config.name,
            queueing=job_config.queueing,
            run_limit=job_config.run_limit,
            all_nodes=job_config.all_nodes,
            node_pool=node_pools[job_config.node] if job_config.node else None,
            scheduler=scheduler_from_config(job_config.schedule, time_zone),
            enabled=job_config.enabled
        )

        new_actions = dict(
            (name, Action.from_config(action_conf, node_pools))
            for name, action_conf in job_config.actions.iteritems()
        )

        for action in new_actions.values():
            for dep in job_config.actions[action.name].requires:
                action.required_actions.append(new_actions[dep])
            job.add_action(action)

        if job_config.cleanup_action:
            action = Action.from_config(job_config.cleanup_action, node_pools)
            job.register_cleanup_action(action)

        return job

    def _register_action(self, action):
        """Prepare an action to be *owned* by this job"""
        if action in self.topo_actions:
            raise Error("Action %s already in jobs %s" % (
                action.name, self.name))

    def register_cleanup_action(self, action):
            self.cleanup_action = action
            action.job = self
            self._register_action(action)

    def notify_state_changed(self):
        """Called to notify this job that its state has changed due to an
        action run state change.
        """
        self.notify(self.EVENT_STATE_CHANGE)

    def add_action(self, action):
        action.job = self
        self._register_action(action)
        self.topo_actions.append(action)

    def __eq__(self, other):
        if (not isinstance(other, Job) or self.name != other.name or
            self.queueing != other.queueing or
            self.scheduler != other.scheduler or
            self.node_pool != other.node_pool or
            len(self.topo_actions) != len(other.topo_actions) or
            self.run_limit != other.run_limit or
            self.all_nodes != other.all_nodes or
            self.cleanup_action != other.cleanup_action):

            return False

        return all([me == you for (me, you) in zip(self.topo_actions,
                                                   other.topo_actions)])

    def __ne__(self, other):
        return not self == other

    def set_context(self, context):
        self.context.next = context

    def enable(self):
        self.enabled = True
        next = self.next_to_finish()

        if next and next.is_queued:
            next.start()

    def remove_run(self, run):
        self.runs.remove(run)

        if os.path.exists(run.output_path):
            shutil.rmtree(run.output_path)

        run.job = None

    def disable(self):
        self.enabled = False

        # We need to get rid of all future runs.
        kill_runs = [run for run in self.runs
                     if (run.is_scheduled or run.is_queued)]
        for run in kill_runs:
            run.cancel()
            self.remove_run(run)

    def newest(self):
        if self.runs[0]:
            return self.runs[0]
        else:
            return None

    def newest_run_by_state(self, state):
        for run in self.runs:
            if run.state.short_name == state:
                return run

        log.warning("No runs with state %s exist", state)

    def next_to_finish(self, node=None):
        """This will return the most recent job if it is not yet running or
        None if the job is complete. Useful for getting the currently running
        job run or next queued/schedule job run.

        Note that Jobs are stored in reverse chronological order.

        Optionally only compare jobs for a given node.
        """
        def choose(next, prev):
            if ((next and next.is_running) or
                (node and prev.node != node) or
                prev.is_done or
                prev.is_unknown
            ):
                return next
            return prev

        return reduce(choose, self.runs, None)

    def get_run_by_num(self, num):
        def choose(chosen, nxt):
            return nxt if nxt.run_num == num else chosen

        return reduce(choose, self.runs, None)

    def get_run_by_state(self, state):
        """Returns the most recent run which matches the state."""
        for run in self.runs:
            if run.state == state:
                return run
        return None

    def remove_old_runs(self):
        """Remove old runs so the number left matches the run limit. However
        only removes runs up to the last success or up to the next to run.
        """

        nxt = self.next_to_finish()
        next_num = nxt.run_num if nxt else self.runs[0].run_num
        succ_num = self.last_success.run_num if self.last_success else 0
        keep_num = min([next_num, succ_num])

        while (len(self.runs) > self.run_limit and
               keep_num > self.runs[-1].run_num):
            self.remove_run(self.runs[-1])

    def get_runs_to_schedule(self):
        """If the scheduler is just a 'best effort' scheduler and this job has
        queued runs, we don't need to schedule any more yet. Otherwise schedule
        the next run.
        """
        best_effort = self.scheduler.is_best_effort

        if best_effort and self.get_run_by_state(ActionRun.STATE_QUEUED):
            return None

        if best_effort and self.get_run_by_state(ActionRun.STATE_SCHEDULED):
            return None

        return self.next_runs()

    # TODO: DELETE
    def next_runs(self):
        """Use the configured scheduler to build the next job runs.  If there
        are runs already scheduled, return those."""
        if not self.scheduler:
            return []

        last_run_time = None
        if self.runs:
            last_run_time = self.runs[0].run_time

        next_run_time = self.scheduler.next_run_time(last_run_time)
        return self.build_and_add_runs(next_run_time)

    @property
    def last_success(self):
        """Last successful JobRun."""
        return self.get_run_by_state(ActionRun.STATE_SUCCEEDED)

    def build_action_dag(self, job_run, all_actions):
        """Build actions and setup requirements"""

        def create_action_inst(action_inst):
            action_run = action_inst.build_run(job_run)
            job_run.action_runs.append(action_run)
            return action_run

        action_runs_by_name = dict(
            (action_inst.name, create_action_inst(action_inst))
            for action_inst in all_actions
        )

        for action_inst in all_actions:
            for req_action in action_inst.required_actions:
                if req_action.name not in action_runs_by_name:
                    raise ConfigBuildMismatchError(
                        "Unknown action %s, configuration mismatch?" %
                        req_action.name)

                # Two-way, waiting runs and required_runs
                req_action_run = action_runs_by_name[req_action.name]
                waiting_action_run = action_runs_by_name[action_inst.name]
                req_action_run.waiting_runs.append(waiting_action_run)
                waiting_action_run.required_runs.append(req_action_run)

    def build_run(self, run_time, node=None, actions=None, run_num=None,
                  cleanup_action=None):
        """Create a JobRun with the specified run_time."""
        job_run = JobRun(self, run_time=run_time, run_num=run_num)

        job_run.node = node or self.node_pool.next()
        log.info("Built run %s", job_run.id)

        # TODO: It would be great if this were abstracted out a bit
        if (os.path.exists(self.output_path) and
            not os.path.exists(job_run.output_path)):
            os.mkdir(job_run.output_path)

        # Actions can be specified when restored from serialized state
        if not actions:
            actions = self.topo_actions

        self.build_action_dag(job_run, actions)

        cleanup_action = cleanup_action or self.cleanup_action
        if cleanup_action is not None:
            cleanup_action_run = self.cleanup_action.build_run(
                job_run, cleanup=True)
            job_run.cleanup_action_run = cleanup_action_run

        self.watch(job_run, JobRun.EVENT_DONE)
        return job_run

    def watcher(self, observable, event):
        """Watch for events from JobRuns."""
        if event == JobRun.EVENT_DONE:
            self.notify_job_run_complete()

    def notify_job_run_complete(self):
        """Called to notify this Job that one of its JobRuns has completed.
        This method will then check to see if a new job run needs to be
        scheduled and if one is scheduled, attempt to start it.
        """

        # TODO: fix this logic
        next = self.next_to_finish()
        if next and next.is_queued:
            next.attempt_start()

        # See if we need to scheduler another Run
        self.schedule_next_run()

    def _schedule(self, run):
        secs = run.seconds_until_run_time()
        reactor.callLater(secs, self.run_job, run)

    def schedule_next_run(self):
        runs = self.get_runs_to_schedule() or []
        for next in runs:
            log.info("Scheduling next job for %s", next.job.name)
            self._schedule(next)

    def run_job(self, job_run):
        """This runs when a job was scheduled.
        Here we run the job and schedule the next time it should run
        """
        if not job_run.job:
            return

        # TODO: do these belong here?
        if not job_run.job.enabled:
            return

        job_run.scheduled_start()
        self.schedule_next_run()

    def build_runs(self, run_time):
        """Builds runs. If all_nodes is set, build a run for every node,
        otherwise just builds a single run on a single node.
        """
        if self.all_nodes:
            return [
            self.build_run(run_time, node=node)
            for node in self.node_pool.nodes
            ]
        return [self.build_run(run_time)]

    def build_and_add_runs(self, run_time):
        """Builds new runs and adds them to the run list. Also trims off old
        runs.
        """
        runs = self.build_runs(run_time)
        for job_run in runs:
            self.runs.appendleft(job_run)
        self.remove_old_runs()
        return runs

    def manual_start(self, run_time=None):
        """Trigger a job run manually (instead of from the scheduler)."""
        run_time = run_time or timeutils.current_time()
        manual_runs = self.build_runs(run_time)

        # Insert this run before any scheduled runs
        scheduled = deque()
        while self.runs and self.runs[0].is_scheduled:
            scheduled.appendleft(self.runs.popleft())

        self.runs.extendleft(manual_runs)
        self.runs.extendleft(scheduled)

        for r in manual_runs:
            r.manual_start()
        return manual_runs

    def absorb_old_job(self, old):
        self.runs = old.runs

        self.output_path = old.output_path
        self.run_num = old.run_num
        self.enabled = old.enabled

        self.context = old.context
        self.event_recorder = old.event_recorder
        self.event_recorder.entity = self
        self.context.base = self

        self.event_recorder.emit_notice("reconfigured")

    @property
    def data(self):
        """This data is used to serialize the state of this job."""
        return {
            'runs': [r.data for r in self.runs],
            'enabled': self.enabled
        }

    def repr_data(self):
        """Returns a dict that is the external representation of this job."""
        last_success = self.last_success.end_time if self.last_success else None
        return {
            'name':         self.name,
            'scheduler':    str(self.scheduler),
            'action_names': [a.name for a in self.topo_actions],
            'node_pool':    [n.hostname for n in self.node_pool.nodes],
            'status':       self.status,
            'last_success': last_success,
        }

    @property
    def status(self):
        """The Jobs current status is determined by its last/next run."""
        current_run = self.next_to_finish()
        if not current_run:
            return self.STATUS_DISABLED
        if current_run.is_running:
            return self.STATUS_RUNNING
        if current_run.is_scheduled:
            return self.STATUS_ENABLED
        return self.STATUS_UNKNOWN

    def restore(self, data):
        self.enabled = data['enabled']

        for r_data in data['runs']:
            try:
                self.restore_run(r_data)
            except Error, e:
                log.warning("Failed to restore job: %r (%r)", r_data, e)
                continue

        self.event_recorder.emit_info("restored")

    def restore_run(self, data):
        action_names = []
        for action in data['runs']:
            action_names.append(action['id'].split('.')[-1])

        def action_filter(topo_action):
            return topo_action.name in action_names

        action_list = filter(action_filter, self.topo_actions)

        ca = (self.cleanup_action
              if self.cleanup_action and action_filter(self.cleanup_action)
              else None)
        # TODO: this seems like it should be easier to restore a runs state
        run = self.build_run(None, run_num=data['run_num'], actions=action_list,
                             cleanup_action=ca)
        self.run_num = max([run.run_num + 1, self.run_num])

        run.restore(data)
        self.runs.append(run)
        return run

    def setup_job_dir(self, working_dir):
        """Setup a directory to store this jobs logs."""
        self.output_path = os.path.join(working_dir, self.name)
        if not os.path.exists(self.output_path):
            os.mkdir(self.output_path)

    def __str__(self):
        return "JOB:%s" % self.name
