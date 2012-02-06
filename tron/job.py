import logging
import os
import shutil
from collections import deque

from tron import action, command_context, event
from tron.utils import timeutils


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


class JobRun(object):
    def __init__(self, job, run_num=None):
        self.job = job
        self.run_num = job.next_num() if run_num is None else run_num
        self.state_callback = job.state_callback
        self.id = "%s.%s" % (job.name, self.run_num)

        self.run_time = None
        self.start_time = None
        self.end_time = None
        self.node = None
        self.action_runs = []
        self.cleanup_action_run = None
        self.context = command_context.CommandContext(self, job.context)
        self.event_recorder = event.EventRecorder(
            self, parent=self.job.event_recorder)
        self.event_recorder.emit_info("created")

    @property
    def output_path(self):
        return os.path.join(self.job.output_path, self.id)

    def set_run_time(self, run_time):
        self.run_time = run_time

        for action in self.action_runs_with_cleanup:
            action.run_time = run_time

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
        self.event_recorder.emit_info("scheduled_start")
        self.attempt_start()

        if self.is_scheduled:
            if self.job.queueing:
                self.event_recorder.emit_notice("queued")
                log.warning("A previous run for %s has not finished - placing"
                            " in queue", self.id)
                self.queue()
            else:
                self.event_recorder.emit_notice("cancelled")
                log.warning("A previous run for %s has not finished"
                            " - cancelling", self.id)
                self.cancel()

    def start(self):
        if not (self.is_scheduled or self.is_queued):
            raise InvalidStartStateError("Not scheduled")

        log.info("Starting action job %s", self.id)
        self.start_time = timeutils.current_time()
        self.end_time = None

        try:
            for action_run in self.action_runs:
                action_run.attempt_start()
            self.event_recorder.emit_info("started")
        except action.Error, e:
            log.warning("Failed to start actions: %r", e)
            raise Error("Failed to start job run")

    def manual_start(self):
        self.event_recorder.emit_info("manual_start")
        self.attempt_start()

        # Similiar to a scheduled start, if the attempt didn't take, that must
        # be because something else is running. We'll assume the user meant to
        # queue this job up without caring whether the job was configured for
        # that.
        if self.is_scheduled:
            self.event_recorder.emit_notice("queued")
            log.warning("A previous run for %s has not finished - placing in"
                        " queue", self.id)
            self.queue()

    def attempt_start(self):
        if self.should_start:
            try:
                self.start()
            except Error, e:
                log.warning("Attempt to start failed: %r", e)

    def last_success_check(self):
        if (not self.job.last_success or self.run_num >
                self.job.last_success.run_num):
            self.job.last_success = self

    def run_completed(self):
        if self.is_success:
            self.last_success_check()

            if self.job.constant and self.job.enabled:
                self.job.build_run().start()

        if self.all_but_cleanup_done:
            if self.cleanup_action_run is None:
                log.info('No cleanup action for %s exists' % self.id)
                self.cleanup_completed()
            else:
                log.info('Running cleanup action for %s' % self.id)
                self.cleanup_action_run.attempt_start()

    def cleanup_completed(self):
        self.end_time = timeutils.current_time()

        if self.is_failure:
            self.event_recorder.emit_critical("failed")
        else:
            self.event_recorder.emit_ok("succeeded")

        next = self.job.next_to_finish()
        if next and next.is_queued:
            next.attempt_start()

    @property
    def data(self):
        data = {'runs': [a.data for a in self.action_runs],
                'cleanup_run': None,
                'run_num': self.run_num,
                'run_time': self.run_time,
                'start_time': self.start_time,
                'end_time': self.end_time
        }
        if self.cleanup_action_run is not None:
            data['cleanup_run'] = self.cleanup_action_run.data
        return data

    def restore(self, data):
        self.start_time = data['start_time']
        self.end_time = data['end_time']
        self.set_run_time(data['run_time'])

        for r, state in zip(self.action_runs, data['runs']):
            r.restore_state(state)

        if self.cleanup_action_run is not None:
            self.cleanup_action_run.restore_state(data['cleanup_run'])

        self.event_recorder.emit_info("restored")

    def schedule(self):
        for r in self.action_runs:
            r.schedule()

    def queue(self):
        for r in self.action_runs:
            r.queue()

    def cancel(self):
        for r in self.action_runs_with_cleanup:
            r.cancel()

    def succeed(self):
        for r in self.action_runs_with_cleanup:
            r.mark_success()

    def fail(self):
        for r in self.action_runs_with_cleanup:
            r.fail(0)

    @property
    def action_runs_with_cleanup(self):
        if self.cleanup_action_run is not None:
            return [a for a in self.action_runs] + [self.cleanup_action_run]
        else:
            return self.action_runs

    @property
    def should_start(self):
        if not self.job.enabled or self.is_running:
            return False
        return (self.job.next_to_finish(self.node
                                        if self.job.all_nodes
                                        else None)
                is self)

    @property
    def is_failure(self):
        return any([r.is_failure for r in self.action_runs_with_cleanup])

    @property
    def all_but_cleanup_success(self):
        return all([r.is_success for r in self.action_runs])

    @property
    def is_success(self):
        return all([r.is_success for r in self.action_runs_with_cleanup])

    @property
    def all_but_cleanup_done(self):
        if self.is_failure:
            return True
        return not any(r.is_running or r.is_queued or r.is_scheduled
                       for r in self.action_runs)

    @property
    def is_done(self):
        return not any([r.is_running or r.is_queued or r.is_scheduled
                        for r in self.action_runs_with_cleanup])

    @property
    def is_queued(self):
        return all([r.is_queued for r in self.action_runs_with_cleanup])

    @property
    def is_starting(self):
        return any([r.is_starting for r in self.action_runs_with_cleanup])

    @property
    def is_running(self):
        return any([r.is_running for r in self.action_runs_with_cleanup])

    @property
    def is_scheduled(self):
        return any([r.is_scheduled for r in self.action_runs_with_cleanup])

    @property
    def is_unknown(self):
        return any([r.is_unknown for r in self.action_runs_with_cleanup])

    @property
    def is_cancelled(self):
        return all([r.is_cancelled for r in self.action_runs_with_cleanup])

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


class Job(object):

    run_num = 0

    def next_num(self):
        self.run_num += 1
        return self.run_num - 1

    def __init__(self, name=None, action=None, context=None,
                 event_recorder=None):
        self.name = name
        self.topo_actions = [action] if action else []
        self.cleanup_action = None
        self.scheduler = None
        self.runs = deque()

        self.queueing = True
        self.all_nodes = False
        self.enabled = True
        self.constant = False
        self.last_success = None

        self.run_limit = RUN_LIMIT
        self.node_pool = None
        self.output_path = None
        self.state_callback = lambda: None
        self.context = command_context.CommandContext(self, context)
        self.event_recorder = event.EventRecorder(self, parent=event_recorder)

    def _register_action(self, action):
        """Prepare an action to be *owned* by this job"""
        if action in self.topo_actions:
            raise Error("Action %s already in jobs %s" %
                        (action.name, job.name))

    def listen(self, spec, callback):
        """Mimic the state machine interface for listening to events"""
        assert spec is True
        self.state_callback = callback

    def _notify(self):
        self.state_callback()

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
            if state == 'SUCC' and run.is_success or \
                state == 'CANC' and run.is_cancelled or \
                state == 'RUNN' and run.is_running or \
                state == 'FAIL' and run.is_failure or \
                state == 'SCHE' and run.is_scheduled or \
                state == 'QUE' and run.is_queued or \
                state == 'UNKWN' and run.is_unknown:
                return run

        log.warning("No runs with state %s exist", state)

    def next_to_finish(self, node=None):
        """Returns the next run to finish(optional node requirement). Useful
        for getting the currently running job run or next queued/schedule job
        run.
        """
        def choose(prev, nxt):
            if ((prev and prev.is_running) or
                (node and nxt.node != node) or
                nxt.is_success or
                nxt.is_failure or
                nxt.is_cancelled or
                nxt.is_unknown):
                return prev
            else:
                return nxt

        return reduce(choose, self.runs, None)

    def get_run_by_num(self, num):
        def choose(chosen, nxt):
            return nxt if nxt.run_num == num else chosen

        return reduce(choose, self.runs, None)

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

    def next_runs(self):
        """Use the configured scheduler to build the next job runs"""
        if not self.scheduler:
            return []

        return self.scheduler.next_runs(self)

    def _make_action_run(self, job_run, action_inst, callback):
            action_run = action_inst.build_run(job_run)

            action_run.node = job_run.node

            action_run.machine.listen(True, self._notify)
            action_run.machine.listen(action.ActionRun.STATE_SUCCEEDED,
                                      callback)
            action_run.machine.listen(action.ActionRun.STATE_FAILED,
                                      callback)

            return action_run

    def build_action_dag(self, job_run, all_actions):
        """Build actions and setup requirements"""
        action_runs_by_name = {}
        for action_inst in all_actions:
            action_run = self._make_action_run(job_run,
                                               action_inst,
                                               job_run.run_completed)

            action_runs_by_name[action_inst.name] = action_run
            job_run.action_runs.append(action_run)

            for req_action in action_inst.required_actions:
                if req_action.name not in action_runs_by_name:
                    raise ConfigBuildMismatchError(
                        "Unknown action %s, configuration mismatch?" %
                        req_action.name)

                # Two-way, waiting runs and required_runs
                action = action_runs_by_name[req_action.name]
                action.waiting_runs.append(action_run)
                action_run.required_runs.append(action)

    def build_run(self, node=None, actions=None, run_num=None):
        job_run = JobRun(self, run_num=run_num)

        job_run.node = node or self.node_pool.next()
        log.info("Built run %s", job_run.id)

        # It would be great if this were abstracted out a bit
        if (os.path.exists(self.output_path) and
            not os.path.exists(job_run.output_path)):
            os.mkdir(job_run.output_path)

        # If the actions aren't specified, then we know this is a normal run
        if not actions:
            self.runs.appendleft(job_run)
            actions = self.topo_actions
            self.remove_old_runs()

        self.build_action_dag(job_run, actions)

        if self.cleanup_action is not None:
            cleanup_action_run = self._make_action_run(
                job_run, self.cleanup_action, job_run.cleanup_completed)
            job_run.cleanup_action_run = cleanup_action_run

        return job_run

    def build_runs(self):
        if self.all_nodes:
            return [self.build_run(node=node) for node in self.node_pool.nodes]
        return [self.build_run()]

    def manual_start(self, run_time=None):
        scheduled = deque()
        while self.runs and self.runs[0].is_scheduled:
            scheduled.appendleft(self.runs.popleft())

        man_runs = self.build_runs()
        self.runs.extendleft(scheduled)

        for r in man_runs:
            r.set_run_time(run_time or timeutils.current_time())
            r.manual_start()

        return man_runs

    def absorb_old_job(self, old):
        self.runs = old.runs

        self.output_path = old.output_path
        self.last_success = old.last_success
        self.run_num = old.run_num
        self.enabled = old.enabled

        self.context = old.context
        self.event_recorder = old.event_recorder
        self.event_recorder.entity = self
        self.context.base = self

        self.event_recorder.emit_notice("reconfigured")

    @property
    def data(self):
        return {'runs': [r.data for r in self.runs],
                'enabled': self.enabled
        }

    def restore(self, data):
        self.enabled = data['enabled']

        for r_data in data['runs']:
            try:
                self.restore_run(r_data)
            except job.Error, e:
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

        run = self.build_run(run_num=data['run_num'], actions=action_list)
        self.run_num = max([run.run_num + 1, self.run_num])

        run.restore(data)
        self.runs.append(run)

        if run.is_success and not self.last_success:
            self.last_success = run

        return run

    def __str__(self):
        return "JOB:%s" % self.name
