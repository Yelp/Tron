import uuid
import logging
import re
import datetime
import os

from twisted.internet import defer

from tron import node
from tron.utils import timeutils

log = logging.getLogger('tron.action')

ACTION_RUN_SCHEDULED = 0
ACTION_RUN_QUEUED = 1
ACTION_RUN_CANCELLED = 2
ACTION_RUN_UNKNOWN = 3
ACTION_RUN_RUNNING = 4
ACTION_RUN_FAILED = 10
ACTION_RUN_SUCCEEDED = 11

class ActionRunVariables(object):
    """Dictionary like object that provides variable subsitution for action commands"""
    def __init__(self, action_run):
        self.run = action_run

    def __getitem__(self, name):
        # Extract any arthimetic stuff
        match = re.match(r'([\w]+)([+-]*)(\d*)', name)
        attr, op, value = match.groups()
        if attr == "shortdate":
            if value:
                delta = datetime.timedelta(days=int(value))
                if op == "-":
                    delta *= -1
                run_date = self.run.job_run.run_time + delta
            else:
                run_date = self.run.job_run.run_time
            
            return "%.4d-%.2d-%.2d" % (run_date.year, run_date.month, run_date.day)
        elif attr == "unixtime":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return int(timeutils.to_timestamp(self.run.job_run.run_time)) + delta
        elif attr == "daynumber":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return self.run.job_run.run_time.toordinal() + delta
        elif attr == "actionname":
            if op:
                raise ValueError("Adjustments not allowed")
            return self.run.action.name
        elif attr == "runid":
            if op:
                raise ValueError("Adjustments not allowed")
            return self.run.id
        else:
            return super(ActionRunVariables, self).__getitem__(name)


class ActionRun(object):
    """An instance of running a action"""
    def __init__(self, action, job_run):
        self.action = action
        self.job_run = job_run
        self.id = "%s.%s" % (job_run.id, action.name)
        
        self.run_time = None    # What time are we supposed to start
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end

        self.state = ACTION_RUN_QUEUED if action.required_actions else ACTION_RUN_SCHEDULED
        self.exit_status = None

        self.output_path = None
        if action.job.output_dir:
            self.output_path = os.path.join(action.job.output_dir, self.id + '.out')
        self.output_file = None
        
        self.node = action.node_pool.next() if action.node_pool else job_run.node
        self.job_run = job_run

        self.required_runs = []
        self.waiting_runs = []
        self.data = {}
        self.state_changed()

    def tail_output(self, num_lines=0):
        try:
            out = open(self.output_path)
        except IOError:
            return []

        if not num_lines or num_lines <= 0:
            return out.readlines()
        
        pos = num_lines
        lines = []
        while len(lines) < num_lines:
            try:
                out.seek(-pos, os.SEEK_END)   
            except IOError:
                out.seek(0)
                break
            finally:
                lines = list(out)
            pos *= 2
        return lines[-num_lines:]

    def attempt_start(self):
        if self.should_start:
            self.start()

    def start(self):
        log.info("Starting action run %s", self.id)
        
        self.start_time = timeutils.current_time()
        self.state = ACTION_RUN_RUNNING
        self._open_output_file()
        self.state_changed()

        # And now we try to actually start some work....
        ret = self.node.run(self)
        if isinstance(ret, defer.Deferred):
            self._setup_callbacks(ret)

    def cancel(self):
        if self.is_scheduled or self.is_queued:
            self.state = ACTION_RUN_CANCELLED
            self.state_changed()
    
    def queue(self):
        if self.is_scheduled or self.is_cancelled:
            self.state = ACTION_RUN_QUEUED
            self.state_changed()

    def _open_output_file(self):
        if self.output_path is None:
            return

        try:
            log.info("Opening file %s for output", self.output_path)
            self.output_file = open(self.output_path, 'w')
        except IOError, e:
            log.error(str(e) + " - Not storing command output!")

    def _handle_errback(self, result):
        """Handle an error where the node wasn't able to give us an exit code"""
        log.info("Action error: %s", str(result))
        if isinstance(result.value, node.ConnectError):
            log.warning("Failed to connect to host %s for run %s", self.node.hostname, self.id)
            self.fail(None)
        elif isinstance(result.value, node.ResultError):
            log.warning("Failed to retrieve exit for run %s after executing command on host %s", self.id, self.node.hostname)
            self.fail_unknown()
        else:
            log.warning("Unknown failure for run %s on host %s: %s", self.id, self.node.hostname, str(result))
            self.fail_unknown()
            
    def _handle_callback(self, exit_code):
        """If the node successfully executes and get's a result from our run, handle the exit code here."""
        if exit_code == 0:
            self.succeed()
        else:
            self.fail(exit_code)
       
        return exit_code
        
    def _setup_callbacks(self, deferred):
        """Execution has been deferred, so setup the callbacks so we can record our own status"""
        deferred.addCallback(self._handle_callback)
        deferred.addErrback(self._handle_errback)

    def start_dependants(self):
        for run in self.waiting_runs:
            run.attempt_start()

    def ignore_dependants(self):
        for run in self.waiting_runs:
            log.info("Not running waiting run %s, the dependant action failed", run.id)

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Action run %s failed with exit status %r", self.id, exit_status)

        self.state = ACTION_RUN_FAILED
        self.exit_status = exit_status
        self.end_time = timeutils.current_time()
        self.job_run.run_completed()
        self.state_changed()

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know what result was"""
        log.info("Lost communication with action run %s", self.id)

        self.state = ACTION_RUN_FAILED
        self.exit_status = None
        self.end_time = None
        self.job_run.run_completed()
        self.state_changed()

    def mark_success(self):
        self.exit_status = 0
        self.state = ACTION_RUN_SUCCEEDED
        self.end_time = timeutils.current_time()
        self.job_run.run_completed()
        self.state_changed()

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Action run %s succeeded", self.id)
        
        self.mark_success()
        self.start_dependants()

    def restore_state(self, state):
        self.id = state['id']
        self.state = state['state']
        self.run_time = state['run_time']
        self.start_time = state['start_time']
        self.end_time = state['end_time']

        if self.is_running:
            self.state = ACTION_RUN_UNKNOWN
        self.state_changed()

    def state_changed(self):
        self.data['id'] = self.id
        self.data['state'] = self.state
        self.data['run_time'] = self.run_time
        self.data['start_time'] = self.start_time
        self.data['end_time'] = self.end_time
        
        if self.run_time:
            self.data['command'] = self.command
        
        if self.action.job.state_callback:
            self.action.job.state_callback()

    @property
    def command(self):
        action_vars = ActionRunVariables(self)
        return self.action.command % action_vars

    @property
    def timeout_secs(self):
        if self.action.timeout is None:
            return None
        else:
            return self.action.timeout.seconds

    @property
    def is_queued(self):
        return self.state == ACTION_RUN_QUEUED
    
    @property
    def is_cancelled(self):
        return self.state == ACTION_RUN_CANCELLED

    @property
    def is_scheduled(self):
        return self.state == ACTION_RUN_SCHEDULED

    @property
    def is_done(self):
        return self.state in (ACTION_RUN_FAILED, ACTION_RUN_SUCCEEDED, ACTION_RUN_CANCELLED)

    @property
    def is_ran(self):
        return self.state in (ACTION_RUN_FAILED, ACTION_RUN_SUCCEEDED)

    @property
    def is_unknown(self):
        return self.state == ACTION_RUN_UNKNOWN

    @property
    def is_running(self):
        return self.state == ACTION_RUN_RUNNING

    @property
    def is_failed(self):
        return self.state == ACTION_RUN_FAILED

    @property
    def is_success(self):
        return self.state == ACTION_RUN_SUCCEEDED

    @property
    def should_start(self):
        return all([r.is_success for r in self.required_runs])
 
class Action(object):
    def __init__(self, name=None, node_pool=None, timeout=None):
        self.name = name
        self.node_pool = node_pool
        self.timeout = timeout

        self.required_actions = []
        self.job = None
        self.command = None

    def build_run(self, job_run):
        """Build an instance of ActionRun for this action
        
        This is used by the scheduler when scheduling a run
        """
        new_run = ActionRun(self, job_run)
        return new_run

