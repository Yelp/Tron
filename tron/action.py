import uuid
import logging
import re
import datetime
import os

from twisted.internet import defer

from tron import node, command_context
from tron.utils import timeutils

log = logging.getLogger('tron.action')

ACTION_RUN_SCHEDULED = 0
ACTION_RUN_QUEUED = 1
ACTION_RUN_CANCELLED = 2
ACTION_RUN_UNKNOWN = 3
ACTION_RUN_RUNNING = 4
ACTION_RUN_FAILED = 10
ACTION_RUN_SUCCEEDED = 11


class ActionRunContext(object):
    """Context object that gives us access to data about the action run itself"""
    def __init__(self, action_run):
        self.action_run = action_run
    
    @property
    def actionname(self):
        return self.action_run.action.name

    @property
    def runid(self):
        return self.action_run.id

    @property
    def node(self):
        return self.action_run.node.hostname

    def __getitem__(self, name):
        # We've got a complex getitem implementaiton because we want to suport crazy date arithmetic syntax for
        # the run time of the action.
        # This allows features like running a job with an argument that is the previous day by doing something like
        #   ./my_job --run-date=%(shortdate-1)s
        run_time = self.action_run.run_time

        match = re.match(r'([\w]+)([+-]*)(\d*)', name)
        attr, op, value = match.groups()
        if attr == "shortdate":
            if value:
                delta = datetime.timedelta(days=int(value))
                if op == "-":
                    delta *= -1
                run_date = run_time + delta
            else:
                run_date = run_time
            
            return "%.4d-%.2d-%.2d" % (run_date.year, run_date.month, run_date.day)
        elif attr == "unixtime":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return int(timeutils.to_timestamp(run_time)) + delta
        elif attr == "daynumber":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return run_time.toordinal() + delta
        else:
            raise KeyError(name)


class ActionRun(object):
    """An instance of running a action"""
    def __init__(self, action):
        self.action = action
        self.id = None
        
        self.run_time = None    # What time are we supposed to start
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end
        self.exit_status = None
        self.state = ACTION_RUN_QUEUED if action.required_actions else ACTION_RUN_SCHEDULED

        self.node = None
        self.context = None
        self.state_callback = lambda:None
        self.complete_callback = lambda:None

        self.stdout_path = None
        self.stderr_path = None
        self.stdout_file = None
        self.stderr_file = None

        self.required_runs = []
        self.waiting_runs = []

    def tail_stdout(self, num_lines=0):
        return self.tail_file(self.stdout_path, num_lines)

    def tail_stderr(self, num_lines=0):
        return self.tail_file(self.stderr_path, num_lines)
    
    def tail_file(self, path, num_lines):
        try:
            out = open(path, 'r')
        except IOError:
            return []

        if not num_lines or num_lines <= 0:
            lines = out.readlines()
            out.close()
            return lines
        
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
           
        out.close()
        return lines[-num_lines:]

    def attempt_start(self):
        if self.should_start:
            self.start()

    def start(self):
        log.info("Starting action run %s", self.id)
        
        self.start_time = timeutils.current_time()
        self.end_time = None
        self.state = ACTION_RUN_RUNNING
        self._open_output_file()

        # And now we try to actually start some work....
        ret = self.node.run(self)
        if isinstance(ret, defer.Deferred):
            self._setup_callbacks(ret)
        
        self.state_callback()

    def cancel(self):
        if self.is_scheduled or self.is_queued:
            self.state = ACTION_RUN_CANCELLED
            self.state_callback()
    
    def schedule(self):
        if not self.required_runs:
            self.state = ACTION_RUN_SCHEDULED
        else:
            self.state = ACTION_RUN_QUEUED
        
        self.state_callback()
    
    def queue(self):
        if self.is_scheduled or self.is_cancelled:
            self.state = ACTION_RUN_QUEUED
            self.state_callback()

    def _open_output_file(self):
        try:
            log.info("Opening file %s for output", self.stdout_path)
            self.stdout_file = open(self.stdout_path, 'a')
            self.stderr_file = open(self.stderr_path, 'a')
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
        self.complete_callback()
        self.state_callback()

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know what result was"""
        log.info("Lost communication with action run %s", self.id)

        self.state = ACTION_RUN_FAILED
        self.exit_status = None
        self.end_time = None
        self.complete_callback()
        self.state_callback()

    def mark_success(self):
        self.exit_status = 0
        self.state = ACTION_RUN_SUCCEEDED
        self.end_time = timeutils.current_time()
        self.complete_callback()

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Action run %s succeeded", self.id)
        
        self.mark_success()
        self.start_dependants()
        self.state_callback()

    def restore_state(self, state):
        self.id = state['id']
        self.state = state['state']
        self.run_time = state['run_time']
        self.start_time = state['start_time']
        self.end_time = state['end_time']

        if self.is_running:
            self.state = ACTION_RUN_UNKNOWN
        self.state_callback()

    @property
    def data(self):
        return {'id': self.id,
                'state': self.state,
                'run_time': self.run_time,
                'start_time': self.start_time,
                'end_time': self.end_time,
                'command': self.command
        }
        
    @property
    def command(self):
        return self.action.command % self.context

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
        return all([r.is_success for r in self.required_runs]) and \
           not (self.is_running or self.is_success)
 
class Action(object):
    def __init__(self, name=None, node_pool=None):
        self.name = name
        self.node_pool = node_pool

        self.required_actions = []
        self.job = None
        self.command = None

    def __eq__(self, other):
        if not isinstance(other, Action) or self.name != other.name or \
           self.node_pool != other.node_pool or self.command != other.command:
            return False

        return all([me == you for (me, you) in zip(self.required_actions, other.required_actions)]) 

    def __ne__(self, other):
        return not self == other

    def build_run(self, context):
        """Build an instance of ActionRun for this action
        
        This is used by the scheduler when scheduling a run
        """
        new_run = ActionRun(self)
        new_context = ActionRunContext(new_run)
        new_run.context = command_context.CommandContext(new_context, context)

        return new_run

