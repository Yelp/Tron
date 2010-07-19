import uuid
import logging
import re
import datetime
import os

from twisted.internet import defer

from tron import node
from tron.utils import timeutils

log = logging.getLogger('tron.task')

TASK_RUN_SCHEDULED = 0
TASK_RUN_QUEUED = 1
TASK_RUN_CANCELLED = 2
TASK_RUN_UNKNOWN = 3
TASK_RUN_RUNNING = 4
TASK_RUN_FAILED = 10
TASK_RUN_SUCCEEDED = 11

class TaskRunVariables(object):
    """Dictionary like object that provides variable subsitution for task commands"""
    def __init__(self, task_run):
        self.run = task_run

    def __getitem__(self, name):
        # Extract any arthimetic stuff
        match = re.match(r'([\w]+)([+-]*)(\d*)', name)
        attr, op, value = match.groups()
        if attr == "shortdate":
            if value:
                delta = datetime.timedelta(days=int(value))
                if op == "-":
                    delta *= -1
                run_date = self.run.run_time + delta
            else:
                run_date = self.run.run_time
            
            return "%.4d-%.2d-%.2d" % (run_date.year, run_date.month, run_date.day)
        elif attr == "unixtime":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return int(timeutils.to_timestamp(self.run.run_time)) + delta
        elif attr == "daynumber":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return self.run.run_time.toordinal() + delta
        elif attr == "taskname":
            if op:
                raise ValueError("Adjustments not allowed")
            return self.run.task.name
        elif attr == "runid":
            if op:
                raise ValueError("Adjustments not allowed")
            return self.run.id
        else:
            return super(TaskRunVariables, self).__getitem__(name)


class TaskRun(object):
    """An instance of running a task"""
    def __init__(self, task):
        self.task = task
        self.id = "%s.%s.%s" % (task.job.name, task.name, len(task.runs))
        
        self.run_time = None    # What time are we supposed to start
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end

        self.state = TASK_RUN_QUEUED if task.required_tasks else TASK_RUN_SCHEDULED
        self.exit_status = None
        self.output_file = None
        self.job_run = None

        self.required_runs = []
        self.waiting_runs = []
        self.data = {}
        self.state_changed()

    def attempt_start(self):
        if self.should_start:
            "NOW ITS STARTING"
            self.start()

    def start(self):
        log.info("Starting task run %s", self.id)
        
        self.start_time = timeutils.current_time()
        self.state = TASK_RUN_RUNNING
        self._open_output_file()
        self.state_changed()

        # And now we try to actually start some work....
        ret = self.task.node.run(self)
        if isinstance(ret, defer.Deferred):
            self._setup_callbacks(ret)

    def cancel(self):
        if self.is_scheduled or self.is_queued:
            self.state = TASK_RUN_CANCELLED
            self.state_changed()
    
    def queue(self):
        if self.is_scheduled or self.is_cancelled:
            self.state = TASK_RUN_QUEUED
            self.state_changed()

    def _open_output_file(self):
        if self.task.output_dir:
            if os.path.isdir(self.task.output_dir):
                file_name = self.task.output_dir + "/" + self.task.name + ".out"
            else:
                file_name = self.task.output_dir
            
            try:
                log.info("Opening file %s for output", file_name)
                self.output_file = open(file_name, 'a')
            except IOError, e:
                log.error(str(e) + " - Not storing command output!")

    def _handle_errback(self, result):
        """Handle an error where the node wasn't able to give us an exit code"""
        log.info("Task error: %s", str(result))
        if isinstance(result.value, node.ConnectError):
            log.warning("Failed to connect to host %s for run %s", self.task.node.hostname, self.id)
            self.fail(None)
        elif isinstance(result.value, node.ResultError):
            log.warning("Failed to retrieve exit for run %s after executing command on host %s", self.id, self.task.node.hostname)
            self.fail_unknown()
        else:
            log.warning("Unknown failure for run %s on host %s: %s", self.id, self.task.node.hostname, str(result))
            self.fail_unknown()
            
        # Maybe someone else wants it ?
        return result

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
            log.info("Not running waiting run %s, the dependant task failed", run.id)

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Task run %s failed with exit status %r", self.id, exit_status)

        self.state = TASK_RUN_FAILED
        self.exit_status = exit_status
        self.end_time = timeutils.current_time()
        self.state_changed()

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know what result was"""
        log.info("Lost communication with task run %s", self.id)

        self.state = TASK_RUN_FAILED
        self.exit_status = None
        self.end_time = None
        self.state_changed()

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Task run %s succeeded", self.id)
        
        self.exit_status = 0
        self.state = TASK_RUN_SUCCEEDED
        self.end_time = timeutils.current_time()
        
        self.job_run.run_completed()
        self.state_changed()
        self.start_dependants()

    def restore_state(self, state):
        self.id = state['id']
        self.state = state['state']
        self.run_time = state['run_time']
        self.start_time = state['start_time']
        self.end_time = state['end_time']

        if self.is_running:
            self.state = TASK_RUN_UNKNOWN
        
    def state_changed(self):
        self.data['id'] = self.id
        self.data['state'] = self.state
        self.data['run_time'] = self.run_time
        self.data['start_time'] = self.start_time
        self.data['end_time'] = self.end_time
        self.data['command'] = self.command
        
        if self.task.job.state_callback:
            self.task.job.state_callback()

    @property
    def command(self):
        task_vars = TaskRunVariables(self)
        return self.task.command % task_vars

    @property
    def timeout_secs(self):
        if self.task.timeout is None:
            return None
        else:
            return self.task.timeout.seconds

    @property
    def is_queued(self):
        return self.state == TASK_RUN_QUEUED
    
    @property
    def is_cancelled(self):
        return self.state == TASK_RUN_CANCELLED

    @property
    def is_scheduled(self):
        return self.state == TASK_RUN_SCHEDULED

    @property
    def is_done(self):
        return self.state in (TASK_RUN_FAILED, TASK_RUN_SUCCEEDED, TASK_RUN_CANCELLED)

    @property
    def is_ran(self):
        return self.state in (TASK_RUN_FAILED, TASK_RUN_SUCCEEDED)

    @property
    def is_unknown(self):
        return self.state == TASK_RUN_UNKNOWN

    @property
    def is_running(self):
        return self.state == TASK_RUN_RUNNING

    @property
    def is_failed(self):
        return self.state == TASK_RUN_FAILED

    @property
    def is_success(self):
        return self.state == TASK_RUN_SUCCEEDED

    @property
    def should_start(self):
        if not self.is_scheduled and not self.is_queued:
            return False

        return all([r.is_success for r in self.required_runs])
 
class Task(object):
    def __init__(self, name=None, node=None, timeout=None):
        self.name = name
        self.node = node
        self.timeout = timeout
        self.runs = []

        self.required_tasks = []
        self.output_dir = None
        self.job = None

    def build_run(self):
        """Build an instance of TaskRun for this task
        
        This is used by the scheduler when scheduling a run
        """
        new_run = TaskRun(self)
        self.runs.append(new_run)
        return new_run

    def restore(self, id, state):
        """Restores an instance of TaskRun for this task

        This is used when tron shut down unexpectedly
        """
        restored = self.build_run()
        restored.id = id
        restored.restore_state(state)
        return restored
    
    def get_run_by_id(self, id):
        runs = filter(lambda cur: cur.id == id, self.runs) 
        return runs[0] if runs else None

