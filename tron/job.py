import uuid
import logging
import re
import datetime
import os

from twisted.internet import defer

from tron import node
from tron.utils import timeutils

log = logging.getLogger('tron.job')

JOB_RUN_SCHEDULED = 0
JOB_RUN_QUEUED = 1
JOB_RUN_CANCELLED = 2
JOB_RUN_UNKNOWN = 3
JOB_RUN_RUNNING = 4
JOB_RUN_FAILED = 10
JOB_RUN_SUCCEEDED = 11

class JobRunVariables(object):
    """Dictionary like object that provides variable subsitution for job commands"""
    def __init__(self, job_run):
        self.run = job_run

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
        elif attr == "jobname":
            if op:
                raise ValueError("Adjustments not allowed")
            return self.run.job.name
        elif attr == "runid":
            if op:
                raise ValueError("Adjustments not allowed")
            return self.run.id
        else:
            return super(JobRunVariables, self).__getitem__(name)


class JobRun(object):
    """An instance of running a job"""
    def __init__(self, job):
        self.job = job
        self.id = "%s.%s" % (job.name, uuid.uuid4().hex)
        
        self.run_time = None    # What time are we supposed to start
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end

        self.state = JOB_RUN_QUEUED if job.required_jobs else JOB_RUN_SCHEDULED
        self.exit_status = None
        self.output_file = None
        self.flow_run = None

        self.required_runs = []
        self.waiting_runs = []
        self.data = {}
        self.state_changed()

    def attempt_start(self):
        if self.should_start:
            self.start()

    def start(self):
        log.info("Starting job run %s", self.id)
        
        self.start_time = timeutils.current_time()
        self.state = JOB_RUN_RUNNING
        self._open_output_file()
        self.state_changed()

        # And now we try to actually start some work....
        ret = self.job.node.run(self)
        if isinstance(ret, defer.Deferred):
            self._setup_callbacks(ret)

    def cancel(self):
        if self.is_scheduled or self.is_queued:
            self.state = JOB_RUN_CANCELLED
            self.state_changed()
    
    def queue(self):
        if self.is_scheduled or self.is_cancelled:
            self.state = JOB_RUN_QUEUED
            self.state_changed()

    def _open_output_file(self):
        if self.job.output_dir:
            if os.path.isdir(self.job.output_dir):
                file_name = self.job.output_dir + "/" + self.job.name + ".out"
            else:
                file_name = self.job.output_dir
            
            try:
                log.info("Opening file %s for output", file_name)
                self.output_file = open(file_name, 'a')
            except IOError, e:
                log.error(str(e) + " - Not storing command output!")

    def _handle_errback(self, result):
        """Handle an error where the node wasn't able to give us an exit code"""
        log.info("Job error: %s", str(result))
        if isinstance(result.value, node.ConnectError):
            log.warning("Failed to connect to host %s for run %s", self.job.node.hostname, self.id)
            self.fail(None)
        elif isinstance(result.value, node.ResultError):
            log.warning("Failed to retrieve exit for run %s after executing command on host %s", self.id, self.job.node.hostname)
            self.fail_unknown()
        else:
            log.warning("Unknown failure for run %s on host %s: %s", self.id, self.job.node.hostname, str(result))
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
            run.run_time = timeutils.current_time()
            run.attempt_start()

    def ignore_dependants(self):
        for run in self.waiting_runs:
            log.info("Not running waiting run %s, the dependant job failed", run.id)

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Job run %s failed with exit status %r", self.id, exit_status)

        self.state = JOB_RUN_FAILED
        self.exit_status = exit_status
        self.end_time = timeutils.current_time()
        self.state_changed()

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know what result was"""
        log.info("Lost communication with job run %s", self.id)

        self.state = JOB_RUN_FAILED
        self.exit_status = None
        self.end_time = None
        self.state_changed()

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Job run %s succeeded", self.id)
        
        self.exit_status = 0
        self.state = JOB_RUN_SUCCEEDED
        self.end_time = timeutils.current_time()
        
        self.flow_run.run_completed()
        self.state_changed()
        self.start_dependants()

    def restore_state(self, state):
        self.id = state['id']
        self.state = state['state']
        self.run_time = state['run_time']
        self.start_time = state['start_time']
        self.end_time = state['end_time']

        if self.is_running:
            self.state = JOB_RUN_UNKNOWN
        
    def state_changed(self):
        self.data['id'] = self.id
        self.data['state'] = self.state
        self.data['run_time'] = self.run_time
        self.data['start_time'] = self.start_time
        self.data['end_time'] = self.end_time
        self.data['command'] = self.command
        
        if self.job.flow.state_callback:
            self.job.flow.state_callback()

    @property
    def command(self):
        job_vars = JobRunVariables(self)
        return self.job.command % job_vars

    @property
    def timeout_secs(self):
        if self.job.timeout is None:
            return None
        else:
            return self.job.timeout.seconds

    @property
    def is_queued(self):
        return self.state == JOB_RUN_QUEUED
    
    @property
    def is_cancelled(self):
        return self.state == JOB_RUN_CANCELLED

    @property
    def is_scheduled(self):
        return self.state == JOB_RUN_SCHEDULED

    @property
    def is_done(self):
        return self.state in (JOB_RUN_FAILED, JOB_RUN_SUCCEEDED, JOB_RUN_CANCELLED)

    @property
    def is_ran(self):
        return self.state in (JOB_RUN_FAILED, JOB_RUN_SUCCEEDED)

    @property
    def is_unknown(self):
        return self.state == JOB_RUN_UNKNOWN

    @property
    def is_running(self):
        return self.state == JOB_RUN_RUNNING

    @property
    def is_success(self):
        return self.state == JOB_RUN_SUCCEEDED

    @property
    def should_start(self):
        if not self.is_scheduled and not self.is_queued:
            return False

        return all([r.is_success for r in self.required_runs])
 
class Job(object):
    def __init__(self, name=None, node=None, timeout=None):
        self.name = name
        self.node = node
        self.timeout = timeout
        self.runs = []

        self.required_jobs = []
        self.output_dir = None
        self.flow = None

    def build_run(self):
        """Build an instance of JobRun for this job
        
        This is used by the scheduler when scheduling a run
        """
        new_run = JobRun(self)
        self.runs.append(new_run)
        return new_run

    def restore(self, id, state):
        """Restores an instance of JobRun for this job

        This is used when tron shut down unexpectedly
        """
        restored = self.build_run()
        restored.id = id
        restored.restore_state(state)
        return restored
    
    def get_run_by_id(self, id):
        runs = filter(lambda cur: cur.id == id, self.runs) 
        return runs[0] if runs else None

    @property
    def scheduler_str(self):
        return str(self.flow.scheduler) if self.flow.scheduler else "FOLLOW:%s" % self.depend.name

