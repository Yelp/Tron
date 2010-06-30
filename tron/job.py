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
JOB_RUN_WAITING = 1
JOB_RUN_RUNNING = 2
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
        super(JobRun, self).__init__()
        self.job = job
        
        self.id = "%s.%s" % (job.name, uuid.uuid4().hex)
        
        self.run_time = None    # What time are we supposed to start
        self.output_file = None
       
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end

        self.state = JOB_RUN_SCHEDULED
        self.exit_status = None
        self.prev = None
        self.waiting = []

    def start(self):
        if not self.prev is None and not self.prev.is_done:
            if self.job.queueing:
                self.prev.waiting.append(self)
                self.state = JOB_RUN_WAITING
                log.warning("Previous job, %s, not finished - placing in queue", self.job.name)
            else:
                log.warning("Previous job, %s, not finished - cancelling", self.job.name)
                self.state = JOB_RUN_FAILED
            return

        log.info("Starting job run %s", self.id)
        
        self.start_time = timeutils.current_time()
        self.state = JOB_RUN_RUNNING

        # And now we try to actually start some work....
        ret = self._execute()
        if isinstance(ret, defer.Deferred):
            self._setup_callbacks(ret)

    def _execute(self):
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
       
        return self.job.node.run(self)

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
        [j.start() for j in self.waiting]
        [j.build_run().start() for j in self.job.dependants]

    def ignore_dependants(self):
        [log.info("Not running job %s, the dependant job failed", self.job.name) for j in self.job.dependants]

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Job run %s failed with exit status %r", self.id, exit_status)

        self.state = JOB_RUN_FAILED
        self.exit_status = exit_status
        self.end_time = timeutils.current_time()

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know what result was"""
        log.info("Lost communication with job run %s", self.id)

        self.state = JOB_RUN_FAILED
        self.exit_status = None
        self.end_time = None

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Job run %s succeeded", self.id)
        self.exit_status = 0
        self.state = JOB_RUN_SUCCEEDED
        self.end_time = timeutils.current_time()
        self.start_dependants()

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
    def is_waiting(self):
        return self.state == JOB_RUN_WAITING

    @property
    def is_scheduled(self):
        return self.state == JOB_RUN_SCHEDULED

    @property
    def is_done(self):
        return self.state in (JOB_RUN_FAILED, JOB_RUN_SUCCEEDED)

    @property
    def is_running(self):
        return self.state == JOB_RUN_RUNNING

    @property
    def is_success(self):
        return self.state == JOB_RUN_SUCCEEDED

    @property
    def should_start(self):
        if self.state != JOB_RUN_SCHEDULED:
            return False
        
        # First things first... is it time to start ?
        if not self.run_time or self.run_time > timeutils.current_time():
            return False
        
        # Ok, it's time, what about our jobs dependencies
        return bool(all(r.ready for r in self.job.resources))


class Job(object):
    def __init__(self, name=None, node=None, timeout=None):
        self.name = name
        self.node = node
        self.scheduler = None
        self.timeout = timeout
        self.runs = []
        self.resources = []
        self.output_dir = None
        self.dependants = []
        self.queueing = True

    def next_run(self):
        """Check the scheduler and decide when the next run should be"""
        if self.scheduler:
            return self.scheduler.next_run(self)
        else:
            return None

    def build_run(self):
        """Build an instance of JobRun for this job
        
        This is used by the scheduler when scheduling a run
        """
        new_run = JobRun(self)
        self.runs.append(new_run)
        return new_run
    

class JobSet(object):
    def __init__(self):
        """docstring for __init__"""
        self.jobs = {}

    def sync_to_config(self, config):
        found_jobs = []
        for job_config in config.jobs:
            found_jobs.append(job_config.name)
            existing_job = self.jobs.get(job_config.name)
            if existing_job is None:
                # Create a new one
                job = Job()
                job.configure(job_config)
                self.jobs[job.name] = job
            else:
                existing_job.configure(job_config)

        for job_name in self.jobs.iterkeys():
            if job_name not in found_jobs:
                dead_job = self.jobs[job_name]
                self.jobs.remove(dead_job)
                
