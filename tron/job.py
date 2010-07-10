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
JOB_RUN_RUNNING = 2
JOB_RUN_CANCELLED = 3
JOB_RUN_UNKNOWN = 4
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
        self.prev = None        # Previous non-cancelled run
        self.next = None        # Next scheduled run

    def scheduled_start(self):
        """Called when the job is scheduled to run.

        If the job should start then it starts.  Otherwise, if queueing is enabled
        it queues the job, if not, cancels the job.
        """
        if self.should_start:
            self.start()

        elif self.job.queueing:
            log.warning("Previous job, %s, not finished - placing in queue", self.job.name)
            self.state = JOB_RUN_QUEUED
        else:
            log.warning("Previous job, %s, not finished - cancelling instance", self.job.name)
            self.state = JOB_RUN_CANCELLED

        self.job.state_changed(self)

    def delayed_start(self):
        if self.is_queued:
           self.start()

    def start(self):
        log.info("Starting job run %s", self.id)
        
        self.start_time = timeutils.current_time()
        self.state = JOB_RUN_RUNNING
        self.job.state_changed(self)

        # And now we try to actually start some work....
        ret = self._execute()
        if isinstance(ret, defer.Deferred):
            self._setup_callbacks(ret)

    def cancel(self):
        if self.is_scheduled or self.is_queued:
            self.state = JOB_RUN_CANCELLED
    
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

    def _execute(self):
        self._open_output_file()
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
        if self.next and self.next.is_queued:
            self.next.delayed_start()
       
        for job in self.job.dependants:
            run = job.build_run()
            run.run_time = timeutils.current_time()
            run.start()

    def ignore_dependants(self):
        if self.next and self.next.is_queued:
            log.info("Not running waiting run %s, the dependant job failed", self.next.id)
        [log.info("Not running job %s, the dependant job failed", j.name) for j in self.job.dependants]

    def _finish(self):
        self.job.state_changed(self)

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Job run %s failed with exit status %r", self.id, exit_status)

        self.state = JOB_RUN_FAILED
        self.exit_status = exit_status
        self.end_time = timeutils.current_time()
        self._finish()

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
        
        self._finish()
        self.start_dependants()

    def restore_state(self, state):
        self.state = state['state']
        self.run_time = state['run_time']
        self.start_time = state['start_time']

        if self.is_running:
            self.state = JOB_RUN_UNKNOWN
        elif 'previous' in state:
            self.prev = self.job.get_run_by_id(state['previous'])
        if 'next' in state:
            self.next = self.job.get_run_by_id(state['next'])

    @property
    def state_data(self):
        data = {'state': self.state, 'run_time': self.run_time, 'start_time': self.start_time, 'command': self.command}
        if self.prev:
            data['previous'] = self.prev.id
        if self.next:
            data['next'] = self.next.id

        return data

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
        if self.state != JOB_RUN_SCHEDULED:
            return False
        if not self.prev is None and not self.prev.is_success:
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
        
        self.depend = None
        self.dependants = []
        self.queueing = False
        
        self.data = {}
        self.state_callback = None

    def state_changed(self, run):
        self.data[run.id] = run.state_data
        if self.state_callback:
            self.state_callback(self)

    def _insert_new_run(self, run):
        """Inserts run into run list ignoring cancelled runs"""
        if not run.prev is None:
            run.prev.next = run
            if run.prev.is_cancelled:
                run.prev = run.prev.prev
                run.prev.next = run

    def remove_run(self, run):
        """Remove a previously non-cancelled run from the run list"""
        assert not run.is_cancelled
        if run.next:
            run.next.prev = run.prev
        if run.prev:
            run.prev.next = run.next

    def reinsert_run(self, run):
        """Reinserts a previously cancelled run into the run list"""
        assert run.is_cancelled
        run.prev.next.prev = run
        run.prev.next = run

    def next_run(self, prev=None):
        """Check the scheduler and decide when the next run should be"""
        if self.scheduler:
            next = self.scheduler.next_run(self)
            if next is None:
                return None

            next.prev = prev
            self._insert_new_run(next)

            self.state_changed(next)
            return next
        
        return None

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
        self.state_changed(restored)
        return restored
    
    def get_run_by_id(self, id):
        runs = filter(lambda cur: cur.id == id, self.runs) 
        return runs[0] if runs else None

    @property
    def scheduler_str(self):
        return str(self.scheduler) if self.scheduler else "FOLLOW:%s" % self.depend.name

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
                
