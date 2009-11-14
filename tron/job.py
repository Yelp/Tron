import uuid
import logging

from tron.utils import time

log = logging.getLogger('tron.job')

JOB_RUN_WAITING = 0
JOB_RUN_RUNNING = 1
JOB_RUN_FAILED = 10
JOB_RUN_SUCCEEDED = 11

class JobRun(object):
    """An instance of running a job"""
    def __init__(self, job):
        super(JobRun, self).__init__()
        self.job = job
        
        self.id = "%s.%s" % (job.name, uuid.uuid4().hex)
        
        self.run_time = None    # What time are we supposed to start

        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end

        self.state = JOB_RUN_WAITING
        self.exit_status = None
        
    def start(self):
        log.info("Starting job run %s", self.id)
        self.start_time = time.current_time()
        self.state = JOB_RUN_RUNNING
        
        # And now we try to actually start some work....
        self._execute()

    def _execute(self):
        self.job.node.run(self)

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Job run %s failed with exit status %r", self.id, exit_status)

        self.state = JOB_RUN_FAILED
        self.exit_status = exit_status
        self.end_time = time.current_time()

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Job run %s succeeded", self.id)
        self.exit_status = 0
        self.state = JOB_RUN_SUCCEEDED
        self.end_time = time.current_time()

    @property
    def command(self):
        return self.job.command

    @property
    def timeout_secs(self):
        if self.job.timeout is None:
            return None
        else:
            return self.job.timeout.seconds

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
        if self.state != JOB_RUN_WAITING:
            return False
        
        # First things first... is it time to start ?
        if self.run_time > time.current_time():
            return False
        
        # Ok, it's time, what about our jobs dependencies
        return bool(all(r.ready for r in self.job.resources))


class Job(object):
    def __init__(self, name=None, node=None, timeout=None):
        self.name = name
        self.node = node
        self.scheduler = None
        self.timeout = None
        self.runs = []
        self.resources = []

    def next_run(self):
        """Check the scheduler and decide when the next run should be"""
        for run in self.runs:
            if not run.is_done:
                return run

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
                