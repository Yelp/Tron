class JobRun(object):
    """An instance of running a job"""
    def __init__(self, job):
        super(JobRun, self).__init__()
        self.job = job
        self.is_done = False

class Job(object):
    def __init__(self):
        self._is_configured = False
        self.scheduler = None
        self.runs = []
        
    def next_run(self):
        """Check the scheduler and decide when the next run should be"""
        for run in self.runs:
            if not run.is_done:
                return run
        else:
            return self.scheduler.pick_next_run()

    def build_run(self):
        """Build an instance of JobRun for this job
        
        This is used by the scheduler when scheduling a run
        """
        return JobRun(self)
            
    def configure(self, config):
        """Update configuration of job"""
        if self._is_configured:
            # We are updating a config
            pass
        else:
            # Initializing a job
            
            self._is_configured = True
    
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
                