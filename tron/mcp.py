import logging
import weakref

log = logging.getLogger('tron.mcp')

class Error(Exception): pass

class JobExistsError(Error): pass

class MasterControlProgram(object):
    """master of tron's domain
    
    This object is responsible for figuring who needs to run and when. It will be the main entry point
    where our daemon finds work to do
    """
    def __init__(self):
        self.jobs = {}
        
        # We keep a sort of index into the runs we know about.
        self.runs = weakref.WeakValueDictionary()
    
    def add_job(self, tron_job):
        if tron_job.name in self.jobs:
            raise JobExistsError(tron_job)
        else:
            self.jobs[tron_job.name] = tron_job
    
    def check_and_run(self):
        """This is where it all happens
        
        Check for work to do, and do it. Should be called regularly (via a timer)
        """
        log.debug("Checking for available jobs")
        current_runs = []
        for tron_job in self.jobs.itervalues():
            job_run = tron_job.next_run()
            
            # Make sure we know about this run instance
            if job_run.id not in self.runs:
                self.runs[job_run.id] = job_run
            
            # Should we actually start this run ?
            if job_run and job_run.should_start:
                job_run.start()
                    