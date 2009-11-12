import logging

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
    
    def add_job(self, job):
        if job.name in self.jobs:
            raise JobExistsError(job)
        else:
            self.jobs[job.name] = job
    
    def check_and_run(self):
        """This is where it all happens
        
        Check for work to do, and do it. Should be called regularly (via a timer)
        """
        current_runs = []
        for job in self.jobs.itervalues():
            run = job.next_run()
            if run and run.should_start():
                run.start()