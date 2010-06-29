import logging
import weakref

from twisted.internet import reactor
from tron.utils import timeutils

SECS_PER_DAY = 86400
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
        self.nodes = []
        self.next_runs = {}

    def add_job(self, tron_job):
        if tron_job.name in self.jobs:
            raise JobExistsError(tron_job)
        else:
            self.jobs[tron_job.name] = tron_job
    
        if tron_job.node not in self.nodes:
            self.nodes.append(tron_job.node) 

    def _sleep_time(self, next_run):
        sleep = next_run - timeutils.current_time()
        seconds = sleep.days * SECS_PER_DAY + sleep.seconds
        return max(0, seconds)

    def _run_job(self, now):
        """This runs when a job was scheduled.
        
        Here we run the job and schedule the next time it should run
        """
        log.debug("Running next scheduled job")
        now.start()

        next = now.job.next_run()
        if not next is None:
            self.next_runs[next.job.name] = next.run_time
            reactor.callLater(self._sleep_time(next.run_time), self._run_job, next)

    def run_jobs(self):
        """This schedules the first time each job runs"""
        for tron_job in self.jobs.itervalues():
            if tron_job.scheduler:
                run = tron_job.next_run()
                self.next_runs[tron_job.name] = run.run_time
                reactor.callLater(self._sleep_time(run.run_time), self._run_job, run)

