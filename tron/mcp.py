import logging
import weakref
import shutil
import yaml
import os

from twisted.internet import reactor
from tron.utils import timeutils

SECS_PER_DAY = 86400
MICRO_SEC = .000001
log = logging.getLogger('tron.mcp')
SCHEDULE_FILE = '.tron_schedule.yaml'

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
        self.schedule = {}
        self.running = {}

    def add_job(self, tron_job):
        if tron_job.name in self.jobs:
            raise JobExistsError(tron_job)
        else:
            self.jobs[tron_job.name] = tron_job
    
        if tron_job.node not in self.nodes:
            self.nodes.append(tron_job.node) 

    def _store_data(self):
        log.info("Storing schedule in %s", SCHEDULE_FILE)
        temp_schedule = open('.tmp' + SCHEDULE_FILE, 'wb')
        yaml.dump({'schedule': self.schedule, 'running': self.running}, temp_schedule, default_flow_style=False)
        temp_schedule.close()
        shutil.move('.tmp' + SCHEDULE_FILE, SCHEDULE_FILE)

    def _load_data(self):
        log.info("Past schedule exists. Restoring schedule")
        schedule_file = open(SCHEDULE_FILE)
        self.schedule = yaml.load(schedule_file)
        schedule_file.close()

    def _sleep_time(self, next_run):
        sleep = next_run - timeutils.current_time()
        seconds = sleep.days * SECS_PER_DAY + sleep.seconds + sleep.microseconds * MICRO_SEC
        return max(0, seconds)

    def _schedule_next_run(self, job):
        next = job.next_run()
        if not next is None:
            log.info("Scheduling next run for %s", job.name)
            reactor.callLater(self._sleep_time(next.run_time), self._run_job, next)
            job.scheduled.append(next.store_data)

        return next

    def _update_job_state(self, job):
        self.schedule[job.name] = job.scheduled
        self.running[job.name] = job.running
        self._store_data()

    def _run_job(self, now):
        """This runs when a job was scheduled.
        
        Here we run the job and schedule the next time it should run
        """
        log.debug("Running next scheduled job")
        now.scheduled_start()
        
        next = self._schedule_next_run(now.job)
        next.prev = now.prev if now.is_cancelled else now
        self._update_job_state(now.job)

    def run_jobs(self):
        """This schedules the first time each job runs"""
        if os.path.isfile(SCHEDULE_FILE):
            self._load_data()
            return
        
        for tron_job in self.jobs.itervalues():
            if tron_job.scheduler:
                self._schedule_next_run(tron_job)

        self._store_data()

