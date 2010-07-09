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
STATE_FILE = 'tron_state.yaml'

def sleep_time(run_time):
    sleep = run_time - timeutils.current_time()
    seconds = sleep.days * SECS_PER_DAY + sleep.seconds + sleep.microseconds * MICRO_SEC
    return max(0, seconds)


class Error(Exception): pass

class JobExistsError(Error): pass

class StateHandler(object):
    def __init__(self, mcp, state_dir=None):
        self.data = {}
        self.mcp = mcp
        self.data['scheduled'] = {}
        self.data['running'] = {}
        self.data['queued'] = {}
        self.state_dir = state_dir

    def _restore_running(self, job):
        job.running = self.data['running'][job.name]
        ids = [id for id, state in job.running.iteritems()]
        for id in ids:
            self.mcp.runs[id] = job.restore(id, state)

    def _restore_queued(self, job):
        job.queued = self.data['queued'][job.name]
        
        for id, state in sorted(job.queued.iteritems(), key=lambda (i, j): j['run_time']):
            self.mcp.runs[id] = job.restore(id, state)
        
    def _restore_scheduled(self, job):
        job.scheduled = self.data['scheduled'][job.name]
        
        for id, state in job.scheduled.iteritems():
            run = job.restore(id, state)
            sleep = sleep_time(run.run_time)
            if sleep == 0:
                run.run_time = timeutils.current_time()

            reactor.callLater(sleep, self.mcp.run_job, run)
            self.mcp.runs[run.id] = run

    def restore_job(self, job):
        self._restore_running(job)
        self._restore_queued(job)
        self._restore_scheduled(job)
    
    def state_changed(self, job):
        self.data['scheduled'][job.name] = job.scheduled
        self.data['running'][job.name] = job.running
        self.data['queued'][job.name] = job.queued
        self._store_data() 

    def has_data(self, job):
        return job.name in self.data['scheduled']
    
    def _store_data(self):
        log.info("Storing schedule in %s", STATE_FILE)
       
        tmp_path = self.state_dir + '/.tmp.' + STATE_FILE
        temp_schedule = open(tmp_path, 'wb')
        
        yaml.dump(self.data, temp_schedule, default_flow_style=False)
        temp_schedule.close()
        shutil.move(tmp_path, self.get_state_file_path())

    def get_state_file_path(self):
        return self.state_dir + '/' + STATE_FILE

    def load_data(self):
        log.info('Restoring state from %s', self.get_state_file_path())
        
        schedule_file = open(self.get_state_file_path())
        self.data = yaml.load(schedule_file)
        schedule_file.close()

class MasterControlProgram(object):
    """master of tron's domain
    
    This object is responsible for figuring who needs to run and when. It will be the main entry point
    where our daemon finds work to do
    """
    def __init__(self, state_dir=None):
        self.jobs = {}
        self.runs = {}
        self.nodes = []
        self.state_handler = StateHandler(self, state_dir)

    def add_job(self, tron_job):
        if tron_job.name in self.jobs:
            raise JobExistsError(tron_job)
        else:
            self.jobs[tron_job.name] = tron_job
    
        if tron_job.node not in self.nodes:
            self.nodes.append(tron_job.node) 
        
        tron_job.state_callback = self.state_handler.state_changed

    def _schedule_next_run(self, job, prev):
        next = job.next_run(prev)
        if not next is None:
            log.info("Scheduling next run for %s", job.name)
            reactor.callLater(sleep_time(next.run_time), self.run_job, next)
            self.runs[next.id] = next

        return next

    def run_job(self, now):
        """This runs when a job was scheduled.
        
        Here we run the job and schedule the next time it should run
        """
        log.debug("Running next scheduled job")
        now.scheduled_start()
        
        next = self._schedule_next_run(now.job, now)

    def run_jobs(self):
        """This schedules the first time each job runs"""
        if os.path.isfile(self.state_handler.get_state_file_path()):
            self.state_handler.load_data()

        for tron_job in self.jobs.itervalues():
            if self.state_handler.has_data(tron_job):
                self.state_handler.restore_job(tron_job)
            elif tron_job.scheduler:
                self._schedule_next_run(tron_job, None)

