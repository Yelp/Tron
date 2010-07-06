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

def request_bool(message = ''):
    input = raw_input(message + ' (y/n): ').lower()
    
    while input != 'y' and input != 'n':
        input = raw_input('Please enter \'y\' or \'n\'. \n' + message + ' (y/n): ').lower()

    return input == 'y'


class Error(Exception): pass

class JobExistsError(Error): pass

class StateHandler(object):
    def __init__(self, jobs={}):
        self.data = {}
        self.data['scheduled'] = {}
        self.data['running'] = {}
        self.data['queued'] = {}
        self.jobs = jobs
    
    def state_changed(self, job):
        self.data['scheduled'][job.name] = job.scheduled
        self.data['running'][job.name] = job.running
        self.data['queued'][job.name] = job.queued
        self._store_data() 

    def has_data(self, job):
        return job.name in self.data['scheduled']
    
    def _store_data(self):
        log.info("Storing schedule in %s", SCHEDULE_FILE)
        
        temp_schedule = open('.tmp' + SCHEDULE_FILE, 'wb')
        yaml.dump(self.data, temp_schedule, default_flow_style=False)
        temp_schedule.close()
        shutil.move('.tmp' + SCHEDULE_FILE, SCHEDULE_FILE)

    def load_data(self):
        log.info("Past schedule exists. Probing user")
        
        if request_bool('Past schedule exists. Restore?'):
            log.info('Restoring state from %s', SCHEDULE_FILE)
            schedule_file = open(SCHEDULE_FILE)
            self.data = yaml.load(schedule_file)
            schedule_file.close()
        else:
            log.info('Ignoring previous schedule')


class MasterControlProgram(object):
    """master of tron's domain
    
    This object is responsible for figuring who needs to run and when. It will be the main entry point
    where our daemon finds work to do
    """
    def __init__(self):
        self.jobs = {}
        self.nodes = []
        self.state_handler = StateHandler()

    def add_job(self, tron_job):
        if tron_job.name in self.jobs:
            raise JobExistsError(tron_job)
        else:
            self.jobs[tron_job.name] = tron_job
    
        if tron_job.node not in self.nodes:
            self.nodes.append(tron_job.node) 
    
    def _sleep_time(self, run_time):
        sleep = run_time - timeutils.current_time()
        seconds = sleep.days * SECS_PER_DAY + sleep.seconds + sleep.microseconds * MICRO_SEC
        return max(0, seconds)

    def _schedule_next_run(self, job, prev):
        next = job.next_run()
        if not next is None:
            log.info("Scheduling next run for %s", job.name)
            
            reactor.callLater(self._sleep_time(next.run_time), self._run_job, next)
            if not prev is None and prev.is_cancelled:
                next.prev = prev.prev
            else:
                next.prev = prev
           
            job.scheduled[next.id] = next.state_data
            self.state_handler.state_changed(job)

        return next

    def _restore_running(self, job):
        job.running = self.state_handler.data['running'][job.name]

        for id, state in job.running.iteritems():
            job.restore(id, state)
 
    def _restore_queued(self, job):
        job.queued = self.state_handler.data['queued'][job.name]
        
        for id, state in sorted(job.queued.iteritems(), key=lambda (i, j): j['run_time']):
            job.restore(id, state)
    
    def _restore_scheduled(self, job):
        job.scheduled = self.state_handler.data['scheduled'][job.name]
        
        for id, state in job.scheduled.iteritems():
            run = job.restore(id, state)
            sleep = self._sleep_time(run.run_time)
            if sleep == 0:
                run.run_time = timeutils.current_time()
                self._run_job(run)
            else:
                reactor.callLater(sleep, self._run_job, run)

    def _resolve_running(self, job):
        for id in [id for (id, state) in job.running.iteritems()]:
            print "There are %d instances for job %s with indeterminate results!" % (len(job.running), job.name)
            
            print "job id: %s" % id
            print yaml.dump(state, default_flow_style=False)
            
            if request_bool('Did this job complete successfully?'):
                job.get_run_by_id(id).succeed()
            else:
                job.get_run_by_id(id).fail_unknown()

    def _restore_job(self, job):
        self._restore_running(job)
        self._restore_queued(job)
        self._restore_scheduled(job)
        self._resolve_running(job)

    def _run_job(self, now):
        """This runs when a job was scheduled.
        
        Here we run the job and schedule the next time it should run
        """
        log.debug("Running next scheduled job")
        now.scheduled_start()
        
        next = self._schedule_next_run(now.job, now)

    def run_jobs(self):
        """This schedules the first time each job runs"""
        if os.path.isfile(SCHEDULE_FILE):
            self.state_handler.load_data()

        for tron_job in self.jobs.itervalues():
            if self.state_handler.has_data(tron_job):
                self._restore_job(tron_job)
            elif tron_job.scheduler:
                self._schedule_next_run(tron_job, None)

