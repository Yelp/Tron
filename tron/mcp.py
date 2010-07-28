import logging
import weakref
import yaml
import os
import sys
import subprocess

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
    def __init__(self, mcp, working_dir, writing=False):
        self.data = {}
        self.mcp = mcp
        self.working_dir = working_dir
        self.write_pid = None
        self.writing_enabled = writing

    def _reschedule(self, run):
        sleep = sleep_time(run.run_time)
        if sleep == 0:
            run.run_time = timeutils.current_time()
        reactor.callLater(sleep, self.mcp.run_job, run)

    def restore_job(self, job):
        prev = None

        for data in self.data[job.name]:
            run = job.restore_run(data, prev)

            if run.is_scheduled:
                self._reschedule(run)
            prev = run

    def state_changed(self):
        if self.writing_enabled:
            self.store_data() 

    def has_data(self, job):
        return job.name in self.data
    
    def store_data(self):
        """Stores the state of tron"""
        # If tron is already storing data, don't start again till it's done
        if self.write_pid and not os.waitpid(self.write_pid, os.WNOHANG)[0]:
            return

        file_path = '%s/%s' % (self.working_dir, STATE_FILE)
        log.info("Storing state in %s", file_path)
        
        pid = os.fork()
        if pid:
            self.write_pid = pid
        else:
            file = open(file_path, 'w')
            dump = yaml.dump(self.data, file, default_flow_style=False, indent=4)
            file.close()
            os._exit(os.EX_OK)

    def get_state_file_path(self):
        return os.path.normpath(self.working_dir + '/' + STATE_FILE)

    def load_data(self):
        log.info('Restoring state from %s', self.get_state_file_path())
        
        data_file = open(self.get_state_file_path())
        self.data = yaml.load(data_file)
        data_file.close()

class MasterControlProgram(object):
    """master of tron's domain
    
    This object is responsible for figuring who needs to run and when. It will be the main entry point
    where our daemon finds work to do
    """
    def __init__(self, working_dir):
        self.jobs = {}
        self.actions = {}
        self.runs = {}
        self.nodes = []
        self.state_handler = StateHandler(self, working_dir)

    def add_nodes(self, node_pool):
        if not node_pool:
            return

        for node in node_pool.nodes:
            if not node in self.nodes:
                self.nodes.append(node)

    def add_job(self, tron_job):
        if tron_job.name in self.jobs:
            raise JobExistsError(tron_job)
        
        tron_job.output_dir = os.path.join(self.state_handler.working_dir, tron_job.name)
        if not os.path.exists(tron_job.output_dir):
            os.mkdir(tron_job.output_dir)

        self.jobs[tron_job.name] = tron_job
        tron_job.state_callback = self.state_handler.state_changed
        self.add_nodes(tron_job.node_pool)

        for tron_action in tron_job.topo_actions:
            self.actions[tron_action.name] = tron_action
            self.add_nodes(tron_action.node_pool)

    def _schedule_next_run(self, job):
        next = job.next_run()
        if not next is None:
            log.info("Scheduling next job for %s", next.job.name)
            reactor.callLater(sleep_time(next.run_time), self.run_job, next)
            for run in next.runs:
                self.runs[run.id] = run

        return next

    def run_job(self, now):
        """This runs when a job was scheduled.
        
        Here we run the job and schedule the next time it should run
        """
        log.debug("Running next scheduled job")
        next = self._schedule_next_run(now.job)
        now.scheduled_start()

    def run_jobs(self):
        """This schedules the first time each job runs"""
        if os.path.isfile(self.state_handler.get_state_file_path()):
            self.state_handler.load_data()

        for tron_job in self.jobs.itervalues():
            if self.state_handler.has_data(tron_job):
                self.state_handler.restore_job(tron_job)
            else:
                self._schedule_next_run(tron_job)
            self.state_handler.data[tron_job.name] = tron_job.data
        
        self.state_handler.writing_enabled = True
        self.state_handler.store_data()

