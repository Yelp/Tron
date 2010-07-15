import logging
import weakref
import yaml
import os
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

class FlowExistsError(Error): pass

class StateHandler(object):
    def __init__(self, mcp, state_dir):
        self.data = {}
        self.mcp = mcp
        self.state_dir = state_dir
        self.write_proc = None
        self.writing_enabled = False

    def _restore_run(self, job, id, state):
        run = job.restore(id, state)
        self.mcp.runs[id] = run
        
        if run.is_scheduled:
            sleep = sleep_time(run.run_time)
            if sleep == 0:
                run.run_time = timeutils.current_time()
            reactor.callLater(sleep, self.mcp.run_job, run)

    def restore_job(self, job):
        old_runs = sorted(self.data[job.name].iteritems(), key=lambda (i, s): s['run_time'])

        for id, state in old_runs:
            self._restore_run(job, id, state)

    def state_changed(self, job):
        self.data[job.name] = job.data
        if self.writing_enabled:
            self.store_data() 

    def has_data(self, job):
        return job.name in self.data
    
    def store_data(self):
        if self.write_proc and self.write_proc.poll() is None:
            return
        
        log.info("Storing schedule in %s", STATE_FILE)
        file_path = '%s/%s' % (self.state_dir, STATE_FILE)
        
        dump = yaml.dump(self.data, default_flow_style=False)
        self.write_proc = subprocess.Popen(['/bin/sh', '-c', 'echo "%s" > %s' % (dump, file_path)])

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
    def __init__(self, state_dir):
        self.flows = {}
        self.jobs = {}
        self.runs = {}
        self.nodes = []
        self.state_handler = StateHandler(self, state_dir)

    def add_flow(self, tron_flow):
        if tron_flow.name in self.flows:
            raise FlowExistsError(tron_flow)
            
        self.flows[tron_flow.name] = tron_flow

        for tron_job in tron_flow.topo_jobs:
            self.jobs[tron_job.name] = tron_job
            
            if tron_job.node not in self.nodes:
                self.nodes.append(tron_job.node) 
            tron_job.state_callback = self.state_handler.state_changed

    def _schedule_next_run(self, flow):
        next = flow.next_run()
        if not next is None:
            log.info("Scheduling next flow for %s", next.flow.name)
            reactor.callLater(sleep_time(next.run_time), self.run_flow, next)
            for run in next.runs:
                self.runs[run.id] = run

        return next

    def run_flow(self, now):
        """This runs when a flow was scheduled.
        
        Here we run the flow and schedule the next time it should run
        """
        log.debug("Running next scheduled flow")
        next = self._schedule_next_run(now.flow)
        now.scheduled_start()

    def run_flows(self):
        """This schedules the first time each flow runs"""
        if os.path.isfile(self.state_handler.get_state_file_path()):
            self.state_handler.load_data()

        for tron_flow in self.flows.itervalues():
            if self.state_handler.has_data(tron_flow):
                self.state_handler.restore_flow(tron_flow)
            elif tron_flow.scheduler:
                self._schedule_next_run(tron_flow)
        
        self.state_handler.writing_enabled = True
        self.state_handler.store_data()

