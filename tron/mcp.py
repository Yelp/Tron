import logging
import weakref
import yaml
import os
import shutil
import sys
import subprocess
import yaml

from tron import job, config, command_context
from twisted.internet import reactor
from tron.utils import timeutils

SECS_PER_DAY = 86400
MICRO_SEC = .000001
log = logging.getLogger('tron.mcp')
STATE_FILE = 'tron_state.yaml'
STATE_SLEEP = 3

def sleep_time(run_time):
    sleep = run_time - timeutils.current_time()
    seconds = sleep.days * SECS_PER_DAY + sleep.seconds + sleep.microseconds * MICRO_SEC
    return max(0, seconds)


class StateHandler(object):
    def __init__(self, mcp, working_dir, writing=False):
        self.mcp = mcp
        self.working_dir = working_dir
        self.write_pid = None
        self.writing_enabled = writing

    def restore_job(self, job, data):
        job.enabled = data['enabled']

        try:
            for e_data in data['enable_runs']:
                run = job.restore_enable_run(e_data)
        except Exception, e:
            log.error("Cannot restore enabling runs for job %s: %s" % (job.name, str(e))) 

        try:
            for d_data in data['disable_runs']:
                run = job.restore_disable_run(d_data)
        except Exception, e:
            log.error("Cannot restore disabling runs for job %s: %s" % (job.name, str(e))) 

        for r_data in data['runs']:
            run = job.restore_main_run(r_data)
            if run.is_scheduled:
                reactor.callLater(sleep_time(run.run_time), self.mcp.run_job, run)

        job.set_context(self.mcp.context)

        next = job.next_to_finish()
        if job.enabled and next and next.is_queued:
            next.start()

    def store_data(self):
        """Stores the state of tron"""
        # If tron is already storing data, don't start again till it's done
        if not self.writing_enabled or (self.write_pid and not os.waitpid(self.write_pid, os.WNOHANG)[0]):
            reactor.callLater(STATE_SLEEP, self.store_data)
            return 

        tmp_path = os.path.join(self.working_dir, '.tmp.' + STATE_FILE)
        file_path = os.path.join(self.working_dir, STATE_FILE)
        log.info("Storing state in %s", file_path)
        
        pid = os.fork()
        if pid:
            self.write_pid = pid
        else:
            file = open(tmp_path, 'w')
            yaml.dump(self.data, file, default_flow_style=False, indent=4)
            file.close()
            shutil.move(tmp_path, file_path)
            os._exit(os.EX_OK)

    def get_state_file_path(self):
        return os.path.join(self.working_dir, STATE_FILE)

    def load_data(self):
        log.info('Restoring state from %s', self.get_state_file_path())
        
        try:
            data_file = open(self.get_state_file_path())
            data = yaml.load(data_file)
            data_file.close()
        except IOError, e:
            data = {}
            log.error("Cannot load state file: %s" % str(e))

        return data
    
    @property
    def data(self):
        data = {}
        for j in self.mcp.jobs.itervalues():
            data[j.name] = j.data
        return data

class MasterControlProgram(object):
    """master of tron's domain
    
    This object is responsible for figuring who needs to run and when. It will be the main entry point
    where our daemon finds work to do
    """
    def __init__(self, working_dir, config_file, context=None):
        self.jobs = {}
        self.nodes = []
        self.state_handler = StateHandler(self, working_dir)
        self.config_file = config_file
        self.context = context

    def live_reconfig(self):
        try:
            self.load_config()
            self.run_jobs()
        except Exception, e:
            log.error("Reconfiguration failed.  Cancelling")
    
    def load_config(self):
        log.info("Loading configuration from %s" % self.config_file)
        try:
            opened_config = open(self.config_file, "r")
            configuration = config.load_config(opened_config)
            configuration.apply(self)
            opened_config.close()
        except (IOError, yaml.YAMLError), e:
            log.error("Error reading configuration from %s" % self.config_file)
            raise config.ConfigError(e)

    def config_lines(self):
        try:
            conf = open(self.config_file, 'r')
            data = conf.read()
            conf.close()
            return data
        except IOError, e:
            log.error(str(e) + " - Cannot open configuration file!")
            return ""

    def rewrite_config(self, lines):
        try:
            conf = open(self.config_file, 'w')
            conf.write(lines)
            conf.close()
        except IOError, e:
            log.error(str(e) + " - Cannot write to configuration file!")

    def add_nodes(self, node_pool):
        if not node_pool:
            return

        for node in node_pool.nodes:
            if node not in self.nodes:
                self.nodes.append(node)

    def add_job_nodes(self, job):
        self.add_nodes(job.node_pool)
        for action in job.topo_actions:
            self.add_nodes(action.node_pool)

    def setup_job_dir(self, job):
        job.output_dir = os.path.join(self.state_handler.working_dir, job.name)
        if not os.path.exists(job.output_dir):
            os.mkdir(job.output_dir)

    def add_job(self, job):
        if job.name in self.jobs:
            # Jobs have a complex eq implementation that allows us to catch jobs that have not changed and thus
            # don't need to be updated during a reconfigure
            if job == self.jobs[job.name]:
                return
            
            # We're updating an existing job, we have to copy over run time information
            job.absorb_old_job(self.jobs[job.name])

            if job.enabled:
                self.disable_job(job)
                self.enable_job(job)
        
        self.jobs[job.name] = job

        job.set_context(self.context)
        self.setup_job_dir(job)
        self.add_job_nodes(job)
        job.store_callback = self.state_handler.store_data

    def _schedule(self, run):
        sleep = sleep_time(run.run_time)
        if sleep == 0:
            run.set_run_time(timeutils.current_time())
        reactor.callLater(sleep, self.run_job, run)

    def schedule_next_run(self, job):
        if job.runs and job.runs[0].is_scheduled:
            return
        
        for next in job.next_runs():
            log.info("Scheduling next job for %s", next.job.name)
            self._schedule(next)

    def run_job(self, now):
        """This runs when a job was scheduled.
        Here we run the job and schedule the next time it should run
        """
        if not now.job.enabled:
            return
        
        if not (now.is_running or now.is_failed or now.is_success):
            log.debug("Running next scheduled job")
            now.scheduled_start()
        
        self.schedule_next_run(now.job)

    def enable_job(self, job):
        job.enable()
        if not job.runs or not job.runs[0].is_scheduled:
            self.schedule_next_run(job)

    def disable_job(self, job):
        job.disable()

    def disable_all(self):
        for jo in self.jobs.itervalues():
            self.disable_job(jo)
            
    def enable_all(self):
        for jo in self.jobs.itervalues():
            self.enable_job(jo)

    def try_restore(self):
        if not os.path.isfile(self.state_handler.get_state_file_path()):
            return 
        
        data = self.state_handler.load_data()
        for name in data.iterkeys():
            if name in self.jobs:
                self.state_handler.restore_job(self.jobs[name], data[name])

    def run_jobs(self):
        """This schedules the first time each job runs"""
        for tron_job in self.jobs.itervalues():
            if tron_job.enabled:
                if tron_job.runs:
                    self.schedule_next_run(tron_job)
                else:
                    self.enable_job(tron_job)
        
        self.state_handler.writing_enabled = True
        self.state_handler.store_data()


