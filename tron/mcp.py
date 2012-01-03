from __future__ import with_statement
import logging
import os
import shutil
import subprocess
import sys
import time
import weakref
import yaml

import tron
from tron import job, config, command_context, event
from twisted.internet import reactor
from tron.utils import timeutils


log = logging.getLogger('tron.mcp')

STATE_FILE = 'tron_state.yaml'
STATE_SLEEP_SECS = 1
WRITE_DURATION_WARNING_SECS = 30


class Error(Exception):
    pass


class StateFileVersionError(Error):
    pass


class UnsupportedVersionError(Error):
    pass


class StateHandler(object):

    def __init__(self, mcp, working_dir, writing=False):
        self.mcp = mcp
        self.working_dir = working_dir
        self.write_pid = None
        self.write_start = None
        self.writing_enabled = writing
        self.store_delayed = False
        self.event_recorder = event.EventRecorder(self,
                                                  parent=mcp.event_recorder)

    def restore_job(self, job_inst, data):
        job_inst.set_context(self.mcp.context)
        job_inst.restore(data)

        for run in job_inst.runs:
            if run.is_scheduled:
                reactor.callLater(run.seconds_until_run_time(),
                                  self.mcp.run_job,
                                  run)

        next = job_inst.next_to_finish()
        if job_inst.enabled and next and next.is_queued:
            next.start()

    def restore_service(self, service, data):
        service.set_context(self.mcp.context)
        service.restore(data)

    def delay_store(self):
        self.store_delayed = False
        self.store_state()

    def check_write_child(self):
        if self.write_pid:
            pid, status = os.waitpid(self.write_pid, os.WNOHANG)
            if pid != 0:
                log.info("State writing completed in in %d seconds",
                         timeutils.current_timestamp() - self.write_start)
                if status != 0:
                    log.warning("State writing process failed with status %d",
                                status)
                    self.event_recorder.emit_critical("write_failed")
                else:
                    self.event_recorder.emit_ok("write_complete")
                self.write_pid = None
                self.write_start = None
            else:
                # Process hasn't exited
                write_duration = (timeutils.current_timestamp() -
                                  self.write_start)
                if write_duration > WRITE_DURATION_WARNING_SECS:
                    log.warning("State writing hasn't completed in %d secs",
                                write_duration)
                    self.event_recorder.emit_notice("write_delayed")

                reactor.callLater(STATE_SLEEP_SECS, self.check_write_child)

    def store_state(self):
        """Stores the state of tron"""
        log.debug("store_state called: %r, %r",
                  self.write_pid, self.writing_enabled)

        # If tron is already storing data, don't start again till it's done
        if self.write_pid or not self.writing_enabled:
            # If a child is writing, we don't want to ignore this change, so
            # lets try it later
            if not self.store_delayed:
                self.store_delayed = True
                reactor.callLater(STATE_SLEEP_SECS, self.delay_store)
            return

        tmp_path = os.path.join(self.working_dir, '.tmp.' + STATE_FILE)
        file_path = os.path.join(self.working_dir, STATE_FILE)
        log.info("Storing state in %s", file_path)

        self.event_recorder.emit_info("storing")

        self.write_start = timeutils.current_timestamp()
        pid = os.fork()
        if pid:
            self.write_pid = pid
            reactor.callLater(STATE_SLEEP_SECS, self.check_write_child)
        else:
            exit_status = os.EX_SOFTWARE
            try:
                with open(tmp_path, 'w') as data_file:
                    yaml.dump(self.data, data_file,
                              default_flow_style=False, indent=4)
                    data_file.flush()
                    os.fsync(data_file.fileno())
                shutil.move(tmp_path, file_path)
                exit_status = os.EX_OK
            except:
                log.exception("Failure while writing state")
            finally:
                os._exit(exit_status)

    def get_state_file_path(self):
        return os.path.join(self.working_dir, STATE_FILE)

    def load_data(self):
        log.info('Restoring state from %s', self.get_state_file_path())
        self.event_recorder.emit_notice("restoring")
        with open(self.get_state_file_path()) as data_file:
            return self._load_data_file(data_file)

    def _load_data_file(self, data_file):
        data = yaml.load(data_file)

        if 'version' not in data:
            # Pre-versioned state files need to be reformatted a bit
            data = {
                'version': [0, 1, 9],
                'jobs': data
            }

        # For properly comparing version, we need to convert this guy to a
        # tuple
        data['version'] = tuple(data['version'])

        # By default we assume backwards compatability.
        if data['version'] == tron.__version_info__:
            return data
        elif data['version'] > tron.__version_info__:
            raise StateFileVersionError("State file has new version: %r",
                                        data['version'])
        elif data['version'] < (0, 2, 0):
            raise UnsupportedVersionError("State file has version %r" %
                                          (data['version'],))
        else:
            # Potential version conversions
            return data

    @property
    def data(self):
        data = {
            'version': tron.__version_info__,
            'create_time': int(time.time()),
            'jobs': {},
            'services': {},
        }

        for j in self.mcp.jobs.itervalues():
            data['jobs'][j.name] = j.data

        for s in self.mcp.services.itervalues():
            data['services'][s.name] = s.data

        return data

    def __str__(self):
        return "STATE_HANDLER"


class MasterControlProgram(object):
    """master of tron's domain

    This object is responsible for figuring who needs to run and when. It is
    the main entry point where our daemon finds work to do.
    """

    def __init__(self, working_dir, config_file, context=None):
        self.jobs = {}
        self.services = {}
        self.nodes = []
        self.config_file = config_file
        self.context = context
        self.monitor = None
        self.time_zone = None
        self.event_recorder = event.EventRecorder(self)
        self.state_handler = StateHandler(self, working_dir)

        root = logging.getLogger('')
        self.base_logging_handlers = list(root.handlers)

    def live_reconfig(self):
        self.event_recorder.emit_info("reconfig")
        try:

            # Temporarily disable state writing because reconfig can cause a
            # lot of state changes
            old_state_writing = self.state_handler.writing_enabled
            self.state_handler.writing_enabled = False

            self.load_config()

            # Any new jobs will need to be scheduled
            self.run_jobs()
        except Exception, e:
            self.event_recorder.emit_critical("reconfig_failure")
            log.exception("Reconfig failure")
            raise
        finally:
            self.state_handler.writing_enabled = old_state_writing

    def load_config(self):
        log.info("Loading configuration from %s" % self.config_file)
        opened_config = open(self.config_file, "r")
        configuration = config.load_config(opened_config)
        configuration.apply(self)
        opened_config.close()

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

    def setup_job_dir(self, job):
        job.output_path = os.path.join(self.state_handler.working_dir,
                                       job.name)
        if not os.path.exists(job.output_path):
            os.mkdir(job.output_path)

    def add_job(self, job):
        if job.name in self.services:
            raise ValueError("Job %s is already a service", job.name)
        if job.name in self.jobs:

            # Jobs have a complex eq implementation that allows us to catch
            # jobs that have not changed and thus don't need to be updated
            # during a reconfigure
            if job == self.jobs[job.name]:
                return

            log.info("re-adding job %s", job.name)

            # We're updating an existing job, we have to copy over run time
            # information
            job.absorb_old_job(self.jobs[job.name])

            if job.enabled:
                self.disable_job(job)
                self.enable_job(job)
        else:
            log.info("adding job %s", job.name)

        self.jobs[job.name] = job

        # update time zone information in scheduler to match config
        if job.scheduler is not None:
            job.scheduler.time_zone = self.time_zone

        job.set_context(self.context)
        job.event_recorder.set_parent(self.event_recorder)
        self.setup_job_dir(job)
        job.listen(True, self.state_handler.store_state)

    def remove_job(self, job_name):
        if job_name not in self.jobs:
            raise ValueError("Job %s unknown", job_name)

        job = self.jobs.pop(job_name)

        job.disable()

    def add_service(self, service):
        if service.name in self.jobs:
            raise ValueError("Service %s is already a job", service.name)

        prev_service = self.services.get(service.name)

        if service == prev_service:
            return

        log.info("(re)adding service %s", service.name)
        service.set_context(self.context)
        service.event_recorder.set_parent(self.event_recorder)

        # Trigger storage on any state changes
        service.listen(True, self.state_handler.store_state)

        self.services[service.name] = service

        if prev_service is not None:
            service.absorb_previous(prev_service)

    def remove_service(self, service_name):
        if service_name not in self.services:
            raise ValueError("Service %s unknown", service_name)

        log.info("Removing services %s", service_name)
        service = self.services.pop(service_name)
        service.stop()

    def _schedule(self, run):
        secs = run.seconds_until_run_time()
        if secs == 0:
            run.set_run_time(timeutils.current_time())
        reactor.callLater(secs, self.run_job, run)

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
        if not now.job:
            return

        if not now.job.enabled:
            return

        if not (now.is_running or now.is_failure or now.is_success):
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
            log.info("No state data found")
            return

        data = self.state_handler.load_data()
        if not data:
            log.warning("Failed to load state data")
            return

        state_load_count = 0
        for name in data['jobs'].iterkeys():
            if name in self.jobs:
                self.state_handler.restore_job(self.jobs[name],
                                               data['jobs'][name])
                state_load_count += 1
            else:
                log.warning("Job name %s from state file unknown", name)

                self.state_handler.restore_job(self.jobs[name], data[name])

        for name in data['services'].iterkeys():
            if name in self.services:
                state_load_count += 1
                self.state_handler.restore_service(self.services[name],
                                                   data['services'][name])
            else:
                log.warning("Service name %s from state file unknown", name)

        log.info("Loaded state for %d jobs", state_load_count)

    def run_jobs(self):
        """This schedules the first time each job runs"""
        for tron_job in self.jobs.itervalues():
            if tron_job.enabled:
                if tron_job.runs:
                    self.schedule_next_run(tron_job)
                else:
                    self.enable_job(tron_job)

    def run_services(self):
        for service in self.services.itervalues():
            if not service.is_started:
                service.start()

    def __str__(self):
        return "MCP"
