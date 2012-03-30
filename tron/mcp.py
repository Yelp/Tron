from __future__ import with_statement
import logging
import logging.handlers
import os
import shutil
import time
import yaml

from twisted.conch.client.options import ConchOptions
from twisted.internet import reactor

import tron
from tron import command_context
from tron import event
from tron import monitor
from tron import node
from tron.config import config_parse
from tron.config.config_parse import ConfigError
from tron.core.job import Job, JobScheduler
from tron.node import Node, NodePool
from tron.scheduler import scheduler_from_config
from tron.serialize import filehandler
from tron.service import Service
from tron.utils import timeutils, emailer
from tron.utils.observer import Observer, Observable


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


class ConfigApplyError(Exception):
    """Errors during config application"""
    pass


class StateHandler(Observer, Observable):
    """StateHandler is responsible for serializing state changes to disk.
    It is an Observer of Jobs and Services and an Observable for an
    EventRecorder.
    """

    EVENT_WRITE_FAILED   = event.EventType(event.LEVEL_CRITICAL, "write_failed")
    EVENT_WRITE_COMPLETE = event.EventType(event.LEVEL_OK, "write_complete")
    EVENT_WRITE_DELAYED  = event.EventType(event.LEVEL_NOTICE, "write_delayed")
    EVENT_RESTORING      = event.EventType(event.LEVEL_NOTICE, "restoring")
    EVENT_STORING        = event.EventType(event.LEVEL_INFO, "storing")

    def __init__(self, mcp, working_dir, writing=False):
        super(StateHandler, self).__init__()
        self.mcp = mcp
        self.working_dir = working_dir
        self.write_pid = None
        self.write_start = None
        self.writing_enabled = writing
        self.store_delayed = False

    def restore_job(self, job_inst, data):
        job_inst.set_context(self.mcp.context)
        job_inst.restore(data)

        for run in job_inst.runs:
            if run.is_scheduled:
                reactor.callLater(
                    run.seconds_until_run_time(),
                    job_inst.run_job,
                    run
                )

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
                    self.notify(self.EVENT_WRITE_FAILED)
                else:
                    self.notify(self.EVENT_WRITE_COMPLETE)
                self.write_pid = None
                self.write_start = None
            else:
                # Process hasn't exited
                write_duration = (timeutils.current_timestamp() -
                                  self.write_start)
                if write_duration > WRITE_DURATION_WARNING_SECS:
                    log.warning("State writing hasn't completed in %d secs",
                                write_duration)
                    self.notify(self.EVENT_WRITE_DELAYED)

                reactor.callLater(STATE_SLEEP_SECS, self.check_write_child)

    def watcher(self, _observable, _event):
        self.store_state()

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

        self.notify(self.EVENT_STORING)

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
        self.notify(self.EVENT_RESTORING)
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


class MasterControlProgram(Observable):
    """master of tron's domain

    This object is responsible for figuring who needs to run and when. It is
    the main entry point where our daemon finds work to do.
    """

    def __init__(self, working_dir, config_file, context=None):
        super(MasterControlProgram, self).__init__()
        self.jobs = {}
        self.job_scheduler = JobScheduler()
        self.services = {}

        self.nodes = node.NodePoolStore.get_instance()

        # Path to the config file
        self.config_file = config_file

        # Root command context
        self.context = context or command_context.CommandContext()

        self.monitor = None

        # Time zone of the system clock
        self.time_zone = None

        # Record events for the entire system. Child event recorders may record
        # events for specific jobs, job runs, actions, action runs, etc. and
        # these events will be propagated up but not down the event recorder
        # tree.
        self.event_manager = event.EventManager.get_instance()
        self.event_recorder = self.event_manager.add(self)

        # Control writing of the state file
        self.state_handler = StateHandler(self, working_dir)
        self.event_manager.add(self.state_handler, parent=self)

    ### CONFIGURATION ###

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
        except Exception:
            self.event_recorder.emit_critical("reconfig_failure")
            log.exception("Reconfig failure")
            raise
        finally:
            self.state_handler.writing_enabled = old_state_writing

    def load_config(self):
        log.info("Loading configuration from %s" % self.config_file)
        with open(self.config_file, 'r') as f:
            self.apply_config(config_parse.load_config(f))

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

    def apply_config(self, conf, skip_env_dependent=False):
        """Apply a configuration. If skip_env_dependent is True we're
        loading this locally to test the config as part of tronfig. We want to
        skip applying some settings because the local machine we're using to
        edit the config may not have the same environment as the live
        trond machine.
        """
        self._apply_working_directory(conf.working_dir)

        if not skip_env_dependent:
            ssh_options = self._ssh_options_from_config(conf.ssh_options)
        else:
            ssh_options = config_parse.valid_ssh_options({})

        self.context.base = conf.command_context
        self._apply_nodes(conf.nodes, ssh_options)
        self._apply_node_pools(conf.node_pools)

        self.time_zone = conf.time_zone
        self._apply_jobs(conf.jobs)
        self._apply_services(conf.services)
        self._apply_notification_options(conf.notification_options)

    def _apply_working_directory(self, wd):
        """Apply working directory.  If wd=None try os.environ['TMPDIR'].
        If that doesn't exist, use /tmp.
        """
        if self.state_handler.working_dir:
            if self.state_handler.working_dir != wd:
                log.warn('trond must be restarted for changes to the working'
                         ' directory to take effect.')
            return

        if not wd:
            if 'TMPDIR' in os.environ:
                wd = os.environ['TMPDIR']
            else:
                wd = '/tmp'

        if not os.path.isdir(wd):
            raise ConfigApplyError("Specified working directory '%s' is not"
                                   " a directory" % wd)

        if not os.access(wd, os.W_OK):
            raise ConfigApplyError("Specified working directory '%s' is not"
                                   " writable" % wd)

        self.state_handler.working_dir = wd

    def _ssh_options_from_config(self, ssh_conf):
        ssh_options = ConchOptions()
        if ssh_conf.agent:
            if 'SSH_AUTH_SOCK' in os.environ:
                ssh_options['agent'] = True
            else:
                raise ConfigError("No SSH Agent available ($SSH_AUTH_SOCK)")
        else:
            ssh_options['noagent'] = True

        for file_name in ssh_conf.identities:
            file_path = os.path.expanduser(file_name)
            if not os.path.exists(file_path):
                raise ConfigError("Private key file '%s' doesn't exist" %
                                  file_name)
            if not os.path.exists(file_path + ".pub"):
                raise ConfigError("Public key '%s' doesn't exist" %
                                  (file_name + ".pub"))

            ssh_options.opt_identity(file_name)

        return ssh_options

    def _apply_nodes(self, node_confs, ssh_options):
        self.nodes.update(
            Node.from_config(config, ssh_options)
            for config in node_confs.itervalues()
        )

    def _apply_node_pools(self, pool_confs):
        self.nodes.update(
            NodePool.from_config(config)
            for config in pool_confs.itervalues()
        )

    def _apply_jobs(self, job_configs):
        """Add and remove jobs based on the configuration."""
        # TODO: attach observer for state changes and events
        for job_config in job_configs.values():
            self.add_job(job_config)

        for job_name in (set(self.jobs.keys()) - set(job_configs.keys())):
            log.debug("Removing job %s", job_name)
            self.remove_job(job_name)

    def _apply_services(self, srv_configs):
        """Add and remove services."""

        services_to_add = []
        for srv_config in srv_configs.values():
            log.debug("Building new services %s", srv_config.name)
            service = Service.from_config(srv_config, self.nodes)
            services_to_add.append(service)

        for srv_name in (set(self.services.keys()) - set(srv_configs.keys())):
            log.debug("Removing service %s", srv_name)
            self.remove_service(srv_name)

        # Go through our constructed services and add them. We'll catch all the
        # failures and throw an exception at the end if anything failed. This
        # is a mitigation against a bug easily cause us to be in an
        # inconsistent state, probably due to bad code elsewhere.
        # TODO: what actually causes this
        failure = False
        for service in services_to_add:
            try:
                self.add_service(service)
            except Exception, e:
                log.exception("Failed adding new service.", e)
                failure = e

        if failure:
            raise ConfigError("Failed adding services %s" % failure)

    def _apply_notification_options(self, notification_conf):
        if notification_conf is not None:
            if self.monitor:
                self.monitor.stop()

            em = emailer.Emailer(notification_conf.smtp_host,
                                 notification_conf.notification_addr)
            self.monitor = monitor.CrashReporter(em, self)
            self.monitor.start()

    ### JOBS ###
    def add_job(self, job_config):
        log.debug("Building new job %s", job_config.name)
        output_path = filehandler.OutputPath(self.state_handler.working_dir)
        scheduler = scheduler_from_config(job_config.schedule, self.time_zone)
        job = Job.from_config(job_config, scheduler, self.context, output_path)

        if job.name in self.jobs:
            # Jobs have a complex eq implementation that allows us to catch
            # jobs that have not changed and thus don't need to be updated
            # during a reconfigure
            if job == self.jobs[job.name]:
                return

            log.info("re-adding job %s", job.name)
            self.jobs[job.name].update_from_job(job)
            self.job_scheduler.schedule_reconfigured(self.jobs[job.name])
            return

        log.info("adding new job %s", job.name)
        self.jobs[job.name] = job
        self.job_scheduler.schedule(job)
        event.EventManager.get_instance().add(job, parent=self)
        self.state_handler.watch(job, Job.EVENT_STATE_CHANGE)

    def remove_job(self, job_name):
        if job_name not in self.jobs:
            raise ValueError("Job %s unknown", job_name)

        job = self.jobs.pop(job_name)
        self.job_scheduler.disable(job)

    ### SERVICES ###

    def add_service(self, service):
        if service.name in self.jobs:
            raise ValueError("Service %s is already a job", service.name)

        prev_service = self.services.get(service.name)

        if service == prev_service:
            return

        log.info("(re)adding service %s", service.name)
        service.set_context(self.context)
        service.event_recorder.set_parent(self.event_recorder)

        # TODO: replace with watch
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

    ### OTHER ACTIONS ###

    def enable_job(self, job):
        job.enable()
        if not job.runs or not job.runs[0].is_scheduled:
            job.schedule_next_run()

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
                    tron_job.schedule_next_run()
                else:
                    self.enable_job(tron_job)

    def __str__(self):
        return "MCP"
