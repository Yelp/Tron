from __future__ import with_statement
import logging
import os

from twisted.conch.client.options import ConchOptions

from tron import command_context
from tron import event
from tron import crash_reporter
from tron import node
from tron.config import manager
from tron.config.config_parse import collate_jobs_and_services, ConfigError
from tron.core.job import Job, JobScheduler
from tron.node import Node, NodePool
from tron.scheduler import scheduler_from_config
from tron.serialize import filehandler
from tron.serialize.runstate import statemanager
from tron.service import Service
from tron.utils import emailer
from tron.utils.observer import Observable


log = logging.getLogger(__name__)


class ConfigApplyError(Exception):
    """Errors during config application"""
    pass


class MasterControlProgram(Observable):
    """master of tron's domain

    Central state object for the Tron daemon. Stores all jobs and services.
    """

    def __init__(self, working_dir, config_path):
        super(MasterControlProgram, self).__init__()
        self.jobs               = {}
        self.services           = {}
        self.nodes              = node.NodePoolStore.get_instance()
        self.output_stream_dir  = None
        self.working_dir        = working_dir
        self.crash_reporter     = None
        self.config             = manager.ConfigManager(config_path)
        self.context            = command_context.CommandContext()
        self.state_watcher      = statemanager.StateChangeWatcher()

        # Time zone of the system clock
        self.time_zone          = None

        # Record events for the entire system. Child event recorders may record
        # events for specific jobs, job runs, actions, action runs, etc. and
        # these events will be propagated up but not down the event recorder
        # tree.
        self.event_manager      = event.EventManager.get_instance()
        self.event_recorder     = self.event_manager.add(self)

    def get_config_manager(self):
        return self.config

    def shutdown(self):
        self.state_watcher.shutdown()

    def graceful_shutdown(self):
        """Tell JobSchedulers that a shutdown has been requested."""
        for job_sched in self.jobs.itervalues():
            job_sched.shutdown_requested = True

    def jobs_shutdown(self):
        """Return True if all jobs have finished their runs after
        shutdown was requested.
        """
        return all(job.is_shutdown for job in self.jobs.itervalues())

    def reconfigure(self):
        """Reconfigure MCP while Tron is already running."""
        self.event_recorder.emit_info("reconfig")
        with self.state_watcher.disabled():
            try:
                self._load_config(reconfigure=True)
            except Exception:
                self.event_recorder.emit_critical("reconfig_failure")
                log.exception("Reconfig failure")
                raise

    def _load_config(self, reconfigure=False):
        """Read config data and apply it."""
        self.apply_config(self.config.load(), reconfigure=reconfigure)

    def initial_setup(self):
        """When the MCP is initialized the config is applied before the state.
        In this case jobs shouldn't be scheduled until the state is applied.
        """
        self._load_config()
        self.restore_state()
        # Any job with existing state would have been scheduled already. Jobs
        # without any state will be scheduled here.
        self.schedule_jobs()

    def apply_config(self, config_container, reconfigure=False):
        """Apply a configuration."""
        master_config = config_container.get_master()
        self.output_stream_dir = master_config.output_stream_dir or self.working_dir
        ssh_options = self._ssh_options_from_config(master_config.ssh_options)
        self.update_state_watcher_config(master_config.state_persistence)

        self.context.base = master_config.command_context
        self.time_zone = master_config.time_zone
        self._apply_nodes(master_config.nodes, ssh_options)
        self._apply_node_pools(master_config.node_pools)
        self._apply_notification_options(master_config.notification_options)

        jobs, services = collate_jobs_and_services(config_container)
        self._apply_jobs(jobs, reconfigure=reconfigure)
        self._apply_services(services)

    def update_state_watcher_config(self, state_config):
        """Update the StateChangeWatcher, and save all state if the state config
        changed.
        """
        if self.state_watcher.update_from_config(state_config):
            for job_sched in self.jobs.itervalues():
                self.state_watcher.save_job(job_sched.job)
            for service in self.services.itervalues():
                self.state_watcher.save_service(service)

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
            msg = None
            if not os.path.exists(file_path):
                msg = "Private key file '%s' doesn't exist" % file_name
            if not os.path.exists(file_path + ".pub"):
                msg = "Public key '%s' doesn't exist" % (file_name + ".pub")
            if msg:
                raise ConfigError(msg)

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

    def _apply_jobs(self, job_configs, reconfigure=False):
        """Add and remove jobs based on the configuration."""
        for job_config in job_configs.itervalues():
            self.add_job(job_config, reconfigure=reconfigure)

        for job_name in (set(self.jobs.keys()) - set(job_configs.keys())):
            log.debug("Removing job %s", job_name)
            self.remove_job(job_name)

    def _apply_services(self, srv_configs):
        """Add and remove services."""

        services_to_add = []
        for srv_config in srv_configs.itervalues():
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
            if self.crash_reporter:
                self.crash_reporter.stop()

            em = emailer.Emailer(notification_conf.smtp_host,
                                 notification_conf.notification_addr)
            self.crash_reporter = crash_reporter.CrashReporter(em, self)
            self.crash_reporter.start()

    def add_job(self, job_config, reconfigure=False):
        log.debug("Building new job %s", job_config.name)
        output_path = filehandler.OutputPath(self.output_stream_dir)
        scheduler = scheduler_from_config(job_config.schedule, self.time_zone)
        job = Job.from_config(job_config, scheduler, self.context, output_path)

        if job.name in self.jobs:
            # Jobs have a complex eq implementation that allows us to catch
            # jobs that have not changed and thus don't need to be updated
            # during a reconfigure
            if job == self.jobs[job.name].job:
                return

            log.info("re-adding job %s", job.name)
            self.jobs[job.name].job.update_from_job(job)
            self.jobs[job.name].schedule_reconfigured()
            return

        log.info("adding new job %s", job.name)
        self.jobs[job.name] = JobScheduler(job)
        self.event_manager.add(job, parent=self)
        self.state_watcher.watch(job, Job.NOTIFY_STATE_CHANGE)

        # If this is not a reconfigure, wait for state to be restored before
        # scheduling job runs.
        if reconfigure:
            self.jobs[job.name].schedule()

    def remove_job(self, job_name):
        if job_name not in self.jobs:
            raise ValueError("Job %s unknown", job_name)

        job_scheduler = self.jobs.pop(job_name)
        job_scheduler.disable()

    def schedule_jobs(self):
        for job_scheduler in self.jobs.itervalues():
            job_scheduler.schedule()

    def get_jobs(self):
        return self.jobs.itervalues()

    def get_job_by_name(self, name):
        return self.jobs.get(name)

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
        self.state_watcher.watch(service.machine)
        self.services[service.name] = service

        if prev_service is not None:
            service.absorb_previous(prev_service)

    def remove_service(self, service_name):
        if service_name not in self.services:
            raise ValueError("Service %s unknown", service_name)

        log.info("Removing services %s", service_name)
        service = self.services.pop(service_name)
        service.stop()

    def restore_state(self):
        """Use the state manager to retrieve to persisted state and apply it
        to the configured Jobs and Services.
        """
        self.event_recorder.emit_notice('restoring')
        job_names     = [job_sched.job.name for job_sched in self.jobs.values()]
        service_names = [service.name for service in self.services.itervalues()]
        job_states, service_states = self.state_watcher.restore(
                job_names, service_names)

        for name, job_state_data in job_states.iteritems():
            self.jobs[name].restore_job_state(job_state_data)
        log.info("Loaded state for %d jobs", len(job_states))

        for name, service_state_data in service_states.iteritems():
            self.services[name].restore_service_state(service_state_data)
        log.info("Loaded state for %d services", len(service_states))

        self.state_watcher.save_metadata()

    def __str__(self):
        return "MCP"