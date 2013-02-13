from __future__ import with_statement
import logging
import os

from twisted.conch.client.options import ConchOptions

from tron import command_context
from tron import event
from tron import crash_reporter
from tron import node
from tron.config import manager
from tron.config.config_parse import ConfigError
from tron.core import service, job
from tron.serialize.runstate import statemanager
from tron.utils import emailer
from tron.utils.observer import Observable


log = logging.getLogger(__name__)


class MasterControlProgram(Observable):
    """master of tron's domain

    Central state object for the Tron daemon. Stores all jobs and services.
    """

    def __init__(self, working_dir, config_path):
        super(MasterControlProgram, self).__init__()
        self.jobs               = job.JobCollection()
        self.services           = service.ServiceCollection()
        self.output_stream_dir  = None
        self.working_dir        = working_dir
        self.crash_reporter     = None
        self.config             = manager.ConfigManager(config_path)
        self.context            = command_context.CommandContext()

        # Time zone of the system clock
        self.time_zone          = None
        self.event_recorder     = event.get_recorder()
        self.event_recorder.ok('started')
        self.state_watcher      = statemanager.StateChangeWatcher()

    def shutdown(self):
        self.state_watcher.shutdown()

    def graceful_shutdown(self):
        """Inform JobCollection that a shutdown has been requested."""
        self.jobs.request_shutdown()

    def reconfigure(self):
        """Reconfigure MCP while Tron is already running."""
        self.event_recorder.ok("reconfigured")
        try:
            self._load_config(reconfigure=True)
        except Exception:
            self.event_recorder.critical("reconfigure_failure")
            log.exception("reconfigure failure")
            raise

    def _load_config(self, reconfigure=False):
        """Read config data and apply it."""
        with self.state_watcher.disabled():
            self.apply_config(self.config.load(), reconfigure=reconfigure)

    def initial_setup(self):
        """When the MCP is initialized the config is applied before the state.
        In this case jobs shouldn't be scheduled until the state is applied.
        """
        self._load_config()
        self.restore_state()
        # Any job with existing state would have been scheduled already. Jobs
        # without any state will be scheduled here.
        self.jobs.schedule()

    def apply_config(self, config_container, reconfigure=False):
        """Apply a configuration."""
        master_config = config_container.get_master()
        self.output_stream_dir = master_config.output_stream_dir or self.working_dir
        ssh_options = self._ssh_options_from_config(master_config.ssh_options)
        self.update_state_watcher_config(master_config.state_persistence)

        self.context.base = master_config.command_context
        self.time_zone = master_config.time_zone
        node.NodePoolStore.update_from_config(
            master_config.nodes, master_config.node_pools, ssh_options)
        self._apply_notification_options(master_config.notification_options)

        args = self.context, self.output_stream_dir, self.time_zone
        factory = job.JobSchedulerFactory(*args)
        self.apply_collection_config(config_container.get_jobs(),
            self.jobs, job.Job.NOTIFY_STATE_CHANGE, factory, reconfigure)

        self.apply_collection_config(config_container.get_services(),
            self.services, service.Service.NOTIFY_STATE_CHANGE, self.context)

    def apply_collection_config(self, config, collection, notify_type, *args):
        items = collection.load_from_config(config, *args)
        self.state_watcher.watch_all(items, notify_type)

    def update_state_watcher_config(self, state_config):
        """Update the StateChangeWatcher, and save all state if the state config
        changed.
        """
        if self.state_watcher.update_from_config(state_config):
            for job_scheduler in self.jobs:
                self.state_watcher.save_job(job_scheduler.get_job())
            for service in self.services:
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

    def _apply_notification_options(self, conf):
        if not conf:
            return

        if self.crash_reporter:
            self.crash_reporter.stop()

        email_sender = emailer.Emailer(conf.smtp_host, conf.notification_addr)
        self.crash_reporter = crash_reporter.CrashReporter(email_sender)
        self.crash_reporter.start()

    def get_job_collection(self):
        return self.jobs

    def get_service_collection(self):
        return self.services

    def get_config_manager(self):
        return self.config

    def restore_state(self):
        """Use the state manager to retrieve to persisted state and apply it
        to the configured Jobs and Services.
        """
        self.event_recorder.notice('restoring')
        job_states, service_states = self.state_watcher.restore(
                self.jobs.get_names(), self.services.get_names())

        self.jobs.restore_state(job_states)
        self.services.restore_state(service_states)
        self.state_watcher.save_metadata()

    def __str__(self):
        return "MCP"