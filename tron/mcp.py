from __future__ import with_statement
import logging

from tron import command_context
from tron import event
from tron import crash_reporter
from tron import node
from tron.config import manager
from tron.core import service, job
from tron.serialize.runstate import statemanager
from tron.utils import emailer


log = logging.getLogger(__name__)


def apply_master_configuration(mapping, master_config):
    def get_config_value(seq):
        return [getattr(master_config, item) for item in seq]

    for entry in mapping:
        func, args = entry[0], get_config_value(entry[1:])
        func(*args)


class MasterControlProgram(object):
    """Central state object for the Tron daemon."""

    def __init__(self, working_dir, config_path):
        super(MasterControlProgram, self).__init__()
        self.jobs               = job.JobCollection()
        self.services           = service.ServiceCollection()
        self.working_dir        = working_dir
        self.crash_reporter     = None
        self.config             = manager.ConfigManager(config_path)
        self.context            = command_context.CommandContext()
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
        master_config_directives = [
            (self.update_state_watcher_config,           'state_persistence'),
            (self.set_context_base,                      'command_context'),
            (node.NodePoolRepository.update_from_config, 'nodes',
                                                         'node_pools',
                                                         'ssh_options'),
            (self.apply_notification_options,            'notification_options'),
        ]
        master_config = config_container.get_master()
        apply_master_configuration(master_config_directives, master_config)

        # TODO: unify NOTIFY_STATE_CHANGE and simplify this
        factory = self.build_job_scheduler_factory(master_config)
        self.apply_collection_config(config_container.get_jobs(),
            self.jobs, job.Job.NOTIFY_STATE_CHANGE, factory, reconfigure)

        self.apply_collection_config(config_container.get_services(),
            self.services, service.Service.NOTIFY_STATE_CHANGE, self.context)

    def apply_collection_config(self, config, collection, notify_type, *args):
        items = collection.load_from_config(config, *args)
        self.state_watcher.watch_all(items, notify_type)

    def build_job_scheduler_factory(self, master_config):
        output_stream_dir = master_config.output_stream_dir or self.working_dir
        args = self.context, output_stream_dir, master_config.time_zone
        return job.JobSchedulerFactory(*args)

    def update_state_watcher_config(self, state_config):
        """Update the StateChangeWatcher, and save all state if the state config
        changed.
        """
        if self.state_watcher.update_from_config(state_config):
            for job_scheduler in self.jobs:
                self.state_watcher.save_job(job_scheduler.get_job())
            for service in self.services:
                self.state_watcher.save_service(service)

    def apply_notification_options(self, conf):
        if not conf:
            return

        if self.crash_reporter:
            self.crash_reporter.stop()

        email_sender = emailer.Emailer(conf.smtp_host, conf.notification_addr)
        self.crash_reporter = crash_reporter.CrashReporter(email_sender)
        self.crash_reporter.start()

    def set_context_base(self, command_context):
        self.context.base = command_context

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