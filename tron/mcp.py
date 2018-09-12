from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import with_statement

import logging

from tron import actioncommand
from tron import command_context
from tron import node
from tron.config import manager
from tron.core import job
from tron.eventbus import make_eventbus
from tron.mesos import MesosClusterRepository
from tron.serialize.runstate import statemanager

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
        self.jobs = job.JobCollection()
        self.working_dir = working_dir
        self.config = manager.ConfigManager(config_path)
        self.context = command_context.CommandContext()
        self.state_watcher = statemanager.StateChangeWatcher()
        self.eventbus = None
        log.info('initialized')

    def shutdown(self):
        if self.eventbus:
            self.eventbus.shutdown()
        self.state_watcher.shutdown()

    def reconfigure(self):
        """Reconfigure MCP while Tron is already running."""
        log.info("reconfigured")
        try:
            self._load_config(reconfigure=True)
        except Exception as e:
            log.exception(
                f"reconfigure failure: {e.__class__.__name__}: {e}"
            )
            raise e

    def _load_config(self, reconfigure=False):
        """Read config data and apply it."""
        with self.state_watcher.disabled():
            self.apply_config(self.config.load(), reconfigure=reconfigure)

    def initial_setup(self):
        """When the MCP is initialized the config is applied before the state.
        In this case jobs shouldn't be scheduled until the state is applied.
        """
        self._load_config()
        self.restore_state(
            actioncommand.create_action_runner_factory_from_config(
                self.config.load().get_master().action_runner
            )
        )
        # Any job with existing state would have been scheduled already. Jobs
        # without any state will be scheduled here.
        self.jobs.run_queue_schedule()

    def apply_config(self, config_container, reconfigure=False):
        """Apply a configuration."""
        master_config_directives = [
            (self.update_state_watcher_config, 'state_persistence'),
            (self.set_context_base, 'command_context'), (
                node.NodePoolRepository.update_from_config,
                'nodes',
                'node_pools',
                'ssh_options',
            ),
            (MesosClusterRepository.configure, 'mesos_options'),
            (self.configure_eventbus, 'eventbus_enabled'),
        ]
        master_config = config_container.get_master()
        apply_master_configuration(master_config_directives, master_config)

        self.state_watcher.watch(MesosClusterRepository)

        # TODO: unify NOTIFY_STATE_CHANGE and simplify this
        factory = self.build_job_scheduler_factory(master_config)
        self.apply_collection_config(
            config_container.get_jobs(),
            self.jobs,
            job.Job.NOTIFY_STATE_CHANGE,
            factory,
            reconfigure,
        )

    def apply_collection_config(self, config, collection, notify_type, *args):
        items = collection.load_from_config(config, *args)
        self.state_watcher.watch_all(items, notify_type)

    def build_job_scheduler_factory(self, master_config):
        output_stream_dir = master_config.output_stream_dir or self.working_dir
        action_runner = actioncommand.create_action_runner_factory_from_config(
            master_config.action_runner,
        )
        return job.JobSchedulerFactory(
            self.context,
            output_stream_dir,
            master_config.time_zone,
            action_runner,
            self.eventbus_publish,
        )

    def eventbus_publish(self, message):
        if self.eventbus:
            self.eventbus.publish(message)

    def update_state_watcher_config(self, state_config):
        """Update the StateChangeWatcher, and save all state if the state config
        changed.
        """
        if self.state_watcher.update_from_config(state_config):
            for job_scheduler in self.jobs:
                self.state_watcher.save_job(job_scheduler.get_job())

    def set_context_base(self, command_context):
        self.context.base = command_context

    def configure_eventbus(self, enabled):
        if enabled:
            if self.eventbus:
                return
            self.eventbus = make_eventbus(f"{self.working_dir}/_events")
            self.eventbus.start()
        else:
            if not self.eventbus:
                return
            self.eventbus.shutdown()
            self.eventbus = None

    def get_job_collection(self):
        return self.jobs

    def get_config_manager(self):
        return self.config

    def restore_state(self, action_runner):
        """Use the state manager to retrieve to persisted state and apply it
        to the configured Jobs.
        """
        log.info('restoring')
        states = self.state_watcher.restore(self.jobs.get_names())
        MesosClusterRepository.restore_state(states.get('mesos_state', {}))

        self.jobs.restore_state(states.get('job_state', {}), action_runner)
        self.state_watcher.save_metadata()

    def __str__(self):
        return "MCP"
