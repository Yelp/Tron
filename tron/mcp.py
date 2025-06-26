import logging
import time

from tron import actioncommand
from tron import command_context
from tron import node
from tron import prom_metrics
from tron.config import manager
from tron.config.schema import MASTER_NAMESPACE
from tron.core.job import Job
from tron.core.job_collection import JobCollection
from tron.core.job_scheduler import JobSchedulerFactory
from tron.core.jobgraph import JobGraph
from tron.eventbus import EventBus
from tron.kubernetes import KubernetesClusterRepository
from tron.mesos import MesosClusterRepository
from tron.serialize.runstate import statemanager

log = logging.getLogger(__name__)


def apply_master_configuration(mapping, master_config):
    def get_config_value(seq):
        return [getattr(master_config, item) for item in seq]

    # Map various MASTER.yaml config options to functions that will apply said options
    # for example, we will have MasterControlProgram.configure_eventbus function mapped to eventbus_enabled option
    for entry in mapping:
        func, args = entry[0], get_config_value(entry[1:])
        func(*args)


class MasterControlProgram:
    """Central state object for the Tron daemon."""

    def __init__(self, working_dir: str, config_path: str, boot_time: float) -> None:
        super().__init__()
        self.jobs = JobCollection()
        self.working_dir = working_dir
        self.config: manager.ConfigManager = manager.ConfigManager(config_path)
        self.context = command_context.CommandContext()
        self.state_watcher = statemanager.StateChangeWatcher()
        self.boot_time = boot_time
        self.read_json = False
        current_time = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime(boot_time))
        log.info(f"Initialized. Tron started on {current_time}!")

    def shutdown(self):
        EventBus.shutdown()
        self.state_watcher.shutdown()

    def reconfigure(self, namespace=None):
        """Reconfigure MCP while Tron is already running."""
        log.info("reconfigured")
        try:
            self._load_config(reconfigure=True, namespace_to_reconfigure=namespace)
        except Exception as e:
            log.exception(f"reconfigure failure: {e.__class__.__name__}: {e}")
            raise e

    def _load_config(self, reconfigure=False, namespace_to_reconfigure=None):
        """Read config data and apply it."""
        with self.state_watcher.disabled():
            self.apply_config(
                self.config.load(),
                reconfigure=reconfigure,
                namespace_to_reconfigure=namespace_to_reconfigure,
            )

    def _update_metrics(self) -> None:
        """Update Prometheus metrics related to jobs and actions"""
        try:
            job_names = self.jobs.get_names()
            job_count = len(job_names) if job_names else 0
            prom_metrics.tron_job_count_gauge.set(job_count)

            total_actions = 0
            if self.jobs:
                for job_scheduler in self.jobs:
                    job = job_scheduler.get_job()
                    if job and job.action_graph and job.action_graph.action_map:
                        num_actions_in_job = len(job.action_graph.action_map)
                        total_actions += num_actions_in_job

            prom_metrics.tron_action_count_gauge.set(total_actions)
        except Exception:
            log.exception("Failed to update job and action count metrics")

    def initial_setup(self):
        """When the MCP is initialized the config is applied before the state.
        In this case jobs shouldn't be scheduled until the state is applied.
        """
        overall_startup_start_time = time.time()

        # The job schedule factories will be created in the function below
        self._load_config()

        # Jobs will also get scheduled (internally) once the state for action runs are restored in restore_state
        with prom_metrics.timer(
            operation_name="full_restore_process",
            log=log,
            histogram_metric=prom_metrics.tron_restore_duration_seconds_histogram,
            gauge_metric=prom_metrics.tron_last_restore_duration_seconds_gauge,
        ):
            self.restore_state(
                actioncommand.create_action_runner_factory_from_config(
                    self.config.load().get_master().action_runner,
                ),
            )

        # Any job with existing state would have been scheduled already. Jobs
        # without any state will be scheduled here.
        self.jobs.run_queue_schedule()

        overall_startup_duration = time.time() - overall_startup_start_time
        prom_metrics.tron_startup_duration_seconds_histogram.observe(overall_startup_duration)
        prom_metrics.tron_last_startup_duration_seconds_gauge.set(overall_startup_duration)
        log.info(f"Tron total startup finished in {overall_startup_duration:.2f}s.")

    def apply_config(self, config_container, reconfigure=False, namespace_to_reconfigure=None):
        """Apply a configuration."""
        master_config_directives = [
            (self.update_state_watcher_config, "state_persistence"),
            (self.set_context_base, "command_context"),
            (
                node.NodePoolRepository.update_from_config,
                "nodes",
                "node_pools",
                "ssh_options",
            ),
            (MesosClusterRepository.configure, "mesos_options"),
            (KubernetesClusterRepository.configure, "k8s_options"),
            (self.configure_eventbus, "eventbus_enabled"),
            (self.set_read_json, "read_json"),
        ]
        master_config = config_container.get_master()
        apply_master_configuration(master_config_directives, master_config)

        self.state_watcher.watch(MesosClusterRepository)
        self.state_watcher.watch(KubernetesClusterRepository)

        # If the master namespace was updated, we should update jobs in all namespaces
        if namespace_to_reconfigure == MASTER_NAMESPACE:
            namespace_to_reconfigure = None

        # TODO: unify NOTIFY_STATE_CHANGE and simplify this
        self.job_graph = JobGraph(config_container)
        # This factory is how Tron internally manages scheduling jobs
        factory = self.build_job_scheduler_factory(master_config, self.job_graph)
        updated_jobs = self.jobs.update_from_config(
            config_container.get_jobs(),
            factory,
            reconfigure,
            namespace_to_reconfigure,
        )

        # We will build the schedulers once the watcher is invoked
        log.info(
            f"Tron built the schedulers for Tron jobs internally! Time elapsed since Tron started {time.time() - self.boot_time}s"
        )
        self.state_watcher.watch_all(updated_jobs, [Job.NOTIFY_STATE_CHANGE, Job.NOTIFY_NEW_RUN])

        # Do this last so that all Job objects, schedulers, and action graphs are fully built and linked within the JobCollection
        self._update_metrics()

    def build_job_scheduler_factory(self, master_config, job_graph):
        """Creates JobSchedulerFactory, which are how Tron tracks job schedules internally"""
        output_stream_dir = master_config.output_stream_dir or self.working_dir
        action_runner = actioncommand.create_action_runner_factory_from_config(
            master_config.action_runner,
        )
        return JobSchedulerFactory(
            self.context,
            output_stream_dir,
            master_config.time_zone,
            action_runner,
            job_graph,
        )

    def update_state_watcher_config(self, state_config):
        """Update the StateChangeWatcher, and save all state if the state config
        changed.
        """
        if self.state_watcher.update_from_config(state_config):
            for job_scheduler in self.jobs:
                self.state_watcher.save_job(job_scheduler.get_job())

    def set_context_base(self, command_context):
        self.context.base = command_context

    def set_read_json(self, read_json):
        self.read_json = read_json

    def configure_eventbus(self, enabled):
        if enabled:
            if not EventBus.instance:
                EventBus.create(f"{self.working_dir}/_events")
                EventBus.start()
        else:
            EventBus.shutdown()

    def get_job_collection(self):
        return self.jobs

    def get_config_manager(self) -> manager.ConfigManager:
        return self.config

    def restore_state(self, action_runner):
        """Use the state manager to retrieve the persisted state from dynamodb and apply it
        to the configured Jobs.
        """
        log.info("Restoring from DynamoDB")

        with prom_metrics.timer(
            operation_name="state_data_retrieval_from_dynamodb",
            log=log,
            histogram_metric=prom_metrics.tron_dynamodb_data_retrieval_duration_seconds_histogram,
            gauge_metric=prom_metrics.tron_last_dynamodb_data_retrieval_duration_seconds_gauge,
        ):
            # restores the state of the jobs and their runs from DynamoDB
            states = self.state_watcher.restore(self.jobs.get_names(), self.read_json)

        log.info("Applying retrieved state to Tron objects...")

        with prom_metrics.timer(
            operation_name="apply_state_to_job_objects",
            log=log,
            histogram_metric=prom_metrics.tron_job_state_application_duration_seconds_histogram,
            gauge_metric=prom_metrics.tron_last_job_state_application_duration_seconds_gauge,
        ):
            self.jobs.restore_state(states.get("job_state", {}), action_runner)

        log.info("Tron state restore complete.")

    def __str__(self):
        return "MCP"
