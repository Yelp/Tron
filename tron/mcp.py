import logging
import time
from contextlib import contextmanager

from tron import actioncommand
from tron import command_context
from tron import node
from tron.config import manager
from tron.config.schema import MASTER_NAMESPACE
from tron.core.job import Job
from tron.core.job_collection import JobCollection
from tron.core.job_scheduler import JobSchedulerFactory
from tron.core.jobgraph import JobGraph
from tron.eventbus import EventBus
from tron.kubernetes import KubernetesClusterRepository
from tron.serialize.runstate import statemanager

log = logging.getLogger(__name__)


@contextmanager
def timer(function_name: str):
    start = time.time()
    try:
        yield
    except Exception:
        pass
    finally:
        end = time.time()
        log.info(f"Execution time for function {function_name}: {end-start}")


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

    def __init__(self, working_dir, config_path, boot_time):
        super().__init__()
        self.jobs = JobCollection()
        self.working_dir = working_dir
        self.config = manager.ConfigManager(config_path)
        self.context = command_context.CommandContext()
        self.state_watcher = statemanager.StateChangeWatcher()
        self.boot_time = boot_time
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

    def initial_setup(self):
        """When the MCP is initialized the config is applied before the state.
        In this case jobs shouldn't be scheduled until the state is applied.
        """
        # The job schedule factories will be created in the function below
        self._load_config()
        # Jobs will also get scheduled (internally) once the state for action runs are restored in restore_state
        with timer("self.restore_state"):
            self.restore_state(
                actioncommand.create_action_runner_factory_from_config(
                    self.config.load().get_master().action_runner,
                ),
            )
        # Any job with existing state would have been scheduled already. Jobs
        # without any state will be scheduled here.
        self.jobs.run_queue_schedule()

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
            (KubernetesClusterRepository.configure, "k8s_options"),
            (self.configure_eventbus, "eventbus_enabled"),
        ]
        master_config = config_container.get_master()
        apply_master_configuration(master_config_directives, master_config)

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

    def configure_eventbus(self, enabled):
        if enabled:
            if not EventBus.instance:
                EventBus.create(f"{self.working_dir}/_events")
                EventBus.start()
        else:
            EventBus.shutdown()

    def get_job_collection(self):
        return self.jobs

    def get_config_manager(self):
        return self.config

    def restore_state(self, action_runner):
        """Use the state manager to retrieve the persisted state from dynamodb and apply it
        to the configured Jobs.
        """
        log.info("Restoring from DynamoDB")
        with timer("restore"):
            # restores the state of the jobs and their runs from DynamoDB
            states = self.state_watcher.restore(self.jobs.get_names())
        log.info(
            f"Tron will start restoring state for the jobs and will start scheduling them! Time elapsed since Tron started {time.time() - self.boot_time}"
        )
        # loads the runs' state and schedule the next run for each job
        with timer("self.jobs.restore_state"):
            self.jobs.restore_state(states.get("job_state", {}), action_runner)
        log.info(
            f"Tron completed restoring state for the jobs. Time elapsed since Tron started {time.time() - self.boot_time}"
        )
        self.state_watcher.save_metadata()

    def __str__(self):
        return "MCP"
