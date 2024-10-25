import datetime
import json
import logging
from typing import Any
from typing import Dict
from typing import Optional
from typing import TypeVar

from tron import command_context
from tron import node
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.core import jobrun
from tron.core.actiongraph import ActionGraph
from tron.core.actionrun import ActionRun
from tron.core.jobrun import JobRunCollection
from tron.node import NodePool
from tron.scheduler import GeneralScheduler
from tron.serialize import filehandler
from tron.utils import maybe_decode
from tron.utils.observer import Observable
from tron.utils.observer import Observer
from tron.utils.persistable import Persistable


class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger(__name__)

T = TypeVar("T", bound="Job")


class Job(Observable, Observer, Persistable):
    """A configurable data object.

    Job uses JobRunCollection to manage its runs, and ActionGraph to manage its
    actions and their dependency graph.
    """

    STATUS_DISABLED = "disabled"
    STATUS_ENABLED = "enabled"
    STATUS_UNKNOWN = "unknown"
    STATUS_RUNNING = "running"

    NOTIFY_STATE_CHANGE = "notify_state_change"
    NOTIFY_RUN_DONE = "notify_run_done"
    NOTIFY_NEW_RUN = "notify_new_run"

    context_class = command_context.JobContext

    # These attributes determine equality between two Job objects
    equality_attributes = [
        "name",
        "queueing",
        "scheduler",
        "node_pool",
        "all_nodes",
        "action_graph",
        "output_path",
        "action_runner",
        "max_runtime",
        "allow_overlap",
        "monitoring",
        "time_zone",
        "expected_runtime",
        "run_limit",
    ]

    def __init__(
        self,
        name: str,
        scheduler: GeneralScheduler,
        queueing: bool = True,
        all_nodes: bool = False,
        monitoring: Optional[Dict[str, Any]] = None,
        node_pool: Optional[NodePool] = None,
        enabled: bool = True,
        action_graph: Optional[ActionGraph] = None,
        run_collection: Optional[JobRunCollection] = None,
        parent_context: Optional[command_context.CommandContext] = None,
        output_path: Optional[filehandler.OutputPath] = None,
        allow_overlap: Optional[bool] = None,
        action_runner: Optional[SubprocessActionRunnerFactory] = None,
        max_runtime: Optional[datetime.timedelta] = None,
        time_zone: Optional[datetime.tzinfo] = None,
        expected_runtime: Optional[datetime.timedelta] = None,
        run_limit: Optional[int] = None,
    ):
        super().__init__()
        self.name = maybe_decode(
            name
        )  # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
        self.monitoring = monitoring
        self.action_graph = action_graph
        self.scheduler = scheduler
        self.runs = run_collection
        self.queueing = queueing
        self.all_nodes = all_nodes
        self.enabled = enabled  # current enabled setting
        self.config_enabled = enabled  # enabled attribute from file
        self.node_pool = node_pool
        self.allow_overlap = allow_overlap
        self.action_runner = action_runner
        self.max_runtime = max_runtime
        self.time_zone = time_zone
        self.expected_runtime = expected_runtime
        self.output_path = output_path or filehandler.OutputPath()
        # if the name doesn't have a period, the "namespace" and the "job-name" will
        # be the same, we don't have to worry about a crash here
        self.output_path.append(name.split(".")[0])  # namespace
        self.output_path.append(name.split(".")[-1])  # job-name
        self.context = command_context.build_context(self, parent_context)
        self.run_limit = run_limit
        log.info(f"{self} created")

    @staticmethod
    def to_json(state_data: dict) -> Optional[str]:
        """Serialize the Job instance to a JSON string."""
        try:
            return json.dumps(state_data)
        except Exception:
            log.exception("Error serializing Job to JSON:")
            raise

    @classmethod
    def from_config(
        cls,
        job_config,
        scheduler,
        parent_context,
        output_path,
        action_runner,
        action_graph,
    ):
        """Factory method to create a new Job instance from configuration."""
        runs = jobrun.JobRunCollection.from_config(job_config)
        node_repo = node.NodePoolRepository.get_instance()

        return cls(
            name=job_config.name,
            monitoring=job_config.monitoring,
            time_zone=job_config.time_zone,
            queueing=job_config.queueing,
            all_nodes=job_config.all_nodes,
            node_pool=node_repo.get_by_name(job_config.node),
            scheduler=scheduler,
            enabled=job_config.enabled,
            run_collection=runs,
            action_graph=action_graph,
            parent_context=parent_context,
            output_path=output_path,
            allow_overlap=job_config.allow_overlap,
            action_runner=action_runner,
            max_runtime=job_config.max_runtime,
            expected_runtime=job_config.expected_runtime,
            run_limit=job_config.run_limit,
        )

    def watch(self, observable, event=True):
        # Overrides default method from Observer.
        # Allows job's watchers to handle updates from job runs independently.
        super().watch(observable, event)
        if isinstance(observable, jobrun.JobRun):
            self.notify(self.NOTIFY_NEW_RUN, event_data=observable)

    def update_from_job(self, job):
        """Update this Jobs configuration from a new config. This method
        actually takes an already constructed job and copies out its
        configuration data.
        """
        for attr in self.equality_attributes:
            setattr(self, attr, getattr(job, attr))

        self.update_action_config()

        # the run_limit is a property on the JobRunCollection, not on the
        # Job itself so we need to handle that separately
        self.runs.run_limit = job.run_limit
        log.info(f"{self} reconfigured")

    def update_action_config(self):
        for job_run in self.runs:
            job_run.update_action_config(self.action_graph)

    @property
    def status(self):
        """Current status."""
        if not self.enabled:
            return self.STATUS_DISABLED
        if self.runs.get_active():
            return self.STATUS_RUNNING

        if self.runs.get_run_by_state(ActionRun.SCHEDULED):
            return self.STATUS_ENABLED

        log.warning(f"{self} in an unknown state: {self.runs}")
        return self.STATUS_UNKNOWN

    def get_name(self):
        return self.name

    def get_monitoring(self):
        return self.monitoring

    def get_time_zone(self):
        return self.time_zone

    def get_runs(self):
        return self.runs

    @property
    def state_data(self):
        """
        This data is used to serialize the state of this job.
        State of job runs is serialized separately.
        """
        return {
            "run_nums": self.runs.get_run_nums(),
            "enabled": self.enabled,
        }

    def get_job_runs_from_state(self, state_data):
        """Apply a previous state to this Job."""
        self.enabled = state_data["enabled"]
        job_runs = jobrun.job_runs_from_state(
            state_data["runs"],
            self.action_graph,
            self.output_path.clone(),
            self.context,
            self.node_pool,
        )
        return job_runs

    def build_new_runs(self, run_time, manual=False):
        """Uses its JobCollection to build new JobRuns. If all_nodes is set,
        build a run for every node, otherwise just builds a single run on a
        single node.
        """
        pool = self.node_pool
        nodes = pool.nodes if self.all_nodes else [pool.next()]
        for n in nodes:
            run = self.runs.build_new_run(self, run_time, n, manual=manual)
            self.watch(run)
            yield run

    def handle_job_run_state_change(self, _job_run, event, event_data=None):
        """Handle state changes from JobRuns and propagate changes to any
        observers.
        """
        # Propagate state change for serialization
        if event == jobrun.JobRun.NOTIFY_STATE_CHANGED:
            self.notify(self.NOTIFY_STATE_CHANGE)
            return

        # Propagate DONE JobRun notifications to JobScheduler
        if event == jobrun.JobRun.NOTIFY_DONE:
            self.notify(self.NOTIFY_RUN_DONE)
            return

    handler = handle_job_run_state_change

    def __eq__(self, other):
        return all(getattr(other, attr, None) == getattr(self, attr, None) for attr in self.equality_attributes)

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Job:%s" % self.name
