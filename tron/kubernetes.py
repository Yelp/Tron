import logging
from logging import Logger
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from task_processing.interfaces.event import Event  # type: ignore  # need to add task_proc type hints
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig  # type: ignore
from task_processing.runners.subscription import Subscription  # type: ignore
from task_processing.task_processor import TaskProcessor  # type: ignore
from twisted.internet.defer import Deferred  # type: ignore  # need to upgrade twisted
from twisted.internet.defer import logError

import tron.metrics as metrics
from tron.actioncommand import ActionCommand
from tron.config.schema import ConfigKubernetes
from tron.config.schema import ConfigVolume
from tron.utils.queue import PyDeferredQueue

if TYPE_CHECKING:
    from tron.serialize.runstate.statemanager import StateChangeWatcher

DEFAULT_POD_LAUNCH_TIMEOUT_S = 300  # arbitrary number, same as Mesos offer timeout of yore

KUBERNETES_TASK_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
KUBERNETES_TASK_OUTPUT_LOGGER = "tron.kubernetes.task_output"

log = logging.getLogger(__name__)


class KubernetesTask(ActionCommand):
    def __init__(self, action_run_id: str, task_config: KubernetesTaskConfig, serializer=None):
        # TODO(TASKPROC-238): use the actual task command once that exists
        super().__init__(id=action_run_id, command="ls", serializer=serializer)

        self.task_config = task_config

        self.log = self.get_event_logger()

    def get_event_logger(self) -> Logger:
        """
        Get or create a logger for a the action run associated with this task.

        Used to make it easier to disambiguate what the log messages emitted
        for event handling and such belong to.
        """
        event_log = logging.getLogger(f"{__name__}.{self.id}")
        # Every time a task gets created, this function runs and will add
        # more stderr handlers to the logger, which results in duplicate log
        # output. We only want to add the stderr handler if the logger does not
        # have a handler yet.
        if not len(event_log.handlers):
            handler = logging.StreamHandler(self.stderr)
            handler.setFormatter(logging.Formatter(KUBERNETES_TASK_LOG_FORMAT))
            event_log.addHandler(handler)

        return event_log

    def report_resources(self, decrement: bool = False) -> None:
        """
        Update internal resource utilization statistics of all tronjobs running for this task's Tron master.
        """
        # TODO(TRON-1612): these should eventually be Prometheus metrics
        multiplier = -1 if decrement else 1
        metrics.count("tron.mesos.cpus", self.task_config.cpus * multiplier)
        metrics.count("tron.mesos.mem", self.task_config.mem * multiplier)
        metrics.count("tron.mesos.disk", self.task_config.disk * multiplier)

    def get_kubernetes_id(self) -> str:
        """
        Get the Kubernetes identifier representing this task.

        This will generally be of the form {pod_name}.{unique_suffix}
        """
        return self.task_config.pod_name  # type: ignore  # TODO: mypy isn't seeing that this is typed in task_proc

    def get_config(self) -> KubernetesTaskConfig:
        """
        Get the task_processing config used to create this task.
        """
        return self.task_config

    def log_event_info(self, event: Event) -> None:
        """
        Helper to log nice-to-have information (may fail).
        """
        # TODO: once we're actually bubbling up events from task_proc, we'll want to log detailed
        # information here.
        pass

    def handle_event(self, event: Event) -> None:
        """
        Transitions Tron's state machine for this task based on events from task_processing.
        """
        event_id = getattr(event, "task_id", None)
        if event_id != self.get_kubernetes_id():
            self.log.warning(
                f"Event task id (id={event_id}) does not match current task id (id={self.get_kubernetes_id()}), ignoring.",
            )
            return

        k8s_type = getattr(event, "platform_type", None)
        self.log.info(f"Got event for task={event_id} (Kubernetes type={k8s_type}).")

        try:
            self.log_event_info(event=event)
        except Exception:
            self.log.exception(f"Unable to log event info for id={event_id}.")

        # TODO(TRON-1611): actually transition to different states


class KubernetesCluster:
    def __init__(
        self,
        kubeconfig_path: str,
        enabled: bool = True,
        default_volumes: Optional[List[ConfigVolume]] = None,
        pod_launch_timeout: Optional[int] = None,
    ):
        # general k8s config
        self.kubeconfig_path = kubeconfig_path
        self.enabled = enabled
        self.default_volumes: Optional[List[ConfigVolume]] = default_volumes or []
        self.pod_launch_timeout = pod_launch_timeout or DEFAULT_POD_LAUNCH_TIMEOUT_S

        # creating a task_proc executor has a couple steps:
        # * create a TaskProcessor
        # * load the desired plugin (in this case, the k8s one)
        # * and then actually create the executor (which we call a runner in tron)
        # this last step requires a bit of setup, which is why we don't do it in-line
        # in this constructor
        self.processor = TaskProcessor()
        self.processor.load_plugin(provider_module="task_processing.plugins.kubernetes")
        self.runner: Optional[Subscription] = None

        # queue to to use for tron<->task_proc communication - will hold k8s events seen
        # by task_processing and held for tron to process.
        self.queue = PyDeferredQueue()
        # this will hold the current event to process (retrieved from the PyDeferredQueue above)
        # which we will eventually wrap with some callbacks to actually process using the Twisted
        # reactor started as part of tron's startup process
        self.deferred: Optional[Deferred] = None

        # map from k8s pod names to the task that said pod corresponds to
        self.tasks: Dict[str, KubernetesTask] = {}

        # actually create the executor/runner, as mentioned above.
        self.connect()

    def connect(self) -> None:
        """
        Starts running our Kubernetes task_processing.
        """
        self.runner = self.get_runner(kubeconfig_path=self.kubeconfig_path, queue=self.queue)
        self.handle_next_event()

    def get_runner(self, kubeconfig_path: str, queue: PyDeferredQueue) -> Optional[Subscription]:
        """
        Gets or creates an instance of our Kubernetes task_processing plugin.
        """
        if not self.enabled:
            log.info("Kubernetes usage is disabled, not creating a runner.")
            return None

        # TODO: Add a stopping/terminating state to the task_proc runner
        if self.runner is not None:
            log.info("Reusing previously created runner.")
            return self.runner

        # TODO: once we start implementing more things in the executor, we'll need to actually pass
        # down some config
        executor = self.processor.executor_from_config(provider="kubernetes", provider_config={})

        # TODO: we should return a Subscription here: but this will crash until we've actually implemented
        # get_event_queue() in our task_proc plugin
        return executor

    def handle_next_event(self, _=None) -> None:
        """
        Pops events off of the shared tron<->task_proc queue and processes them.

        We only expect two types of events:
        * control: events regarding how the task_proc plugin is running - handled directly
        * task: events regarding how the actual tasks/Pods we're running our doing - forwarded to KubernetesTask
        """
        if self.deferred and not self.deferred.called:
            log.warning("Already have handlers waiting for next event in queue, not adding more")
            return

        self.deferred = self.queue.get()

        # we want to process the event we just popped off the queue, but we also want
        # to form a sort of event loop, so we add two callbacks:
        # * one to actually deal with the event
        # * and another to grab the next event, in this way creating an event loop :)
        self.deferred.addCallback(self.process_event)
        self.deferred.addCallback(self.handle_next_event)

        # should an exception be thrown, these callbacks will be run instead
        self.deferred.addErrback(logError)
        self.deferred.addErrback(self.handle_next_event)

    def process_event(self, event: Event) -> None:
        """
        Central router for all events received from task_processing.
        """
        if event.kind == "control":
            self._handle_control_event(event)
        elif event.kind == "task":
            self._handle_task_event(event)
        else:
            log.warning(f"Unknown event type ({event.kind}): {event}")

    def _handle_control_event(self, event: Event) -> None:
        """
        Helper method to handle any control-plane events sent from task_processing.
        """
        message = getattr(event, "message", None)
        log.info(f"Processing a control event with message: {message}")

    def _handle_task_event(self, event: Event) -> None:
        """
        Helper method to correctly route task-related events to the appropiate task.
        """
        task_id = getattr(event, "task_id", None)
        if task_id is None:
            log.warning(f"Received a malformed event with no task_id: {event}")
            return

        if task_id not in self.tasks.keys():
            log.warning(f"Got event for unknown task ({task_id} not in {self.tasks.keys()}): {event}")
            return

        task = self.tasks[task_id]
        task.handle_event(event)
        if task.is_done:
            del self.tasks[event.task_id]

    def kill(self, task_id: str) -> bool:
        """
        Instructs task_processing to stop running a given task given a Pod name.
        """
        return self.runner.kill(task_id)  # type: ignore  # we need to add type annotation to task_proc

    def stop(self, fail_tasks: bool = False) -> None:
        """
        Stops the configured task_processing runner and optionally fails all currently running tasks.

        Will also clear the message queue and any unprocessed events.
        """
        if self.runner:
            self.runner.stop()

        # Clear message queue
        if self.deferred:
            self.deferred.cancel()
            self.deferred = None
        self.queue = PyDeferredQueue()

        if fail_tasks:
            for key, task in self.tasks.items():
                # set the task status to unknown
                task.exited(exit_status=None)
                del self.tasks[key]

    def set_enabled(self, is_enabled: bool) -> None:
        """
        Toggles use of the configured Kubernetes cluster.

        Will fail all running tasks if toggled off.
        """
        self.enabled = is_enabled
        if self.enabled:
            self.connect()
        else:
            self.stop(fail_tasks=True)

    def configure_tasks(self, default_volumes: Optional[List[ConfigVolume]]):
        self.default_volumes = default_volumes

    def create_task(self, action_run_id: str, serializer, task_id: Optional[str] = None) -> Optional[KubernetesTask]:
        """
        Given the execution parameters for a task, create a KubernetesTask that encapsulate those parameters.

        This task will not actually be run until KubernetesCluster::submit() is called.
        """
        if self.runner is None:
            log.error(
                f"Attempted to create a task for {action_run_id}, but no task_processing runner has been started."
            )
            return None

        # TODO: fill out required fields once they're ready
        task_config = self.runner.TASK_CONFIG_INTERFACE()

        if task_id is not None:
            try:
                task_config = task_config.set_pod_name(task_id)
            except ValueError:
                log.error(f"Invalid {task_id} for {action_run_id}")
                return None

        return KubernetesTask(action_run_id=action_run_id, task_config=task_config, serializer=serializer)

    def submit(self, task: KubernetesTask) -> None:
        """
        Given a KubernetesTask, submit it to the configured Kubernetes cluster in order to attempt to run it.
        """
        pass

    def recover(self, task: KubernetesTask) -> None:
        """
        Given an instance of a KubernetesTask, attempt to reconcile the current state of the task from Kubernetes.
        """
        pass


class KubernetesClusterRepository:
    # Kubernetes config
    kubernetes_enabled: bool = False
    kubeconfig_path: Optional[str] = None
    pod_launch_timeout: Optional[int] = None
    default_volumes: Optional[List[ConfigVolume]] = None

    # metadata config
    clusters: Dict[str, KubernetesCluster] = {}

    # state management config
    state_data = {}  # type: ignore  # not used yet
    state_watcher: Optional["StateChangeWatcher"] = None

    @classmethod
    def attach(cls, _, observer):
        cls.state_watcher = observer

    @classmethod
    def get_cluster(cls, kubeconfig_path: Optional[str] = None) -> Optional[KubernetesCluster]:
        if kubeconfig_path is None:
            kubeconfig_path = cls.kubeconfig_path
            return None

        if kubeconfig_path not in cls.clusters:
            cluster = KubernetesCluster(
                kubeconfig_path=kubeconfig_path, enabled=cls.kubernetes_enabled, default_volumes=cls.default_volumes
            )
            cls.clusters[kubeconfig_path] = cluster

        return cls.clusters[kubeconfig_path]

    @classmethod
    def shutdown(cls) -> None:
        for cluster in cls.clusters.values():
            cluster.stop()

    @classmethod
    def configure(cls, kubernetes_options: ConfigKubernetes) -> None:
        cls.kubeconfig_path = kubernetes_options.kubeconfig_path
        cls.kubernetes_enabled = kubernetes_options.enabled
        cls.default_volumes = kubernetes_options.default_volumes

        for cluster in cls.clusters.values():
            cluster.set_enabled(cls.kubernetes_enabled)
            cluster.configure_tasks(default_volumes=cls.default_volumes)
