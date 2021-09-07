import logging
from logging import Logger
from typing import cast
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from task_processing.interfaces.event import Event
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.runners.subscription import Subscription
from task_processing.task_processor import TaskProcessor
from twisted.internet.defer import Deferred  # type: ignore  # need to upgrade twisted
from twisted.internet.defer import logError

import tron.metrics as metrics
from tron.actioncommand import ActionCommand
from tron.config.schema import ConfigKubernetes
from tron.config.schema import ConfigNodeAffinity
from tron.config.schema import ConfigSecretSource
from tron.config.schema import ConfigVolume
from tron.serialize.filehandler import OutputStreamSerializer
from tron.utils.queue import PyDeferredQueue

if TYPE_CHECKING:
    from tron.serialize.runstate.statemanager import StateChangeWatcher

DEFAULT_POD_LAUNCH_TIMEOUT_S = 300  # arbitrary number, same as Mesos offer timeout of yore
DEFAULT_DISK_LIMIT = 1024.0  # arbitrary, same as what was chosen for Mesos-based Tronjobs

KUBERNETES_TASK_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
KUBERNETES_TASK_OUTPUT_LOGGER = "tron.kubernetes.task_output"

log = logging.getLogger(__name__)


def combine_volumes(defaults: Collection[ConfigVolume], overrides: Collection[ConfigVolume],) -> List[ConfigVolume]:
    """Helper to reconcile lists of volume mounts.

    If any volumes have the same container path, the one in overrides wins.
    """
    result = {mount.container_path: mount for mount in defaults}
    for mount in overrides:
        result[mount.container_path] = mount
    return list(result.values())


class KubernetesTask(ActionCommand):
    def __init__(self, action_run_id: str, task_config: KubernetesTaskConfig, serializer=None):
        super().__init__(id=action_run_id, command=task_config.command, serializer=serializer)

        self.task_config = task_config

        self.log = self.get_event_logger()

        self.log.info(f"Kubernetes task {self.get_kubernetes_id()} created with config {self.get_config()}")

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
        metrics.count("tron.mesos.mem", self.task_config.memory * multiplier)
        metrics.count("tron.mesos.disk", self.task_config.disk * multiplier)

    def get_kubernetes_id(self) -> str:
        """
        Get the Kubernetes identifier representing this task.

        This will generally be of the form {pod_name}.{unique_suffix}
        """
        return self.task_config.pod_name

    def get_config(self) -> KubernetesTaskConfig:
        """
        Get the task_processing config used to create this task.
        """
        return self.task_config

    def log_event_info(self, event: Event) -> None:
        """
        Helper to log nice-to-have information (may fail).
        """
        k8s_type = getattr(event, "platform_type", None)
        if k8s_type == "running":
            hostname = event.raw.get("spec", {}).get("nodeName", "UNKNOWN")
            self.log.info(f"Running on hostname: {hostname}")

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

        if k8s_type == "running":
            self.started()
        elif k8s_type == "finished":
            self.exited(0)
        elif k8s_type == "failed":
            self.exited(1)
        elif k8s_type == "lost":
            # Using 'lost' instead of 'unknown' for now until we are sure that before reconcile() is called,
            # the tasks inside task_metadata map are all UNKNOWN
            self.log.warning("Kubernetes does not know anything about this task, it is LOST")
            self.log.warning(
                "This can happen for any number of reasons, and Tron can't know if the task ran or not at all!"
            )
            self.log.warning("If you want Tron to RUN it (again) anyway, retry it with:")
            self.log.warning(f"    tronctl retry {self.id}")
            self.log.warning("If you want Tron to NOT run it and consider it as a success, skip it with:")
            self.log.warning(f"    tronctl skip {self.id}")
            self.log.warning("If you want Tron to NOT run it and consider it as a failure, fail it with:")
            self.log.warning(f"    tronctl fail {self.id}")
            self.exited(None)
        else:
            self.log.info(f"Did not handle unknown kubernetes event type: {event}",)

        if event.terminal:
            self.log.info("This Kubernetes event was terminal, ending this action")
            self.report_resources(decrement=True)

            exit_code = int(not getattr(event, "success", False))
            # Returns False if we've already exited normally above
            unexpected_error = self.exited(exit_code)
            if unexpected_error:
                self.log.error("Unexpected failure, exiting")

            self.done()


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

        executor = self.processor.executor_from_config(
            provider="kubernetes",
            provider_config={
                "namespace": "tron",
                "kubeconfig_path": self.kubeconfig_path,
                "task_configs": [task.get_config() for task in self.tasks.values()],
            },
        )

        return Subscription(executor, queue)

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
        if task.is_done and event.task_id is not None:
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
            # NOTE: we're turning this into a list on purpose: otherwise we're modifying the dict we're iterating over
            for key, task in list(self.tasks.items()):
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

    def create_task(
        self,
        action_run_id: str,
        serializer: OutputStreamSerializer,
        command: str,
        cpus: Optional[float],
        mem: Optional[float],
        disk: Optional[float],
        docker_image: str,
        env: Dict[str, str],
        secret_env: Dict[str, ConfigSecretSource],
        volumes: Collection[ConfigVolume],
        cap_add: Collection[str],
        cap_drop: Collection[str],
        node_selectors: Dict[str, str],
        node_affinities: ConfigNodeAffinity,
        pod_labels: Dict[str, str],
        task_id: Optional[str] = None,
    ) -> Optional[KubernetesTask]:
        """
        Given the execution parameters for a task, create a KubernetesTask that encapsulate those parameters.

        This task will not actually be run until KubernetesCluster::submit() is called.
        """
        if self.runner is None:
            log.error(
                f"Attempted to create a task for {action_run_id}, but no task_processing runner has been started."
            )
            return None

        task_config = cast(
            KubernetesTaskConfig,
            self.runner.TASK_CONFIG_INTERFACE(
                name=action_run_id,
                command=command,
                image=docker_image,
                cpus=cpus,
                memory=mem,
                disk=DEFAULT_DISK_LIMIT if disk is None else disk,
                environment=env,
                secret_environment={k: v._asdict() for k, v in secret_env.items()},
                cap_add=cap_add,
                cap_drop=cap_drop,
                volumes=[
                    volume._asdict()
                    for volume in combine_volumes(defaults=self.default_volumes or [], overrides=volumes)
                ],
                node_selectors=node_selectors,
                node_affinities=[affinity._asdict() for affinity in node_affinities],
                labels=pod_labels,
            ),
        )

        # this should only ever be non-null when we're recovering from a Tron restart
        # and are recreating the previous state - when actually creating a new task
        # we'll always let task_processing come up with a Pod name for us
        if task_id is not None:
            try:
                task_config = task_config.set_pod_name(task_id)
            except ValueError:
                log.error(f"Invalid {task_id} for {action_run_id}")
                return None

        return KubernetesTask(action_run_id=action_run_id, task_config=task_config, serializer=serializer,)

    def _check_connection(self) -> None:
        """
        Helper to ensure that the task_processing plugin is in a running state and event handling
        is correctly setup in case we've disabled k8s at some point during operation.
        """
        if self.runner is None or self.runner.stopping:
            log.info("k8s plugin never created or stopped, restarting.")
            self.connect()
        # re-add callbacks just in case they're missing
        elif self.deferred is None or self.deferred.called:
            self.handle_next_event()

    def submit(self, task: KubernetesTask) -> None:
        """
        Given a KubernetesTask, submit it to the configured Kubernetes cluster in order to attempt to run it.
        """
        # Submitting a task while k8s usage is disabled should fail the task so that
        # users know that they have to take action and re-run whatever was scheduled
        # during the time this killswitch is active
        if not self.enabled:
            task.log.info("Not starting task, Kubernetes usage is disabled.")
            task.exited(1)
            return

        # it's possible that we're the first task submission following k8s going from
        # disabled -> enabled, so make sure everything is correctly setup
        self._check_connection()
        assert self.runner is not None, "Unable to correctly setup k8s runner!"

        # store the task to be launched before actually launching it so that there's
        # no race conditions later on with processing an event for that Pod before
        # Tron know that that Pod is for a task it cares about
        self.tasks[task.get_kubernetes_id()] = task

        # XXX: if spark-on-k8s ends up running through task_processing, we'll need to revist
        # reimplementing the clusterman resource reporting that MesosCluster::submit() used to do
        if not self.runner.run(task.get_config()):
            log.warning(f"Unable to submit task {task.get_kubernetes_id()} to configured k8s cluster.")
            task.exited(1)
        log.info(f"Submitted task {task.get_kubernetes_id()} to configured k8s cluster.")

        # update internal resource usage tracker (this isn't connected at all to clusterman)
        task.report_resources()

    def recover(self, task: KubernetesTask) -> None:
        """
        Given an instance of a KubernetesTask, attempt to reconcile the current state of the task from Kubernetes.
        """
        if not task:
            return

        if not self.enabled:
            task.log.info("Could not recover task, Kubernetes usage is disabled.")
            task.exited(None)
            return

        self._check_connection()
        assert self.runner is not None, "Unable to correctly setup k8s runner!"

        # the task/kubernetes id is really just the pod name
        task_id = task.get_kubernetes_id()
        self.tasks[task_id] = task
        task.log.info("TRON RESTARTED! Starting recovery procedure by reconciling state for this task from Kubernetes")
        task.started()
        self.runner.reconcile(task.get_config())
        task.report_resources()


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
            if cls.kubeconfig_path is None:
                return None
            kubeconfig_path = cls.kubeconfig_path

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
