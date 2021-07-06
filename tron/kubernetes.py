import logging
from logging import Logger

from task_processing.interfaces.event import Event  # type: ignore  # TODO: mypy isn't finding hints
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig  # type: ignore  # TODO: mypy isn't finding hints

import tron.metrics as metrics
from tron.actioncommand import ActionCommand

KUBERNETES_TASK_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
KUBERNETES_TASK_OUTPUT_LOGGER = "tron.kubernetes.task_output"


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
        log = logging.getLogger(f"{__name__}.{self.id}")
        # Every time a task gets created, this function runs and will add
        # more stderr handlers to the logger, which results in duplicate log
        # output. We only want to add the stderr handler if the logger does not
        # have a handler yet.
        if not len(log.handlers):
            handler = logging.StreamHandler(self.stderr)
            handler.setFormatter(logging.Formatter(KUBERNETES_TASK_LOG_FORMAT))
            log.addHandler(handler)

        return log

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
