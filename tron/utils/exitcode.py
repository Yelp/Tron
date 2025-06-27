from typing import Dict


EXIT_INVALID_COMMAND = -1
EXIT_NODE_ERROR = -2
EXIT_STOP_KILL = -3
EXIT_TRIGGER_TIMEOUT = -4
EXIT_MESOS_DISABLED = -5
EXIT_KUBERNETES_DISABLED = -6
EXIT_KUBERNETES_NOT_CONFIGURED = -7
EXIT_KUBERNETES_TASK_INVALID = -8
EXIT_KUBERNETES_ABNORMAL = -9
EXIT_KUBERNETES_SPOT_INTERRUPTION = -10
EXIT_KUBERNETES_NODE_SCALEDOWN = -11
EXIT_KUBERNETES_TASK_LOST = -12
EXIT_KUBERNETES_EPHEMERAL_STORAGE_EVICTION = -13

EXIT_REASONS: Dict[int, str] = {
    EXIT_INVALID_COMMAND: "Invalid command",
    EXIT_NODE_ERROR: "Node error",
    EXIT_STOP_KILL: "Stopped or killed",
    EXIT_TRIGGER_TIMEOUT: "Timed out waiting for trigger",
    EXIT_MESOS_DISABLED: "Mesos disabled",
    EXIT_KUBERNETES_DISABLED: "Kubernetes disabled",
    EXIT_KUBERNETES_NOT_CONFIGURED: "Kubernetes enabled, but not configured",
    EXIT_KUBERNETES_TASK_INVALID: "Kubernetes task was not valid",
    EXIT_KUBERNETES_ABNORMAL: "Kubernetes task failed in an unexpected manner",
    EXIT_KUBERNETES_SPOT_INTERRUPTION: "Kubernetes task failed due to spot interruption",
    EXIT_KUBERNETES_NODE_SCALEDOWN: "Kubernetes task failed due to the autoscaler scaling down a node",
    EXIT_KUBERNETES_TASK_LOST: "Kubernetes task is lost and the final outcome unknown",
    EXIT_KUBERNETES_EPHEMERAL_STORAGE_EVICTION: "Kubernetes task failed due to exceeding disk-space usage limits",
}
