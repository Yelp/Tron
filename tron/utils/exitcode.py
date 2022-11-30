# TRON-1826 Refactored code and moved exit codes to a new file to be imported in both Kubernetes.py and actionrun.py
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

EXIT_REASONS = {
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
}
