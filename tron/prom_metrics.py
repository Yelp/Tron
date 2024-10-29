from prometheus_client import Counter
from prometheus_client import Gauge


tron_cpu_gauge = Gauge("tron_k8s_cpus", "Total number of CPUs allocated to Tron-launched containers")
tron_memory_gauge = Gauge("tron_k8s_mem", "Total amount of memory allocated to Tron-launched containers (in megabytes)")
tron_disk_gauge = Gauge("tron_k8s_disk", "Total amount of disk allocated to Tron-launched containers (in megabytes)")

json_serialization_errors_counter = Counter(
    "json_serialization_errors_total",
    "Total number of errors encountered while serializing state_data as JSON. These errors occur before writing to DynamoDB.",
)
