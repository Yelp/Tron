from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram


tron_cpu_gauge = Gauge("tron_k8s_cpus", "Total number of CPUs allocated to Tron-launched containers")
tron_memory_gauge = Gauge("tron_k8s_mem", "Total amount of memory allocated to Tron-launched containers (in megabytes)")
tron_disk_gauge = Gauge("tron_k8s_disk", "Total amount of disk allocated to Tron-launched containers (in megabytes)")

json_serialization_errors_counter = Counter(
    "json_serialization_errors_total",
    "Total number of errors encountered while serializing state_data as JSON. These errors occur before writing to DynamoDB.",
)

json_deserialization_errors_counter = Counter(
    "json_deserialization_errors_total",
    "Total number of errors encountered while deserializing state_data from JSON. These errors occur after reading from DynamoDB.",
)

# Our current peak is about 10-12 partitions, so this should be more than sufficient.
# Anything above 20 would get grouped into an inf bucket until we expand this.
tron_dynamodb_partitions_histogram = Histogram(
    "tron_dynamodb_partitions",
    "Distribution of partitions per item in DynamoDB",
    buckets=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
)
