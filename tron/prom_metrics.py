from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Histogram


tron_cpu_gauge = Gauge("tron_k8s_cpus", "Total number of CPUs allocated to Tron-launched containers")
tron_memory_gauge = Gauge("tron_k8s_mem", "Total amount of memory allocated to Tron-launched containers (in megabytes)")
tron_disk_gauge = Gauge("tron_k8s_disk", "Total amount of disk allocated to Tron-launched containers (in megabytes)")

# TODO: prefix with tron_ to be consistent with other metrics
json_serialization_errors_counter = Counter(
    "json_serialization_errors_total",
    "Total number of errors encountered while serializing state_data as JSON. These errors occur before writing to DynamoDB.",
)

# TODO: prefix with tron_ to be consistent with other metrics
json_deserialization_errors_counter = Counter(
    "json_deserialization_errors_total",
    "Total number of errors encountered while deserializing state_data from JSON. These errors occur after reading from DynamoDB.",
)

# Our current peak is about 10-12 partitions, so this should be more than sufficient.
# Anything above 20 would get grouped into an inf bucket until we expand this.
# This Histogram tracks the distribution of partition counts *per save/set operation*.
tron_dynamodb_partitions_histogram = Histogram(
    "tron_dynamodb_partitions",
    "Distribution of partitions per item observed during save operations in DynamoDB",
    buckets=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, float("inf")],
)

tron_job_count_gauge = Gauge("tron_job_count", "Total number of Jobs configured in Tron")
tron_job_runs_created_counter = Counter("tron_job_runs_created", "Total number of JobRuns created")
tron_action_count_gauge = Gauge("tron_action_count", "Total number of Actions configured in Tron (sum across all jobs)")
tron_action_runs_created_counter = Counter(
    "tron_action_runs_created", "Total number of ActionRuns created", ["executor"]
)
tron_action_runs_valid_counter = Counter(
    "tron_action_runs_valid_total", "Total number of Valid ActionRuns created", ["executor"]
)
# We experience some variability in the time it takes to restore, but
# this captures the distribution in different environments pretty well.
duration_buckets_sec = [
    1.0,
    2.5,
    5.0,
    10.0,
    15.0,
    30.0,
    45.0,
    60.0,
    90.0,
    120.0,
    180.0,
    240.0,
    300.0,
    360.0,
    420.0,
    480.0,
    540.0,
    600.0,
    750.0,
    900.0,
    1200.0,
    float("inf"),
]

# We can get more granular with these, but it's a good start. As it is right now, this looks like:
#
# Total Startup Time (tron_last_startup_duration_seconds)
#   |-- _load_config()
#   |-- Total Restore Time (tron_last_restore_duration_seconds)
#   |     |-- state_watcher.restore() (tron_last_dynamodb_data_retrieval_duration_seconds_gauge)
#   |     |-- jobs.restore_state() (tron_last_job_state_application_duration_seconds_gauge)
#   |     |-- overhead within restore_state()
#   |-- jobs.run_queue_schedule()
#   |-- initial_setup()
tron_restore_duration_seconds_histogram = Histogram(
    "tron_restore_duration_seconds",
    "Distribution of time taken for the complete state restore process during startup",
    buckets=duration_buckets_sec,
)
tron_dynamodb_data_retrieval_duration_seconds_histogram = Histogram(
    "tron_dynamodb_data_retrieval_duration_seconds",
    "Distribution of time taken to retrieve all state data from DynamoDB during restore",
    buckets=duration_buckets_sec,
)
tron_job_state_application_duration_seconds_histogram = Histogram(
    "tron_job_state_application_duration_seconds",
    "Distribution of time taken to apply retrieved state data to Job/JobRun objects during restore",
    buckets=duration_buckets_sec,
)
tron_startup_duration_seconds_histogram = Histogram(
    "tron_startup_duration_seconds",
    "Distribution of total time taken for Tron to start up and be ready",
    buckets=duration_buckets_sec,
)

# We use a gauge for the *last* restore. Histogram is useful for quantiles (p90, p95, etc.),
# but this is nice for breaking down the most recent restore duration into components.
tron_last_restore_duration_seconds_gauge = Gauge(
    "tron_last_restore_duration_seconds", "Duration of the most recent complete state restore process during startup"
)
tron_last_dynamodb_data_retrieval_duration_seconds_gauge = Gauge(
    "tron_last_dynamodb_data_retrieval_duration_seconds",
    "Duration of the most recent retrieval of all state data from DynamoDB during restore",
)
tron_last_job_state_application_duration_seconds_gauge = Gauge(
    "tron_last_job_state_application_duration_seconds",
    "Duration of the most recent application of retrieved state data to Job/JobRun objects",
)
tron_last_startup_duration_seconds_gauge = Gauge(
    "tron_last_startup_duration_seconds", "Duration of the most recent total Tron startup process"
)
