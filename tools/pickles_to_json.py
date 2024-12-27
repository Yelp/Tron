import logging
import math
import pickle
from typing import List
from typing import Optional

import boto3
from boto3.resources.base import ServiceResource

from tron.core.job import Job
from tron.core.jobrun import JobRun
from tron.serialize import runstate

# TODO: partitioned pickles!

# Max DynamoDB object size is 400KB. Since we save two copies of the object (pickled and JSON),
# we need to consider this max size applies to the entire item, so we use a max size of 200KB
# for each version.
#
# In testing I could get away with 201_000 for both partitions so this should be enough overhead
# to contain other attributes like object name and number of partitions.
OBJECT_SIZE = 200_000

# TODO: use logging for all the prints
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Get Table
def get_dynamodb_table(
    aws_profile: str = "dev", table: str = "infrastage-tron-state", region: str = "us-west-1"
) -> ServiceResource:
    """
    Get the DynamoDB table resource.

    :param aws_profile: The name of the AWS profile to use (default is "dev").
    :param table: The name of the table to get (default is "infrastage-tron-state").
    :param region: The region of the table (default is "us-west-1").
    :return: The DynamoDB table resource.
    """
    session = boto3.Session(profile_name=aws_profile)
    return session.resource("dynamodb", region_name=region).Table(table)


# Get Jobs
def summarize_table(source_table: ServiceResource) -> None:
    """
    Summarize the DynamoDB table and output basic info about each key.

    :param source_table: The DynamoDB table resource to scan.
    """
    response = source_table.scan()
    items = response.get("Items", [])

    print(f"{'Key':<120} {'Has json_val':<15} {'Num JSON Partitions':<20} {'Num Pickle Partitions':<20}")

    for item in items:
        key = item.get("key", "Unknown Key")
        has_json_val = "json_val" in item
        num_json_partitions = int(item.get("num_json_val_partitions", 0))
        num_pickle_partitions = int(item.get("num_partitions", 0))

        print(f"{key:<120} {str(has_json_val):<15} {num_json_partitions:<20} {num_pickle_partitions:<20}")


def get_all_jobs(source_table: ServiceResource) -> List[str]:
    """
    Scan the DynamoDB table and return a list of all job keys.

    :param source_table: The DynamoDB table resource to scan.
    :return: A list of all job keys.
    """
    response = source_table.scan()
    items = response.get("Items", [])

    all_job_keys = [item.get("key", "Unknown Key") for item in items]
    return all_job_keys


def get_jobs_without_json_val(source_table: ServiceResource, partitioned: Optional[bool] = None) -> List[str]:
    """
    Scan the DynamoDB table and return a list of jobs that don't have a json_val.
    Optionally filter by whether the jobs are partitioned.

    :param source_table: The DynamoDB table resource to scan.
    :param partitioned: If specified, filter jobs by partitioned status.
                        True for partitioned, False for non-partitioned, None for no filter.
    :return: A list of job keys without json_val, filtered by partitioned status if specified.
    """
    response = source_table.scan()
    items = response.get("Items", [])

    jobs_without_json_val = set()

    for item in items:
        key = item.get("key", "Unknown Key")
        has_json_val = "json_val" not in item
        num_partitions = int(item.get("num_partitions", 0))

        if has_json_val:
            if partitioned is None:
                jobs_without_json_val.add(key)
            elif partitioned and num_partitions > 1:
                jobs_without_json_val.add(key)
            elif not partitioned and num_partitions <= 1:
                jobs_without_json_val.add(key)

    return list(jobs_without_json_val)


# Load and Print
def load_and_combine_partitions(source_table: ServiceResource, key: str) -> bytes:
    """
    Load and combine all partitions of a pickled item from DynamoDB.

    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :return: The combined pickled data as bytes.
    """
    combined_data = bytearray()
    index = 0

    while True:
        response = source_table.get_item(Key={"key": key, "index": index})
        if "Item" not in response:
            break

        item = response["Item"]
        combined_data.extend(item["val"].value)
        index += 1

    return bytes(combined_data)


def load_and_print_single_pickle(source_table: ServiceResource, key: str, print_full: bool = False) -> None:
    """
    Load the pickled data from DynamoDB for a given key, handle partitioned items,
    and print the loaded pickle. Optionally print the entire pickle based on a flag.

    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :param print_full: Flag to indicate whether to print the entire pickle (default is False).
    """
    try:
        pickled_data = load_and_combine_partitions(source_table, key)
        loaded_pickle = pickle.loads(pickled_data)

        if print_full:
            print(f"Key: {key:<100}\nFull Pickle:")
            print(loaded_pickle)
        else:
            print(f"Key: {key:<100} Pickle successfully loaded")

    except Exception as e:
        logger.error(f"Key: {key} - Failed to load pickle: {e}")


def load_and_print_pickles(source_table: ServiceResource, keys: List[str]) -> None:
    """
    Load and print pickles for the given list of keys.

    :param source_table: The DynamoDB table resource.
    :param keys: A list of keys for which to load and print pickles.
    """
    for key in keys:
        load_and_print_single_pickle(source_table, key)


def load_and_print_json(source_table: ServiceResource, key: str) -> None:
    """
    Load the JSON data from DynamoDB for a given key and print it.

    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    """
    try:
        response = source_table.get_item(Key={"key": key, "index": 0})
        item = response.get("Item", {})

        if "json_val" in item:
            json_data = item["json_val"]
            print(f"Key: {key:<120} - JSON successfully loaded and printed")
            print(json_data)
        else:
            print(f"Key: {key:<120} - No JSON value found")

    except Exception as e:
        logger.error(f"Key: {key} - Failed to load JSON: {e}")


# Convert
# KKASP: REVIEW
def convert_pickle_to_json_and_update_table(source_table: ServiceResource, key: str, dry_run: bool = True) -> None:
    """
    Convert a single pickled item to JSON and update the DynamoDB entry.

    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :param dry_run: If True, simulate the conversion without updating the table.
    """
    try:
        # Skip conversion for job_state MASTER and job_run_state MASTER jobs
        if key.startswith("job_state MASTER") or key.startswith("job_run_state MASTER"):
            # logger.info(f"Skipping conversion for key: {key}")
            return

        pickled_data = load_and_combine_partitions(source_table, key)
        state_data = pickle.loads(pickled_data)

        state_type = key.split()[0]
        if state_type == runstate.JOB_STATE:
            json_data = Job.to_json(state_data)
        elif state_type == runstate.JOB_RUN_STATE:
            json_data = JobRun.to_json(state_data)
        else:
            raise ValueError(f"Unknown type: {state_type}")

        num_json_partitions = math.ceil(len(json_data) / OBJECT_SIZE)
        for partition_index in range(num_json_partitions):
            json_partition = json_data[
                partition_index * OBJECT_SIZE : min((partition_index + 1) * OBJECT_SIZE, len(json_data))
            ]

            if not dry_run:
                source_table.update_item(
                    Key={"key": key, "index": partition_index},
                    UpdateExpression="SET json_val = :json, num_json_val_partitions = :num_partitions",
                    ExpressionAttributeValues={
                        ":json": json_partition,
                        ":num_partitions": num_json_partitions,  # KKASP: is this correct?
                    },
                )

        if dry_run:
            logger.info(f"DRY RUN: Key: {key} - Pickle would have been converted to JSON and updated")
        else:
            logger.info(f"Key: {key} - Pickle converted to JSON and updated")
    except Exception as e:
        logger.error(f"Key: {key} - Failed to convert pickle to JSON: {e}")


def convert_all_pickles_to_json_and_update_table(source_table: ServiceResource, dry_run: bool = True) -> None:
    """
    Convert all pickled items in the DynamoDB table to JSON and update the entries.

    :param source_table: The DynamoDB table resource.
    :param dry_run: If True, simulate the conversion without updating the table.
    """
    items = scan_table(source_table)
    total_keys = len(items)
    converted_keys = 0

    for item in items:
        key = item.get("key", "Unknown Key")
        try:
            convert_pickle_to_json_and_update_table(source_table, key, dry_run)
            converted_keys += 1
        except Exception as e:
            logger.error(f"Key: {key} - Failed to convert pickle to JSON: {e}")

    print(f"Total keys in the table: {total_keys}")
    print(f"Number of keys converted: {converted_keys}")


def scan_table(source_table: ServiceResource) -> List[dict]:
    """
    Scan the DynamoDB table and return all items, handling pagination.

    :param source_table: The DynamoDB table resource to scan.
    :return: A list of all items in the table.
    """
    items = []
    response = source_table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    return items


aws_profile = "dev"
table_name = "infrastage-tron-state"
# table_name = "norcal-devc-tron-state"
table_region = "us-west-1"
source_table = get_dynamodb_table(aws_profile, table_name, table_region)

convert_all_pickles_to_json_and_update_table(source_table, dry_run=False)
# load_and_print_json(source_table, "job_run_state paasta-contract-monitor.k8s.1573")

# 1. Scan table
# summarize_table(source_table)
# print(f"All jobs:\n{get_all_jobs(source_table)}\n")
# print(f"Jobs without json_val:\n{get_jobs_without_json_val(source_table, False)}\n")

# # 2. Load and print a single job_state pickle
# load_and_print_single_pickle(source_table, "job_state compute-infra-test-service.test_load_foo1")

# # 2.1 Load and print a single job_run_state pickle
# load_and_print_single_pickle(source_table, "job_run_state katamari_test_service.test_load_foo2.255")

# 3. Load and print all pickles
# load_and_print_pickles(source_table, get_all_jobs(source_table))

# # 4. Convert a single pickle to JSON
# convert_pickle_to_json(source_table, "job_state compute-infra-test-service.test_load_foo1", dry_run=True)

# 5. Convert all pickles to JSON
# convert_all_pickles_to_json(source_table, dry_run=True)


# KKASP:
# 1. Test getting job keys
# DONE: infrastage-tron-state
# TODO: norcal-devc-tron-state
# 2. Test loading pickles
# DONE: infrastrage-tron-state
# 3. Test converting pickles to JSON
# TODO: convert and print a single pickle and compare to a printed JSON?
# 4. Add dry run flag that doesn't update the table
