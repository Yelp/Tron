import logging
import math
import pickle
from typing import List

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


def get_jobs_without_json_val(source_table: ServiceResource) -> List[str]:
    """
    Scan the DynamoDB table and return a list of jobs that don't have a json_val.
    Also log the job names.

    :param source_table: The DynamoDB table resource to scan.
    :return: A list of job keys without json_val.
    """
    response = source_table.scan()
    items = response.get("Items", [])

    jobs_without_json_val = []

    for item in items:
        key = item.get("key", "Unknown Key")
        has_json_val = "json_val" not in item

        if has_json_val:
            jobs_without_json_val.append(key)
            logger.info(f"Job without json_val: {key}")

    return jobs_without_json_val


def load_and_print_pickle(source_table: ServiceResource, key: str, index: int = 0) -> None:
    """
    Load the pickled data from DynamoDB for a given key and log a success message.

    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :param index: The index of the partition to retrieve (default is 0).
    """
    try:
        response = source_table.get_item(Key={"key": key, "index": index})

        if "Item" in response:
            item = response["Item"]
            pickle.loads(item["val"].value)  # TODO: use in conversion to JSON
            print(f"Key: {key:<120} - Pickle successfully loaded")
        else:
            print(f"Key: {key:<120} - Item not found")

    except Exception as e:
        logger.error(f"Key: {key} - Failed to load pickle: {e}")


def load_and_print_all_pickles(source_table: ServiceResource) -> None:
    """
    Scan the entire DynamoDB table, attempt to load the pickled data for each item,
    and log a success message for each key.

    :param source_table: The DynamoDB table resource.
    """
    response = source_table.scan()
    items = response.get("Items", [])

    for item in items:
        key = item.get("key", "Unknown Key")
        index = int(item.get("index", 0))

        try:
            pickle.loads(item["val"].value)  # TODO: use data in conversion to JSON
            print(f"Key: {key:<120} Index: {index:<10} - Pickle successfully loaded")
        except Exception as e:
            logger.error(f"Key: {key}, Index: {index} - Failed to load pickle: {e}")


def convert_pickle_to_json(source_table: ServiceResource, key: str, index: int = 0) -> None:
    """
    Convert a single pickled item to JSON and update the DynamoDB entry.

    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :param index: The index of the partition to retrieve (default is 0).
    """
    try:
        response = source_table.get_item(Key={"key": key, "index": index})

        if "Item" in response:
            item = response["Item"]
            pickled_data = pickle.loads(item["val"].value)

            state_type = key.split()[0]
            if state_type == runstate.JOB_STATE:
                json_data = Job.to_json(pickled_data)
            elif state_type == runstate.JOB_RUN_STATE:
                json_data = JobRun.to_json(pickled_data)
            else:
                raise ValueError(f"Unknown type: {state_type}")

            num_json_partitions = math.ceil(len(json_data) / OBJECT_SIZE)
            for partition_index in range(num_json_partitions):
                json_partition = json_data[
                    partition_index * OBJECT_SIZE : min((partition_index + 1) * OBJECT_SIZE, len(json_data))
                ]

                source_table.update_item(
                    Key={"key": key, "index": partition_index},
                    UpdateExpression="SET json_val = :json, num_json_val_partitions = :num_partitions",
                    ExpressionAttributeValues={":json": json_partition, ":num_partitions": num_json_partitions},
                )

            logger.info(f"Key: {key} - Pickle converted to JSON and updated")
        else:
            logger.warning(f"Key: {key} - Item not found")

    except Exception as e:
        logger.error(f"Key: {key} - Failed to convert pickle to JSON: {e}")


def convert_all_pickles_to_json(source_table: ServiceResource) -> None:
    """
    Convert all pickled items in the DynamoDB table to JSON and update the entries.

    :param source_table: The DynamoDB table resource.
    """
    response = source_table.scan()
    items = response.get("Items", [])

    for item in items:
        key = item.get("key", "Unknown Key")
        index = int(item.get("index", 0))

        convert_pickle_to_json(source_table, key, index)


aws_profile = "dev"
table_name = "infrastage-tron-state"
table_region = "us-west-1"
source_table = get_dynamodb_table(aws_profile, table_name, table_region)

# # 1. Scan table
# summarize_table(source_table)

# # 2. Load and print a single job_state pickle
# load_and_print_pickle(source_table, "job_state compute-infra-test-service.test_load_foo1")

# # 2.1 Load and print a single job_run_state pickle
# load_and_print_pickle(source_table, "job_run_state katamari_test_service.test_load_foo2.255")

# 3. Load and print all pickles
load_and_print_all_pickles(source_table)

# # 4. Convert a single pickle to JSON
# convert_pickle_to_json(source_table, "job_state compute-infra-test-service.test_load_foo1")

# # 5. Convert all pickles to JSON
# convert_all_pickles_to_json(source_table)
