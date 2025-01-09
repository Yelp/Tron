import argparse
import math
import os
import pickle
from typing import List
from typing import Optional

import boto3
import requests
from boto3.resources.base import ServiceResource

from tron.core.job import Job
from tron.core.jobrun import JobRun
from tron.serialize import runstate

# Max DynamoDB object size is 400KB. Since we save two copies of the object (pickled and JSON),
# we need to consider this max size applies to the entire item, so we use a max size of 200KB
# for each version.
OBJECT_SIZE = 200_000


def get_dynamodb_table(
    aws_profile: Optional[str] = None, table: str = "infrastage-tron-state", region: str = "us-west-1"
) -> ServiceResource:
    """
    Get the DynamoDB table resource.
    :param aws_profile: The name of the AWS profile to use.
    :param table: The name of the table to get.
    :param region: The region of the table.
    :return: The DynamoDB table resource.
    """
    session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
    return session.resource("dynamodb", region_name=region).Table(table)


def get_all_jobs(source_table: ServiceResource) -> List[str]:
    """
    Scan the DynamoDB table and return a list of unique job keys.
    :param source_table: The DynamoDB table resource to scan.
    :return: A list of all job keys.
    """
    items = scan_table(source_table)
    unique_keys = {item.get("key", "Unknown Key") for item in items}
    return list(unique_keys)


def get_job_names(base_url: str) -> List[str]:
    """
    Get the list of job names from the Tron API.
    :param base_url: The base URL of the Tron API.
    :return: A list of job names.
    """
    try:
        full_url = f"http://{base_url}.yelpcorp.com:8089/api/jobs?include_job_runs=0"
        response = requests.get(full_url)
        response.raise_for_status()
        data = response.json()
        job_names = [job["name"] for job in data.get("jobs", [])]
        return job_names
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return []


def combine_pickle_partitions(source_table: ServiceResource, key: str) -> bytes:
    """
    Load and combine all partitions of a pickled item from DynamoDB.
    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :return: The combined pickled data as bytes.
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        raise Exception(f"No item found for key {key} at index 0")
    item = response["Item"]
    num_partitions = int(item.get("num_partitions", 1))
    combined_data = bytearray()
    for index in range(num_partitions):
        response = source_table.get_item(Key={"key": key, "index": index}, ConsistentRead=True)
        if "Item" not in response:
            raise Exception(f"Missing partition {index} for key {key}")
        item = response["Item"]
        combined_data.extend(item["val"].value)
    return bytes(combined_data)


def dump_pickle_key(source_table: ServiceResource, key: str) -> None:
    """
    Load the pickled data from DynamoDB for a given key, handling partitioned
    items, and print the full pickle data.
    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    """
    try:
        pickled_data = combine_pickle_partitions(source_table, key)
        loaded_pickle = pickle.loads(pickled_data)
        print(f"Key: {key} - Pickle data:")
        print(loaded_pickle)
    except Exception as e:
        print(f"Key: {key} - Failed to load pickle: {e}")
        raise


def dump_pickle_keys(source_table: ServiceResource, keys: List[str]) -> None:
    """
    Load and print pickles for the given list of keys.
    :param source_table: The DynamoDB table resource.
    :param keys: A list of keys for which to load and print pickles.
    """
    for key in keys:
        dump_pickle_key(source_table, key)


def dump_json_key(source_table: ServiceResource, key: str) -> None:
    """
    Load the JSON data from DynamoDB for a given key and print it.
    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    """
    try:
        json_data = combine_json_partitions(source_table, key)
        if json_data is not None:
            print(f"Key: {key} - JSON data:")
            print(json_data)
        else:
            print(f"Key: {key} - No JSON value found")
    except Exception as e:
        print(f"Key: {key} - Failed to load JSON: {e}")


def dump_json_keys(source_table: ServiceResource, keys: List[str]) -> None:
    """
    Load and print JSON data for the given list of keys.
    :param source_table: The DynamoDB table resource.
    :param keys: A list of keys for which to load and print JSON data.
    """
    for key in keys:
        dump_json_key(source_table, key)


# TODO: clean up old run history for valid jobs? something something look at job_state, then whitelist those runs instead of whitelisting entire jobs
def delete_keys(source_table: ServiceResource, keys: List[str]) -> None:
    """
    Delete items with the given list of keys from the DynamoDB table.
    :param source_table: The DynamoDB table resource.
    :param keys: A list of keys to delete.
    """
    total_keys = len(keys)
    deleted_count = 0
    failure_count = 0
    for key in keys:
        try:
            num_partitions = get_num_partitions(source_table, key)
            for index in range(num_partitions):
                source_table.delete_item(Key={"key": key, "index": index})
            print(f"Key: {key} - Successfully deleted")
            deleted_count += 1
        except Exception as e:
            print(f"Key: {key} - Failed to delete: {e}")
            failure_count += 1
    print(f"Total keys: {total_keys}")
    print(f"Successfully deleted: {deleted_count}")
    print(f"Failures: {failure_count}")


def get_num_partitions(source_table: ServiceResource, key: str) -> int:
    """
    Get the number of partitions for a given key in the DynamoDB table.
    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :return: The number of partitions for the key.
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        return 0
    item = response["Item"]
    num_partitions = int(item.get("num_partitions", 1))
    num_json_val_partitions = int(item.get("num_json_val_partitions", 0))
    return max(num_partitions, num_json_val_partitions)


def combine_json_partitions(source_table: ServiceResource, key: str) -> Optional[str]:
    """
    Combine all partitions of a JSON item from DynamoDB.
    :param source_table: The DynamoDB table resource.
    :param key: The primary key of the item to retrieve.
    :return: The combined JSON data as a string, or None if not found.
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        return None
    item = response["Item"]
    num_json_partitions = int(item.get("num_json_val_partitions", 0))
    if num_json_partitions == 0:
        return None
    combined_json = ""
    for index in range(num_json_partitions):
        response = source_table.get_item(Key={"key": key, "index": index}, ConsistentRead=True)
        if "Item" not in response:
            raise Exception(f"Missing JSON partition {index} for key {key}")
        item = response["Item"]
        if "json_val" in item:
            combined_json += item["json_val"]
        else:
            raise Exception(f"No 'json_val' in partition {index} for key {key}")
    return combined_json


def convert_pickle_to_json_and_update_table(source_table: ServiceResource, key: str, dry_run: bool = True) -> bool:
    """
    Convert a single pickled item to JSON and update the DynamoDB entry.
    Returns True if the conversion was successful, False if skipped.
    Raises an exception if conversion fails.
    """
    try:
        # Skip conversion for job_state MASTER and job_run_state MASTER jobs that are from infrastage testing (i.e., not real jobs)
        if key.startswith("job_state MASTER") or key.startswith("job_run_state MASTER"):
            print(f"Skipping conversion for key: {key}")
            return False
        pickled_data = combine_pickle_partitions(source_table, key)
        state_data = pickle.loads(pickled_data)
        state_type = key.split()[0]
        if state_type == runstate.JOB_STATE:
            json_data = Job.to_json(state_data)
        elif state_type == runstate.JOB_RUN_STATE:
            json_data = JobRun.to_json(state_data)
        else:
            # This will skip the state metadata and any other non-standard keys we have in the table
            print(f"Key: {key} - Unknown state type: {state_type}. Skipping.")
            return False
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
                        ":num_partitions": num_json_partitions,
                    },
                )
        if dry_run:
            print(f"DRY RUN: Key: {key} - Pickle would have been converted to JSON and updated")
        else:
            print(f"Key: {key} - Pickle converted to JSON and updated")
        return True
    except Exception as e:
        print(f"Key: {key} - Failed to convert pickle to JSON: {e}")
        raise


def convert_pickles_to_json_and_update_table(
    source_table: ServiceResource,
    keys: List[str],
    dry_run: bool = True,
    keys_file: Optional[str] = None,
    job_names: List[str] = [],
) -> None:
    """
    Convert pickled items in the DynamoDB table to JSON and update the entries.
    :param source_table: The DynamoDB table resource.
    :param keys: List of keys to convert.
    :param dry_run: If True, simulate the conversion without updating the table.
    :param keys_file: File to write failed keys to in dry run.
    """
    total_keys = len(keys)
    converted_keys = 0
    skipped_keys = 0
    failed_keys = []
    delete_keys = []

    for key in keys:
        # Extract the job name from the key
        parts = key.split()
        if len(parts) < 2:
            continue

        state_type, job_info = parts[0], parts[1]

        # Ignore run_num for job_run_state keys
        if state_type == "job_run_state":
            job_name = ".".join(job_info.split(".")[:-1])
        else:
            job_name = job_info

        if job_name not in job_names:
            delete_keys.append(key)
            continue

        try:
            result = convert_pickle_to_json_and_update_table(source_table, key, dry_run)
            if result:
                converted_keys += 1
            else:
                skipped_keys += 1
        except Exception as e:
            print(f"Key: {key} - Failed to convert pickle to JSON: {e}")
            failed_keys.append(key)

    print(f"Total keys processed: {total_keys}")
    print(f"Conversions attempted: {total_keys - skipped_keys}")
    print(f"Conversions succeeded: {converted_keys}")
    print(f"Conversions skipped: {skipped_keys}")
    print(f"Conversions failed: {len(failed_keys)}")
    print(f"Keys to be deleted: {len(delete_keys)}")

    if keys_file:
        with open(keys_file, "w") as f:
            for key in failed_keys + delete_keys:  # TODO: failed keys to separate file?
                f.write(f"{key}\n")
        print(f"Failed and delete keys have been written to {keys_file}")
    if dry_run:
        print("Dry run complete. No changes were made to the DynamoDB table.")


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


def main():
    parser = argparse.ArgumentParser(
        description="Utilities for working with pickles and JSON items in Tron's DynamoDB state store.",
        epilog="""
Actions:
  convert           Convert pickled state data to JSON format and update the DynamoDB table.
  dump-pickle       Load and print the pickles for specified keys.
  dump-json         Load and print JSON data for specified keys.
  delete-keys       Delete the specified keys from the DynamoDB table.

Examples:
  Validate pickles (dry run, write failed keys to keys.txt):
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action convert --all --dry-run --keys-file keys.txt --tron-api-url tron-infrastage
  Convert all pickles to JSON (dry run):
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action convert --all --dry-run --tron-api-url tron-infrastage
  Convert specific pickles to JSON using keys from a file:
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action convert --keys-file keys.txt --tron-api-url tron-infrastage
  Convert specific pickles to JSON:
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action convert --keys "key1" "key2" --tron-api-url tron-infrastage
  Load and print specific JSON keys using keys from a file:
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action dump-json --keys-file keys.txt
  Delete specific keys:
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action delete-keys --keys "key1" "key2"
  Delete keys from a file:
    pickles_to_json.py --table-name infrastage-tron-state --table-region us-west-1 --action delete-keys --keys-file keys.txt
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--aws-profile",
        default=os.environ.get("AWS_PROFILE", None),
        help="AWS profile to use (default: taken from AWS_PROFILE environment variable)",
    )
    parser.add_argument("--table-name", required=True, help="Name of the DynamoDB table")
    parser.add_argument("--table-region", required=True, help="AWS region of the DynamoDB table")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the action without making any changes to the DynamoDB table",
    )
    parser.add_argument(
        "--action",
        choices=["convert", "dump-pickle", "dump-json", "delete-keys"],
        required=True,
        help="Action to perform",
    )
    parser.add_argument(
        "--keys",
        nargs="+",
        required=False,
        help="Specific key(s) to perform the action on.",
    )
    parser.add_argument(
        "--keys-file",
        required=False,
        help="File containing keys to perform the action on. One key per line. On dry run, failed keys will be written to this file.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Apply the action to all keys in the table.",
    )
    parser.add_argument(
        "--tron-api-url",
        required=True,
        help="URL of the Tron API to fetch job names from.",
    )
    args = parser.parse_args()
    source_table = get_dynamodb_table(args.aws_profile, args.table_name, args.table_region)
    if not args.keys and not args.keys_file and not args.all:
        parser.error("You must provide either --keys, --keys-file, or --all.")
    if args.all:
        print("Processing all keys in the table...")
        keys = get_all_jobs(source_table)
    else:
        keys = []
        if args.keys:
            keys.extend(args.keys)
        if args.keys_file:
            try:
                with open(args.keys_file) as f:
                    keys_from_file = [line.strip() for line in f if line.strip()]
                    keys.extend(keys_from_file)
            except Exception as e:
                parser.error(f"Error reading keys from file {args.keys_file}: {e}")
        if not keys:
            parser.error("No keys provided. Please provide keys via --keys or --keys-file.")
        keys = list(set(keys))

    # Get job names from the Tron API using the provided URL
    job_names = get_job_names(args.tron_api_url)

    if args.action == "convert":
        convert_pickles_to_json_and_update_table(
            source_table, keys=keys, dry_run=args.dry_run, keys_file=args.keys_file, job_names=job_names
        )
    elif args.action == "dump-pickle":
        dump_pickle_keys(source_table, keys)
    elif args.action == "dump-json":
        dump_json_keys(source_table, keys)
    elif args.action == "delete-keys":
        confirm = (
            input(f"Are you sure you want to delete {len(keys)} keys from the table '{args.table_name}'? [y/N]: ")
            .strip()
            .lower()
        )
        if confirm in ("y", "yes"):
            delete_keys(source_table, keys)
        else:
            print("Deletion cancelled.")
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
