import argparse
import gzip
import math
import os
import sys
import threading
import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

import boto3
from boto3.resources.base import ServiceResource

from tron.core.job import Job
from tron.core.jobrun import JobRun
from tron.serialize import runstate

# Max DynamoDB object size is 400KB. Since we save two copies of the object (pickled and JSON),
# we need to consider this max size applies to the entire item, so we use a max size of 200KB
# for each version.
OBJECT_SIZE = 150_000

# DynamoDB TransactWriteItems supports up to 100 items per call. We use 25
# which comfortably fits under the 4MB request size limit (25 × 150KB = 3.75MB
# worst case). This is sufficient to handle our largest items (~20 partitions
# uncompressed) in a single atomic transaction after compression.
MAX_TRANSACT_WRITE_ITEMS = 25


def get_dynamodb_table(
    aws_profile: str | None = None, table: str = "infrastage-tron-state", region: str = "us-west-1"
) -> ServiceResource:
    session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
    return session.resource("dynamodb", region_name=region).Table(table)


def get_dynamodb_client(
    aws_profile: str | None = None,
    region: str = "us-west-1",
):
    session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
    return session.client("dynamodb", region_name=region)


def scan_keys(source_table: ServiceResource) -> set[str]:
    """Streaming scan that only projects the key attribute and collects unique partition keys.

    Never stores full items in memory — only the 'key' strings.
    """
    unique_keys: set[str] = set()
    scan_kwargs = {
        "ProjectionExpression": "#k",
        "ExpressionAttributeNames": {"#k": "key"},
    }
    response = source_table.scan(**scan_kwargs)
    for item in response.get("Items", []):
        unique_keys.add(item.get("key", "Unknown Key"))
    while "LastEvaluatedKey" in response:
        response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], **scan_kwargs)
        for item in response.get("Items", []):
            unique_keys.add(item.get("key", "Unknown Key"))
    return unique_keys


def resolve_keys(args, parser, source_table: ServiceResource) -> list[str]:
    if not args.keys and not args.keys_file and not args.all:
        parser.error("You must provide either --keys, --keys-file, or --all.")

    if args.all:
        print("Scanning table for all keys (keys-only projection)...")
        keys_set = scan_keys(source_table)
        print(f"Found {len(keys_set)} unique keys.")
        return list(keys_set)

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
    return list(set(keys))


def is_compressed(json_val) -> bool:
    """Check if a json_val from DynamoDB is compressed (Binary type) vs uncompressed (String type).

    boto3 high-level resource API returns:
    - "S" type as Python str
    - "B" type as boto3.dynamodb.types.Binary (which wraps bytes and has a .value attribute)
    """
    if hasattr(json_val, "value"):
        # boto3.dynamodb.types.Binary
        return True
    if isinstance(json_val, bytes):
        # I don't think this is possible in the high-level API, but it's harmless to check
        return True
    return False


def get_json_val_bytes(json_val) -> bytes:
    """Extract raw bytes from a json_val, whether it's a Binary wrapper or raw bytes."""
    if hasattr(json_val, "value"):
        return bytes(json_val.value)
    if isinstance(json_val, bytes):
        return bytes(json_val)
    raise TypeError(f"Unexpected json_val type: {type(json_val)}")


def classify_item(item: dict) -> str:
    """Classify a partition-0 item into one of: compressed, uncompressed, pickle_only, no_data."""
    has_json = "json_val" in item
    has_pickle = "val" in item

    if has_json:
        if is_compressed(item["json_val"]):
            return "compressed"
        else:
            return "uncompressed"
    elif has_pickle:
        # Shouldn't exist. If we have items with only pickle data we want to know
        return "pickle_only"
    else:
        # Shouldn't exist. If we have funky items with no json_val/val we want to know
        return "no_data"


def compress_json_for_key(
    source_table: ServiceResource, client, table_name: str, key: str, dry_run: bool = True
) -> str:
    """Compress uncompressed JSON for a single key.

    Reads all json_val partitions via get_item (ConsistentRead), gzip-compresses the combined JSON,
    and writes the compressed data back using TransactWriteItems with ConditionExpressions to guard
    against concurrent trond writes.

    Returns a status string: "compressed", "already_compressed", "no_json", "skipped",
    "concurrent_update", or raises on error (including throttle exhaustion).
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        print(f"  SKIP (no item found): {key}")
        return "skipped"

    item_0 = response["Item"]

    if "json_val" not in item_0:
        print(f"  SKIP (no json_val — needs pickles_to_json.py first): {key}")
        return "no_json"

    json_val = item_0["json_val"]
    if is_compressed(json_val):
        print(f"  SKIP (already compressed): {key}")
        return "already_compressed"

    # It's an uncompressed string — collect all partitions via get_item
    num_json_partitions = int(item_0.get("num_json_val_partitions", 1))
    combined_json = ""
    for index in range(num_json_partitions):
        if index == 0:
            partition_item = item_0
        else:
            resp = source_table.get_item(Key={"key": key, "index": index}, ConsistentRead=True)
            if "Item" not in resp:
                raise Exception(f"Missing JSON partition {index} for key {key}")
            partition_item = resp["Item"]

        if "json_val" not in partition_item:
            raise Exception(f"No 'json_val' in partition {index} for key {key}")
        combined_json += partition_item["json_val"]

    # Validate JSON round-trips before compressing
    state_type = key.split()[0]
    if state_type == runstate.JOB_STATE:
        Job.from_json(combined_json)
    elif state_type == runstate.JOB_RUN_STATE:
        JobRun.from_json(combined_json)
    else:
        print(f"  SKIP (unknown state type '{state_type}'): {key}")
        return "skipped"

    # Compress
    compressed = gzip.compress(combined_json.encode("utf-8"))
    num_compressed_partitions = math.ceil(len(compressed) / OBJECT_SIZE)

    original_size = len(combined_json.encode("utf-8"))
    compressed_size = len(compressed)
    ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

    if dry_run:
        print(
            f"  DRY RUN (would compress): {key} "
            f"({original_size:,} bytes -> {compressed_size:,} bytes, {ratio:.1f}% reduction, "
            f"{num_json_partitions} partitions -> {num_compressed_partitions} partitions)"
        )
    else:
        # Build TransactWriteItems with conditional expressions.
        # Conditions on partition 0 ensure the item is still uncompressed and has the same
        # number of partitions we read — if trond wrote concurrently the condition fails.
        transact_items = []
        for i in range(num_compressed_partitions):
            chunk = compressed[i * OBJECT_SIZE : (i + 1) * OBJECT_SIZE]
            update = {
                "Update": {
                    "TableName": table_name,
                    "Key": {
                        "key": {"S": key},
                        "index": {"N": str(i)},
                    },
                    "UpdateExpression": "SET json_val = :json, num_json_val_partitions = :n",
                    "ExpressionAttributeValues": {
                        ":json": {"B": chunk},
                        ":n": {"N": str(num_compressed_partitions)},
                    },
                },
            }
            if i == 0:
                # Condition on partition 0: item must still be uncompressed with expected partition count
                update["Update"]["ConditionExpression"] = (
                    "attribute_exists(json_val) "
                    "AND attribute_type(json_val, :string_type) "
                    "AND num_json_val_partitions = :expected_partitions"
                )
                update["Update"]["ExpressionAttributeValues"][":string_type"] = {"S": "S"}
                update["Update"]["ExpressionAttributeValues"][":expected_partitions"] = {"N": str(num_json_partitions)}
            transact_items.append(update)

        # Clean up excess partitions (REMOVE json_val where compressed needs fewer partitions)
        for i in range(num_compressed_partitions, num_json_partitions):
            transact_items.append(
                {
                    "Update": {
                        "TableName": table_name,
                        "Key": {
                            "key": {"S": key},
                            "index": {"N": str(i)},
                        },
                        "UpdateExpression": "REMOVE json_val",
                    },
                }
            )

        if len(transact_items) > MAX_TRANSACT_WRITE_ITEMS:
            raise Exception(
                f"Compression requires {len(transact_items)} transaction items for key {key}, "
                f"exceeding single-transaction limit of {MAX_TRANSACT_WRITE_ITEMS}. "
                f"({num_json_partitions} uncompressed partitions, "
                f"{num_compressed_partitions} compressed partitions)"
            )
        if compressed_size > 3_500_000:
            raise Exception(
                f"Compressed data too large ({compressed_size:,} bytes) for key {key} — "
                f"risks exceeding 4MB TransactWriteItems request limit."
            )

        # Single atomic transaction — all updates and removes in one call. We retry
        # on ThrottlingException with exponential backoff on top of boto3's built-in
        # retries. During testing we are getting throttled almost immediately (on hot
        # keys) and quickly exhausting the default retry budget. This slows things
        # down a lot but allows the process to complete without manual intervention
        # or re-runs.
        max_retries = 5
        for attempt in range(max_retries + 1):
            try:
                client.transact_write_items(TransactItems=transact_items)
                break
            except client.exceptions.TransactionCanceledException as e:
                reasons = e.response.get("CancellationReasons", [])
                if any(r.get("Code") in ("ConditionalCheckFailed", "TransactionConflict") for r in reasons):
                    print(f"  SKIPPED (concurrent update detected): {key}")
                    return "concurrent_update"
                # Check if cancellation was due to throttling
                if any(r.get("Code") == "ThrottlingError" for r in reasons):
                    if attempt < max_retries:
                        wait = 2**attempt
                        print(f"  THROTTLED (attempt {attempt + 1}/{max_retries + 1}, retrying in {wait}s): {key}")
                        time.sleep(wait)
                    continue
                raise
            except Exception as e:
                if "ThrottlingException" in str(e) or "Throughput exceeds" in str(e):
                    if attempt < max_retries:
                        wait = 2**attempt
                        print(f"  THROTTLED (attempt {attempt + 1}/{max_retries + 1}, retrying in {wait}s): {key}")
                        time.sleep(wait)
                    continue
                raise
        else:
            raise Exception(f"Throttled after {max_retries + 1} attempts")

        print(
            f"  COMPRESSED: {key} "
            f"({original_size:,} bytes -> {compressed_size:,} bytes, {ratio:.1f}% reduction, "
            f"{num_json_partitions} partitions -> {num_compressed_partitions} partitions)"
        )

    return "compressed"


def verify_compressed_json_for_key(source_table: ServiceResource, key: str) -> bool:
    """Re-read all compressed JSON partitions, gunzip, and validate via from_json.

    Returns True if verification succeeds, False otherwise. Prints the reason on failure.
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        print(f"  VERIFY FAIL (no item found): {key}")
        return False

    item_0 = response["Item"]

    if "json_val" not in item_0:
        print(f"  VERIFY FAIL (no json_val): {key}")
        return False

    if not is_compressed(item_0["json_val"]):
        print(f"  VERIFY FAIL (json_val is not compressed): {key}")
        return False

    num_json_partitions = int(item_0.get("num_json_val_partitions", 1))

    # Reassemble compressed bytes from all partitions
    compressed_data = bytearray()
    for index in range(num_json_partitions):
        if index == 0:
            partition_item = item_0
        else:
            resp = source_table.get_item(Key={"key": key, "index": index}, ConsistentRead=True)
            if "Item" not in resp:
                print(f"  VERIFY FAIL (missing partition {index}): {key}")
                return False
            partition_item = resp["Item"]

        if "json_val" not in partition_item:
            print(f"  VERIFY FAIL (no json_val in partition {index}): {key}")
            return False

        compressed_data += get_json_val_bytes(partition_item["json_val"])

    # Decompress
    try:
        json_str = gzip.decompress(bytes(compressed_data)).decode("utf-8")
    except Exception as e:
        print(f"  VERIFY FAIL (gunzip failed: {e}): {key}")
        return False

    # Validate via from_json
    state_type = key.split()[0]
    try:
        if state_type == runstate.JOB_STATE:
            Job.from_json(json_str)
        elif state_type == runstate.JOB_RUN_STATE:
            JobRun.from_json(json_str)
        else:
            print(f"  VERIFY FAIL (unknown state type '{state_type}'): {key}")
            return False
    except Exception as e:
        print(f"  VERIFY FAIL (from_json failed: {e}): {key}")
        return False

    return True


def delete_pickle_for_key(source_table: ServiceResource, key: str, dry_run: bool = True) -> str:
    """Delete pickle data (val, num_partitions) for a single key.

    Verifies compressed JSON can be fully decoded and parsed before deleting.

    Returns a status string: "deleted", "refused", "no_pickle", "skipped", or raises on error.
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        print(f"  SKIP (no item found): {key}")
        return "skipped"

    item_0 = response["Item"]

    # Safety checks
    if "json_val" not in item_0:
        print(f"  REFUSE (no json_val at all): {key}")
        return "refused"

    if not is_compressed(item_0["json_val"]):
        print(f"  REFUSE (json_val is uncompressed — run 'compress' first): {key}")
        return "refused"

    if "val" not in item_0:
        print(f"  SKIP (no pickle data to delete): {key}")
        return "no_pickle"

    # Verify compressed JSON is valid before deleting pickle
    if not verify_compressed_json_for_key(source_table, key):
        print(f"  REFUSE (compressed JSON verification failed): {key}")
        return "refused"

    num_partitions = int(item_0.get("num_partitions", 1))
    num_json_partitions = int(item_0.get("num_json_val_partitions", 1))
    max_partitions = max(num_partitions, num_json_partitions)

    if dry_run:
        print(
            f"  DRY RUN (would delete pickle): {key} ({num_partitions} pickle partitions across {max_partitions} items)"
        )
    else:
        for i in range(max_partitions):
            source_table.update_item(
                Key={"key": key, "index": i},
                UpdateExpression="REMOVE val, num_partitions",
            )
        print(f"  DELETED pickle: {key} ({num_partitions} pickle partitions removed across {max_partitions} items)")

    return "deleted"


def cmd_compress(args, source_table: ServiceResource, client, table_name: str, keys: list[str]) -> None:
    dry_run = not args.execute
    workers = args.workers
    total = len(keys)
    counts = {
        "compressed": 0,
        "already_compressed": 0,
        "no_json": 0,
        "skipped": 0,
        "concurrent_update": 0,
        "failed": 0,
    }
    failed_keys = []
    lock = threading.Lock()
    completed = [0]  # mutable counter for progress

    mode = "DRY RUN" if dry_run else "EXECUTING"
    print(f"\n=== Compress JSON ({mode}, {workers} workers) ===")
    print(f"Processing {total} keys...\n")

    # Pre-create one Table resource per worker thread. The high-level
    # resource is not thread-safe, so each worker gets its own, but we
    # create them once upfront instead of per-key.
    thread_tables = [get_dynamodb_table(args.aws_profile, args.table_name, args.table_region) for _ in range(workers)]
    # Map thread IDs to table resources as workers claim them.
    thread_local = threading.local()

    def get_thread_table() -> ServiceResource:
        if not hasattr(thread_local, "table"):
            with lock:
                thread_local.table = thread_tables.pop()
        return thread_local.table

    def process_key(key: str) -> None:
        thread_table = get_thread_table()
        try:
            result = compress_json_for_key(thread_table, client, table_name, key, dry_run=dry_run)
        except Exception as e:
            result = "failed"
            with lock:
                failed_keys.append(key)
            print(f"  FAILED ({key}): {e}")

        with lock:
            counts[result] += 1
            completed[0] += 1
            if completed[0] % 500 == 0 or completed[0] == total:
                print(f"  Progress: {completed[0]}/{total} keys processed")

    sorted_keys = sorted(keys)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(process_key, key): key for key in sorted_keys}
        for future in as_completed(futures):
            # Exceptions are already handled inside process_key, but catch
            # anything truly unexpected so one bad future doesn't kill the pool.
            try:
                future.result()
            except Exception as e:
                key = futures[future]
                print(f"  UNEXPECTED ERROR ({key}): {e}")
                with lock:
                    counts["failed"] += 1
                    failed_keys.append(key)

    print("\n=== Summary ===")
    print(f"Total keys:           {total}")
    print(f"Compressed:           {counts['compressed']}")
    print(f"Already compressed:   {counts['already_compressed']}")
    print(f"No JSON (pickle-only):{counts['no_json']}")
    print(f"Skipped:              {counts['skipped']}")
    print(f"Concurrent updates:   {counts['concurrent_update']}")
    print(f"Failed:               {counts['failed']}")

    if dry_run:
        print("\nDry run complete. No changes were made.")

    if args.failed_keys_output and failed_keys:
        with open(args.failed_keys_output, "w") as f:
            for key in failed_keys:
                f.write(f"{key}\n")
        print(f"Failed keys written to {args.failed_keys_output}")


def cmd_delete_pickles(args, source_table: ServiceResource, keys: list[str]) -> None:
    dry_run = not args.execute

    if args.execute and not args.i_hereby_declare_we_no_longer_need_pickles:
        print(
            "ERROR: --execute for delete-pickles requires the safety flag:\n"
            "  --i-hereby-declare-we-no-longer-need-pickles\n\n"
            "This operation is DESTRUCTIVE and IRREVERSIBLE. It removes all pickle data\n"
            "from DynamoDB items. Only proceed if you are certain that:\n"
            "  1. All items have valid compressed JSON (run 'status' to verify)\n"
            "  2. Tron is configured to read from JSON (read_json=True)\n"
            "  3. You have verified restores work from JSON on replica tables"
        )
        sys.exit(1)

    total = len(keys)
    counts = {"deleted": 0, "refused": 0, "no_pickle": 0, "skipped": 0, "failed": 0}
    failed_keys = []

    mode = "DRY RUN" if dry_run else "EXECUTING"
    print(f"\n=== Delete Pickles ({mode}) ===")
    print(f"Processing {total} keys...\n")

    for i, key in enumerate(sorted(keys), 1):
        print(f"[{i}/{total}] {key}")
        try:
            result = delete_pickle_for_key(source_table, key, dry_run=dry_run)
            counts[result] += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            counts["failed"] += 1
            failed_keys.append(key)

    print("\n=== Summary ===")
    print(f"Total keys:           {total}")
    print(f"Deleted:              {counts['deleted']}")
    print(f"Refused (no comp. JSON): {counts['refused']}")
    print(f"No pickle to delete:  {counts['no_pickle']}")
    print(f"Skipped:              {counts['skipped']}")
    print(f"Failed:               {counts['failed']}")

    if dry_run:
        print("\nDry run complete. No changes were made.")

    if args.failed_keys_output and failed_keys:
        with open(args.failed_keys_output, "w") as f:
            for key in failed_keys:
                f.write(f"{key}\n")
        print(f"Failed keys written to {args.failed_keys_output}")


def cmd_status(args, source_table: ServiceResource) -> None:
    print(f"\n=== Status: {args.table_name} ({args.table_region}) ===")
    print("Scanning partition-0 items...")

    counts = {"compressed": 0, "uncompressed": 0, "pickle_only": 0, "no_data": 0}
    total = 0

    # Streaming scan filtered to partition 0 only, projecting just the attributes
    # needed for classification. This avoids per-key get_item calls.
    scan_kwargs = {
        "FilterExpression": "#idx = :zero",
        "ProjectionExpression": "#k, #idx, json_val, val",
        "ExpressionAttributeNames": {"#k": "key", "#idx": "index"},
        "ExpressionAttributeValues": {":zero": 0},
    }
    response = source_table.scan(**scan_kwargs)
    for item in response.get("Items", []):
        total += 1
        counts[classify_item(item)] += 1
    while "LastEvaluatedKey" in response:
        response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"], **scan_kwargs)
        for item in response.get("Items", []):
            total += 1
            counts[classify_item(item)] += 1

    print(f"\nTotal unique keys: {total:,}\n")
    for label, count_key in [
        ("Compressed JSON (ready for pickle deletion)", "compressed"),
        ("Uncompressed JSON (needs compression)", "uncompressed"),
        ("Pickle only (anomalous, needs pickles_to_json.py)", "pickle_only"),
        ("No data (anomalous, needs investigation)", "no_data"),
    ]:
        count = counts[count_key]
        pct = (count / total * 100) if total > 0 else 0
        print(f"  {label + ':':<50} {count:>8,} ({pct:.1f}%)")


def add_key_arguments(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument(
        "--keys",
        nargs="+",
        required=False,
        help="Specific key(s) to process.",
    )
    subparser.add_argument(
        "--keys-file",
        required=False,
        help="Input file containing keys to process. One key per line.",
    )
    subparser.add_argument(
        "--all",
        action="store_true",
        help="Process all keys in the table.",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Compress JSON and delete pickle data in Tron's DynamoDB state store.",
        epilog="""
Sub-commands:
  compress              Compress uncompressed JSON ("S" type) to gzip-compressed binary ("B" type).
  delete-pickles        Remove pickle data (val, num_partitions) from items that have compressed JSON.
  status                Report the state of all keys in the table.

Examples:
  Check status of a table:
    compress_json.py --table-name infrastage-tron-state --table-region us-west-1 status

  Dry-run compression for all keys:
    compress_json.py --table-name infrastage-tron-state --table-region us-west-1 compress --all

  Execute compression for specific keys:
    compress_json.py --table-name infrastage-tron-state --table-region us-west-1 compress --keys "job_state myjob" --execute

  Dry-run pickle deletion:
    compress_json.py --table-name infrastage-tron-state --table-region us-west-1 delete-pickles --all

  Execute pickle deletion (requires safety flag):
    compress_json.py --table-name infrastage-tron-state --table-region us-west-1 delete-pickles --all --execute --i-hereby-declare-we-no-longer-need-pickles
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

    subparsers = parser.add_subparsers(dest="action", required=True, help="Action to perform")

    # compress sub-command
    compress_parser = subparsers.add_parser(
        "compress",
        help="Compress uncompressed JSON to gzip-compressed binary.",
    )
    add_key_arguments(compress_parser)
    compress_parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually perform the compression. Dry-run by default.",
    )
    compress_parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent workers (default: 8). Increase for faster throughput; decrease if throttling is excessive.",
    )
    compress_parser.add_argument(
        "--failed-keys-output",
        required=False,
        help="Output file to write keys that failed compression. One key per line.",
    )

    # delete-pickles sub-command
    delete_parser = subparsers.add_parser(
        "delete-pickles",
        help="Remove pickle data from items that have compressed JSON.",
    )
    add_key_arguments(delete_parser)
    delete_parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually perform the deletion. Dry-run by default.",
    )
    delete_parser.add_argument(
        "--i-hereby-declare-we-no-longer-need-pickles",
        action="store_true",
        default=False,
        help="Required safety flag when using --execute. Confirms you understand this is destructive and irreversible.",
    )
    delete_parser.add_argument(
        "--failed-keys-output",
        required=False,
        help="Output file to write keys that failed deletion. One key per line.",
    )

    # status sub-command
    subparsers.add_parser(
        "status",
        help="Report the state of all keys in the table.",
    )

    args = parser.parse_args()
    source_table = get_dynamodb_table(args.aws_profile, args.table_name, args.table_region)
    client = get_dynamodb_client(args.aws_profile, args.table_region)

    if args.action == "status":
        cmd_status(args, source_table)
    elif args.action == "compress":
        keys = resolve_keys(args, parser, source_table)
        cmd_compress(args, source_table, client, args.table_name, keys)
    elif args.action == "delete-pickles":
        keys = resolve_keys(args, parser, source_table)
        cmd_delete_pickles(args, source_table, keys)


if __name__ == "__main__":
    main()
