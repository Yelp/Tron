import argparse
import gzip
import math
import os
import sys

import boto3
from boto3.resources.base import ServiceResource

from tron.core.job import Job
from tron.core.jobrun import JobRun
from tron.serialize import runstate

# Max DynamoDB object size is 400KB. Since we save two copies of the object (pickled and JSON),
# we need to consider this max size applies to the entire item, so we use a max size of 200KB
# for each version.
OBJECT_SIZE = 150_000


def get_dynamodb_table(
    aws_profile: str | None = None, table: str = "infrastage-tron-state", region: str = "us-west-1"
) -> ServiceResource:
    session = boto3.Session(profile_name=aws_profile) if aws_profile else boto3.Session()
    return session.resource("dynamodb", region_name=region).Table(table)


def scan_table(source_table: ServiceResource) -> list[dict]:
    items = []
    response = source_table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = source_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return items


def get_all_keys(source_table: ServiceResource) -> list[str]:
    items = scan_table(source_table)
    unique_keys = {item.get("key", "Unknown Key") for item in items}
    return list(unique_keys)


def resolve_keys(args, parser, source_table: ServiceResource) -> list[str]:
    if not args.keys and not args.keys_file and not args.all:
        parser.error("You must provide either --keys, --keys-file, or --all.")

    if args.all:
        print("Scanning table for all keys...")
        keys = get_all_keys(source_table)
        print(f"Found {len(keys)} unique keys.")
        return keys

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


def compress_json_for_key(source_table: ServiceResource, key: str, dry_run: bool = True) -> str:
    """Compress uncompressed JSON for a single key.

    Reads all json_val partitions, gzip-compresses the combined JSON, and writes the compressed
    data back. If compression reduces the number of partitions, the excess partitions have their
    json_val attribute removed.

    Returns a status string: "compressed", "already_compressed", "no_json", "skipped", or raises on error.
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

    # It's an uncompressed string — collect all partitions
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

    # Validate JSON round-trips
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
        # Write compressed data
        for i in range(num_compressed_partitions):
            chunk = compressed[i * OBJECT_SIZE : (i + 1) * OBJECT_SIZE]
            source_table.update_item(
                Key={"key": key, "index": i},
                UpdateExpression="SET json_val = :json, num_json_val_partitions = :n",
                ExpressionAttributeValues={":json": chunk, ":n": num_compressed_partitions},
            )
        # Clean up excess partitions if compressed has fewer than uncompressed
        for i in range(num_compressed_partitions, num_json_partitions):
            source_table.update_item(
                Key={"key": key, "index": i},
                UpdateExpression="REMOVE json_val",
            )
        print(
            f"  COMPRESSED: {key} "
            f"({original_size:,} bytes -> {compressed_size:,} bytes, {ratio:.1f}% reduction, "
            f"{num_json_partitions} partitions -> {num_compressed_partitions} partitions)"
        )

    return "compressed"


def delete_pickle_for_key(source_table: ServiceResource, key: str, dry_run: bool = True) -> str:
    """Delete pickle data (val, num_partitions) for a single key.

    Returns a status string: "deleted", "refused", "no_pickle", "skipped", or raises on error.
    """
    response = source_table.get_item(Key={"key": key, "index": 0}, ConsistentRead=True)
    if "Item" not in response:
        print(f"  SKIP (no item found): {key}")
        return "skipped"

    item_0 = response["Item"]

    # Some extra safety checks to avoid deleting pickles if we don't have valid compressed JSON
    if "json_val" not in item_0:
        print(f"  REFUSE (no json_val at all): {key}")
        return "refused"

    if not is_compressed(item_0["json_val"]):
        print(f"  REFUSE (json_val is uncompressed — run 'compress' first): {key}")
        return "refused"

    if "val" not in item_0:
        print(f"  SKIP (no pickle data to delete): {key}")
        return "no_pickle"

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


def cmd_compress(args, source_table: ServiceResource, keys: list[str]) -> None:
    dry_run = not args.execute
    total = len(keys)
    counts = {"compressed": 0, "already_compressed": 0, "no_json": 0, "skipped": 0, "failed": 0}
    failed_keys = []

    mode = "DRY RUN" if dry_run else "EXECUTING"
    print(f"\n=== Compress JSON ({mode}) ===")
    print(f"Processing {total} keys...\n")

    for i, key in enumerate(sorted(keys), 1):
        print(f"[{i}/{total}] {key}")
        try:
            result = compress_json_for_key(source_table, key, dry_run=dry_run)
            counts[result] += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            counts["failed"] += 1
            failed_keys.append(key)

    print("\n=== Summary ===")
    print(f"Total keys:           {total}")
    print(f"Compressed:           {counts['compressed']}")
    print(f"Already compressed:   {counts['already_compressed']}")
    print(f"No JSON (pickle-only):{counts['no_json']}")
    print(f"Skipped:              {counts['skipped']}")
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
    print("Scanning table...")

    items = scan_table(source_table)

    # Group items by key and find partition 0 for each
    keys_items: dict[str, dict] = {}
    for item in items:
        key = item.get("key", "Unknown Key")
        index = int(item.get("index", 0))
        if index == 0:
            keys_items[key] = item

    total = len(keys_items)
    counts = {"compressed": 0, "uncompressed": 0, "pickle_only": 0, "no_data": 0}

    for key, item in sorted(keys_items.items()):
        classification = classify_item(item)
        counts[classification] += 1

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
  compress          Compress uncompressed JSON ("S" type) to gzip-compressed binary ("B" type).
  delete-pickles    Remove pickle data (val, num_partitions) from items that have compressed JSON.
  status            Report the state of all keys in the table.

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

    if args.action == "status":
        cmd_status(args, source_table)
    elif args.action == "compress":
        keys = resolve_keys(args, parser, source_table)
        cmd_compress(args, source_table, keys)
    elif args.action == "delete-pickles":
        keys = resolve_keys(args, parser, source_table)
        cmd_delete_pickles(args, source_table, keys)


if __name__ == "__main__":
    main()
