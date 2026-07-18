"""
This is a tool that reads individual Tron DynamoDB items for debugging purposes

It already handles multi-partition reassembly, gzip decompression, and deserialization so you don't have to do it ad-hoc during incidents.

Example:

Checking a job's state:

AWS_PROFILE=devc python3 tools/read_dynamodb_item.py --table infrastage-tron-state --region us-west-1 --type job_state --name compute-i
nfra-test-service.test_partitions

Example_Output:


{
  "run_nums": [
    11,
    10,
    9,
    8,
    7,
    6,
    5,
    3,
    1
  ],
  "enabled": true
}

You can then check its full state by running:

AWS_PROFILE=devc python3 tools/read_dynamodb_item.py --table infrastage-tron-state --region us-west-1 --type job_run_state --name compute-infra-test-service.test_partitions.11

Add --raw to get the raw JSON without deserialization and --metadata to get the partition metadata without the full payload.

"""
import argparse
import gzip
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config


VALID_TYPES = ("job_state", "job_run_state")


def build_key(state_type: str, name: str) -> str:
    return f"{state_type} {name}"


def get_client(table_name: str, region: str):
    retry_config = Config(retries={"max_attempts": 5, "mode": "standard"})
    client = boto3.client("dynamodb", region_name=region, config=retry_config)
    return client, table_name


def fetch_all_partitions(client, table_name: str, key: str) -> list[dict]:
    """Fetch partition 0, then the rest of the partitions based on num_json_val_partitions."""
    response = client.get_item(
        TableName=table_name,
        Key={"key": {"S": key}, "index": {"N": "0"}},
        ConsistentRead=True,
    )
    first = response.get("Item")
    if not first:
        return []

    num_partitions = int(first["num_json_val_partitions"]["N"])
    all_items = [first]

    if num_partitions > 1:
        remaining_keys = [{"key": {"S": key}, "index": {"N": str(i)}} for i in range(1, num_partitions)]

        # Chunk into batches of 100 (DynamoDB BatchGetItem limit)
        for chunk_start in range(0, len(remaining_keys), 100):
            chunk = remaining_keys[chunk_start : chunk_start + 100]
            unprocessed = chunk
            for attempt in range(10):
                resp = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed, "ConsistentRead": True}})
                all_items.extend(resp.get("Responses", {}).get(table_name, []))
                unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
                if not unprocessed:
                    break
                time.sleep(min(2**attempt, 10))
            else:
                print(f"Warning: {len(unprocessed)} keys still unprocessed after retries", file=sys.stderr)

    all_items.sort(key=lambda x: int(x["index"]["N"]))
    return all_items


def reassemble_json(partitions: list[dict]) -> str:
    """Concatenate json_val from all partitions and decompress."""
    compressed = bytearray()
    for part in partitions:
        if "json_val" in part and "B" in part["json_val"]:
            compressed += part["json_val"]["B"]
    if not compressed:
        raise ValueError("No json_val data found in partitions")
    return gzip.decompress(compressed).decode("utf-8")


def print_metadata(partitions: list[dict], key: str) -> None:
    """Print partition metadata without the full payload."""
    if not partitions:
        print(f"No item found for key: {key}", file=sys.stderr)
        sys.exit(1)

    first = partitions[0]
    num_partitions = int(first["num_json_val_partitions"]["N"])
    total_compressed_bytes = sum(
        len(p["json_val"]["B"]) for p in partitions if "json_val" in p and "B" in p["json_val"]
    )
    print(
        json.dumps(
            {
                "key": key,
                "num_json_val_partitions": num_partitions,
                "total_compressed_bytes": total_compressed_bytes,
                "partitions_fetched": len(partitions),
            },
            indent=2,
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Read individual Tron DynamoDB items for debugging.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--table", required=True, help="DynamoDB table name")
    parser.add_argument("--region", required=True, help="AWS region (e.g. us-west-2)")
    parser.add_argument(
        "--type",
        required=True,
        choices=VALID_TYPES,
        help="State type: job_state or job_run_state",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Identifier (e.g. MASTER.my_job for job_state, MASTER.my_job.42 for job_run_state)",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Dump decompressed JSON without deserializing through Tron's from_json",
    )
    parser.add_argument(
        "--metadata",
        action="store_true",
        help="Show only partition metadata (count, sizes) without the payload",
    )
    args = parser.parse_args()

    key = build_key(args.type, args.name)
    client, table_name = get_client(args.table, args.region)

    partitions = fetch_all_partitions(client, table_name, key)
    if not partitions:
        print(f"No item found for key: {key}", file=sys.stderr)
        sys.exit(1)

    if args.metadata:
        print_metadata(partitions, key)
        return

    raw_json = reassemble_json(partitions)

    if args.raw:
        parsed = json.loads(raw_json)
        print(json.dumps(parsed, indent=2, default=str))
        return

    # Deserialize through Tron's from_json for a structured view
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from tron.core.job import Job
        from tron.core.jobrun import JobRun

        if args.type == "job_state":
            deserialized = Job.from_json(raw_json)
        elif args.type == "job_run_state":
            deserialized = JobRun.from_json(raw_json)
        else:
            deserialized = json.loads(raw_json)

        print(json.dumps(deserialized, indent=2, default=str))
    except Exception as e:
        print(f"Deserialization failed ({e}), falling back to raw JSON:", file=sys.stderr)
        parsed = json.loads(raw_json)
        print(json.dumps(parsed, indent=2, default=str))


if __name__ == "__main__":
    main()
