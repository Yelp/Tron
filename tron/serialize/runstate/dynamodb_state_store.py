import concurrent.futures
import copy
import logging
import math
import os
import threading
import time
from collections import defaultdict
from collections import OrderedDict
from typing import Any
from typing import cast
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Sequence
from typing import TypeVar

import boto3  # type: ignore
import botocore
from botocore.config import Config

import tron.prom_metrics as prom_metrics
from tron.core.job import Job
from tron.core.jobrun import JobRun
from tron.metrics import timer
from tron.serialize import runstate

# Max DynamoDB object size is 400KB. This limit gives us enough overhead to store other data like name and
# number of partitions.
OBJECT_SIZE = 400_000
MAX_SAVE_QUEUE = 500
# This is distinct from the number of retries in the retry_config as this is used for handling unprocessed
# keys outside the bounds of something like retrying on a ThrottlingException. We need this limit to avoid
# infinite loops in the case where a key is truly unprocessable. We allow for more retries than it should
# ever take to avoid failing restores due to transient issues.
MAX_UNPROCESSED_KEYS_RETRIES = 30
log = logging.getLogger(__name__)
T = TypeVar("T")


class DynamoDBStateStore:
    def __init__(self, name, dynamodb_region, stopping=False, max_transact_write_items=8) -> None:
        # Standard mode includes an exponential backoff by a base factor of 2 for a
        # maximum backoff time of 20 seconds (min(b*r^i, MAX_BACKOFF) where b is a
        # random number between 0 and 1 and r is the base factor of 2). This might
        # look like:
        #
        # seconds_to_sleep = min(1 × 2^1, 20) = min(2, 20) = 2 seconds
        #
        # By our 5th retry (2^5 is 32) we will be sleeping *up to* 20 seconds, depending
        # on the random jitter.
        #
        # It handles transient errors like RequestTimeout and ConnectionError, as well
        # as Service-side errors like Throttling, SlowDown, and LimitExceeded.
        retry_config = Config(retries={"max_attempts": 5, "mode": "standard"})

        self.dynamodb = boto3.resource("dynamodb", region_name=dynamodb_region, config=retry_config)
        self.client = boto3.client("dynamodb", region_name=dynamodb_region, config=retry_config)
        self.name = name
        self.dynamodb_region = dynamodb_region
        self.table = self.dynamodb.Table(name)
        self.stopping = stopping
        self.max_transact_write_items = max_transact_write_items
        self.save_queue: OrderedDict = OrderedDict()
        self.save_lock = threading.Lock()
        self.save_errors = 0
        self.save_thread = threading.Thread(target=self._save_loop, args=(), daemon=True)
        self.save_thread.start()

    def build_key(self, type, iden) -> str:
        """
        It builds a unique partition key. The key could be objects with __str__ method.
        """
        return f"{type} {iden}"

    # KKASP: removed read_json so I think we can remove this parameter from the config
    def restore(self, keys) -> dict:
        """
        Fetch all under the same partition key(s).
        ret: <dict of key to states>
        """
        # format of the keys always passed here is
        # job_state job_name --> high level info about the job: enabled, run_nums
        # job_run_state job_run_name --> high level info about the job run
        first_items = self._get_first_partitions(keys)
        remaining_items = self._get_remaining_partitions(first_items)
        vals = self._merge_items(first_items, remaining_items)
        return vals

    def chunk_keys(self, keys: Sequence[T]) -> List[Sequence[T]]:
        """Generates a list of chunks of keys to be used to read from DynamoDB"""
        # have a for loop here for all the key chunks we want to go over
        cand_keys_chunks = []
        for i in range(0, len(keys), 100):
            # chunks of at most 100 keys will be in this list as there could be smaller chunks
            cand_keys_chunks.append(keys[i : min(len(keys), i + 100)])
        return cand_keys_chunks

    def _calculate_backoff_delay(self, attempt: int) -> int:
        # Clamp attempt to 1 to avoid negative or zero exponent
        safe_attempt = max(attempt, 1)
        base_delay_seconds = 1
        max_delay_seconds = 10
        delay: int = min(base_delay_seconds * (2 ** (safe_attempt - 1)), max_delay_seconds)
        return delay

    def _get_items(self, table_keys: list) -> object:
        items = []
        # let's avoid potentially mutating our input :)
        cand_keys_list = copy.copy(table_keys)
        attempts = 0

        # TODO: TRON-2363 - We should refactor this to not consume attempts when we are still making progress
        while len(cand_keys_list) != 0 and attempts < MAX_UNPROCESSED_KEYS_RETRIES:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                responses = [
                    executor.submit(
                        self.client.batch_get_item,
                        RequestItems={self.name: {"Keys": chunked_keys, "ConsistentRead": True}},
                    )
                    for chunked_keys in self.chunk_keys(cand_keys_list)
                ]
                # let's wipe the state so that we can loop back around
                # if there are any un-processed keys
                # NOTE: we'll re-chunk when submitting to the threadpool
                # since it's possible that we've had several chunks fail
                # enough keys that we'd otherwise send > 100 keys in a
                # request otherwise
                cand_keys_list = []
            for resp in concurrent.futures.as_completed(responses):
                try:
                    result = resp.result()
                    items.extend(result.get("Responses", {}).get(self.name, []))

                    # If DynamoDB returns unprocessed keys, we need to collect them and retry
                    unprocessed_keys = result.get("UnprocessedKeys", {}).get(self.name, {}).get("Keys", [])
                    if unprocessed_keys:
                        cand_keys_list.extend(unprocessed_keys)
                except botocore.exceptions.ClientError as e:
                    log.exception(f"ClientError during batch_get_item: {e.response}")
                    raise
                except Exception:
                    log.exception("Encountered issues retrieving data from DynamoDB")
                    raise
            if cand_keys_list:
                # We use _calculate_backoff_delay to get a delay that increases exponentially
                # with each retry. These retry attempts are distinct from the boto3 retry_config
                # and are used specifically to handle unprocessed keys.
                attempts += 1
                delay = self._calculate_backoff_delay(attempts)
                log.warning(
                    f"Attempt {attempts}/{MAX_UNPROCESSED_KEYS_RETRIES} - "
                    f"Retrying {len(cand_keys_list)} unprocessed keys after {delay}s delay."
                )
                time.sleep(delay)
        if cand_keys_list:
            msg = f"tron_dynamodb_restore_failure: failed to retrieve items with keys \n{cand_keys_list}\n from dynamodb after {MAX_UNPROCESSED_KEYS_RETRIES} retries."
            log.error(msg)

            raise KeyError(msg)
        return items

    def _get_first_partitions(self, keys: list):
        new_keys = [{"key": {"S": key}, "index": {"N": "0"}} for key in keys]
        return self._get_items(new_keys)

    def _get_remaining_partitions(self, items: list):
        """Get items in the remaining partitions: N = 1 and beyond"""
        keys_for_remaining_items = []
        for item in items:
            num_partitions = int(item["num_json_val_partitions"]["N"])

            prom_metrics.tron_dynamodb_partitions_histogram.observe(num_partitions)

            remaining_items = [
                {"key": {"S": str(item["key"]["S"])}, "index": {"N": str(i)}}
                # we start from 1 since we already have the 0th partition from get_first_partitions KKASP: confirm?
                for i in range(1, num_partitions)
            ]
            keys_for_remaining_items.extend(remaining_items)
        return self._get_items(keys_for_remaining_items)

    def _merge_items(self, first_items, remaining_items) -> dict:
        """Helper to merge multi-partition data into a single entry."""
        items = defaultdict(list)
        json_items: DefaultDict[str, str] = defaultdict(str)
        # item = job run, each job run can have multiple rows and each row is called a partition
        # Merge all items based their keys and deserialize their values
        if remaining_items:
            first_items.extend(remaining_items)
        for item in first_items:
            key = item["key"]["S"]
            items[key].append(item)
        for key, item in items.items():
            item.sort(key=lambda x: int(x["index"]["N"]))
            for json_val in item:
                try:
                    json_items[key] += json_val["json_val"]["S"]
                except Exception:
                    # KKASP: what should we do here? If we can't read the json_val we are cooked
                    log.exception(f"json_val not found for key {key}")
        try:
            deserialized_items = {k: self._deserialize_item(k, val) for k, val in json_items.items()}
        except Exception as e:
            # KKASP: what should we do here? If we can't read the json_val we are cooked
            log.exception(f"Error deserializing JSON items: {repr(e)}")
            raise

        return deserialized_items

    def save(self, key_value_pairs) -> None:
        """Add items to the save_queue to be later consumed by _consume_save_queue"""
        for key, val in key_value_pairs:
            while True:
                qlen = len(self.save_queue)
                if qlen > MAX_SAVE_QUEUE:
                    log.info(f"save queue size {qlen} > {MAX_SAVE_QUEUE}, sleeping 5s")
                    time.sleep(5)
                    continue
                with self.save_lock:
                    # KKASP: what is this again?
                    if val is None:
                        self.save_queue[key] = val
                    else:
                        state_type = self.get_type_from_key(key)
                        val = self._serialize_item(state_type, val)
                        self.save_queue[key] = val
                break

    def _consume_save_queue(self):
        """Consume the save_queue and save the items to dynamodb"""
        qlen = len(self.save_queue)
        saved = 0
        start = time.time()
        for _ in range(qlen):
            try:
                with self.save_lock:
                    key, val = self.save_queue.popitem(last=False)
                # Remove all previous data with the same partition key
                # TODO: only remove excess partitions if new data has fewer
                self._delete_item(key)
                # This check is for our hacky delete where we add keys with None values to the save queue
                if val is not None:
                    self[key] = val
                # reset errors count if we can successfully save
                saved += 1
            except Exception as e:
                error = "tron_dynamodb_save_failure: failed to save key " f'"{key}" to dynamodb:\n{repr(e)}'
                log.error(error)
                with self.save_lock:
                    self.save_queue[key] = val
        duration = time.time() - start
        log.info(f"saved {saved} items in {duration}s")

        if saved < qlen:
            self.save_errors += 1
        else:
            self.save_errors = 0

    def get_type_from_key(self, key: str) -> str:
        return key.split()[0]

    # TODO: TRON-2305 - In an ideal world, we wouldn't be passing around state/state_data dicts. It would be a lot nicer to have regular objects here
    def _serialize_item(self, key: Literal[runstate.JOB_STATE, runstate.JOB_RUN_STATE], state: Dict[str, Any]) -> Optional[str]:  # type: ignore
        try:
            if key == runstate.JOB_STATE:
                log.info(f"Serializing Job: {state.get('job_name')}")
                return Job.to_json(state)
            elif key == runstate.JOB_RUN_STATE:
                log.info(f"Serializing JobRun: {state.get('job_name')}.{state.get('run_num')}")
                return JobRun.to_json(state)
            else:
                raise ValueError(f"Unknown type: key {key}")
        except Exception:
            log.exception(f"Serialization error for key {key}")
            prom_metrics.json_serialization_errors_counter.inc()
            return None

    def _deserialize_item(self, key: str, state: str) -> Dict[str, Any]:
        try:
            json_key = key.split(" ")[0]
            if json_key == runstate.JOB_STATE:
                job_data = Job.from_json(state)
                return cast(Dict[str, Any], job_data)
            elif json_key == runstate.JOB_RUN_STATE:
                job_run_data = JobRun.from_json(state)
                return cast(Dict[str, Any], job_run_data)
            else:
                raise ValueError(f"Unknown type: key {key}")
        except Exception:
            log.exception(f"Deserialization error for key {key}")
            prom_metrics.json_deserialization_errors_counter.inc()
            raise

    def _save_loop(self):
        while True:
            if self.stopping:
                self._consume_save_queue()
                return

            if len(self.save_queue) == 0:
                log.debug("save queue empty, sleeping 5s")
                time.sleep(5)
                continue

            self._consume_save_queue()
            if self.save_errors > 100:
                log.error("too many dynamodb errors in a row, crashing")
                os.exit(1)

    def __setitem__(self, key: str, value: str) -> None:
        """
        Partition the item and write up to self.max_transact_write_items
        partitions atomically using TransactWriteItems.

        The function examines the size of json_val, splits it into multiple
        segments based on OBJECT_SIZE, and stores each segment under the
        same partition key.

        It relies on the boto3/botocore retry_config to handle
        certain errors (e.g. throttling). If an error is not
        addressed by boto3's internal logic, the transaction fails
        and raises an exception. It is the caller's responsibility
        to implement further retries.
        """
        start = time.time()

        num_partitions = math.ceil(len(value) / OBJECT_SIZE) if value else 0

        prom_metrics.tron_dynamodb_partitions_histogram.observe(num_partitions)

        items = []

        for index in range(num_partitions):
            item = {
                "Put": {
                    "Item": {
                        "key": {
                            "S": key,
                        },
                        "index": {
                            "N": str(index),
                        },
                        "json_val": {
                            "S": value[index * OBJECT_SIZE : min(index * OBJECT_SIZE + OBJECT_SIZE, len(value))],
                        },
                        "num_json_val_partitions": {
                            "N": str(num_partitions),
                        },
                    },
                    "TableName": self.name,
                },
            }

            items.append(item)

            # We want to write the items when we've either reached the max number of items
            # for a transaction, or when we're done processing all partitions
            if len(items) == self.max_transact_write_items or index == num_partitions - 1:
                try:
                    self.client.transact_write_items(TransactItems=items)
                    items = []
                except Exception:
                    timer(
                        name="tron.dynamodb.setitem",
                        delta=time.time() - start,
                    )
                    # TODO: TRON-2419 - We should be smarter here. While each batch is atomic, a sufficiently
                    # large JobRun could exceed the max size of a single transaction (e.g. a JobRun with 12
                    # partitions). While one batch might succeed (saving partitions 1-8), the next one (for
                    # partitions 9-12) might fail. We should to handle this case or we will see more hanging
                    # chads in DynamoDB.
                    log.exception(f"Failed to save partition for key: {key}")
                    raise
        timer(
            name="tron.dynamodb.setitem",
            delta=time.time() - start,
        )

    def _delete_item(self, key: str) -> None:
        start = time.time()
        try:
            num_partitions = self._get_num_of_partitions(key)
            with self.table.batch_writer() as batch:
                for index in range(num_partitions):
                    batch.delete_item(
                        Key={
                            "key": key,
                            "index": index,
                        },
                    )
        finally:
            timer(
                name="tron.dynamodb.delete",
                delta=time.time() - start,
            )

    def _get_num_of_partitions(self, key: str) -> int:
        """
        Return the number of partitions an item is divided into.
        """
        try:
            partition = self.table.get_item(
                Key={
                    "key": key,
                    "index": 0,
                },
                ProjectionExpression="num_json_val_partitions",
                ConsistentRead=True,
            )
            num_partitions = int(partition.get("Item", {}).get("num_json_val_partitions", 0))
            return num_partitions
        except self.client.exceptions.ResourceNotFoundException:
            return 0

    def cleanup(self) -> None:
        self.stopping = True
        self.save_thread.join()
        return
