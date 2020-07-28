import logging
import math
import os
import pickle
import threading
import time
from collections import defaultdict
from collections import OrderedDict

import boto3

from tron.metrics import timer

OBJECT_SIZE = 400000
MAX_SAVE_QUEUE = 500
log = logging.getLogger(__name__)


class DynamoDBStateStore(object):
    def __init__(self, name, dynamodb_region, stopping=False) -> None:
        self.dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
        self.client = boto3.client('dynamodb', region_name=dynamodb_region)
        self.name = name
        self.dynamodb_region = dynamodb_region
        self.table = self.dynamodb.Table(name)
        self.stopping = stopping
        self.save_queue = OrderedDict()
        self.save_lock = threading.Lock()
        self.save_errors = 0
        self.save_thread = threading.Thread(target=self._save_loop, args=(), daemon=True)
        self.save_thread.start()

    def build_key(self, type, iden) -> str:
        """
        It builds a unique partition key. The key could be objects with __str__ method.
        """
        return f"{type} {iden}"

    def restore(self, keys) -> dict:
        """
        Fetch all under the same parition key(keys).
        ret: <dict of key to states>
        """
        first_items = self._get_first_partitions(keys)
        remaining_items = self._get_remaining_partitions(first_items)
        vals = self._merge_items(first_items, remaining_items)
        return vals

    def _get_items(self, keys: list) -> object:
        items = []
        for i in range(0, len(keys), 100):
            count = 0
            cand_keys = keys[i:min(len(keys), i + 100)]
            while True:
                resp = self.client.batch_get_item(
                    RequestItems={
                        self.name: {
                            'Keys': cand_keys,
                            'ConsistentRead': True
                        },
                    }
                )
                items.extend(resp['Responses'][self.name])
                if resp['UnprocessedKeys'].get(self.name) and count < 10:
                    cand_keys = resp['UnprocessedKeys'][self.name]['Keys']
                    count += 1
                elif count >= 10:
                    error = Exception(f'tron_dynamodb_restore_failure: failed to retrieve items with keys \n{cand_keys}\n from dynamodb\n{resp}')
                    raise error
                else:
                    break
        return items

    def _get_first_partitions(self, keys: list):
        new_keys = [{'key': {'S': key}, 'index': {'N': '0'}} for key in keys]
        return self._get_items(new_keys)

    def _get_remaining_partitions(self, items: list):
        keys_for_remaining_items = []
        for item in items:
            remaining_items = [{'key': {'S': str(item['key']['S'])}, 'index': {'N': str(i)}}
                               for i in range(1, int(item['num_partitions']['N']))]
            keys_for_remaining_items.extend(remaining_items)
        return self._get_items(keys_for_remaining_items)

    def _merge_items(self, first_items, remaining_items) -> dict:
        items = defaultdict(list)
        raw_items = defaultdict(bytearray)
        # Merge all items based their keys and deserialize their values
        if remaining_items:
            first_items.extend(remaining_items)
        for item in first_items:
            key = item['key']['S']
            items[key].append(item)
        for key, item in items.items():
            item.sort(key=lambda x: int(x['index']['N']))
            for val in item:
                raw_items[key] += bytes(val['val']['B'])
        deserialized_items = {k: pickle.loads(v) for k, v in raw_items.items()}
        return deserialized_items

    def save(self, key_value_pairs) -> None:
        for key, val in key_value_pairs:
            while True:
                qlen = len(self.save_queue)
                if qlen > MAX_SAVE_QUEUE:
                    log.info(f"save queue size {qlen} > {MAX_SAVE_QUEUE}, sleeping 5s")
                    time.sleep(5)
                    continue
                with self.save_lock:
                    self.save_queue[key] = val
                break

    def _consume_save_queue(self):
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
                if val is not None:
                    self[key] = pickle.dumps(val)
                # reset errors count if we can successfully save
                saved += 1
            except Exception as e:
                error = (
                    'tron_dynamodb_save_failure: failed to save key'
                    f'"{key}" to dynamodb:\n{repr(e)}'
                )
                log.error(error)
                with self.save_lock:
                    self.save_queue[key] = val
        duration = time.time() - start
        log.info(f"saved {saved} items in {duration}s")

        if saved < qlen:
            self.save_errors += 1
        else:
            self.save_errors = 0

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

    def __setitem__(self, key: str, val: bytes) -> None:
        """
        Partition the item and write up to 10 partitions atomically.
        Retry up to 3 times on failure

        Examine the size of `val`, and splice it into
        different parts under 400KB with different sort keys,
        and save them under the same partition key built.
        """
        start = time.time()
        num_partitions = math.ceil(len(val) / OBJECT_SIZE)
        items = []
        for index in range(num_partitions):
            item = {
                'Put': {
                    'Item': {
                        'key': {
                            'S': key,
                        },
                        'index': {
                            'N': str(index),
                        },
                        'val': {
                            'B': val[index * OBJECT_SIZE:min(index * OBJECT_SIZE + OBJECT_SIZE, len(val))],
                        },
                        'num_partitions': {
                            'N': str(num_partitions),
                        }
                    },
                    'TableName': self.name,
                },
            }
            count = 0
            items.append(item)
            # Only up to 10 items are allowed per transactions
            while len(items) == 10 or index == num_partitions - 1:
                try:
                    self.client.transact_write_items(TransactItems=items)
                    items = []
                    break  # exit the while loop on successful writing
                except Exception as e:
                    count += 1
                    if count > 3:
                        timer(
                            name='tron.dynamodb.setitem',
                            delta=time.time() - start,
                        )
                        raise e
                    else:
                        log.warning(f'Got error while saving, trying again: {repr(e)}')
        timer(
            name='tron.dynamodb.setitem',
            delta=time.time() - start,
        )

    def _delete_item(self, key: str) -> None:
        start = time.time()
        try:
            with self.table.batch_writer() as batch:
                for index in range(self._get_num_of_partitions(key)):
                    batch.delete_item(
                        Key={
                            'key': key,
                            'index': index,
                        }
                    )
        finally:
            timer(
                name='tron.dynamodb.delete',
                delta=time.time() - start,
            )

    def _get_num_of_partitions(self, key: str) -> int:
        """
        Return how many parts is the item partitioned into
        """
        try:
            partition = self.table.get_item(
                Key={
                    'key': key,
                    'index': 0,
                },
                ProjectionExpression='num_partitions',
                ConsistentRead=True
            )
            return int(partition.get('Item', {}).get('num_partitions', 0))
        except self.client.exceptions.ResourceNotFoundException:
            return 0

    def cleanup(self) -> None:
        self.stopping = True
        self.save_thread.join()
        return
