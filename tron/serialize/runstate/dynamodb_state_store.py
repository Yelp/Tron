import logging
import math
import pickle
import time
from collections import defaultdict

import boto3

OBJECT_SIZE = 400000
log = logging.getLogger(__name__)


class DynamoDBStateStore(object):
    def __init__(self, name, dynamodb_region) -> None:
        self.dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
        self.client = boto3.client('dynamodb', region_name=dynamodb_region)
        self.name = name
        self.dynamodb_region = dynamodb_region
        self.table = self.dynamodb.Table(name)

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

    def alert(self, name: str, msg: str, error: str):
        import pysensu_yelp
        result_dict = {
            'name': name,
            'runbook': '',
            'status': 1,
            'output': '\n'.join(msg, error),
            'team': 'compute-infra',
            'tip': '',
            'page': None,
            'notification_email': 'mingqiz@yelp.com',
            'irc_channels': None,
            'slack_channels': None,
            'alert_after': '1m',
            'check_every': '10m',
            'realert_every': -1,
            'ttl': None
        }
        pysensu_yelp.send_event(**result_dict)

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
                    error = Exception('failed to retrieve items from dynamodb\n{}'.format(resp))
                    self.alert(
                        'tron_dynamodb_restore_failure',
                        'tron failed to restore for unknown reason with keys {}'.format(cand_keys),
                        str(error)
                    )
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
        """
        Remove all previous data with the same partition key, examine the size of state_data,
        and splice it into different parts under 400KB with different sort keys,
        and save them under the same partition key built.
        """
        try:
            for key, val in key_value_pairs:
                self._delete_item(key)
                self[key] = pickle.dumps(val)
        except Exception as e:
            log.error(str(e))
            self.alert(
                'tron_dynamodb_save_failure',
                'tron failed to save for unknown reason with keys {}'.format(str(key_value_pairs.keys())),
                str(e)
            )

    def __setitem__(self, key: str, val: bytes) -> None:
        """
        Partition the item and write up to 10 partitions atomically.
        Retry up to 3 times on failure
        """
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
            resp = None
            items.append(item)
            # Only up to 10 items are allowed per transactions
            while len(items) == 10 or index == num_partitions - 1:
                try:
                    resp = self.client.transact_write_items(TransactItems=items)
                    items = []
                    break  # exit the while loop on successful writing
                except Exception as e:
                    count += 1
                    if count > 3:
                        log.error(str(e))
                        self.alert(
                            'tron_dynamodb_save_failure',
                            'tron failed to save due to transact_write_items failure with key {}\n{}'.format(key, resp),
                            str(e)
                        )
                        time.sleep(1)
                        break

    def _delete_item(self, key: str) -> None:
        try:
            with self.table.batch_writer() as batch:
                for index in range(self._get_num_of_partitions(key)):
                    batch.delete_item(
                        Key={
                            'key': key,
                            'index': index,
                        }
                    )
        except Exception as e:
            msg = 'Tron would work normally but redundant data might not be deleted if this is not resolved'
            log.error(str(e))
            self.alert(
                'tron_dynamodb_save_failure',
                'tron failed to delete for unknown reason {}\n{}'.format(key, msg),
                str(e)
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
        return
