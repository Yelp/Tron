import logging
import math
import pickle
from collections import defaultdict

import boto3

from tron.serialize.runstate.shelvestore import ShelveKey

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
        translated_items = {}
        try:
            first_items = self._get_first_partitions(keys)
            remaining_items = self._get_remaining_partitions(first_items)
            items = self._merge_items(first_items, remaining_items)
            #TODO: remove this after berkleyDB is removed.
            for key in keys:
                if str(key) in items:
                    translated_items[key] = items[str(key)]
        except Exception as e:
            self.alert(str(e))
        return translated_items

    def alert(self, msg: str):
        import pysensu_yelp
        result_dict = {
            'name': 'tron_dynamodb_check',
            'runbook': '',
            'status': 1,
            'output': msg,
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
        table_name = self.name.replace('/', '-')
        for i in range(0, len(keys), 100):
            vals = self.client.batch_get_item(
                RequestItems={
                    table_name: {
                        'Keys': keys[i:min(len(keys), i + 100)],
                        'ConsistentRead': True
                    },
                }
            )['Responses'][table_name]
            items.extend(vals)
        return items

    def _get_first_partitions(self, keys: list):
        new_keys = [{'key': {'S': str(key)}, 'index': {'N': '0'}} for key in keys]
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
            self.alert(str(e))

    def __setitem__(self, key: ShelveKey, val: bytes) -> None:
        num_partitions = math.ceil(len(val) / OBJECT_SIZE)
        with self.table.batch_writer() as batch:
            for index in range(num_partitions):
                batch.put_item(
                    Item={
                        'key': str(key),
                        'index': index,
                        'val': val[index * OBJECT_SIZE:min(index * OBJECT_SIZE + OBJECT_SIZE, len(val))],
                        'num_partitions': num_partitions,
                    }
                )

    def _delete_item(self, key: ShelveKey) -> None:
        with self.table.batch_writer() as batch:
            for index in range(self._get_num_of_partitions(key)):
                batch.delete_item(
                    Key={
                        'key': str(key),
                        'index': index,
                    }
                )

    def _get_num_of_partitions(self, key: ShelveKey) -> int:
        """
        Return how many parts is the item partitioned into
        """
        try:
            partition = self.table.get_item(
                Key={
                    'key': str(key),
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
