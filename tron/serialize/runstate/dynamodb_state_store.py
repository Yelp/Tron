import logging
import math
import pickle

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
        try:
            items = zip(
                keys,
                (self[key] for key in keys),
            )
        except Exception as e:
            self.alert(str(e))
        return {k: v for k, v in items if v}

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

    def __getitem__(self, key: ShelveKey) -> object:
        """
        It returns an object which is deserialized from binary
        """
        table_name = self.name.replace('/', '-')
        keys = [{'key': {'S': str(key)}, 'index': {'N': str(index)}} for index in range(self._get_num_of_partitions(key))]
        if not keys:
            return None
        vals = self.client.batch_get_item(
            RequestItems={
                table_name: {
                    'Keys': keys,
                    'ConsistentRead': True
                },
            }
        )['Responses'][table_name]
        vals.sort(key=lambda x: x['index']['N'])
        res = bytearray()
        for val in vals:
            res += bytes(val['val']['B'])

        return pickle.loads(res) if res else None

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
