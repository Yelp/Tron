import copy
import math
import pickle
from threading import Thread

import boto3

from tron.serialize.runstate.shelvestore import ShelveKey
from tron.serialize.runstate.shelvestore import ShelveStateStore

OBJECT_SIZE = 400000


class DynamoDBStateStore(object):
    def __init__(self, name, dynamodb_region) -> None:
        self.dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
        self.client = boto3.client('dynamodb', region_name=dynamodb_region)
        self.name = name
        self.shelve = ShelveStateStore(name)
        self.table = self.dynamodb.Table(name.replace('/', '-'))

    def build_key(self, type, iden) -> ShelveKey:
        """
        It builds a unique partition key. The key could be objects with __str__ method.
        """
        return ShelveKey(type, iden)

    def isValid(self, shelve_kv_pairs: dict, dynamo_kv_pairs: dict) -> bool:
        """
        It checks if all keys in DynamoDB are in shelveStateStore and have the same values.
        It is possible that some keys are in shelveStateStore but is not in DynamoDB when the
        migration starts.
        """
        count = 0
        for k, v in shelve_kv_pairs.items():
            if k in dynamo_kv_pairs.keys():
                count += 1
                if pickle.dumps(dynamo_kv_pairs[k]) != pickle.dumps(shelve_kv_pairs[k]):
                    return False
        return count == len(dynamo_kv_pairs)

    def restore(self, keys) -> dict:
        """
        Fetch all under the same parition key(keys).
        ret: <dict of key to states>
        """
        keys = list(keys)

        def shelve_restore(keys, res):
            res.update({k: v for k, v in self.shelve.restore(keys).items()})

        def dynamodb_restore(keys, res):
            items = zip(
                keys,
                (self[key] for key in keys),
            )
            res.update({k: v for k, v in items if v})

        dynamo_kv_pairs, shelve_kv_pairs = {}, {}
        thread1 = Thread(target = dynamodb_restore, args=(copy.deepcopy(keys), dynamo_kv_pairs,))
        thread2 = Thread(target = shelve_restore, args=(keys, shelve_kv_pairs,))
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        if not self.isValid(shelve_kv_pairs, dynamo_kv_pairs):
            self.alert()
        return shelve_kv_pairs

    def alert(self):
        import pysensu_yelp
        result_dict = {
            'check_name': 'tron_dynamoDB_synchronization_check',
            'runbook': '',
            'status': 1,
            'output': 'Data in dynamoDB is not synced to BerkleyDB. This is not critical \
                      since only BerkleyDB is used to restore states right now',
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
                    'ProjectionExpression': 'val, index',
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
        key_value_pairs = list(key_value_pairs)

        def dynamodb_save(key_value_pairs):
            for key, val in key_value_pairs:
                self._delete_item(key)
                self[key] = pickle.dumps(val)

        thread1 = Thread(target = dynamodb_save, args=(copy.deepcopy(key_value_pairs),))
        thread2 = Thread(target = self.shelve.save, args=(key_value_pairs,))
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

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
                ConsistentRead=True
            )
            return int(partition.get('Item', {}).get('num_partitions', 0))
        except self.client.exceptions.ResourceNotFoundException:
            return 0

    def cleanup(self) -> None:
        self.shelve.cleanup()
