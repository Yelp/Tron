import math
import pickle
from collections import namedtuple

import boto3

DynamoDBStateKey = namedtuple('DynamoDBStateKey', ['type', 'id'])
OBJECT_SIZE = 400000
REGION_NAME = 'us-west-1'
TABLE_NAME = 'Tron_states'


class DynamoDBStateStore(object):
    def __init__(self, name):
        self.dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
        self.client = boto3.client('dynamodb', region_name=REGION_NAME)
        self.name = name
        #create a table if it doesn't exist
        self.table = self.dynamodb.Table(TABLE_NAME)

    def build_key(self, type, iden):
        """
        It builds a unique partition key.
        """
        # TODO: build shorter keys?
        return DynamoDBStateKey(type, iden)

    def restore(self, keys):
        """
        Fetch all under the same parition key(keys)
        """
        items = zip(
            keys,
            (self[key] for key in keys),
        )
        # Filter out values that are None.
        return dict(filter(lambda x: x[1], items))

    def __getitem__(self, key):
        val = bytearray()
        for index in range(self._get_num_of_partitions(key)):
            val += bytes(self.table.get_item(
                Key={
                    'key': str(key),
                    'index': index
                },
                ProjectionExpression='val',
                ConsistentRead=True
            )['Item']['val'].value)
        return pickle.loads(val) if val else None

    def save(self, key_value_pairs):
        """
        Remove all previous data with the same partition key, examine the size of state_data,
        and splice it into different parts under 400KB with different sort keys,
        and save them under the same partition key built.
        """
        for key, val in key_value_pairs:
            self._delete_item(key)
            self[key] = pickle.dumps(val)

    def __setitem__(self, key, val):
        size = math.ceil(len(val) / OBJECT_SIZE)
        for index in range(size):
            self.table.put_item(
                Item={
                    'key': str(key),
                    'index': index,
                    'val': val[index * OBJECT_SIZE:min(index * OBJECT_SIZE + OBJECT_SIZE, len(val))],
                    'size': size,
                }
            )

    def _delete_item(self, key):
        for index in range(self._get_num_of_partitions(key)):
            self.table.delete_item(
                Key={
                    'key': str(key),
                    'index': index,
                }
            )

    def _get_num_of_partitions(self, key) -> int:
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
            return int(partition.get('Item', {}).get('size', 0))
        except self.client.exceptions.ResourceNotFoundException:
            return 0

    def cleanup(self):
        return
