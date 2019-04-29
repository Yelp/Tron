import os
import pickle
import tempfile

import boto3
import pytest
from moto import mock_dynamodb2

from testifycompat import assert_equal
from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore


filename = os.path.join(tempfile.mkdtemp(), 'state')


@pytest.fixture
def store():
    with mock_dynamodb2():
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        store = DynamoDBStateStore(filename, 'us-west-2')
        store.table = dynamodb.create_table(
            TableName=filename.replace('/', '-'),
            KeySchema=[
                {
                    'AttributeName': 'key',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'index',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'key',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'index',
                    'AttributeType': 'N'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        store.client = boto3.client('dynamodb', region_name='us-west-2')
        # Has to be yield here for moto to work
        yield store


@pytest.fixture
def small_object():
    yield pickle.dumps({'this': 'data'})


@pytest.fixture
def large_object():
    yield pickle.dumps([i for i in range(1000000)])


@pytest.mark.usefixtures("store", "small_object", "large_object")
class TestDynamoDBStateStore:
    def test_save(self, store, small_object, large_object):
        key_value_pairs = [
            (
                store.build_key("DynamoDBTest", "two"),
                small_object,
            ),
            (
                store.build_key("DynamoDBTest2", "four"),
                small_object,
            ),
        ]
        store.save(key_value_pairs)

        keys = [store.build_key("DynamoDBTest", "two"), store.build_key("DynamoDBTest2", "four")]
        vals = store.restore(keys)
        for key, value in key_value_pairs:
            assert_equal(vals[key], value)

    def test_save_more_than_4KB(self, store, small_object, large_object):
        key_value_pairs = [
            (
                store.build_key("DynamoDBTest", "two"),
                large_object
            )
        ]
        store.save(key_value_pairs)

        keys = [store.build_key("DynamoDBTest", "two")]
        vals = store.restore(keys)
        for key, value in key_value_pairs:
            assert_equal(vals[key], value)

    def test_restore_more_than_4KB(self, store, small_object, large_object):
        keys = [store.build_key("thing", i) for i in range(3)]
        value = pickle.loads(large_object)
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)

        vals = store.restore(keys)
        for key in keys:
            assert_equal(pickle.dumps(vals[key]), large_object)

    def test_restore(self, store, small_object, large_object):
        keys = [store.build_key("thing", i) for i in range(3)]
        value = pickle.loads(small_object)
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)

        vals = store.restore(keys)
        for key in keys:
            assert_equal(pickle.dumps(vals[key]), small_object)

    def test_delete(self, store, small_object, large_object):
        pairs = [(store.build_key("thing", i), large_object) for i in range(3)]

        store.save(pairs)

        for key in pairs:
            store._delete_item(key)

        for key in pairs:
            assert_equal(store._get_num_of_partitions(key), 0)
