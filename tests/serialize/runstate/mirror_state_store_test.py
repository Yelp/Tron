import os
import pickle
import tempfile

import boto3
import pytest
from moto import mock_dynamodb2

from testifycompat import assert_equal
from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore
from tron.serialize.runstate.mirror_state_store import MirrorStateStore
from tron.serialize.runstate.shelvestore import Py2Shelf
from tron.serialize.runstate.shelvestore import ShelveStateStore


filename = os.path.join(tempfile.mkdtemp(), 'state')


@pytest.fixture
def store():
    with mock_dynamodb2():
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        dynamodb_store = DynamoDBStateStore(filename, 'us-west-2')
        dynamodb_store.table = dynamodb.create_table(
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
        dynamodb_store.client = boto3.client('dynamodb', region_name='us-west-2')
        # Has to be yield here for moto to work
        shelve_store = ShelveStateStore(filename)
        yield MirrorStateStore(shelve_store, dynamodb_store)


@pytest.fixture
def small_object():
    yield pickle.dumps({'this': 'data'})


@pytest.fixture
def large_object():
    yield pickle.dumps([i for i in range(100000)])


@pytest.mark.usefixtures("store", "small_object", "large_object")
class TestDynamoDBStateStore:
    def test_saved_to_both(self, store, small_object, large_object):
        key_value_pairs = [
            (
                store.build_key("DynamoDBTest", "two"),
                large_object
            )
        ]
        # Save to both
        store.save(key_value_pairs)
        # Retreive from both and check if equal
        stored_data = Py2Shelf(filename)

        for key, value in key_value_pairs:
            assert_equal(store.dynamodb_store[key], value)
            assert_equal(stored_data[str(key.key)], value)

        # Clean up
        for key, value in key_value_pairs:
            store.dynamodb_store._delete_item(key)
        store.cleanup()

    def test_restore_from_shelve_after_dynamodb_dies(self, store, small_object, large_object):
        key_value_pairs = [
            (
                store.build_key("DynamoDBTest", "two"),
                large_object
            )
        ]
        # Save to both
        store.save(key_value_pairs)
        # This only cleans up data in dynamoDB
        keys = [k for k, v in key_value_pairs]
        for key in keys:
            store.dynamodb_store._delete_item(key)
        # Check if retrievd data is valid
        retrieved_data = store.restore(keys)
        for key in keys:
            assert_equal(retrieved_data[key], large_object)
        store.cleanup()
