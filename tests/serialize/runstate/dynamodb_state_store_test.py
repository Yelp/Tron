import pickle
from unittest import mock

import boto3
import pytest
from moto import mock_dynamodb2
from moto.dynamodb2.responses import dynamo_json_dump

from testifycompat import assert_equal
from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore


def mock_transact_write_items(self):
    """
    This mocks moto.dynamodb2.responses.DynamoHandler.transact_write_items,
    which is used to mock dynamodb client. This function calls put_item,
    update_item, and delete_item based on the arguments of transact_write_item.
    """

    def put_item(item):
        name = item['TableName']
        record = item['Item']
        return self.dynamodb_backend.put_item(name, record)

    def delete_item(item):
        name = item['TableName']
        keys = item['Key']
        return self.dynamodb_backend.delete_item(name, keys)

    def update_item(item):
        name = item['TableName']
        key = item['Key']
        update_expression = item.get('UpdateExpression')
        attribute_updates = item.get('AttributeUpdates')
        expression_attribute_names = item.get('ExpressionAttributeNames', {})
        expression_attribute_values = item.get('ExpressionAttributeValues', {})
        return self.dynamodb_backend.update_item(
            name, key, update_expression, attribute_updates, expression_attribute_names,
            expression_attribute_values)

    transact_items = self.body['TransactItems']

    for transact_item in transact_items:
        if 'Put' in transact_item:
            put_item(transact_item['Put'])
        elif 'Update' in transact_item:
            update_item(transact_item['Update'])
        elif 'Delete' in transact_item:
            delete_item(transact_item['Delete'])

    return dynamo_json_dump({})


@pytest.fixture(autouse=True)
def store():
    with mock.patch(
        'moto.dynamodb2.responses.DynamoHandler.transact_write_items',
        new=mock_transact_write_items, create=True
    ), mock_dynamodb2():
        dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
        table_name = 'tmp'
        store = DynamoDBStateStore(table_name, 'us-west-2')
        store.table = dynamodb.create_table(
            TableName=table_name,
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
        keys = [store.build_key("thing", i) for i in range(3)]
        value = pickle.loads(large_object)
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)

        for key in pairs:
            store._delete_item(key)

        for key in pairs:
            assert_equal(store._get_num_of_partitions(key), 0)

    def test_retry_saving(self, store, small_object, large_object):
        with mock.patch(
            'moto.dynamodb2.responses.DynamoHandler.transact_write_items',
            side_effect=KeyError('foo')
        ) as mock_failed_write, mock.patch(
            'tron.serialize.runstate.dynamodb_state_store.DynamoDBStateStore.alert'
        ) as mock_alert:
            keys = [store.build_key("thing", i) for i in range(1)]
            value = pickle.loads(small_object)
            pairs = zip(keys, (value for i in range(len(keys))))
            try:
                store.save(pairs)
            except Exception:
                assert_equal(mock_failed_write.call_count, 3)
                assert_equal(mock_alert.call_count, 1)

    def test_retry_reading(self, store, small_object, large_object):
        unprocessed_value = {
            'Responses': {
                store.name: [
                    {
                        'index': {'N': '0'},
                        'key': {'S': 'thing 0'}
                    }
                ]
            },
            'UnprocessedKeys':
            {
                store.name: {
                    'ConsistentRead': True,
                    'Keys': [{
                        'index': {'N': '0'},
                        'key': {'S': 'thing 0'}
                    }]
                }
            },
            'ResponseMetadata': {}
        }
        keys = [store.build_key("thing", i) for i in range(1)]
        value = pickle.loads(small_object)
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)
        with mock.patch.object(
            store.client,
            'batch_get_item',
            return_value=unprocessed_value
        ) as mock_failed_read, mock.patch(
            'tron.serialize.runstate.dynamodb_state_store.DynamoDBStateStore.alert'
        ) as mock_alert:
            try:
                store.restore(keys)
            except Exception:
                assert_equal(mock_failed_read.call_count, 11)
                assert_equal(mock_alert.call_count, 1)
