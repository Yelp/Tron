import gzip
import json
import pickle
from unittest import mock

import boto3
import pytest
import staticconf.testing
from boto3.dynamodb.types import Binary
from moto import mock_dynamodb
from moto.dynamodb.responses import dynamo_json_dump

from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore
from tron.serialize.runstate.dynamodb_state_store import MAX_UNPROCESSED_KEYS_RETRIES


def mock_transact_write_items(self):
    """
    This mocks moto.dynamodb2.responses.DynamoHandler.transact_write_items,
    which is used to mock dynamodb client. This function calls put_item,
    update_item, and delete_item based on the arguments of transact_write_item.
    """

    def put_item(item):
        name = item["TableName"]
        record = item["Item"]
        return self.dynamodb_backend.put_item(name, record)

    def delete_item(item):
        name = item["TableName"]
        keys = item["Key"]
        return self.dynamodb_backend.delete_item(name, keys)

    def update_item(item):
        name = item["TableName"]
        key = item["Key"]
        update_expression = item.get("UpdateExpression")
        attribute_updates = item.get("AttributeUpdates")
        expression_attribute_names = item.get("ExpressionAttributeNames", {})
        expression_attribute_values = item.get("ExpressionAttributeValues", {})
        return self.dynamodb_backend.update_item(
            name,
            key,
            update_expression,
            attribute_updates,
            expression_attribute_names,
            expression_attribute_values,
        )

    transact_items = self.body["TransactItems"]

    for transact_item in transact_items:
        if "Put" in transact_item:
            put_item(transact_item["Put"])
        elif "Update" in transact_item:
            update_item(transact_item["Update"])
        elif "Delete" in transact_item:
            delete_item(transact_item["Delete"])

    return dynamo_json_dump({})


@pytest.fixture(autouse=True)
def store():
    with mock.patch(
        "moto.dynamodb.responses.DynamoHandler.transact_write_items",
        new=mock_transact_write_items,
        create=True,
    ), mock_dynamodb():
        dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
        table_name = "tmp"
        store = DynamoDBStateStore(table_name, "us-west-2", stopping=True)
        store.table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    "AttributeName": "key",
                    "KeyType": "HASH",
                },  # Partition key
                {
                    "AttributeName": "index",
                    "KeyType": "RANGE",
                },  # Sort key
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "key",
                    "AttributeType": "S",
                },
                {
                    "AttributeName": "index",
                    "AttributeType": "N",
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 10,
                "WriteCapacityUnits": 10,
            },
        )
        store.client = boto3.client("dynamodb", region_name="us-west-2")
        # Has to be yield here for moto to work
        yield store


@pytest.fixture
def small_object():
    yield {
        "job_name": "example_job",
        "run_num": 1,
        "run_time": None,
        "time_zone": None,
        "node_name": "example_node",
        "runs": [],
        "cleanup_run": None,
        "manual": False,
    }


@pytest.fixture
def large_object():
    yield {
        "job_name": "example_job",
        "run_num": 1,
        "run_time": None,
        "time_zone": None,
        "node_name": "example_node",
        "runs": [],
        "cleanup_run": None,
        "manual": False,
        "large_data": [i for i in range(200_000)],
    }


@pytest.mark.usefixtures("store")
class TestDynamoDBStateStore:
    @pytest.mark.parametrize("read_json", [False, True])
    def test_save(self, store, small_object, read_json):
        key_value_pairs = [
            (
                store.build_key("job_state", "two"),
                small_object,
            ),
            (
                store.build_key("job_run_state", "four"),
                small_object,
            ),
        ]
        store.save(key_value_pairs)
        store._consume_save_queue()

        assert store.save_errors == 0
        keys = [
            store.build_key("job_state", "two"),
            store.build_key("job_run_state", "four"),
        ]
        mock_config = {"read_json.enable": read_json}
        mock_configuration = staticconf.testing.MockConfiguration(mock_config, namespace="tron")
        with mock_configuration, mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore(keys, read_json=read_json)
        for key, value in key_value_pairs:
            assert vals[key] == value

        for key in keys:
            item = store.table.get_item(Key={"key": key, "index": 0})
            assert "Item" in item
            assert "json_val" in item["Item"]

            compressed_val = item["Item"]["json_val"]
            assert isinstance(compressed_val, Binary)

            decompressed_json = gzip.decompress(compressed_val.value)
            assert json.loads(decompressed_json) == small_object

    def test_save_multi_partition_object(self, store, large_object):
        key_value_pairs = [
            (
                store.build_key("job_state", "two"),
                large_object,
            ),
        ]
        store.save(key_value_pairs)
        store._consume_save_queue()

        assert store.save_errors == 0
        keys = [store.build_key("job_state", "two")]

        with mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore(keys)
        for key, value in key_value_pairs:
            assert vals[key] == value

    @pytest.mark.parametrize("read_json", [False, True])
    def test_restore(self, store, small_object, read_json):
        keys = [store.build_key("job_state", i) for i in range(3)]
        value = small_object
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)
        store._consume_save_queue()

        assert store.save_errors == 0
        mock_config = {"read_json.enable": read_json}
        mock_configuration = staticconf.testing.MockConfiguration(mock_config, namespace="tron")
        with mock_configuration, mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore(keys)
        for key in keys:
            assert vals[key] == small_object

    @pytest.mark.parametrize("read_json", [False, True])
    def test_restore_multi_partition_object(self, store, large_object, read_json):
        keys = [store.build_key("job_state", i) for i in range(3)]
        value = large_object
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)
        store._consume_save_queue()

        assert store.save_errors == 0

        for key in keys:
            num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
            assert num_json_val_partitions > 1
            assert num_partitions > 1

        mock_config = {"read_json.enable": read_json}
        mock_configuration = staticconf.testing.MockConfiguration(mock_config, namespace="tron")
        with mock_configuration, mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore(keys)
        for key in keys:
            assert vals[key] == large_object

    @pytest.mark.parametrize("read_json", [False, True])
    def test_restore_legacy_uncompressed_json(self, store, small_object, read_json):
        key = store.build_key("job_run_state", "legacy_job.1")
        json_val = json.dumps(small_object)
        store.table.put_item(
            Item={
                "key": key,
                "index": 0,
                "val": pickle.dumps(small_object),
                "json_val": json_val,
                "num_partitions": 1,
                "num_json_val_partitions": 1,
            }
        )
        restored = store.restore([key], read_json=read_json)
        assert key in restored
        assert restored[key] == small_object

    def test_fallback_to_pickle_on_json_error(self, store, small_object):
        # TODO: TRON-2240 - Remove once we delete pickles as there will be no fallback path
        key = store.build_key("job_run_state", "a_job.1")

        store.save([(key, small_object)])
        store._consume_save_queue()

        error_message = "Simulating a JSON deserialization error"
        with mock.patch(
            "tron.serialize.runstate.dynamodb_state_store.DynamoDBStateStore._deserialize_item",
            side_effect=Exception(error_message),
            autospec=True,
        ) as mock_deserialize:
            restored_vals = store.restore([key], read_json=True)

        mock_deserialize.assert_called_once()
        assert key in restored_vals
        assert restored_vals[key] == small_object

    def test_delete_item(self, store, small_object):
        keys = [store.build_key("job_state", i) for i in range(3)]
        pairs = list(zip(keys, (small_object for _ in range(len(keys)))))

        store.save(pairs)
        store._consume_save_queue()

        for key, _ in pairs:
            store._delete_item(key)

        for key, _ in pairs:
            num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
            assert num_partitions == 0
            assert num_json_val_partitions == 0

    def test_delete_multi_partition_item(self, store, large_object):
        keys = [store.build_key("job_state", i) for i in range(3)]
        pairs = list(zip(keys, (large_object for _ in range(len(keys)))))

        store.save(pairs)
        store._consume_save_queue()

        for key, _ in pairs:
            num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
            assert num_partitions > 1
            assert num_json_val_partitions > 1

        for key, _ in pairs:
            store._delete_item(key)

        for key, _ in pairs:
            num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
            assert num_partitions == 0
            assert num_json_val_partitions == 0

    def test_delete_if_val_is_none(self, store, small_object):
        key_value_pairs = [
            (
                store.build_key("job_state", "two"),
                small_object,
            ),
            (
                store.build_key("job_run_state", "four"),
                small_object,
            ),
        ]
        store.save(key_value_pairs)
        store._consume_save_queue()

        delete = [
            (
                store.build_key("job_state", "two"),
                None,
            ),
        ]
        store.save(delete)
        store._consume_save_queue()

        assert store.save_errors == 0
        # Try to restore both, we should just get one back
        keys = [
            store.build_key("job_state", "two"),
            store.build_key("job_run_state", "four"),
        ]
        with mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore(keys)
        assert vals == {"job_run_state four": small_object}

    @pytest.mark.parametrize(
        "test_object, side_effects, expected_save_errors, expected_queue_length",
        [
            # All attempts fail
            ("small_object", [KeyError("foo")] * 3, 3, 1),
            ("large_object", [KeyError("foo")] * 3, 3, 1),
            # Failure followed by success
            ("small_object", [KeyError("foo"), {}], 0, 0),
            ("large_object", [KeyError("foo")] + [{}] * 10, 0, 0),
        ],
    )
    def test_retry_saving(
        self, test_object, side_effects, expected_save_errors, expected_queue_length, store, small_object, large_object
    ):
        object_mapping = {
            "small_object": small_object,
            "large_object": large_object,
        }
        value = object_mapping[test_object]

        with mock.patch.object(
            store.client,
            "transact_write_items",
            side_effect=side_effects,
        ) as mock_transact_write:
            keys = [store.build_key("job_state", 0)]
            pairs = zip(keys, [value])
            store.save(pairs)

            for _ in side_effects:
                store._consume_save_queue()

            assert mock_transact_write.called
            assert store.save_errors == expected_save_errors
            assert len(store.save_queue) == expected_queue_length

    @pytest.mark.parametrize(
        "attempt, expected_delay",
        [
            (1, 1),
            (2, 2),
            (3, 4),
            (4, 8),
            (5, 10),
            (6, 10),
            (7, 10),
        ],
    )
    def test_calculate_backoff_delay(self, store, attempt, expected_delay):
        delay = store._calculate_backoff_delay(attempt)
        assert delay == expected_delay

    def test_retry_reading(self, store):
        unprocessed_value = {
            "Responses": {},
            "UnprocessedKeys": {
                store.name: {
                    "Keys": [{"key": {"S": store.build_key("job_state", 0)}, "index": {"N": "0"}}],
                    "ConsistentRead": True,
                }
            },
        }

        keys = [store.build_key("job_state", 0)]

        with mock.patch.object(
            store.client,
            "batch_get_item",
            return_value=unprocessed_value,
        ) as mock_batch_get_item, mock.patch("time.sleep") as mock_sleep, pytest.raises(Exception) as exec_info:
            store.restore(keys)
        assert "failed to retrieve items with keys" in str(exec_info.value)
        assert mock_batch_get_item.call_count == MAX_UNPROCESSED_KEYS_RETRIES
        assert mock_sleep.call_count == MAX_UNPROCESSED_KEYS_RETRIES

    def test_restore_exception_propagation(self, store):
        # This test is to ensure that restore propagates exceptions upwards: see DAR-2328
        keys = [store.build_key("job_state", i) for i in range(3)]

        mock_future = mock.MagicMock()
        mock_future.result.side_effect = Exception("mocked exception")
        with mock.patch("concurrent.futures.Future", return_value=mock_future, autospec=True):
            with mock.patch("concurrent.futures.as_completed", return_value=[mock_future], autospec=True):
                with pytest.raises(Exception) as exec_info, mock.patch(
                    "tron.config.static_config.load_yaml_file", autospec=True
                ), mock.patch("tron.config.static_config.build_configuration_watcher", autospec=True):
                    store.restore(keys)
                assert str(exec_info.value) == "mocked exception"
