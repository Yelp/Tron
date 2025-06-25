import json
from unittest import mock

import boto3
import pytest
from moto import mock_dynamodb2
from moto.dynamodb2.responses import dynamo_json_dump

from testifycompat import assert_equal
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
        "moto.dynamodb2.responses.DynamoHandler.transact_write_items",
        new=mock_transact_write_items,
        create=True,
    ), mock_dynamodb2():
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
def job_state_object():
    yield {
        "enabled": True,
        "run_nums": [0, 1],
    }


@pytest.fixture
def small_jobrun_state_object():
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


# KKASP: fix this
@pytest.fixture
def large_jobrun_state_object():
    yield {
        "job_name": "example_job",
        "run_num": 1,
        "run_time": None,
        "time_zone": None,
        "node_name": "example_node",
        "cleanup_run": None,
        "manual": False,
        "runs": [  # Add this missing field with some basic structure
            {
                "job_run_id": "example_job.1",
                "action_name": "large_step",
                "state": "succeeded",
                "original_command": "echo test",
                "start_time": None,
                "end_time": None,
                "node_name": "example_node",
                "exit_status": 0,
                "attempts": [],
                "retries_remaining": None,
                "retries_delay": None,
                "action_runner": {"status_path": "/tmp/status", "exec_path": "/usr/bin"},
                "executor": "ssh",
                "trigger_downstreams": None,
                "triggered_by": None,
                "on_upstream_rerun": None,
                "trigger_timeout_timestamp": None,
            }
        ]
        * 5000,
    }


@pytest.mark.usefixtures("store", "job_state_object", "small_jobrun_state_object", "large_jobrun_state_object")
class TestDynamoDBStateStore:
    def test_save(self, store, job_state_object, small_jobrun_state_object):
        key_value_pairs = [
            (
                store.build_key("job_state", "example_job"),
                job_state_object,
            ),
            (
                store.build_key("job_run_state", "example_jobrun_small"),
                small_jobrun_state_object,
            ),
        ]
        store.save(key_value_pairs)
        store._consume_save_queue()

        assert store.save_errors == 0
        keys = [key for key, _ in key_value_pairs]

        vals = store.restore(keys)
        for key, value in key_value_pairs:
            assert vals[key] == value
            item = store.table.get_item(Key={"key": key, "index": 0})
            assert "Item" in item
            assert "json_val" in item["Item"]
            assert json.loads(item["Item"]["json_val"]) == value

    def test_delete_if_val_is_none(self, store, small_jobrun_state_object, large_jobrun_state_object):
        key_value_pairs = [
            (
                store.build_key("job_state", "two"),
                small_jobrun_state_object,
            ),
            (
                store.build_key("job_run_state", "four"),
                small_jobrun_state_object,
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
        assert vals == {keys[1]: small_jobrun_state_object}

    def test_save_more_than_4KB(self, store, small_jobrun_state_object, large_jobrun_state_object):
        key_value_pairs = [
            (
                store.build_key("job_run_state", "example_jobrun_large"),
                large_jobrun_state_object,
            ),
        ]
        store.save(key_value_pairs)
        store._consume_save_queue()

        assert store.save_errors == 0
        keys = [store.build_key("job_run_state", "example_jobrun_large")]

        with mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore(keys)
        for key, value in key_value_pairs:
            assert_equal(vals[key], value)

    def test_restore_more_than_4KB(self, store, small_jobrun_state_object, large_jobrun_state_object):
        keys = [store.build_key("job_run_state", i) for i in range(3)]
        value = large_jobrun_state_object
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)
        store._consume_save_queue()

        assert store.save_errors == 0

        vals = store.restore(keys)
        for key in keys:
            assert_equal(vals[key], large_jobrun_state_object)

    def test_restore(self, store, small_jobrun_state_object, large_jobrun_state_object):
        keys = [store.build_key("job_run_state", i) for i in range(3)]
        value = small_jobrun_state_object
        pairs = zip(keys, (value for i in range(len(keys))))
        store.save(pairs)
        store._consume_save_queue()

        assert store.save_errors == 0

        vals = store.restore(keys)
        for key in keys:
            assert_equal(vals[key], small_jobrun_state_object)

    def test_delete_item(self, store, small_jobrun_state_object, large_jobrun_state_object):
        keys = [store.build_key("job_state", i) for i in range(3)]
        value = large_jobrun_state_object
        pairs = list(zip(keys, (value for i in range(len(keys)))))
        store.save(pairs)
        store._consume_save_queue()

        for key, value in pairs:
            store._delete_item(key)
        for key, value in pairs:
            num_partitions = store._get_num_of_partitions(key)
            assert num_partitions == 0

    def test_delete_item_with_json_partitions(self, store, small_jobrun_state_object, large_jobrun_state_object):
        key = store.build_key("job_state", "test_job")
        value = large_jobrun_state_object

        store.save([(key, value)])
        store._consume_save_queue()

        num_partitions = store._get_num_of_partitions(key)
        assert num_partitions > 0

        store._delete_item(key)

        num_partitions = store._get_num_of_partitions(key)
        assert num_partitions == 0

        with mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore([key])
        assert key not in vals

    @pytest.mark.parametrize(
        "test_object, side_effects, expected_save_errors, expected_queue_length",
        [
            # All attempts fail
            ("small_jobrun_state_object", [KeyError("foo")] * 3, 3, 1),
            ("large_jobrun_state_object", [KeyError("foo")] * 3, 3, 1),
            # Failure followed by success
            ("small_jobrun_state_object", [KeyError("foo"), {}], 0, 0),
            ("large_jobrun_state_object", [KeyError("foo")] + [{}] * 10, 0, 0),
        ],
    )
    def test_retry_saving(
        self,
        test_object,
        side_effects,
        expected_save_errors,
        expected_queue_length,
        store,
        small_jobrun_state_object,
        large_jobrun_state_object,
    ):
        object_mapping = {
            "small_jobrun_state_object": small_jobrun_state_object,
            "large_jobrun_state_object": large_jobrun_state_object,
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
        assert_equal(delay, expected_delay)

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

    def test_restore_exception_propagation(self, store, small_jobrun_state_object):
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
