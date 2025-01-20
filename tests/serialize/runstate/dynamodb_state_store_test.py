import json
from unittest import mock

import boto3
import pytest
import staticconf.testing
from moto import mock_dynamodb2
from moto.dynamodb2.responses import dynamo_json_dump

from testifycompat import assert_equal
from testifycompat.assertions import assert_in
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
        "large_data": [i for i in range(1_000_000)],
    }


@pytest.mark.usefixtures("store", "small_object", "large_object")
class TestDynamoDBStateStore:
    @pytest.mark.parametrize("read_json", [False, True])
    def test_save(self, store, small_object, large_object, read_json):
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
            vals = store.restore(keys)
        for key, value in key_value_pairs:
            assert_equal(vals[key], value)

        for key in keys:
            item = store.table.get_item(Key={"key": key, "index": 0})
            assert "Item" in item
            assert "json_val" in item["Item"]
            assert_equal(json.loads(item["Item"]["json_val"]), small_object)

    def test_delete_if_val_is_none(self, store, small_object, large_object):
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
        assert vals == {keys[1]: small_object}

    def test_save_more_than_4KB(self, store, small_object, large_object):
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
            assert_equal(vals[key], value)

    @pytest.mark.parametrize("read_json", [False, True])
    def test_restore_more_than_4KB(self, store, small_object, large_object, read_json):
        keys = [store.build_key("job_state", i) for i in range(3)]
        value = large_object
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
            assert_equal(vals[key], large_object)

    @pytest.mark.parametrize("read_json", [False, True])
    def test_restore(self, store, small_object, large_object, read_json):
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
            assert_equal(vals[key], small_object)

    def test_delete_item(self, store, small_object, large_object):
        keys = [store.build_key("job_state", i) for i in range(3)]
        value = large_object
        pairs = list(zip(keys, (value for i in range(len(keys)))))
        store.save(pairs)
        store._consume_save_queue()

        for key, value in pairs:
            store._delete_item(key)
        for key, value in pairs:
            num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
            assert_equal(num_partitions, 0)
            assert_equal(num_json_val_partitions, 0)

    def test_delete_item_with_json_partitions(self, store, small_object, large_object):
        key = store.build_key("job_state", "test_job")
        value = large_object

        store.save([(key, value)])
        store._consume_save_queue()

        num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
        assert num_partitions > 0
        assert num_json_val_partitions > 0

        store._delete_item(key)

        num_partitions, num_json_val_partitions = store._get_num_of_partitions(key)
        assert_equal(num_partitions, 0)
        assert_equal(num_json_val_partitions, 0)

        with mock.patch("tron.config.static_config.load_yaml_file", autospec=True), mock.patch(
            "tron.config.static_config.build_configuration_watcher", autospec=True
        ):
            vals = store.restore([key])
        assert key not in vals

    @mock.patch("time.sleep", return_value=None)
    def test_retry_saving(self, mock_sleep, store, small_object, large_object):
        with mock.patch(
            "moto.dynamodb2.responses.DynamoHandler.transact_write_items",
            side_effect=KeyError("foo"),
        ) as mock_failed_write:
            keys = [store.build_key("job_state", i) for i in range(1)]
            value = small_object
            pairs = zip(keys, (value for i in range(len(keys))))
            try:
                store.save(pairs)
            except Exception:
                assert_equal(mock_failed_write.call_count, 3)

    @mock.patch("time.sleep")
    @mock.patch("random.uniform")
    def test_retry_reading(self, mock_random_uniform, mock_sleep, store, small_object, large_object):
        unprocessed_value = {
            "Responses": {},
            "UnprocessedKeys": {
                store.name: {
                    "Keys": [
                        {
                            "key": {"S": "job_state 0"},
                            "index": {"N": "0"},
                        }
                    ],
                    "ConsistentRead": True,
                }
            },
        }
        keys = [store.build_key("job_state", i) for i in range(1)]
        value = small_object
        pairs = zip(keys, [value] * len(keys))
        store.save(pairs)
        store._consume_save_queue()

        # Mock random.uniform to return the upper limit of the range so that we are simulating max jitter
        def side_effect_random_uniform(a, b):
            return b

        mock_random_uniform.side_effect = side_effect_random_uniform

        with mock.patch.object(
            store.client,
            "batch_get_item",
            return_value=unprocessed_value,
        ) as mock_failed_read:
            with pytest.raises(Exception) as exec_info, mock.patch(
                "tron.config.static_config.load_yaml_file", autospec=True
            ), mock.patch("tron.config.static_config.build_configuration_watcher", autospec=True):
                store.restore(keys)
            assert_in("failed to retrieve items with keys", str(exec_info.value))
            assert_equal(mock_failed_read.call_count, MAX_UNPROCESSED_KEYS_RETRIES)

        # We also need to verify that sleep was called with expected delays
        expected_delays = []
        base_delay_seconds = 0.5
        max_delay_seconds = 10
        for attempt in range(1, MAX_UNPROCESSED_KEYS_RETRIES + 1):
            expected_delay = min(base_delay_seconds * (2 ** (attempt - 1)), max_delay_seconds)
            expected_delays.append(expected_delay)
        actual_delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert_equal(actual_delays, expected_delays)

    def test_restore_exception_propagation(self, store, small_object):
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
