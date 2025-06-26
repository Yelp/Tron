from unittest import mock

import boto3
import pytest
from moto import mock_dynamodb2

from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore
from tron.serialize.runstate.dynamodb_state_store import MAX_UNPROCESSED_KEYS_RETRIES


@pytest.fixture
def table_name():
    return "test-tron-state"


@pytest.fixture
def store(table_name):
    """Provides a DynamoDBStateStore instance with a mocked DynamoDB backend."""
    with mock_dynamodb2():
        client = boto3.client("dynamodb", region_name="us-west-2")

        client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "key", "KeyType": "HASH"},
                {"AttributeName": "index", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "key", "AttributeType": "S"},
                {"AttributeName": "index", "AttributeType": "N"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        # Pass `stopping=True` to prevent the background save thread from running during tests
        state_store = DynamoDBStateStore(table_name, "us-west-2", stopping=True)
        yield state_store


@pytest.fixture
def job_state_object():
    return {"enabled": True, "run_nums": [0, 1]}


@pytest.fixture
def small_jobrun_state_object():
    return {
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
def large_jobrun_state_object():
    """Models a JobRun state object that is larger than the partition size."""
    base_run = {
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
    return {
        "job_name": "example_job",
        "run_num": 1,
        "run_time": None,
        "time_zone": None,
        "node_name": "example_node",
        "cleanup_run": None,
        "manual": False,
        "runs": [base_run] * 5000,  # gives us a ~2000KB JobRun, ensuring at least a few partitions.
    }


@pytest.mark.usefixtures("store", "job_state_object", "small_jobrun_state_object", "large_jobrun_state_object")
class TestDynamoDBStateStore:
    @pytest.mark.parametrize(
        "object_fixture_name",
        ["small_jobrun_state_object", "large_jobrun_state_object"],
    )
    def test_save_and_restore(self, store, object_fixture_name, request):
        """Verify that objects of different sizes can be saved and restored correctly."""
        state_object = request.getfixturevalue(object_fixture_name)
        key = store.build_key("job_run_state", "test_jobrun")
        key_value_pairs = [(key, state_object)]

        store.save(key_value_pairs)
        store._consume_save_queue()

        restored_vals = store.restore([key])
        assert store.save_errors == 0
        assert restored_vals[key] == state_object

        num_partitions = store._get_num_of_partitions(key)
        if object_fixture_name == "small_jobrun_state_object":
            assert num_partitions == 1
        else:
            # For large objects, we expect multiple partitions, the number of partitions can vary depending on our object size
            # and the DynamoDB partition size limit. We assert that there is more than one partition.
            assert num_partitions > 1

    def test_delete_if_val_is_none(self, store, small_jobrun_state_object):
        """We have a hacky delete path that deletes items that are added to the save queue with a value of None."""
        key_to_keep = store.build_key("job_run_state", "keep")
        key_to_delete = store.build_key("job_run_state", "delete")

        store.save(
            [
                (key_to_keep, small_jobrun_state_object),
                (key_to_delete, small_jobrun_state_object),
            ]
        )
        store._consume_save_queue()

        store.save([(key_to_delete, None)])
        store._consume_save_queue()

        restored_vals = store.restore([key_to_keep, key_to_delete])
        assert store.save_errors == 0
        assert restored_vals == {key_to_keep: small_jobrun_state_object}
        assert key_to_delete not in restored_vals

    def test_delete_item_with_partitions(self, store, large_jobrun_state_object):
        """Verify that _delete_item cleans up all partitions of a large object."""
        key = store.build_key("job_run_state", "test_jobrun_large")
        store.save([(key, large_jobrun_state_object)])
        store._consume_save_queue()
        assert store._get_num_of_partitions(key) > 1

        store._delete_item(key)

        assert store._get_num_of_partitions(key) == 0
        vals = store.restore([key])
        assert key not in vals

    @pytest.mark.parametrize(
        "object_fixture, side_effects, expected_save_errors, expected_queue_len",
        [
            # All attempts fail for a small object
            ("small_jobrun_state_object", [Exception("FAIL")] * 3, 3, 1),
            # All attempts fail for a large object
            ("large_jobrun_state_object", [Exception("FAIL")] * 3, 3, 1),
            # Failure followed by success
            ("small_jobrun_state_object", [Exception("FAIL"), {}], 0, 0),
            # Failure followed by many successes (for large object partitions)
            ("large_jobrun_state_object", [Exception("FAIL")] + [{}] * 10, 0, 0),
        ],
    )
    def test_retry_saving(
        self,
        object_fixture,
        side_effects,
        expected_save_errors,
        expected_queue_len,
        store,
        request,
    ):
        """Verify that failed save operations are retried."""
        state_object = request.getfixturevalue(object_fixture)
        key = store.build_key("job_state", "retried_job")

        with mock.patch.object(store.client, "transact_write_items", side_effect=side_effects) as mock_transact:
            store.save([(key, state_object)])
            # Consume the queue for each potential attempt
            for _ in side_effects:
                store._consume_save_queue()

            assert mock_transact.called
            assert store.save_errors == expected_save_errors
            assert len(store.save_queue) == expected_queue_len

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

    def test_retry_reading(self, store, table_name):
        """Verify that reading from DynamoDB retries when there are unprocessed keys."""
        key = store.build_key("job_state", 0)
        unprocessed_keys_response = {
            "Responses": {},
            "UnprocessedKeys": {
                table_name: {
                    "Keys": [{"key": {"S": key}, "index": {"N": "0"}}],
                    "ConsistentRead": True,
                }
            },
        }

        with mock.patch.object(
            store.client, "batch_get_item", return_value=unprocessed_keys_response
        ) as mock_batch_get, mock.patch("time.sleep") as mock_sleep, pytest.raises(KeyError) as exc_info:
            store.restore([key])

        assert "failed to retrieve items with keys" in str(exc_info.value)
        assert mock_batch_get.call_count == MAX_UNPROCESSED_KEYS_RETRIES
        assert mock_sleep.call_count == MAX_UNPROCESSED_KEYS_RETRIES

    def test_restore_exception_propagation(self, store, small_jobrun_state_object):
        """Verify that restore propagates exceptions upwards: see DAR-2328"""
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
