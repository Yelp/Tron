import os
import pickle
import tempfile

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron.serialize.runstate.DynamoDBStateStore import DynamoDBStateStore
from tron.serialize.runstate.shelvestore import Py2Shelf


class TestDynamoDBStateStore(TestCase):
    @setup
    def setup_store(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tmpdir, 'state')
        self.store = DynamoDBStateStore(self.filename)
        self.large_object = pickle.dumps([i for i in range(100000)])
        self.small_object = pickle.dumps({'this': 'data'})

    def test_save(self):
        key_value_pairs = [
            (
                self.store.build_key("DynamoDBTest", "two"),
                self.small_object,
            ),
            (
                self.store.build_key("DynamoDBTest2", "four"),
                self.small_object,
            ),
        ]
        self.store.save(key_value_pairs)

        for key, value in key_value_pairs:
            assert_equal(self.store[key], value)

        for key, value in key_value_pairs:
            self.store._delete_item(key)
        self.store.cleanup()

    def test_save_more_than_4KB(self):
        key_value_pairs = [
            (
                self.store.build_key("DynamoDBTest", "two"),
                self.large_object
            )
        ]
        self.store.save(key_value_pairs)

        for key, value in key_value_pairs:
            assert_equal(self.store[key], value)

        for key, value in key_value_pairs:
            self.store._delete_item(key)
        self.store.cleanup()

    def test_restore_more_than_4KB(self):
        keys = [self.store.build_key("thing", i) for i in range(3)]
        value = self.large_object
        for key in keys:
            self.store[key] = value

        vals = self.store.restore(keys)
        for key in keys:
            assert_equal(vals[key], pickle.loads(value))

        for key in keys:
            self.store._delete_item(key)
        self.store.cleanup()

    def test_restore(self):
        keys = [self.store.build_key("thing", i) for i in range(3)]
        value = self.small_object
        for key in keys:
            self.store[key] = value

        vals = self.store.restore(keys)
        for key in keys:
            assert_equal(vals[key], pickle.loads(value))

        for key in keys:
            self.store._delete_item(key)
        self.store.cleanup()

    def test_delete(self):
        keys = [self.store.build_key("thing", i) for i in range(3)]
        value = self.large_object
        for key in keys:
            self.store[key] = value

        for key in keys:
            self.store._delete_item(key)

        for key in keys:
            assert_equal(self.store._get_num_of_partitions(key), 0)
        self.store.cleanup()

    def test_saved_to_both(self):
        key_value_pairs = [
            (
                self.store.build_key("DynamoDBTest", "two"),
                self.large_object
            )
        ]
        self.store.save(key_value_pairs)

        stored_data = Py2Shelf(self.filename)
        for key, value in key_value_pairs:
            assert_equal(self.store[key], value)
            assert_equal(stored_data[str(key.key)], value)
        self.store.cleanup()

    def test_restore_from_shelve_after_dynamodb_dies(self):
        key_value_pairs = [
            (
                self.store.build_key("DynamoDBTest", "two"),
                self.large_object
            )
        ]
        self.store.save(key_value_pairs)

        keys = [k for k, v in key_value_pairs]
        # This only cleans up data in dynamoDB
        for key in keys:
            self.store._delete_item(key)

        retrieved_data = self.store.restore(keys)
        for key in keys:
            assert_equal(retrieved_data[key], self.large_object)
        self.store.cleanup()


if __name__ == "__main__":
    run()
