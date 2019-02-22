import pickle

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron.serialize.runstate.DynamoDBStateStore import DynamoDBStateStore


class TestDynamoDBStateStore(TestCase):
    @setup
    def setup_store(self):
        self.store = DynamoDBStateStore('DynamoDBTest')
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

    def test_delete(self):
        keys = [self.store.build_key("thing", i) for i in range(3)]
        value = self.large_object
        for key in keys:
            self.store[key] = value

        for key in keys:
            self.store._delete_item(key)

        for key in keys:
            assert_equal(self.store._get_num_of_partitions(key), 0)


if __name__ == "__main__":
    run()
