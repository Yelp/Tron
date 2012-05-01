import os
import shelve
import tempfile
from testify import TestCase, run, setup, assert_equal
from testify import teardown
from tron.serialize.runstate.shelvestore import ShelveStateStore, ShelveKey


class ShelveStateStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'state')
        self.store = ShelveStateStore(self.filename)

    @teardown
    def teardown_store(self):
        os.unlink(self.filename)

    def test__init__(self):
        assert_equal(self.filename, self.store.filename)

    def test_save(self):
        key_value_pairs = [
            (ShelveKey("one", "two"), {'this': 'data'}),
            (ShelveKey("three", "four"), {'this': 'data2'})
        ]
        self.store.save(key_value_pairs)
        self.store.cleanup()

        stored_data = shelve.open(self.filename)
        for key, value in key_value_pairs:
            assert_equal(stored_data[key.key], value)

    def test_restore(self):
        keys = [ShelveKey("thing", i) for i in xrange(5)]
        value = {'this': 'data'}
        store = shelve.open(self.filename)
        for key in keys:
            store[key.key] = value
        store.close()

        retrieved_data = self.store.restore(keys)
        for key in keys:
            assert_equal(retrieved_data[key], value)


if __name__ == "__main__":
    run()