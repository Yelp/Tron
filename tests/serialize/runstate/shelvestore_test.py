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
        key = ShelveKey("one", "two")
        value = {'this': 'data'}
        self.store.save(key, value)
        self.store.cleanup()

        stored_data = shelve.open(self.filename)
        assert_equal(stored_data[key.key], value)

    def test_restore(self):
        key = ShelveKey("three", "four")
        value = {'this': 'data'}
        store = shelve.open(self.filename)
        store[key.key] = value
        store.close()

        retrieved_data = self.store.restore([key])
        assert_equal(retrieved_data[0], (key, value))


if __name__ == "__main__":
    run()