import os
from testify import TestCase, assert_equal, setup, run

from tests.assertions import assert_raises, assert_call, assert_length
from tests.testingutils import Turtle
import tron
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.statemanager import PersistentStateManager
from tron.serialize.runstate.statemanager import StateSaveBuffer
from tron.serialize.runstate.statemanager import StateMetadata
from tron.serialize.runstate.statemanager import PersistenceStoreError
from tron.serialize.runstate.statemanager import VersionMismatchError
from tron.serialize.runstate.statemanager import PersistenceManagerFactory


class PersistenceManagerFactoryTestCase(TestCase):

    def test_from_config_shelve(self):
        thefilename = 'thefilename'
        config = Turtle(store_type='shelve', name=thefilename, buffer_size=0)
        manager = PersistenceManagerFactory.from_config(config)
        store = manager._impl
        assert_equal(store.filename, config.name)
        assert isinstance(store, ShelveStateStore)
        os.unlink(thefilename)


class StateMetadataTestCase(TestCase):

    def test_validate_metadata(self):
        metadata = [{'version': (0, 1, 1)}]
        StateMetadata.validate_metadata(metadata)

    def test_validate_metadata_no_state_data(self):
        metadata = []
        StateMetadata.validate_metadata(metadata)

    def test_validate_metadata_mismatch(self):
        metadata = [{'version': (200, 1, 1)}]
        assert_raises(
                VersionMismatchError, StateMetadata.validate_metadata, metadata)


class StateSaveBufferTestCase(TestCase):

    @setup
    def setup_buffer(self):
        self.buffer_size = 5
        self.buffer = StateSaveBuffer(self.buffer_size)

    def test_save(self):
        assert self.buffer.save(1, 2)
        assert not self.buffer.save(1, 3)
        assert not self.buffer.save(1, 4)
        assert not self.buffer.save(1, 5)
        assert not self.buffer.save(1, 6)
        assert self.buffer.save(1, 7)
        assert_equal(self.buffer.buffer[1], 7)

    def test__iter__(self):
        self.buffer.save(1, 2)
        self.buffer.save(2, 3)
        items = list(self.buffer)
        assert not self.buffer.buffer
        assert_equal(items, [(1,2), (2,3)])


class PersistentStateManagerTestCase(TestCase):

    @setup
    def setup_manager(self):
        self.store = Turtle()
        self.buffer = StateSaveBuffer(1)
        self.manager = PersistentStateManager(self.store, self.buffer)

    def test__init__(self):
        assert_equal(self.manager._impl, self.store)
        assert_equal(self.manager.metadata_key, self.store.build_key.returns[0])

    def test_keys_for_items(self):
        items = [Turtle(), Turtle()]
        key_to_item_map = self.manager._keys_for_items('type', items)

        # Skip first return, its from the constructor
        assert_equal(key_to_item_map,
                dict(zip(self.store.build_key.returns[1:], items)))

    def test_restore_dicts(self):
        items = [Turtle(), Turtle()]
        self.manager._keys_for_items = lambda t, i: {'1': items[0], '2': items[1]}
        self.store.restore = lambda keys: {
            '1': {'state': 'data'}, '2': {'state': '2data'}
        }
        state_data = self.manager._restore_dicts('type', items)
        expected = {
            items[0].name: {'state': 'data'},
            items[1].name: {'state': '2data'}
        }
        assert_equal(expected, state_data)

    def test_save_job(self):
        job = Turtle()
        self.manager.save_job(job)
        assert_call(self.store.build_key, 1, runstate.JOB_STATE, job.name)
        assert_call(
            self.store.save, 0, self.store.build_key.returns[1], job.state_data)

    def test_save_service(self):
        service = Turtle()
        self.manager.save_service(service)
        assert_call(self.store.build_key, 1, runstate.SERVICE_STATE, service.name)
        assert_call(self.store.save, 0,
                self.store.build_key.returns[1], service.state_data)

    def test_save_metadata(self):
        self.manager.save_metadata()
        assert_call(self.store.build_key, 1,
                runstate.MCP_STATE, StateMetadata.name)

        key, state_data = self.store.save.calls[0][0]
        assert_equal(key, self.store.build_key.returns[1])
        assert_equal(state_data['version'], tron.__version_info__)

    def test_save_failed(self):
        def err(_k, _d):
            raise PersistenceStoreError("blah")
        self.store.save = err
        assert_raises(PersistenceStoreError, self.manager._save, None, Turtle())

    def test_save_while_disabled(self):
        with self.manager.disabled():
            self.manager._save("something", StateMetadata())
        assert_length(self.store.save.calls, 0)

    def test_cleanup(self):
        self.manager.cleanup()
        assert_call(self.store.cleanup, 0)

    def test_disabled(self):
        with self.manager.disabled():
            assert not self.manager.enabled
        assert self.manager.enabled

    def test_disabled_with_exception(self):
        def testfunc():
            with self.manager.disabled():
                raise ValueError()
        assert_raises(ValueError, testfunc)
        assert self.manager.enabled



if __name__ == "__main__":
    run()