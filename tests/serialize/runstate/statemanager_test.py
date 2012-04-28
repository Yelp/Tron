import os
from testify import TestCase, assert_equal, setup, run

from tests.assertions import assert_raises, assert_call, assert_length
from tests.testingutils import Turtle
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.statemanager import PersistentStateManager
from tron.serialize.runstate.statemanager import StateMetadata
from tron.serialize.runstate.statemanager import PersistenceStoreError
from tron.serialize.runstate.statemanager import VersionMismatchError
from tron.serialize.runstate.statemanager import PersistenceManagerFactory


class PersistenceManagerFactoryTestCase(TestCase):

    def test_from_config_shelve(self):
        thefilename = 'thefilename'
        config = Turtle(store_type='shelve', name=thefilename)
        manager = PersistenceManagerFactory.from_config(config)
        store = manager._impl
        assert_equal(store.filename, config.name)
        assert isinstance(store, ShelveStateStore)
        os.unlink(thefilename)


class PersistentStateManagerTestCase(TestCase):

    @setup
    def setup_manager(self):
        self.store = Turtle()
        self.manager = PersistentStateManager(self.store)

    def test__init__(self):
        assert_equal(self.manager._impl, self.store)
        assert_equal(self.manager.metadata_key, self.store.build_key.returns[0])

    def test_keys_for_items(self):
        items = [Turtle(), Turtle()]
        pairs = self.manager._keys_for_items('type', items)

        # Skip first return, its from the constructor
        assert_equal(pairs, dict(zip(self.store.build_key.returns[1:], items)))

    def test_validate_version(self):
        self.store.restore = lambda k: [{'version': (0, 1, 1)}]
        self.manager._validate_version()

    def test_validate_version_no_state_data(self):
        self.store.restore = lambda k: []
        self.manager._validate_version()

    def test_validate_version_mismatch(self):
        self.store.restore = lambda k: [{'version': (200, 1, 1)}]
        assert_raises(VersionMismatchError, self.manager._validate_version)

    def test_restore_dicts(self):
        items = [Turtle(), Turtle()]
        self.manager._keys_for_items = lambda t, i: {'1': items[0], '2': items[1]}
        self.store.restore = lambda keys: [
            ('1', {'state': 'data'}), ('2', {'state': '2data'})
        ]
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
        assert_equal(state_data['version'], self.manager.version)

    def test_save_failed(self):
        def err(_k, _d):
            raise PersistenceStoreError("blah")
        self.store.save = err
        assert_raises(PersistenceStoreError, self.manager._save, None, Turtle())

    def test_save_while_disabled(self):
        with self.manager.disabled():
            self.manager._save("something", StateMetadata({}))
        assert_length(self.store.save.calls, 0)

    def test_cleanup(self):
        self.manager.cleanup()
        assert_call(self.store.cleanup, 0)


if __name__ == "__main__":
    run()