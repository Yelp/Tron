import os
import mock
from testify import TestCase, assert_equal, setup, run

from tests.assertions import assert_raises
from tests.testingutils import autospec_method
from tron.config import schema
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
        config = schema.ConfigState(
            store_type='shelve', name=thefilename, buffer_size=0,
            connection_details=None)
        manager = PersistenceManagerFactory.from_config(config)
        store = manager._impl
        assert_equal(store.filename, config.name)
        assert isinstance(store, ShelveStateStore)
        os.unlink(thefilename)


class StateMetadataTestCase(TestCase):

    def test_validate_metadata(self):
        metadata = {'version': (0, 5, 2)}
        StateMetadata.validate_metadata(metadata)

    def test_validate_metadata_no_state_data(self):
        metadata = None
        StateMetadata.validate_metadata(metadata)

    def test_validate_metadata_mismatch(self):
        metadata = {'version': (200, 1, 1)}
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
        self.store = mock.Mock()
        self.store.build_key.side_effect = lambda t, i: '%s%s' % (t, i)
        self.buffer = StateSaveBuffer(1)
        self.manager = PersistentStateManager(self.store, self.buffer)

    def test__init__(self):
        assert_equal(self.manager._impl, self.store)

    def test_keys_for_items(self):
        names = ['namea', 'nameb']
        key_to_item_map = self.manager._keys_for_items('type', names)

        keys = ['type%s' % name for name in names]
        assert_equal(key_to_item_map, dict(zip(keys, names)))

    def test_restore_dicts(self):
        names = ['namea', 'nameb']
        autospec_method(self.manager._keys_for_items)
        self.manager._keys_for_items.return_value = dict(enumerate(names))
        self.store.restore.return_value = {
            0: {'state': 'data'}, 1: {'state': '2data'}}
        state_data = self.manager._restore_dicts('type', names)
        expected = {
            names[0]: {'state': 'data'},
            names[1]: {'state': '2data'}}
        assert_equal(expected, state_data)

    def test_save_job(self):
        mock_job = mock.Mock()
        self.manager.save_job(mock_job)
        key = '%s%s' % (runstate.JOB_STATE, mock_job.name)
        self.store.save.assert_called_with([(key, mock_job.state_data)])

    def test_save_service(self):
        mock_service = mock.Mock()
        self.manager.save_service(mock_service)
        key = '%s%s' % (runstate.SERVICE_STATE, mock_service.name)
        self.store.save.assert_called_with([(key, mock_service.state_data)])

    def test_save_metadata(self):
        patcher = mock.patch('tron.serialize.runstate.statemanager.StateMetadata')
        with patcher as mock_state_metadata:
            self.manager.save_metadata()
            meta_data = mock_state_metadata.return_value
            expected_key = '%s%s' % (runstate.MCP_STATE, meta_data.name)
            expected_data = meta_data.state_data
            self.store.save.assert_called_with([(expected_key, expected_data)])

    def test_save_failed(self):
        self.store.save.side_effect = PersistenceStoreError("blah")
        assert_raises(PersistenceStoreError, self.manager._save, None, mock.Mock())

    def test_save_while_disabled(self):
        with self.manager.disabled():
            self.manager._save("something", StateMetadata())
        assert not self.store.save.mock_calls

    def test_cleanup(self):
        self.manager.cleanup()
        self.store.cleanup.assert_called_with()

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

    def test_disabled_nested(self):
        self.manager.enabled = False
        with self.manager.disabled():
            pass
        assert not self.manager.enabled


if __name__ == "__main__":
    run()