import os
import mock
import contextlib
from testify import TestCase, assert_equal, setup, run, setup_teardown

from tests.assertions import assert_raises
from tests.testingutils import autospec_method
from tron.config import schema
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.statemanager import PersistentStateManager, StateChangeWatcher
from tron.serialize.runstate.statemanager import StateSaveBuffer
from tron.serialize.runstate.statemanager import StateMetadata
from tron.serialize.runstate.statemanager import PersistenceStoreError
from tron.serialize.runstate.statemanager import VersionMismatchError
from tron.serialize.runstate.statemanager import PersistenceManagerFactory
from tron.serialize.runstate.statemanager import NullStateManager


class PersistenceManagerFactoryTestCase(TestCase):

    @setup_teardown
    def setup_factory_and_enumerate(self):
        self.mock_buffer_size = 25
        self.mock_config = mock.Mock(buffer_size=self.mock_buffer_size)
        with contextlib.nested(
            mock.patch('tron.serialize.runstate.statemanager.ParallelStore',
                autospec=True),
            mock.patch('%s.%s' % (PersistentStateManager.__module__, PersistentStateManager.__name__),
                autospec=True),
            mock.patch('%s.%s' % (StateSaveBuffer.__module__, StateSaveBuffer.__name__),
                autospec=True)
        ) as (self.parallel_patch, self.state_patch, self.buffer_patch):
            yield

    def test_from_config_all_valid_enum_types(self):
        for store_type in schema.StatePersistenceTypes.values:
            self.mock_config.configure_mock(store_type=store_type)
            for transport_method in schema.StateTransportTypes.values:
                self.mock_config.configure_mock(transport_method=transport_method)
                if store_type in ('sql', 'mongo'):
                    self.mock_config.configure_mock(db_store_method=transport_method)

                assert_equal(PersistenceManagerFactory.from_config(self.mock_config),
                    self.state_patch(self.parallel_patch, self.buffer_patch))
                self.parallel_patch.assert_called_with(self.mock_config)
                self.buffer_patch.assert_called_with(self.mock_buffer_size)

    def test_from_config_invalid_store_type(self):
        self.mock_config.configure_mock(store_type='play_the_game')
        for transport_method in schema.StateTransportTypes.values:
            self.mock_config.configure_mock(transport_method=transport_method)
            assert_raises(PersistenceStoreError, PersistenceManagerFactory.from_config, self.mock_config)

    def test_from_config_invalid_transport_type(self):
        self.mock_config.configure_mock(transport_method='ghosts_cant_eat')
        for store_type in schema.StatePersistenceTypes.values:
            self.mock_config.configure_mock(store_type=store_type)
            if store_type in ('sql', 'mongo'):
                self.mock_config.configure_mock(db_store_method='json')
            assert_raises(PersistenceStoreError, PersistenceManagerFactory.from_config, self.mock_config)

    def test_from_config_invalid_db_store_method(self):
        self.mock_config.configure_mock(db_store_method='im_running_out_of_strs')
        for store_type in ('sql', 'mongo'):
            self.mock_config.configure_mock(store_type=store_type)
            for transport_method in schema.StateTransportTypes.values:
                self.mock_config.configure_mock(transport_method=transport_method)
                assert_raises(PersistenceStoreError, PersistenceManagerFactory.from_config, self.mock_config)


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

    def test_save(self):
        name, state_data = 'name', mock.Mock()
        self.manager.save(runstate.JOB_STATE, name, state_data)
        key = '%s%s' % (runstate.JOB_STATE, name)
        self.store.save.assert_called_with([(key, state_data)])

    def test_save_failed(self):
        self.store.save.side_effect = PersistenceStoreError("blah")
        assert_raises(PersistenceStoreError, self.manager.save, None, None, None)

    def test_save_while_disabled(self):
        with self.manager.disabled():
            self.manager.save("something", 'name', mock.Mock())
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

    def test_update_config(self):
        new_config = mock.Mock()
        fake_return = 'reelin_in_the_years'
        self.store.load_config.configure_mock(return_value=fake_return)
        with mock.patch.object(self.manager, '_save_from_buffer') as save_patch:
            assert_equal(self.manager.update_from_config(new_config), fake_return)
            save_patch.assert_called_once_with()
            self.store.load_config.assert_called_once_with(new_config)


class StateChangeWatcherTestCase(TestCase):

    @setup
    def setup_watcher(self):
        self.watcher = StateChangeWatcher()
        self.state_manager = mock.create_autospec(PersistentStateManager)
        self.watcher.state_manager = self.state_manager

    def test_update_from_config_no_change(self):
        self.watcher.config = state_config = mock.Mock()
        assert not self.watcher.update_from_config(state_config)
        autospec_method(self.watcher.shutdown)
        assert_equal(self.watcher.state_manager, self.state_manager)
        assert not self.watcher.shutdown.mock_calls

    @mock.patch('tron.serialize.runstate.statemanager.PersistenceManagerFactory',
    autospec=True)
    def test_update_from_config_no_state_manager(self, mock_factory):
        state_config = mock.Mock()
        self.watcher.state_manager = NullStateManager
        assert self.watcher.update_from_config(state_config)
        assert_equal(self.watcher.config, state_config)
        assert_equal(self.watcher.state_manager,
            mock_factory.from_config.return_value)
        mock_factory.from_config.assert_called_with(state_config)

    def test_update_from_config_with_state_manager_success(self):
        state_config = mock.Mock()
        assert self.watcher.update_from_config(state_config)
        assert_equal(self.watcher.config, state_config)
        self.state_manager.update_from_config.assert_called_once_with(state_config)

    def test_update_from_config_failure(self):
        self.state_manager.update_from_config.configure_mock(return_value=False)
        state_config = self.watcher.config
        fake_config = mock.Mock()
        assert not self.watcher.update_from_config(fake_config)
        assert_equal(self.watcher.config, state_config)
        self.state_manager.update_from_config.assert_called_once_with(fake_config)

    def test_save_job(self):
        mock_job = mock.Mock()
        self.watcher.save_job(mock_job)
        self.watcher.state_manager.save.assert_called_with(
            runstate.JOB_STATE, mock_job.id, mock_job.state_data)

    def test_save_service(self):
        mock_service = mock.Mock()
        self.watcher.save_service(mock_service)
        self.watcher.state_manager.save.assert_called_with(
            runstate.SERVICE_STATE, mock_service.id, mock_service.state_data)

    def test_save_metadata(self):
        patcher = mock.patch('tron.serialize.runstate.statemanager.StateMetadata')
        with patcher as mock_state_metadata:
            self.watcher.save_metadata()
            meta_data = mock_state_metadata.return_value
            self.watcher.state_manager.save.assert_called_with(
                runstate.MCP_STATE, meta_data.id, meta_data.state_data)

    def test_shutdown(self):
        self.watcher.shutdown()
        assert not self.watcher.state_manager.enabled
        self.watcher.state_manager.cleanup.assert_called_with()

    def test_disabled(self):
        context = self.watcher.disabled()
        assert_equal(self.watcher.state_manager.disabled.return_value, context)

    def test_restore(self):
        jobs, services = mock.Mock(), mock.Mock()
        self.watcher.restore(jobs, services)
        self.watcher.state_manager.restore.assert_called_with(jobs, services)



if __name__ == "__main__":
    run()
