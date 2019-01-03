from __future__ import absolute_import
from __future__ import unicode_literals

import os
import shutil
import tempfile

import mock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_raises
from tests.testingutils import autospec_method
from tron.config import schema
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.statemanager import PersistenceManagerFactory
from tron.serialize.runstate.statemanager import PersistenceStoreError
from tron.serialize.runstate.statemanager import PersistentStateManager
from tron.serialize.runstate.statemanager import StateChangeWatcher
from tron.serialize.runstate.statemanager import StateMetadata
from tron.serialize.runstate.statemanager import StateSaveBuffer
from tron.serialize.runstate.statemanager import VersionMismatchError


class TestPersistenceManagerFactory(TestCase):
    def test_from_config_shelve(self):
        tmpdir = tempfile.mkdtemp()
        try:
            fname = os.path.join(tmpdir, 'state')
            config = schema.ConfigState(
                store_type='shelve',
                name=fname,
                buffer_size=0,
                connection_details=None,
            )
            manager = PersistenceManagerFactory.from_config(config)
            store = manager._impl
            assert_equal(store.filename, config.name)
            assert isinstance(store, ShelveStateStore)
        finally:
            shutil.rmtree(tmpdir)


class TestStateMetadata(TestCase):
    def test_validate_metadata(self):
        metadata = {'version': (0, 5, 2)}
        StateMetadata.validate_metadata(metadata)

    def test_validate_metadata_no_state_data(self):
        metadata = None
        StateMetadata.validate_metadata(metadata)

    def test_validate_metadata_mismatch(self):
        metadata = {'version': (200, 1, 1)}
        assert_raises(
            VersionMismatchError,
            StateMetadata.validate_metadata,
            metadata,
        )


class TestStateSaveBuffer(TestCase):
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
        assert_equal(items, [(1, 2), (2, 3)])


class TestPersistentStateManager(TestCase):
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
            0: {
                'state': 'data',
            },
            1: {
                'state': '2data',
            },
        }
        state_data = self.manager._restore_dicts('type', names)
        expected = {
            names[0]: {
                'state': 'data',
            },
            names[1]: {
                'state': '2data',
            },
        }
        assert_equal(expected, state_data)

    def test_save(self):
        name, state_data = 'name', mock.Mock()
        self.manager.save(runstate.JOB_STATE, name, state_data)
        key = '%s%s' % (runstate.JOB_STATE, name)
        self.store.save.assert_called_with([(key, state_data)])

    def test_save_failed(self):
        self.store.save.side_effect = PersistenceStoreError("blah")
        assert_raises(
            PersistenceStoreError,
            self.manager.save,
            None,
            None,
            None,
        )

    def test_save_while_disabled(self):
        with self.manager.disabled():
            self.manager.save("something", 'name', mock.Mock())
        assert not self.store.save.mock_calls

    def test_check_consistency(self):
        state_data = mock.Mock()
        state = {runstate.JOB_STATE: {'name': state_data}}
        key = f"{runstate.JOB_STATE}name"

        self.manager._check_consistency(state)

        assert self.store.save.call_count == 1
        assert self.store.save.call_args == mock.call([(key, state_data)])

    def test_check_consistency_failed(self):
        state = {runstate.JOB_STATE: {'name': mock.Mock()}}

        with mock.patch.object(
            self.store,
            'save',
            side_effect=PersistenceStoreError,
            autospec=None,
        ):
            assert_raises(
                PersistenceStoreError,
                self.manager._check_consistency,
                state
            )

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


class TestStateChangeWatcher(TestCase):
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

    @mock.patch(
        'tron.serialize.runstate.statemanager.PersistenceManagerFactory',
        autospec=True,
    )
    def test_update_from_config_changed(self, mock_factory):
        state_config = mock.Mock()
        autospec_method(self.watcher.shutdown)
        assert self.watcher.update_from_config(state_config)
        assert_equal(self.watcher.config, state_config)
        self.watcher.shutdown.assert_called_with()
        assert_equal(
            self.watcher.state_manager,
            mock_factory.from_config.return_value,
        )
        mock_factory.from_config.assert_called_with(state_config)

    def test_save_job(self):
        mock_job = mock.Mock()
        self.watcher.save_job(mock_job)
        self.watcher.state_manager.save.assert_called_with(
            runstate.JOB_STATE,
            mock_job.name,
            mock_job.state_data,
        )

    @mock.patch(
        'tron.serialize.runstate.statemanager.StateMetadata', autospec=None
    )
    def test_save_metadata(self, mock_state_metadata):
        self.watcher.save_metadata()
        meta_data = mock_state_metadata.return_value
        self.watcher.state_manager.save.assert_called_with(
            runstate.MCP_STATE,
            meta_data.name,
            meta_data.state_data,
        )

    def test_shutdown(self):
        self.watcher.shutdown()
        assert not self.watcher.state_manager.enabled
        self.watcher.state_manager.cleanup.assert_called_with()

    def test_disabled(self):
        context = self.watcher.disabled()
        assert_equal(self.watcher.state_manager.disabled.return_value, context)

    def test_restore(self):
        jobs = mock.Mock()
        self.watcher.restore(jobs)
        self.watcher.state_manager.restore.assert_called_with(jobs)


if __name__ == "__main__":
    run()
