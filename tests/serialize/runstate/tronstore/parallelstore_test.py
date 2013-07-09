import contextlib
import mock
from testify import TestCase, run, setup_teardown, assert_equal
from tron.serialize import runstate
from tron.serialize.runstate.tronstore.parallelstore import ParallelStore, ParallelKey
from tron.serialize.runstate.tronstore import msg_enums


class ParallelStoreTestCase(TestCase):

    @setup_teardown
    def setup_store(self):
        self.config = mock.Mock(
            name='test_config',
            transport_method='pickle',
            store_type='shelve',
            connection_details=None,
            db_store_method=None,
            buffer_size=1
        )
        with contextlib.nested(
            mock.patch('twisted.internet.reactor.spawnProcess', autospec=True),
            mock.patch('twisted.internet.reactor.run', autospec=True),
            mock.patch('tron.serialize.runstate.tronstore.parallelstore.StoreProcessProtocol', autospec=True)
        ) as (self.spawn_patch, self.run_patch, self.process_patch):
            self.store = ParallelStore(self.config)
            yield

    def test__init__(self):
        self.process_patch.assert_called_once_with(self.store.response_factory)

    def test_start_process(self):
        self.spawn_patch.assert_called_once_with(
            self.store.process,
            "serialize/runstate/tronstore/tronstore",
            ["tronstore",
            self.config.name,
            self.config.transport_method,
            self.config.store_type,
            self.config.connection_details,
            self.config.db_store_method])
        self.run_patch.assert_called_once_with()

    def test_build_key(self):
        key_type = runstate.JOB_STATE
        key_name = 'the_fun_ends_here'
        assert_equal(self.store.build_key(key_type, key_name), ParallelKey(key_type, key_name))

    def test_save(self):
        key_value_pairs = [
            (self.store.build_key(runstate.JOB_STATE, 'riki_the_pubstar'),
                {'butterfly': 'time_to_buy_mkb'}),
            (self.store.build_key(runstate.JOB_STATE, 'you_died_30_seconds_in'),
                {'it_was_lag': 'i_swear'})
        ]
        with mock.patch.object(self.store.request_factory, 'build') as build_patch:
            self.store.save(key_value_pairs)
            for key, state_data in key_value_pairs:
                build_patch.assert_any_call(msg_enums.REQUEST_SAVE, key.type, (key.key, state_data))
                assert self.store.process.send_request.called

    def test_restore_single_success(self):
        key = self.store.build_key(runstate.JOB_STATE, 'zeus_ult')
        fake_response = mock.Mock(data=10, successful=True)
        with contextlib.nested(
            mock.patch.object(self.store.request_factory, 'build'),
            mock.patch.object(self.store.process, 'send_request_get_response', return_value=fake_response),
        ) as (build_patch, send_patch):
            assert_equal(self.store.restore_single(key), fake_response.data)
            build_patch.assert_called_once_with(msg_enums.REQUEST_RESTORE, key.type, key.key)
            assert send_patch.called

    def test_restore_single_failure(self):
        key = self.store.build_key(runstate.JOB_STATE, 'rip_ryan_davis')
        fake_response = mock.Mock(data=777, successful=False)
        with contextlib.nested(
            mock.patch.object(self.store.request_factory, 'build'),
            mock.patch.object(self.store.process, 'send_request_get_response', return_value=fake_response),
        ) as (build_patch, send_patch):
            assert not self.store.restore_single(key)
            build_patch.assert_called_once_with(msg_enums.REQUEST_RESTORE, key.type, key.key)
            assert send_patch.called

    def test_restore(self):
        keys = [self.store.build_key(runstate.JOB_STATE, 'true_steel'),
                self.store.build_key(runstate.JOB_STATE, 'the_test')]
        fake_response = mock.Mock()
        response_dict = dict((key, fake_response) for key in keys)
        with mock.patch.object(self.store, 'restore_single', return_value=fake_response) as restore_patch:
            assert_equal(self.store.restore(keys), response_dict)
            for key in keys:
                restore_patch.assert_any_call(key)

    def test_cleanup(self):
        with mock.patch.object(self.store, 'cleanup') as clean_patch:
            self.store.cleanup()
            clean_patch.assert_called_once_with()

    def test_load_config(self):
        new_config = mock.Mock()
        with contextlib.nested(
            mock.patch.object(self.store.request_factory, 'update_method'),
            mock.patch.object(self.store.response_factory, 'update_method'),
            mock.patch.object(self.store.process, 'shutdown'),
            mock.patch.object(self.store, 'start_process'),
            mock.patch('tron.serialize.runstate.tronstore.process.StoreProcessProtocol', autospec=True)
        ) as (request_patch, response_patch, shutdown_patch, start_patch, process_patch):
            self.store.load_config(new_config)
            request_patch.assert_called_once_with(new_config.transport_method)
            response_patch.assert_called_once_with(new_config.transport_method)
            shutdown_patch.assert_called_once_with()
            self.process_patch.assert_any_call(self.store.response_factory)
            start_patch.assert_called_once_with()
            assert_equal(self.store.config, new_config)
