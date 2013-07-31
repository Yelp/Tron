import os
import shelve
import tempfile
import mock
import contextlib
from testify import TestCase, run, setup, assert_equal, teardown
from tron.serialize.runstate.tronstore.store import ShelveStore, SQLStore, MongoStore, YamlStore, SyncStore, NullStore
from tron.serialize.runstate.tronstore.transport import JSONTransport
from tron.serialize import runstate


class ShelveStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'state')
        self.store = ShelveStore(self.filename, None, None)

    @teardown
    def teardown_store(self):
        os.unlink(self.filename)
        self.store.cleanup()

    def test__init__(self):
        assert_equal(self.filename, self.store.fname)

    def test_save(self):
        data_type = runstate.JOB_STATE
        key_value_pairs = [
            ("one", {'some': 'data'}),
            ("two", {'its': 'fake'})
        ]
        for key, value in key_value_pairs:
            self.store.save(key, value, data_type)
        self.store.cleanup()
        stored = shelve.open(self.filename)
        for key, value in key_value_pairs:
            assert_equal(stored['(%s__%s)' % (data_type, key)], value)

    def test_restore_success(self):
        data_type = runstate.JOB_STATE
        keys = ["three", "four"]
        value = {'some': 'data'}
        store = shelve.open(self.filename)
        for key in keys:
            store['(%s__%s)' % (data_type, key)] = value
        store.close()

        for key in keys:
            assert_equal((True, value), self.store.restore(key, data_type))

    def test_restore_failure(self):
        keys = ["nope", "theyre not there"]
        for key in keys:
            assert_equal((False, None), self.store.restore(key, 'data_type'))


class SQLStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        details = 'sqlite:///:memory:'
        self.store = SQLStore('name', details, JSONTransport)

    @teardown
    def teardown_store(self):
        self.store.cleanup()

    def test_create_engine(self):
        assert_equal(self.store.engine.url.database, ':memory:')

    def test_create_tables(self):
        assert self.store.job_state_table.name
        assert self.store.job_run_table.name
        assert self.store.service_table.name
        assert self.store.metadata_table.name

    def test_save(self):
        data_type = runstate.SERVICE_STATE
        key = 'dotes'
        state_data = {'the_true_victim_is': 'roshan'}
        self.store.save(key, state_data, data_type)

        rows = self.store.engine.execute(self.store.service_table.select())
        assert_equal(rows.fetchone(), ('dotes', self.store.serializer.serialize(state_data)))

    def test_restore_success(self):
        data_type = runstate.JOB_STATE
        key = '20minbf'
        state_data = {'ogre_magi': 'pure_skill'}

        self.store.save(key, state_data, data_type)
        assert_equal((True, state_data), self.store.restore(key, data_type))

    def test_restore_failure(self):
        data_type = runstate.JOB_RUN_STATE
        key = 'someone_get_gem'

        assert_equal((False, None), self.store.restore(key, data_type))


class MongoStoreTestCase(TestCase):

    store = None

    @setup
    def setup_store(self):
        import mock
        self.db_name = 'test_base'
        details = "hostname=localhost&port=5555"
        with mock.patch('pymongo.Connection', autospec=True):
            self.store = MongoStore(self.db_name, details, None)

    # Since we mocked the pymongo connection, a teardown isn't needed.

    def _create_doc(self, key, doc, data_type):
        import pymongo
        db = pymongo.Connection()[self.db_name]
        doc['_id'] = key
        db[self.store.TYPE_TO_COLLECTION_MAP[data_type]].save(doc)
        db.connection.disconnect()

    def test__init__(self):
        assert_equal(self.store.db_name, self.db_name)

    def test_parse_connection_details(self):
        details = "hostname=mongoserver&port=55555"
        params = self.store._parse_connection_details(details)
        assert_equal(params, {'hostname': 'mongoserver', 'port': '55555'})

    def test_parse_connection_details_with_user_creds(self):
        details = "hostname=mongoserver&port=55555&username=ted&password=sam"
        params = self.store._parse_connection_details(details)
        expected = {
            'hostname': 'mongoserver',
            'port':     '55555',
            'username': 'ted',
            'password': 'sam'}
        assert_equal(params, expected)

    def test_parse_connection_details_none(self):
        params = self.store._parse_connection_details(None)
        assert_equal(params, {})

    def test_parse_connection_details_empty(self):
        params = self.store._parse_connection_details("")
        assert_equal(params, {})

    def test_save(self):
        import mock
        collection = mock.Mock()
        key = 'gotta_have_that_dotes'
        state_data = {'skywrath_mage': 'more_like_early_game_page'}
        data_type = runstate.JOB_STATE
        with mock.patch.object(self.store, 'db',
            new={self.store.TYPE_TO_COLLECTION_MAP[data_type]:
                collection}
        ):
            self.store.save(key, state_data, data_type)
            state_data['_id'] = key
            collection.save.assert_called_once_with(state_data)

    def test_restore_success(self):
        import mock
        key = 'stop_feeding'
        state_data = {'0_and_7': 'only_10_minutes_in'}
        data_type = runstate.JOB_RUN_STATE
        collection = mock.Mock()
        collection.find_one = mock.Mock(return_value=state_data)
        with mock.patch.object(self.store, 'db',
            new={self.store.TYPE_TO_COLLECTION_MAP[data_type]:
                collection}
        ):
            assert_equal(self.store.restore(key, data_type), (True, state_data))
            collection.find_one.assert_called_once_with(key)

    def test_restore_failure(self):
        import mock
        key = 'gg_team_fed'
        data_type = runstate.SERVICE_STATE
        collection = mock.Mock()
        collection.find_one = mock.Mock(return_value=None)
        with mock.patch.object(self.store, 'db',
            new={self.store.TYPE_TO_COLLECTION_MAP[data_type]:
                collection}
        ):
            assert_equal(self.store.restore(key, data_type), (False, None))
            collection.find_one.assert_called_once_with(key)


class YamlStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'yaml_state')
        self.store = YamlStore(self.filename, None, None)
        self.test_data = {
            self.store.TYPE_MAPPING[runstate.JOB_STATE]: {'a': 1},
            self.store.TYPE_MAPPING[runstate.JOB_RUN_STATE]: {'b': 2},
            self.store.TYPE_MAPPING[runstate.SERVICE_STATE]: {'c': 3}
        }

    @teardown
    def teardown_store(self):
        try:
            os.unlink(self.filename)
        except OSError:
            pass

    def test_restore_success(self):
        import yaml
        with open(self.filename, 'w') as fh:
            yaml.dump(self.test_data, fh)
        self.store = YamlStore(self.filename, None, None)

        data_types = [runstate.JOB_STATE, runstate.JOB_RUN_STATE, runstate.SERVICE_STATE]
        for data_type in data_types:
            for key in self.test_data[self.store.TYPE_MAPPING[data_type]].keys():
                success, value = self.store.restore(key, data_type)
                assert success
                assert_equal(self.test_data[self.store.TYPE_MAPPING[data_type]][key], value)

    def test_restore_failure(self):
        assert_equal(self.store.restore('gg_stick_pro_build', runstate.JOB_STATE), (False, None))

    def test_save(self):
        import yaml
        job_data = {'euls_on_sk': 'sounds_legit'}
        run_data = {'phantom_cancer': 'needs_diffusal_level_2'}
        service_data = {'everyone_go_dagon': 'hey_look_we_won'}
        expected = {
            self.store.TYPE_MAPPING[runstate.JOB_STATE]: job_data,
            self.store.TYPE_MAPPING[runstate.JOB_RUN_STATE]: run_data,
            self.store.TYPE_MAPPING[runstate.SERVICE_STATE]: service_data,
        }
        self.store.save(job_data.keys()[0], job_data.values()[0], runstate.JOB_STATE)
        self.store.save(run_data.keys()[0], run_data.values()[0], runstate.JOB_RUN_STATE)
        self.store.save(service_data.keys()[0], service_data.values()[0], runstate.SERVICE_STATE)

        assert_equal(self.store.buffer, expected)
        with open(self.filename, 'r') as fh:
            actual = yaml.load(fh)
        assert_equal(actual, expected)


class SyncStoreTestCase(TestCase):

    @setup
    def setup_sync_store(self):
        self.fake_config = mock.Mock(
            name='we_must_be_swift_as_a_coursing_river',
            store_type='with_all_the_force_of_a_great_typhoon',
            connection_details='with_all_the_strength_of_a_raging_fire',
            db_store_method='mysterious_as_the_dark_side_of_the_moon')
        self.store_class = mock.Mock()
        with contextlib.nested(
            mock.patch.object(runstate.tronstore.store, 'build_store', return_value=self.store_class),
            mock.patch('tron.serialize.runstate.tronstore.store.Lock', autospec=True)
        ) as (self.build_patch, self.lock_patch):
            self.store = SyncStore(self.fake_config)
            self.lock = self.lock_patch.return_value

    def test__init__(self):
        self.lock_patch.assert_called_once_with()
        self.build_patch.assert_called_once_with(
            self.fake_config.name,
            self.fake_config.store_type,
            self.fake_config.connection_details,
            self.fake_config.db_store_method
        )
        assert_equal(self.store_class, self.store.store)

    def test__init__null_config(self):
        store = SyncStore(None)
        assert isinstance(store.store, NullStore)

    def test_save(self):
        fake_arg = 'catch_a_ride'
        fake_kwarg = 'no_refunds'
        self.store.save(fake_arg, fake_kwarg=fake_kwarg)
        self.lock.__enter__.assert_called_once_with()
        self.lock.__exit__.assert_called_once_with(None, None, None)
        self.store_class.save.assert_called_once_with(fake_arg, fake_kwarg=fake_kwarg)

    def test_restore(self):
        fake_arg = 'catch_a_ride'
        fake_kwarg = 'no_refunds'
        self.store.restore(fake_arg, fake_kwarg=fake_kwarg)
        self.lock.__enter__.assert_called_once_with()
        self.lock.__exit__.assert_called_once_with(None, None, None)
        self.store_class.restore.assert_called_once_with(fake_arg, fake_kwarg=fake_kwarg)

    def test_cleanup(self):
        self.store.cleanup()
        self.lock.__enter__.assert_called_once_with()
        self.lock.__exit__.assert_called_once_with(None, None, None)
        self.store_class.cleanup.assert_called_once_with()


if __name__ == "__main__":
    run()
