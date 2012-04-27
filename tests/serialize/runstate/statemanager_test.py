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
        config = Turtle(store_type='shelve', name='thefilename')
        manager = PersistenceManagerFactory.from_config(config)
        store = manager._impl
        assert_equal(store.filename, config.name)
        assert isinstance(store, ShelveStateStore)


class MockStateData(dict):
    name = 'mockstatedata'


class PersistentStateManagerTestCase(TestCase):

    @setup
    def setup_manager(self):
        self.restored = [MockStateData()]
        self.store = Turtle(restore=lambda k:  self.restored)
        self.manager = PersistentStateManager(self.store)

    def test__init__(self):
        assert_equal(self.manager._impl, self.store)
        assert_equal(self.manager.metadata_key, self.store.build_key.returns[0])

    def test_restore(self):
        def restore(k):
            return [MockStateData({'version': (0, 1), 'key': list(k)})]
        self.store.restore = restore

        jobs = [Turtle(name='job0'), Turtle(name='job1')]
        services = [Turtle(name='serv0'), Turtle(name='serv1')]

        job_data, service_data = self.manager.restore(jobs, services)
        assert_equal(job_data['mockstatedata']['key'],
                self.store.build_key.returns[1:3])
        assert_equal(service_data['mockstatedata']['key'],
            self.store.build_key.returns[3:5])

    def test_keys_for_items(self):
        items = [Turtle(), Turtle()]
        keys = list(self.manager._keys_for_items('type', items))

        # Skip first return, its from the constructor
        assert_equal(keys, self.store.build_key.returns[1:])

    def test_validate_version(self):
        self.store.restore = lambda k: [{'version': (0, 1, 1)}]
        self.manager._validate_version()

    def test_validate_version_mismatch(self):
        self.store.restore = lambda k: [{'version': (200, 1, 1)}]
        assert_raises(VersionMismatchError, self.manager._validate_version)

    def test_restore_dict(self):
        keys = ['one', 'two']
        state_data = self.manager._restore_dict(keys)
        expected = dict((t.name, t) for t in self.restored)
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