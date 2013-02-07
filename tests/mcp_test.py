import shutil
import tempfile

import mock
from testify import TestCase, setup, teardown
from testify import  assert_equal, run
from tests.assertions import assert_call, assert_length
from tests.testingutils import Turtle, autospec_method

from tron import mcp, event
from tron.core import service
from tron.serialize.runstate import statemanager
from tron.config import config_parse


class MasterControlProgramTestCase(TestCase):

    TEST_CONFIG = 'tests/data/test_config.yaml'

    @setup
    def setup_mcp(self):
        self.working_dir    = tempfile.mkdtemp()
        self.config_path    = tempfile.mkdtemp()
        self.mcp            = mcp.MasterControlProgram(
                                        self.working_dir, self.config_path)

    @teardown
    def teardown_mcp(self):
        event.EventManager.reset()
        shutil.rmtree(self.config_path)
        shutil.rmtree(self.working_dir)

    def test_reconfigure(self):
        autospec_method(self.mcp._load_config)
        self.mcp.state_manager = mock.MagicMock()
        self.mcp.reconfigure()
        self.mcp._load_config.assert_called_with(reconfigure=True)
        self.mcp.state_manager.disabled.assert_called_with()

    def test_ssh_options_from_config(self):
        ssh_conf = mock.Mock(agent=False, identities=[])
        ssh_options = self.mcp._ssh_options_from_config(ssh_conf)

        assert_equal(ssh_options['agent'], False)
        assert_equal(ssh_options.identitys, [])
        # TODO: tests with agent and identities

    def test_graceful_shutdown(self):
        self.mcp.graceful_shutdown()
        for job_sched in self.mcp.get_jobs():
            assert job_sched.shutdown_requested

    @mock.patch('tron.mcp.PersistenceManagerFactory', autospec=True)
    def test_apply_config(self, mock_state_factory):
        config_container = mock.create_autospec(config_parse.ConfigContainer)
        master_config = config_container.get_master.return_value
        self.mcp.apply_config(config_container)
        mock_state_factory.from_config.assert_called_with(
            master_config.state_persistence)
        assert_equal(self.mcp.output_stream_dir, master_config.output_stream_dir)
        assert_equal(self.mcp.time_zone, master_config.time_zone)
        assert_equal(self.mcp.context.base, master_config.command_context)


class MasterControlProgramRestoreStateTestCase(TestCase):

    @setup
    def setup_mcp(self):
        self.working_dir        = tempfile.mkdtemp()
        self.config_path    = tempfile.mkdtemp()
        self.mcp                = mcp.MasterControlProgram(
                                    self.working_dir, self.config_path)
        self.mcp.jobs           = {'1': Turtle(), '2': Turtle()}
        self.mcp.services       = mock.create_autospec(service.ServiceCollection)
        self.mcp.state_manager = mock.create_autospec(statemanager.PersistentStateManager)

    @teardown
    def teardown_mcp(self):
        event.EventManager.reset()
        shutil.rmtree(self.working_dir)

    def test_restore_state(self):
        service_state_data = {'3': 'things', '4': 'things'}
        job_state_data = {'1': 'things', '2': 'things'}
        self.mcp.state_manager.restore.return_value = job_state_data, service_state_data
        self.mcp.restore_state()
        for job in self.mcp.jobs.values():
            assert_call(job.restore_job_state, 0, 'things')
        self.mcp.services.restore_state.assert_called_with(service_state_data)

    def test_restore_state_no_state(self):
        service_state_data = mock.Mock()
        job_state_data = {}
        self.mcp.state_manager.restore.return_value = job_state_data, service_state_data
        self.mcp.restore_state()
        for job in self.mcp.jobs.values():
            assert_length(job.restore_job_state.calls, 0)
        self.mcp.services.restore_state.assert_called_with(service_state_data)

    def test_restore_state_partial(self):
        self.mcp.state_manager.restore.return_value = {'1': 'thing'}, {'2': 'thing'}
        self.mcp.restore_state()

        assert_call(self.mcp.jobs['1'].restore_job_state, 0, 'thing')
        assert_length(self.mcp.jobs['2'].restore_job_state.calls, 0)
        self.mcp.services.restore_state.assert_called_with({'2': 'thing'})

if __name__ == '__main__':
    run()
