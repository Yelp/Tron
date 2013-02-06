import shutil
import tempfile

import mock
from testify import TestCase, setup, teardown
from testify import  assert_equal, run
from tests.assertions import assert_call, assert_length
from tests.testingutils import Turtle, autospec_method

from tron import mcp
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
        self.mcp.nodes.clear()
        self.mcp.event_manager.clear()
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
        self.config_file        = tempfile.NamedTemporaryFile(
                                    dir=self.working_dir)
        self.mcp                = mcp.MasterControlProgram(
                                    self.working_dir, self.config_file.name)
        self.mcp.jobs           = {'1': Turtle(), '2': Turtle()}
        self.mcp.services       = {'1': Turtle(), '2': Turtle()}

    @teardown
    def teardown_mcp(self):
        self.mcp.nodes.clear()
        self.mcp.event_manager.clear()
        shutil.rmtree(self.working_dir)

    def test_restore_state(self):
        def restore(jobs, services):
            state_data = {'1': 'things', '2': 'things'}
            return state_data, state_data
        self.mcp.state_manager = Turtle(restore=restore)
        self.mcp.restore_state()
        for job in self.mcp.jobs.values():
            assert_call(job.restore_job_state, 0, 'things')
        for service in self.mcp.services.values():
            assert_call(service.restore_service_state, 0, 'things')

    def test_restore_state_no_state(self):
        def restore(jobs, services):
            return {}, {}
        self.mcp.state_manager = Turtle(restore=restore)
        self.mcp.restore_state()
        for job in self.mcp.jobs.values():
            assert_length(job.restore_job_state.calls, 0)
        for service in self.mcp.services.values():
            assert_length(service.restore_service_state.calls, 0)

    def test_restore_state_partial(self):
        def restore(jobs, services):
            return {'1': 'thing'}, {'2': 'thing'}
        self.mcp.state_manager = Turtle(restore=restore)
        self.mcp.restore_state()

        assert_call(self.mcp.jobs['1'].restore_job_state, 0, 'thing')
        assert_length(self.mcp.jobs['2'].restore_job_state.calls, 0)
        assert_length(self.mcp.services['1'].restore_service_state.calls, 0)
        assert_call(self.mcp.services['2'].restore_service_state, 0, 'thing')

if __name__ == '__main__':
    run()
