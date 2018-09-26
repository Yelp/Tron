from __future__ import absolute_import
from __future__ import unicode_literals

import shutil
import tempfile

import mock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tests.testingutils import autospec_method
from tron import mcp
from tron.config import config_parse
from tron.config import manager
from tron.core.job_collection import JobCollection
from tron.serialize.runstate import statemanager


class TestMasterControlProgram(TestCase):

    TEST_CONFIG = 'tests/data/test_config.yaml'

    @setup
    def setup_mcp(self):
        self.working_dir = tempfile.mkdtemp()
        self.config_path = tempfile.mkdtemp()
        self.mcp = mcp.MasterControlProgram(
            self.working_dir,
            self.config_path,
        )
        self.mcp.state_watcher = mock.create_autospec(
            statemanager.StateChangeWatcher,
        )

    @teardown
    def teardown_mcp(self):
        shutil.rmtree(self.config_path)
        shutil.rmtree(self.working_dir)

    def test_reconfigure(self):
        autospec_method(self.mcp._load_config)
        self.mcp.state_watcher = mock.MagicMock()
        self.mcp.reconfigure()
        self.mcp._load_config.assert_called_with(reconfigure=True)

    def test_load_config(self):
        autospec_method(self.mcp.apply_config)
        self.mcp.config = mock.create_autospec(manager.ConfigManager)
        self.mcp._load_config()
        self.mcp.state_watcher.disabled.assert_called_with()
        self.mcp.apply_config.assert_called_with(
            self.mcp.config.load.return_value,
            reconfigure=False,
        )

    @mock.patch('tron.mcp.MesosClusterRepository', autospec=True)
    @mock.patch('tron.mcp.node.NodePoolRepository', autospec=True)
    def test_apply_config(self, mock_repo, mock_cluster_repo):
        config_container = mock.create_autospec(config_parse.ConfigContainer)
        master_config = config_container.get_master.return_value
        autospec_method(self.mcp.apply_collection_config)
        autospec_method(self.mcp.build_job_scheduler_factory)
        self.mcp.apply_config(config_container)
        self.mcp.state_watcher.update_from_config.assert_called_with(
            master_config.state_persistence,
        )
        assert_equal(self.mcp.context.base, master_config.command_context)
        assert_equal(len(self.mcp.apply_collection_config.mock_calls), 1)
        mock_repo.update_from_config.assert_called_with(
            master_config.nodes,
            master_config.node_pools,
            master_config.ssh_options,
        )
        mock_cluster_repo.configure.assert_called_with(
            master_config.mesos_options,
        )
        self.mcp.build_job_scheduler_factory(master_config)

    def test_update_state_watcher_config_changed(self):
        self.mcp.state_watcher.update_from_config.return_value = True
        self.mcp.jobs = mock.create_autospec(JobCollection)
        self.mcp.jobs.__iter__.return_values = {
            'a': mock.Mock(),
            'b': mock.Mock(),
        }
        state_config = mock.Mock()
        self.mcp.update_state_watcher_config(state_config)
        self.mcp.state_watcher.update_from_config.assert_called_with(
            state_config,
        )
        assert_equal(
            self.mcp.state_watcher.save_job.mock_calls,
            [mock.call(j.job) for j in self.mcp.jobs],
        )

    def test_update_state_watcher_config_no_change(self):
        self.mcp.state_watcher.update_from_config.return_value = False
        self.mcp.jobs = {'a': mock.Mock(), 'b': mock.Mock()}
        state_config = mock.Mock()
        self.mcp.update_state_watcher_config(state_config)
        assert not self.mcp.state_watcher.save_job.mock_calls


class TestMasterControlProgramRestoreState(TestCase):
    @setup
    def setup_mcp(self):
        self.working_dir = tempfile.mkdtemp()
        self.config_path = tempfile.mkdtemp()
        self.mcp = mcp.MasterControlProgram(
            self.working_dir,
            self.config_path,
        )
        self.mcp.jobs = mock.create_autospec(JobCollection)
        self.mcp.state_watcher = mock.create_autospec(
            statemanager.StateChangeWatcher,
        )

    @teardown
    def teardown_mcp(self):
        shutil.rmtree(self.working_dir)
        shutil.rmtree(self.config_path)

    @mock.patch('tron.mcp.MesosClusterRepository', autospec=True)
    def test_restore_state(self, mock_cluster_repo):
        job_state_data = {'1': 'things', '2': 'things'}
        mesos_state_data = {'3': 'things', '4': 'things'}
        state_data = {
            'mesos_state': mesos_state_data,
            'job_state': job_state_data
        }
        self.mcp.state_watcher.restore.return_value = state_data
        action_runner = mock.Mock()
        self.mcp.restore_state(action_runner)
        mock_cluster_repo.restore_state.assert_called_with(mesos_state_data, )
        self.mcp.jobs.restore_state.assert_called_with(
            job_state_data, action_runner
        )


if __name__ == '__main__':
    run()
