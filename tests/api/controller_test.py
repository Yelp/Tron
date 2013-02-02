import mock

from testify import setup, TestCase, run
from tron import mcp

from tron.api.controller import JobController, ConfigController
from tron.config import ConfigError
from tron.core import job


class JobControllerTestCase(TestCase):

    @setup
    def setup_controller(self):
        self.jobs           = [mock.create_autospec(job.JobScheduler)]
        self.mcp            = mock.create_autospec(mcp.MasterControlProgram)
        self.controller     = JobController(self.mcp)

    def test_disable_all(self):
        self.mcp.get_jobs.return_value = self.jobs
        self.controller.disable_all()
        self.mcp.get_jobs.assert_called_with()
        for job in self.jobs:
            job.disable.assert_called_with()

    def test_enable_all(self):
        self.mcp.get_jobs.return_value = self.jobs
        self.controller.enable_all()
        self.mcp.get_jobs.assert_called_with()
        for job in self.jobs:
            job.enable.assert_called_with()


class ConfigControllerTestCase(TestCase):

    @setup
    def setup_controller(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.controller = ConfigController(self.mcp)
        self.manager = self.mcp.get_config_manager.return_value

    def test_read_config(self):
        name = 'MASTER'
        self.controller.read_config(name)
        self.manager.read_config.assert_called_with(name)

    def test_update_config(self):
        name, content = None, mock.Mock()
        assert not self.controller.update_config(name, content)
        self.manager.write_config.assert_called_with(name, content)
        self.mcp.reconfigure.assert_called_with()

    def test_update_config_failure(self):
        name, content = None, mock.Mock()
        self.manager.write_config.side_effect = ConfigError("It broke")
        assert self.controller.update_config(name, content)
        self.manager.write_config.assert_called_with(name, content)
        assert not self.mcp.reconfigure.call_count


if __name__ == "__main__":
    run()
