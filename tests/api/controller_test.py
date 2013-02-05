import mock

from testify import setup, TestCase, run, assert_equal
from testify.assertions import assert_in
from tests.testingutils import autospec_method
from tron import mcp

from tron.api.controller import JobController, ConfigController
from tron.config import ConfigError, manager, config_parse
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
        self.manager = mock.create_autospec(manager.ConfigManager)
        self.mcp.get_config_manager.return_value = self.manager
        self.controller = ConfigController(self.mcp)

    def test_render_template(self):
        config_content = "asdf asdf"
        container = self.manager.load.return_value = mock.create_autospec(
            config_parse.ConfigContainer)
        container.get_node_names.return_value = ['one', 'two', 'three']
        container.get_master.return_value.command_context = {'zing': 'stars'}
        content = self.controller.render_template(config_content)
        assert_in('# one\n# three\n# two\n', content)
        assert_in('# %-30s: %s' % ('zing', 'stars'), content)
        assert_in(config_content, content)

    def test_strip_header_master(self):
        name, content = 'MASTER', mock.Mock()
        assert_equal(self.controller.strip_header(name, content), content)

    def test_strip_header_named(self):
        expected = "\nthing"
        name, content = 'something', self.controller.TEMPLATE + expected
        assert_equal(self.controller.strip_header(name, content), expected)

    def test_strip_header_named_missing(self):
        name, content = 'something', 'whatever content'
        assert_equal(self.controller.strip_header(name, content), content)

    def test_get_config_content_new(self):
        self.manager.__contains__.return_value = False
        content = self.controller._get_config_content('name')
        assert_equal(content, self.controller.DEFAULT_NAMED_CONFIG)
        assert not self.manager.read_raw_config.call_count

    def test_get_config_content_old(self):
        self.manager.__contains__.return_value = True
        name = 'the_name'
        content = self.controller._get_config_content(name)
        assert_equal(content, self.manager.read_raw_config.return_value)
        self.manager.read_raw_config.assert_called_with(name)

    def test_read_config_master(self):
        self.manager.__contains__.return_value = True
        name = 'MASTER'
        resp = self.controller.read_config(name)
        self.manager.read_raw_config.assert_called_with(name)
        assert_equal(resp, self.manager.read_raw_config.return_value)

    def test_read_config_named(self):
        name = 'some_name'
        autospec_method(self.controller._get_config_content)
        autospec_method(self.controller.render_template)
        resp = self.controller.read_config(name)
        self.controller._get_config_content.assert_called_with(name)
        self.controller.render_template.assert_called_with(
            self.controller._get_config_content.return_value)
        assert_equal(resp, self.controller.render_template.return_value)

    def test_update_config(self):
        autospec_method(self.controller.strip_header)
        name, content = None, mock.Mock()
        assert not self.controller.update_config(name, content)
        striped_content = self.controller.strip_header.return_value
        self.manager.write_config.assert_called_with(name, striped_content)
        self.mcp.reconfigure.assert_called_with()
        self.controller.strip_header.assert_called_with(name, content)

    def test_update_config_failure(self):
        autospec_method(self.controller.strip_header)
        striped_content = self.controller.strip_header.return_value
        name, content = None, mock.Mock()
        self.manager.write_config.side_effect = ConfigError("It broke")
        error = self.controller.update_config(name, striped_content)
        assert_equal(error, "It broke")
        self.manager.write_config.assert_called_with(name, striped_content)
        assert not self.mcp.reconfigure.call_count


if __name__ == "__main__":
    run()
