from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from testify import assert_equal
from testify import run
from testify import setup
from testify import TestCase
from testify.assertions import assert_in

from tests.testingutils import autospec_method
from tron import mcp
from tron.api import controller
from tron.api.controller import ConfigController
from tron.api.controller import JobCollectionController
from tron.config import config_parse
from tron.config import ConfigError
from tron.config import manager
from tron.core import actionrun
from tron.core import job
from tron.core import jobrun


class JobCollectionControllerTestCase(TestCase):
    @setup
    def setup_controller(self):
        self.collection = mock.create_autospec(
            job.JobCollection,
            enable=mock.Mock(),
            disable=mock.Mock(),
        )
        self.controller = JobCollectionController(self.collection)

    def test_handle_command_enabled(self):
        self.controller.handle_command('enableall')
        self.collection.enable.assert_called_with()

    def test_handle_command_disable(self):
        self.controller.handle_command('disableall')
        self.collection.disable.assert_called_with()


class ActionRunControllerTestCase(TestCase):
    @setup
    def setup_controller(self):
        self.action_run = mock.create_autospec(
            actionrun.ActionRun,
            cancel=mock.Mock(),
        )
        self.job_run = mock.create_autospec(jobrun.JobRun)
        self.job_run.is_scheduled = False
        self.controller = controller.ActionRunController(
            self.action_run,
            self.job_run,
        )

    def test_handle_command_start_failed(self):
        self.job_run.is_scheduled = True
        result = self.controller.handle_command('start')
        assert not self.action_run.start.mock_calls
        assert_in("can not be started", result)

    def test_handle_command_mapped_command(self):
        result = self.controller.handle_command('cancel')
        self.action_run.cancel.assert_called_with()
        assert_in("now in state", result)

    def test_handle_command_mapped_command_failed(self):
        self.action_run.cancel.return_value = False
        result = self.controller.handle_command('cancel')
        self.action_run.cancel.assert_called_with()
        assert_in("Failed to cancel", result)

    def test_handle_termination_not_implemented(self):
        self.action_run.stop.side_effect = NotImplementedError
        result = self.controller.handle_termination('stop')
        assert_in("Failed to stop", result)

    def test_handle_termination_success(self):
        result = self.controller.handle_termination('kill')
        assert_in("Attempting to kill", result)


class JobRunControllerTestCase(TestCase):
    @setup
    def setup_controller(self):
        self.job_run = mock.create_autospec(
            jobrun.JobRun,
            run_time=mock.Mock(),
            cancel=mock.Mock(),
        )
        self.job_scheduler = mock.create_autospec(job.JobScheduler)
        self.controller = controller.JobRunController(
            self.job_run,
            self.job_scheduler,
        )

    def test_handle_command_restart(self):
        self.controller.handle_command('restart')
        self.job_scheduler.manual_start.assert_called_with(
            self.job_run.run_time,
        )

    def test_handle_mapped_command(self):
        result = self.controller.handle_command('start')
        self.job_run.start.assert_called_with()
        assert_in('now in state', result)

    def test_handle_mapped_command_failure(self):
        self.job_run.cancel.return_value = False
        result = self.controller.handle_command('cancel')
        self.job_run.cancel.assert_called_with()
        assert_in('Failed to cancel', result)


class JobControllerTestCase(TestCase):
    @setup
    def setup_controller(self):
        self.job_scheduler = mock.create_autospec(job.JobScheduler)
        self.controller = controller.JobController(self.job_scheduler)

    def test_handle_command_enable(self):
        self.controller.handle_command('enable')
        self.job_scheduler.enable.assert_called_with()

    def test_handle_command_disable(self):
        self.controller.handle_command('disable')
        self.job_scheduler.disable.assert_called_with()

    def test_handle_command_start(self):
        run_time = mock.Mock()
        self.controller.handle_command('start', run_time)
        self.job_scheduler.manual_start.assert_called_with(run_time=run_time)


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
            config_parse.ConfigContainer,
        )
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
        name, content = 'something', "{}{}".format(
            self.controller.TEMPLATE,
            expected,
        )
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
        self.manager.get_hash.assert_called_with(name)
        assert_equal(resp['config'], self.manager.read_raw_config.return_value)
        assert_equal(resp['hash'], self.manager.get_hash.return_value)

    def test_read_config_named(self):
        name = 'some_name'
        autospec_method(self.controller._get_config_content)
        autospec_method(self.controller.render_template)
        resp = self.controller.read_config(name)
        self.controller._get_config_content.assert_called_with(name)
        self.controller.render_template.assert_called_with(
            self.controller._get_config_content.return_value,
        )
        assert_equal(
            resp['config'],
            self.controller.render_template.return_value,
        )
        assert_equal(resp['hash'], self.manager.get_hash.return_value)

    def test_read_config_no_header(self):
        name = 'some_name'
        autospec_method(self.controller._get_config_content)
        autospec_method(self.controller.render_template)
        resp = self.controller.read_config(name, add_header=False)
        assert not self.controller.render_template.called
        assert_equal(
            resp['config'],
            self.controller._get_config_content.return_value,
        )

    def test_update_config(self):
        autospec_method(self.controller.strip_header)
        name, content, config_hash = None, mock.Mock(), mock.Mock()
        self.manager.get_hash.return_value = config_hash
        assert not self.controller.update_config(name, content, config_hash)
        striped_content = self.controller.strip_header.return_value
        self.manager.write_config.assert_called_with(name, striped_content)
        self.mcp.reconfigure.assert_called_with()
        self.controller.strip_header.assert_called_with(name, content)
        self.manager.get_hash.assert_called_with(name)

    def test_update_config_failure(self):
        autospec_method(self.controller.strip_header)
        striped_content = self.controller.strip_header.return_value
        name, config_hash = None, mock.Mock()
        self.manager.get_hash.return_value = config_hash
        self.manager.write_config.side_effect = ConfigError("It broke")
        error = self.controller.update_config(
            name,
            striped_content,
            config_hash,
        )
        assert_equal(error, "It broke")
        self.manager.write_config.assert_called_with(name, striped_content)
        assert not self.mcp.reconfigure.call_count

    def test_update_config_hash_mismatch(self):
        name, content, config_hash = None, mock.Mock(), mock.Mock()
        error = self.controller.update_config(name, content, config_hash)
        assert_equal(error, "Configuration has changed. Please try again.")

    def test_delete_config(self):
        name, content, config_hash = None, "", mock.Mock()
        self.manager.get_hash.return_value = config_hash
        assert not self.controller.delete_config(name, content, config_hash)
        self.manager.delete_config.assert_called_with(name)
        self.mcp.reconfigure.assert_called_with()
        self.manager.get_hash.assert_called_with(name)

    def test_delete_config_failure(self):
        name, content, config_hash = None, "", mock.Mock()
        self.manager.get_hash.return_value = config_hash
        self.manager.delete_config.side_effect = Exception("some error")
        error = self.controller.delete_config(name, content, config_hash)
        assert error
        self.manager.delete_config.assert_called_with(name)
        assert not self.mcp.reconfigure.call_count

    def test_delete_config_hash_mismatch(self):
        name, content, config_hash = None, "", mock.Mock()
        error = self.controller.delete_config(name, content, config_hash)
        assert_equal(error, "Configuration has changed. Please try again.")

    def test_delete_config_content_not_empty(self):
        name, content, config_hash = None, "content", mock.Mock()
        error = self.controller.delete_config(name, content, config_hash)
        assert error

    def test_get_namespaces(self):
        result = self.controller.get_namespaces()
        self.manager.get_namespaces.assert_called_with()
        assert_equal(result, self.manager.get_namespaces.return_value)


if __name__ == "__main__":
    run()
