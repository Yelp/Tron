import mock
import pytest

from tron import mcp
from tron.api import controller
from tron.api.controller import ConfigController
from tron.api.controller import EventsController
from tron.api.controller import JobCollectionController
from tron.api.controller import UnknownCommandError
from tron.config import ConfigError
from tron.config import manager
from tron.core import actionrun
from tron.core import jobrun
from tron.core.job_collection import JobCollection
from tron.core.job_scheduler import JobScheduler


class TestJobCollectionController:
    @pytest.fixture(autouse=True)
    def setup_controller(self):
        self.collection = mock.create_autospec(
            JobCollection,
            enable=mock.Mock(),
            disable=mock.Mock(),
        )
        self.controller = JobCollectionController(self.collection)

    def test_handle_command_unknown(self):
        with pytest.raises(UnknownCommandError):
            self.controller.handle_command('enableall')
            self.controller.handle_command('disableall')

    def test_handle_command_move_non_existing_job(self):
        self.collection.get_names.return_value = []
        result = self.controller.handle_command(
            'move', old_name='old.test', new_name='new.test'
        )
        assert "doesn't exist" in result

    def test_handle_command_move_to_existing_job(self):
        self.collection.get_names.return_value = ['old.test', 'new.test']
        result = self.controller.handle_command(
            'move', old_name='old.test', new_name='new.test'
        )
        assert "exists already" in result

    def test_handle_command_move(self):
        self.collection.get_names.return_value = ['old.test']
        result = self.controller.handle_command(
            'move', old_name='old.test', new_name='new.test'
        )
        assert "Error" not in result


class TestActionRunController:
    @pytest.fixture(autouse=True)
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
        assert "can not be started" in result

    def test_handle_command_mapped_command(self):
        result = self.controller.handle_command('cancel')
        self.action_run.cancel.assert_called_with()
        assert "now in state" in result

    def test_handle_command_mapped_command_failed(self):
        self.action_run.cancel.return_value = False
        result = self.controller.handle_command('cancel')
        self.action_run.cancel.assert_called_with()
        assert "Failed to cancel" in result

    def test_handle_termination_not_implemented(self):
        self.action_run.stop.side_effect = NotImplementedError
        result = self.controller.handle_termination('stop')
        assert "Failed to stop" in result

    def test_handle_termination_success_without_extra_msg(self):
        self.action_run.kill.return_value = None
        result = self.controller.handle_termination('kill')
        assert "Attempting to kill" in result

    def test_handle_termination_success_with_extra_msg(self):
        self.action_run.kill.return_value = "Warning Message"
        result = self.controller.handle_termination('kill')
        assert "Attempting to kill" in result
        assert "Warning Message" in result


class TestJobRunController:
    @pytest.fixture(autouse=True)
    def setup_controller(self):
        self.job_run = mock.create_autospec(
            jobrun.JobRun,
            run_time=mock.Mock(),
            cancel=mock.Mock(),
        )
        self.job_scheduler = mock.create_autospec(JobScheduler)
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
        assert 'now in state' in result

    def test_handle_mapped_command_failure(self):
        self.job_run.cancel.return_value = False
        result = self.controller.handle_command('cancel')
        self.job_run.cancel.assert_called_with()
        assert 'Failed to cancel' in result


class TestJobController:
    @pytest.fixture(autouse=True)
    def setup_controller(self):
        self.job_scheduler = mock.create_autospec(JobScheduler)
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


class TestConfigController:
    @pytest.fixture(autouse=True)
    def setup_controller(self):
        self.mcp = mock.create_autospec(mcp.MasterControlProgram)
        self.manager = mock.create_autospec(manager.ConfigManager)
        self.mcp.get_config_manager.return_value = self.manager
        self.controller = ConfigController(self.mcp)

    def test_get_config_content_new(self):
        self.manager.__contains__.return_value = False
        content = self.controller._get_config_content('name')
        assert content == self.controller.DEFAULT_NAMED_CONFIG
        assert not self.manager.read_raw_config.call_count

    def test_get_config_content_old(self):
        self.manager.__contains__.return_value = True
        name = 'the_name'
        content = self.controller._get_config_content(name)
        assert content == self.manager.read_raw_config.return_value
        self.manager.read_raw_config.assert_called_with(name)

    def test_read_config(self):
        self.manager.__contains__.return_value = True
        name = 'MASTER'
        resp = self.controller.read_config(name)
        self.manager.read_raw_config.assert_called_with(name)
        self.manager.get_hash.assert_called_with(name)
        assert resp['config'] == self.manager.read_raw_config.return_value
        assert resp['hash'] == self.manager.get_hash.return_value

    def test_update_config(self):
        name, content, config_hash = None, mock.Mock(), mock.Mock()
        self.manager.get_hash.return_value = config_hash
        assert not self.controller.update_config(name, content, config_hash)
        self.manager.get_hash.assert_called_with(name)
        self.manager.write_config.assert_called_with(name, content)
        self.mcp.reconfigure.assert_called_with()

    def test_update_config_failure(self):
        name, content, config_hash = None, mock.Mock(), mock.Mock()
        self.manager.get_hash.return_value = config_hash
        self.manager.write_config.side_effect = ConfigError("It broke")
        error = self.controller.update_config(
            name,
            content,
            config_hash,
        )
        assert error == "It broke"
        self.manager.write_config.assert_called_with(name, content)
        assert not self.mcp.reconfigure.call_count

    def test_update_config_hash_mismatch(self):
        name, content, config_hash = None, mock.Mock(), mock.Mock()
        error = self.controller.update_config(name, content, config_hash)
        assert error == "Configuration has changed. Please try again."

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
        assert error == "Configuration has changed. Please try again."

    def test_delete_config_content_not_empty(self):
        name, content, config_hash = None, "content", mock.Mock()
        error = self.controller.delete_config(name, content, config_hash)
        assert error

    def test_get_namespaces(self):
        result = self.controller.get_namespaces()
        self.manager.get_namespaces.assert_called_with()
        assert result == self.manager.get_namespaces.return_value


class TestEventsController:
    @pytest.fixture(autouse=True)
    def setup(self):
        with mock.patch('tron.api.controller.EventBus', autospec=True) as eb:
            eb.instance = mock.Mock()
            self.eventbus = eb
            self.controller = EventsController()
            yield

    def test_info(self):
        self.eventbus.instance = None
        assert self.controller.info() == dict(error='EventBus disabled')

        self.eventbus.instance = mock.Mock()
        assert self.controller.info() == dict(response=self.eventbus.instance.event_log)

    def test_publish(self):
        event = mock.Mock()
        self.eventbus.instance = None
        self.eventbus.has_event.return_value = True
        self.eventbus.publish.return_value = False

        assert self.controller.info() == dict(error='EventBus disabled')
        assert len(self.eventbus.publish.mock_calls) == 0

        self.eventbus.instance = mock.Mock()
        assert self.controller.publish(event) == dict(response=f'event {event} already published')
        assert len(self.eventbus.publish.mock_calls) == 0

        self.eventbus.has_event.return_value = False
        assert self.controller.publish(event) == dict(error=f'could not publish {event}')
        assert len(self.eventbus.publish.mock_calls) == 1

        self.eventbus.publish.return_value = True
        assert self.controller.publish(event) == dict(response=f'OK')
        assert len(self.eventbus.publish.mock_calls) == 2

    def test_discard(self):
        event = mock.Mock()
        self.eventbus.instance = None
        self.eventbus.discard.return_value = False

        assert self.controller.info() == dict(error='EventBus disabled')
        assert len(self.eventbus.discard.mock_calls) == 0

        self.eventbus.instance = mock.Mock()
        assert self.controller.discard(event) == dict(error=f'could not discard {event}')
        assert len(self.eventbus.discard.mock_calls) == 1

        self.eventbus.discard.return_value = True
        assert self.controller.discard(event) == dict(response=f'OK')
        assert len(self.eventbus.discard.mock_calls) == 2
