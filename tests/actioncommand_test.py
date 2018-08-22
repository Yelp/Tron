from __future__ import absolute_import
from __future__ import unicode_literals

import shlex

import mock

from testifycompat import assert_equal
from testifycompat import assert_not_equal
from testifycompat import setup
from testifycompat import TestCase
from tests.testingutils import autospec_method
from tron import actioncommand
from tron.actioncommand import ActionCommand
from tron.config import schema
from tron.serialize import filehandler


class TestActionCommand(TestCase):
    @setup
    def setup_command(self):
        self.serializer = mock.create_autospec(filehandler.FileHandleManager)
        self.serializer.open.return_value = filehandler.NullFileHandle
        self.ac = ActionCommand("action.1.do", "do", self.serializer)

    def test_init(self):
        assert_equal(self.ac.state, ActionCommand.PENDING)

    def test_init_no_serializer(self):
        ac = ActionCommand("action.1.do", "do")
        ac.write_stdout("something")
        ac.write_stderr("else")
        assert_equal(ac.stdout, filehandler.NullFileHandle)
        ac.done()

    def test_started(self):
        assert self.ac.started()
        assert self.ac.start_time is not None
        assert_equal(self.ac.state, ActionCommand.RUNNING)

    def test_started_already_started(self):
        self.ac.started()
        assert not self.ac.started()

    def test_exited(self):
        self.ac.started()
        assert self.ac.exited(123)
        assert_equal(self.ac.exit_status, 123)
        assert self.ac.end_time is not None

    def test_exited_from_pending(self):
        assert self.ac.exited(123)
        assert_equal(self.ac.state, ActionCommand.FAILSTART)

    def test_exited_bad_state(self):
        self.ac.started()
        self.ac.exited(123)
        assert not self.ac.exited(1)

    def test_write_stderr_no_fh(self):
        message = "this is the message"
        # Test without a stderr
        self.ac.write_stderr(message)

    def test_write_stderr(self):
        message = "this is the message"
        serializer = mock.create_autospec(filehandler.FileHandleManager)
        fh = serializer.open.return_value = mock.create_autospec(
            filehandler.FileHandleWrapper,
        )
        ac = ActionCommand("action.1.do", "do", serializer)

        ac.write_stderr(message)
        fh.write.assert_called_with(message)

    def test_done(self):
        self.ac.started()
        self.ac.exited(123)
        assert self.ac.done()

    def test_done_bad_state(self):
        assert not self.ac.done()

    def test_handle_errback(self):
        message = "something went wrong"
        self.ac.handle_errback(message)
        assert_equal(self.ac.state, ActionCommand.FAILSTART)
        assert self.ac.end_time

    def test_is_unknown(self):
        assert self.ac.is_unknown

    def test_is_failed(self):
        assert not self.ac.is_failed

    def test_is_failed_true(self):
        self.ac.exit_status = 255
        assert self.ac.is_failed

    def test_is_complete(self):
        assert not self.ac.is_complete

    def test_is_complete_true(self):
        self.ac.machine.state = self.ac.COMPLETE
        assert self.ac.is_complete, self.ac.machine.state

    def test_is_done(self):
        self.ac.machine.state = self.ac.FAILSTART
        assert self.ac.is_done, self.ac.machine.state
        self.ac.machine.state = self.ac.COMPLETE
        assert self.ac.is_done, self.ac.machine.state


class TestCreateActionCommandFactoryFromConfig(TestCase):
    def test_create_default_action_command_no_config(self):
        config = ()
        factory = actioncommand.create_action_runner_factory_from_config(
            config,
        )
        assert_equal(type(factory), actioncommand.NoActionRunnerFactory)

    def test_create_default_action_command(self):
        config = schema.ConfigActionRunner('none', None, None)
        factory = actioncommand.create_action_runner_factory_from_config(
            config,
        )
        assert_equal(type(factory), actioncommand.NoActionRunnerFactory)

    def test_create_action_command_with_simple_runner(self):
        status_path, exec_path = '/tmp/what', '/remote/bin'
        config = schema.ConfigActionRunner(
            'subprocess',
            status_path,
            exec_path,
        )
        factory = actioncommand.create_action_runner_factory_from_config(
            config,
        )
        assert_equal(factory.status_path, status_path)
        assert_equal(factory.exec_path, exec_path)


class TestSubprocessActionRunnerFactory(TestCase):
    @setup
    def setup_factory(self):
        self.status_path = 'status_path'
        self.exec_path = 'exec_path'
        self.factory = actioncommand.SubprocessActionRunnerFactory(
            self.status_path,
            self.exec_path,
        )

    def test_from_config(self):
        config = mock.Mock()
        runner_factory = actioncommand.SubprocessActionRunnerFactory.from_config(
            config,
        )
        assert_equal(runner_factory.status_path, config.remote_status_path)
        assert_equal(runner_factory.exec_path, config.remote_exec_path)

    def test_create(self):
        serializer = mock.create_autospec(actioncommand.StringBufferStore)
        id, command = 'id', 'do a thing'
        autospec_method(self.factory.build_command)
        action_command = self.factory.create(id, command, serializer)
        assert_equal(action_command.id, id)
        assert_equal(
            action_command.command,
            self.factory.build_command.return_value,
        )
        assert_equal(action_command.stdout, serializer.open.return_value)
        assert_equal(action_command.stderr, serializer.open.return_value)

    def test_build_command_complex_quoting(self):
        id = 'id'
        command = '/bin/foo -c "foo" --foo "bar"'
        exec_name = "action_runner.py"
        actual = self.factory.build_command(id, command, exec_name)
        assert_equal(
            shlex.split(actual),
            [
                "%s/%s" % (self.exec_path, exec_name),
                "%s/%s" % (self.status_path, id),
                command,
                id,
            ],
        )

    def test_build_stop_action_command(self):
        id, command = 'id', 'do a thing'
        autospec_method(self.factory.build_command)
        action_command = self.factory.build_stop_action_command(id, command)
        assert_equal(
            action_command.id,
            '%s.%s' % (id, self.factory.build_command.return_value),
        )
        assert_equal(
            action_command.command,
            self.factory.build_command.return_value,
        )

    def test__eq__true(self):
        first = actioncommand.SubprocessActionRunnerFactory('a', 'b')
        second = actioncommand.SubprocessActionRunnerFactory('a', 'b')
        assert_equal(first, second)

    def test__eq__false(self):
        first = actioncommand.SubprocessActionRunnerFactory('a', 'b')
        second = actioncommand.SubprocessActionRunnerFactory('a', 'c')
        assert_not_equal(first, second)
        assert_not_equal(first, None)
        assert_not_equal(first, actioncommand.NoActionRunnerFactory)
