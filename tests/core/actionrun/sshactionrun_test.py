import mock
import pytest

from tron import actioncommand
from tron import node
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.core.actionrun.base import ActionRun
from tron.core.actionrun.ssh import ActionCommand
from tron.core.actionrun.ssh import INITIAL_RECOVER_DELAY
from tron.core.actionrun.ssh import MAX_RECOVER_TRIES
from tron.core.actionrun.ssh import SSHActionRun


class TestSSHActionRun:
    @pytest.fixture(autouse=True)
    def setup_action_run(self, output_path):
        self.action_runner = mock.create_autospec(
            actioncommand.NoActionRunnerFactory,
        )
        self.command = "do command {actionname}"
        self.action_run = SSHActionRun(
            job_run_id="job_name.5",
            name="action_name",
            node=mock.create_autospec(node.Node),
            bare_command=self.command,
            output_path=output_path,
            action_runner=self.action_runner,
        )

    def test_start_node_error(self):
        def raise_error(c):
            raise node.Error("The error")

        self.action_run.node = mock.MagicMock()
        self.action_run.node.submit_command.side_effect = raise_error
        self.action_run.machine.transition('ready')
        assert not self.action_run.start()
        assert self.action_run.exit_status == -2
        assert self.action_run.is_failed

    @mock.patch('tron.core.actionrun.ssh.filehandler', autospec=True)
    def test_build_action_command(self, mock_filehandler):
        self.action_run.watch = mock.MagicMock()
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        action_command = self.action_run.build_action_command()
        assert action_command == self.action_run.action_command
        assert action_command == self.action_runner.create.return_value
        self.action_runner.create.assert_called_with(
            self.action_run.id,
            self.action_run.command,
            serializer,
        )
        mock_filehandler.OutputStreamSerializer.assert_called_with(
            self.action_run.output_path,
        )
        self.action_run.watch.assert_called_with(action_command)

    def test_handler_running(self):
        self.action_run.build_action_command()
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.RUNNING,
        )
        assert self.action_run.is_running

    def test_handler_failstart(self):
        self.action_run.build_action_command()
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.FAILSTART,
        )
        assert self.action_run.is_failed

    def test_handler_exiting_fail(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = -1
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.is_failed
        assert self.action_run.exit_status == -1

    def test_handler_exiting_success(self):
        self.action_run.build_action_command()
        self.action_run.action_command.exit_status = 0
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.is_succeeded
        assert self.action_run.exit_status == 0

    def test_handler_exiting_failunknown(self):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            exit_status=None,
        )
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.is_unknown
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is not None

    def test_handler_unhandled(self):
        self.action_run.build_action_command()
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.PENDING,
        ) is None
        assert self.action_run.is_scheduled

    def test_recover_no_action_runner(self):
        # Default setup has no action runner
        assert not self.action_run.recover()


class TestSSHActionRunRecover:
    @pytest.fixture(autouse=True)
    def setup_action_run(self, output_path):
        self.action_runner = SubprocessActionRunnerFactory(
            status_path='/tmp/foo',
            exec_path='/bin/foo',
        )
        self.command = "do command {actionname}"
        self.action_run = SSHActionRun(
            job_run_id="job_name.5",
            name="action_name",
            node=mock.create_autospec(node.Node),
            bare_command=self.command,
            output_path=output_path,
            action_runner=self.action_runner,
        )

    def test_recover_incorrect_state(self):
        # Should return falsy if not UNKNOWN.
        self.action_run.machine.state = ActionRun.FAILED
        assert not self.action_run.recover()

    def test_recover_action_runner(self):
        self.action_run.end_time = 1000
        self.action_run.exit_status = 0
        self.action_run.machine.state = ActionRun.UNKNOWN
        assert self.action_run.recover()
        assert self.action_run.machine.state == ActionRun.RUNNING
        assert self.action_run.end_time is None
        assert self.action_run.exit_status is None
        self.action_run.node.submit_command.assert_called_once()

        # Check recovery command
        submit_args = self.action_run.node.submit_command.call_args[0]
        assert len(submit_args) == 1
        recovery_command = submit_args[0]
        assert recovery_command.command == '/bin/foo/recover_batch.py /tmp/foo/job_name.5.action_name/status'
        assert recovery_command.start_time is not None  # already started

    @mock.patch('tron.core.actionrun.ssh.reactor', autospec=True)
    def test_handler_exiting_failunknown(self, mock_reactor):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            exit_status=None,
        )
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        delay_deferred = self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert delay_deferred == mock_reactor.callLater.return_value
        assert self.action_run.is_running
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is None

        call_args = mock_reactor.callLater.call_args[0]
        assert call_args[0] == INITIAL_RECOVER_DELAY
        assert call_args[1] == self.action_run.submit_recovery_command

        # Check recovery run
        recovery_run = call_args[2]
        assert 'recovery' in recovery_run.name
        assert isinstance(recovery_run, SSHActionRun)
        # Recovery run should not be recovering itself, parent run handles its unknown status
        assert recovery_run.recover() is None

        # Check command
        recovery_command = call_args[3]
        assert recovery_command.command == '/bin/foo/recover_batch.py /tmp/foo/job_name.5.action_name/status'
        assert recovery_command.start_time is not None  # already started

    @mock.patch('tron.core.actionrun.ssh.SSHActionRun.do_recover', autospec=True)
    @mock.patch('tron.core.actionrun.ssh.reactor', autospec=True)
    def test_handler_exiting_failunknown_max_retries(self, mock_reactor, mock_do_recover):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            exit_status=None,
        )
        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')

        def exit_unknown(*args, **kwargs):
            self.action_run.handler(
                self.action_run.action_command,
                ActionCommand.EXITING,
            )
        # Each time do_recover is called, end up exiting unknown again
        mock_do_recover.side_effect = exit_unknown

        # Start the cycle
        exit_unknown()

        assert mock_do_recover.call_count == MAX_RECOVER_TRIES
        last_call = mock_do_recover.call_args
        expected_delay = INITIAL_RECOVER_DELAY * (3 ** (MAX_RECOVER_TRIES - 1))
        assert last_call == mock.call(self.action_run, delay=expected_delay)

        assert self.action_run.is_unknown
        assert self.action_run.exit_status is None
        assert self.action_run.end_time is not None
