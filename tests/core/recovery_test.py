from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from mock import call
from mock import Mock
from testify import setup
from testify import TestCase

from tron.actioncommand import NoActionRunnerFactory
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun
from tron.core.recovery import build_recovery_command
from tron.core.recovery import filter_action_runs_needing_recovery
from tron.core.recovery import filter_recoverable_action_runs
from tron.core.recovery import filter_recovery_candidates
from tron.core.recovery import launch_recovery_actionruns_for_job_runs
from tron.core.recovery import recover_action_run
from tron.utils import timeutils


class TestRecovery(TestCase):
    @setup
    def fake_action_runs(self):
        mock_unknown_machine = Mock(autospec=True)
        mock_ok_machine = Mock(autospec=True)

        mock_unknown_machine.state = ActionRun.STATE_UNKNOWN
        mock_ok_machine.state = ActionRun.STATE_SUCCEEDED
        self.action_runs = [
            SSHActionRun(
                job_run_id="test.unknown",
                name="test.unknown",
                node=Mock(),
                machine=mock_unknown_machine,
            ),
            SSHActionRun(
                job_run_id="test.succeeded",
                name="test.succeeded",
                node=Mock(),
                machine=mock_ok_machine,
            ),
            MesosActionRun(
                job_run_id="test.succeeded",
                name="test.succeeded",
                node=Mock(),
                machine=mock_ok_machine,
            ),
        ]

    def test_filter_action_runs_needing_recovery(self):
        assert filter_action_runs_needing_recovery(self.action_runs) == \
            [self.action_runs[0]]

    def test_build_recovery_command(self):
        assert build_recovery_command(
            "/bin/foo",
            "/tmp/foo",
        ) == "/bin/foo /tmp/foo"

    def test_recover_action_run_no_action_runner(self):
        no_action_runner = SSHActionRun(
            job_run_id="test.succeeded",
            name="test.succeeded",
            node=Mock(),
        )
        assert recover_action_run(
            no_action_runner, no_action_runner.action_runner
        ) is None

    def test_recover_action_run_action_runner(self):
        action_runner = SubprocessActionRunnerFactory(
            status_path='/tmp/foo',
            exec_path='/bin/foo',
        )
        mock_node = mock.Mock()
        action_run = SSHActionRun(
            job_run_id="test.succeeded",
            name="test.succeeded",
            node=mock_node,
            action_runner=action_runner,
            end_time=timeutils.current_time()
        )
        action_run.machine.state = action_run.STATE_UNKNOWN
        recover_action_run(action_run, action_runner)
        mock_node.submit_command.assert_called_once()
        assert action_run.machine.state == action_run.STATE_RUNNING
        assert action_run.end_time is None

    def test_filter_recoverable_action_runs(self):
        assert filter_recoverable_action_runs(self.action_runs) == \
            [self.action_runs[0], self.action_runs[1]]

    def test_filter_recovery_candidates(self):
        with mock.patch('tron.core.recovery.filter_recoverable_action_runs') as mock_filter_recoverable, \
                mock.patch('tron.core.recovery.filter_action_runs_needing_recovery') as mock_filter_needing_recovery:

            mock_filter_needing_recovery.return_value = ['foo']
            filter_recovery_candidates(self.action_runs)
            mock_filter_recoverable.assert_called_once_with(
                action_runs=['foo']
            )
            mock_filter_needing_recovery.assert_called_once_with(
                action_runs=self.action_runs
            )

    def test_launch_recovery_actionruns_for_job_runs(self):
        with mock.patch('tron.core.recovery.filter_recovery_candidates') as mock_filter_recovery_candidates, \
                mock.patch('tron.core.recovery.recover_action_run') as mock_recover_action_run:

            mock_values = [
                mock.Mock(
                    autospec=True, action_runner=NoActionRunnerFactory()
                ),
                mock.Mock(
                    autospec=True,
                    action_runner=SubprocessActionRunnerFactory(
                        status_path='/tmp/foo', exec_path=('/tmp/foo')
                    )
                ),
            ]

            mock_filter_recovery_candidates.return_value = mock_values
            mock_action_runner = mock.Mock(autospec=True)

            mock_job_run = mock.Mock()
            launch_recovery_actionruns_for_job_runs([mock_job_run],
                                                    mock_action_runner)
            calls = [
                call(mock_values[0], mock_action_runner),
                call(mock_values[1], mock_values[1].action_runner)
            ]
            mock_recover_action_run.assert_has_calls(calls, any_order=True)
