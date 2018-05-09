from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from mock import Mock
from testify import setup
from testify import TestCase

from tron.actioncommand import SubprocessActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import PaaSTAActionRun
from tron.core.actionrun import SSHActionRun
from tron.core.recovery import build_recovery_command
from tron.core.recovery import filter_action_runs_needing_recovery
from tron.core.recovery import filter_recoverable_action_runs
from tron.core.recovery import filter_recovery_candidates
from tron.core.recovery import recover_action_run


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
            PaaSTAActionRun(
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
        action_run = SSHActionRun(
            job_run_id="test.succeeded",
            name="test.succeeded",
            node=Mock(),
            action_runner=action_runner
        )

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
