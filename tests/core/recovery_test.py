from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from mock import Mock

from testifycompat import setup
from testifycompat import TestCase
from tron.actioncommand import NoActionRunnerFactory
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun
from tron.core.recovery import filter_action_runs_needing_recovery
from tron.core.recovery import launch_recovery_actionruns_for_job_runs
from tron.utils import timeutils


class TestRecovery(TestCase):
    @setup
    def fake_action_runs(self):
        mock_unknown_machine = Mock(autospec=True)
        mock_ok_machine = Mock(autospec=True)

        mock_unknown_machine.state = ActionRun.UNKNOWN
        mock_ok_machine.state = ActionRun.SUCCEEDED
        self.action_runs = [
            SSHActionRun(
                job_run_id="test.unknown",
                name="test.unknown",
                node=Mock(),
                machine=mock_unknown_machine,
                end_time=timeutils.current_time(),
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
            MesosActionRun(
                job_run_id="test.unknown-mesos",
                name="test.unknown-mesos",
                node=Mock(),
                machine=mock_unknown_machine,
            ),
            MesosActionRun(
                job_run_id="test.unknown-mesos-done",
                name="test.unknown-mesos-done",
                node=Mock(),
                machine=mock_unknown_machine,
                end_time=timeutils.current_time(),
            ),
        ]

    def test_filter_action_runs_needing_recovery(self):
        assert filter_action_runs_needing_recovery(self.action_runs) == (
            [self.action_runs[0]],
            [self.action_runs[3]],
        )

    @mock.patch('tron.core.recovery.filter_action_runs_needing_recovery', autospec=True)
    def test_launch_recovery_actionruns_for_job_runs(self, mock_filter):
        mock_actions = (
            [
                mock.Mock(
                    action_runner=NoActionRunnerFactory(), spec=SSHActionRun
                ),
                mock.Mock(
                    action_runner=SubprocessActionRunnerFactory(
                        status_path='/tmp/foo', exec_path=('/tmp/foo')
                    ),
                    spec=SSHActionRun,
                ),
            ],
            [
                mock.Mock(
                    action_runner=NoActionRunnerFactory(), spec=MesosActionRun
                ),
            ],
        )

        mock_filter.return_value = mock_actions
        mock_action_runner = mock.Mock(autospec=True)

        mock_job_run = mock.Mock()
        launch_recovery_actionruns_for_job_runs([mock_job_run],
                                                mock_action_runner)
        ssh_runs = mock_actions[0]
        for run in ssh_runs:
            assert run.recover.call_count == 1

        mesos_run = mock_actions[1][0]
        assert mesos_run.recover.call_count == 1

    @mock.patch('tron.core.recovery.filter_action_runs_needing_recovery', autospec=True)
    def test_launch_recovery_actionruns_empty_job_run(self, mock_filter):
        """_action_runs=None shouldn't prevent other job runs from being recovered"""
        empty_job_run = mock.Mock(_action_runs=None)
        other_job_run = mock.Mock(_action_runs=[mock.Mock()])
        mock_action_runner = mock.Mock()
        mock_filter.return_value = ([], [])

        launch_recovery_actionruns_for_job_runs(
            [empty_job_run, other_job_run],
            mock_action_runner,
        )
        mock_filter.assert_called_with(other_job_run._action_runs)
