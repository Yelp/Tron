from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from mock import call
from mock import Mock

from testifycompat import setup
from testifycompat import TestCase
from tron.actioncommand import NoActionRunnerFactory
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun
from tron.core.recovery import build_recovery_command
from tron.core.recovery import filter_action_runs_needing_recovery
from tron.core.recovery import group_by_actionrun_type
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
                eventbus_publish=lambda: None,
                machine=mock_unknown_machine,
            ),
            SSHActionRun(
                job_run_id="test.succeeded",
                name="test.succeeded",
                node=Mock(),
                eventbus_publish=lambda: None,
                machine=mock_ok_machine,
            ),
            MesosActionRun(
                job_run_id="test.succeeded",
                name="test.succeeded",
                node=Mock(),
                eventbus_publish=lambda: None,
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
            eventbus_publish=lambda: None,
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
            eventbus_publish=lambda: None,
            action_runner=action_runner,
            end_time=timeutils.current_time(),
            exit_status=0
        )
        action_run.machine.state = action_run.STATE_UNKNOWN
        recover_action_run(action_run, action_runner)
        mock_node.submit_command.assert_called_once()
        assert action_run.machine.state == action_run.STATE_RUNNING
        assert action_run.end_time is None
        assert action_run.exit_status is None

    def test_group_by_actionrun_type(self):
        assert group_by_actionrun_type(self.action_runs) == \
            ([self.action_runs[0], self.action_runs[1]], [self.action_runs[2]])

    def test_launch_recovery_actionruns_for_job_runs(self):
        with mock.patch('tron.core.recovery.filter_action_runs_needing_recovery', autospec=True) as mock_filter, \
                mock.patch('tron.core.recovery.recover_action_run', autospec=True) as mock_recover_action_run:

            mock_actions = [
                mock.Mock(action_runner=NoActionRunnerFactory(), spec=SSHActionRun),
                mock.Mock(
                    action_runner=SubprocessActionRunnerFactory(
                        status_path='/tmp/foo', exec_path=('/tmp/foo')
                    ),
                    spec=SSHActionRun,
                ),
                mock.Mock(action_runner=NoActionRunnerFactory(), spec=MesosActionRun),
            ]

            mock_filter.return_value = mock_actions
            mock_action_runner = mock.Mock(autospec=True)

            mock_job_run = mock.Mock()
            launch_recovery_actionruns_for_job_runs([mock_job_run],
                                                    mock_action_runner)
            ssh_runs = mock_actions[:2]
            calls = [
                call(ssh_runs[0], mock_action_runner),
                call(ssh_runs[1], ssh_runs[1].action_runner)
            ]
            mock_recover_action_run.assert_has_calls(calls, any_order=True)

            mesos_run = mock_actions[2]
            assert mesos_run.recover.call_count == 1
