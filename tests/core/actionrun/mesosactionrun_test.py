import mock
import pytest

from tron import actioncommand
from tron import node
from tron.config.schema import ConfigConstraint
from tron.config.schema import ConfigParameter
from tron.config.schema import ConfigVolume
from tron.config.schema import ExecutorTypes
from tron.core.actionrun.mesos import ActionCommand
from tron.core.actionrun.mesos import ActionRun
from tron.core.actionrun.mesos import MesosActionRun


class TestMesosActionRun:
    @pytest.fixture(autouse=True)
    def setup_action_run(self):
        self.output_path = mock.MagicMock()
        self.command = "do the command"
        self.extra_volumes = [ConfigVolume('/mnt/foo', '/mnt/foo', 'RO')]
        self.constraints = [ConfigConstraint('an attr', 'an op', 'a val')]
        self.docker_parameters = [ConfigParameter('init', 'true')]
        self.other_task_kwargs = {
            'cpus': 1,
            'mem': 50,
            'disk': 42,
            'docker_image': 'container:v2',
            'env': {
                'TESTING': 'true',
                'TRON_JOB_NAMESPACE': 'mynamespace',
                'TRON_JOB_NAME': 'myjob',
                'TRON_RUN_NUM': '42',
                'TRON_ACTION': 'action_name',
            },
        }
        self.action_run = MesosActionRun(
            job_run_id="mynamespace.myjob.42",
            name="action_name",
            node=mock.create_autospec(node.Node),
            rendered_command=self.command,
            output_path=self.output_path,
            executor=ExecutorTypes.mesos.value,
            extra_volumes=self.extra_volumes,
            constraints=self.constraints,
            docker_parameters=self.docker_parameters,
            **self.other_task_kwargs
        )

    @mock.patch('tron.core.actionrun.mesos.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_submit_command(self, mock_cluster_repo, mock_filehandler):
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        # submit_command should reset the task_id
        self.action_run.mesos_task_id = 'last_attempt'
        with mock.patch.object(
            self.action_run,
            'watch',
            autospec=True,
        ) as mock_watch:
            self.action_run.submit_command()

            mock_get_cluster = mock_cluster_repo.get_cluster
            mock_get_cluster.assert_called_once_with()

            mock_get_cluster.return_value.create_task.assert_called_once_with(
                action_run_id=self.action_run.id,
                command=self.command,
                serializer=serializer,
                task_id=None,
                extra_volumes=[e._asdict() for e in self.extra_volumes],
                constraints=[['an attr', 'an op', 'a val']],
                docker_parameters=[{'key': 'init', 'value': 'true'}],
                **self.other_task_kwargs
            )
            task = mock_get_cluster.return_value.create_task.return_value
            mock_get_cluster.return_value.submit.assert_called_once_with(task)
            mock_watch.assert_called_once_with(task)
            assert self.action_run.mesos_task_id == task.get_mesos_id.return_value

        mock_filehandler.OutputStreamSerializer.assert_called_with(
            self.action_run.output_path,
        )

    @mock.patch('tron.core.actionrun.mesos.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_submit_command_task_none(
        self, mock_cluster_repo, mock_filehandler
    ):
        # Task is None if Mesos is disabled
        mock_get_cluster = mock_cluster_repo.get_cluster
        mock_get_cluster.return_value.create_task.return_value = None
        self.action_run.submit_command()

        mock_get_cluster.assert_called_once_with()
        assert mock_get_cluster.return_value.submit.call_count == 0
        assert self.action_run.is_failed

    @mock.patch('tron.core.actionrun.mesos.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_recover(self, mock_cluster_repo, mock_filehandler):
        self.action_run.machine.state = ActionRun.UNKNOWN
        self.action_run.mesos_task_id = 'my_mesos_id'
        serializer = mock_filehandler.OutputStreamSerializer.return_value
        with mock.patch.object(
            self.action_run,
            'watch',
            autospec=True,
        ) as mock_watch:
            assert self.action_run.recover()

            mock_get_cluster = mock_cluster_repo.get_cluster
            mock_get_cluster.assert_called_once_with()
            mock_get_cluster.return_value.create_task.assert_called_once_with(
                action_run_id=self.action_run.id,
                command=self.command,
                serializer=serializer,
                task_id='my_mesos_id',
                extra_volumes=[e._asdict() for e in self.extra_volumes],
                constraints=[['an attr', 'an op', 'a val']],
                docker_parameters=[{'key': 'init', 'value': 'true'}],
                **self.other_task_kwargs
            ), mock_get_cluster.return_value.create_task.calls
            task = mock_get_cluster.return_value.create_task.return_value
            mock_get_cluster.return_value.recover.assert_called_once_with(task)
            mock_watch.assert_called_once_with(task)

        assert self.action_run.is_running
        assert self.action_run.end_time is None
        mock_filehandler.OutputStreamSerializer.assert_called_with(
            self.action_run.output_path,
        )

    @mock.patch('tron.core.actionrun.mesos.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_recover_done_no_change(self, mock_cluster_repo, mock_filehandler):
        self.action_run.machine.state = ActionRun.SUCCEEDED
        self.action_run.mesos_task_id = 'my_mesos_id'

        assert not self.action_run.recover()
        assert mock_cluster_repo.get_cluster.call_count == 0
        assert self.action_run.is_succeeded

    @mock.patch('tron.core.actionrun.mesos.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_recover_no_mesos_task_id(
        self, mock_cluster_repo, mock_filehandler
    ):
        self.action_run.machine.state = ActionRun.UNKNOWN
        self.action_run.mesos_task_id = None

        assert not self.action_run.recover()
        assert mock_cluster_repo.get_cluster.call_count == 0
        assert self.action_run.is_unknown
        assert self.action_run.end_time is not None

    @mock.patch('tron.core.actionrun.mesos.filehandler', autospec=True)
    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_recover_task_none(self, mock_cluster_repo, mock_filehandler):
        self.action_run.machine.state = ActionRun.UNKNOWN
        self.action_run.mesos_task_id = 'my_mesos_id'
        # Task is None if Mesos is disabled
        mock_get_cluster = mock_cluster_repo.get_cluster
        mock_get_cluster.return_value.create_task.return_value = None
        assert not self.action_run.recover()

        mock_get_cluster.assert_called_once_with()
        assert self.action_run.is_unknown
        assert mock_get_cluster.return_value.recover.call_count == 0
        assert self.action_run.end_time is not None

    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_kill_task(self, mock_cluster_repo):
        mock_get_cluster = mock_cluster_repo.get_cluster
        self.action_run.mesos_task_id = 'fake_task_id'
        self.action_run.machine.state = ActionRun.RUNNING

        self.action_run.kill()
        mock_get_cluster.return_value.kill.assert_called_once_with(
            self.action_run.mesos_task_id
        )

    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_kill_task_no_task_id(self, mock_cluster_repo):
        self.action_run.machine.state = ActionRun.RUNNING
        error_message = self.action_run.kill()
        assert error_message == "Error: Can't find task id for the action."

    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_stop_task(self, mock_cluster_repo):
        mock_get_cluster = mock_cluster_repo.get_cluster
        self.action_run.mesos_task_id = 'fake_task_id'
        self.action_run.machine.state = ActionRun.RUNNING

        self.action_run.stop()
        mock_get_cluster.return_value.kill.assert_called_once_with(
            self.action_run.mesos_task_id
        )

    @mock.patch('tron.core.actionrun.mesos.MesosClusterRepository', autospec=True)
    def test_stop_task_no_task_id(self, mock_cluster_repo):
        self.action_run.machine.state = ActionRun.RUNNING
        error_message = self.action_run.stop()
        assert error_message == "Error: Can't find task id for the action."

    def test_handler_exiting_unknown(self):
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

    def test_handler_exiting_unknown_retry(self):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            exit_status=None,
        )
        self.action_run.retries_remaining = 1
        self.action_run.exit_statuses = []
        self.action_run.start = mock.Mock()

        self.action_run.machine.transition('start')
        self.action_run.machine.transition('started')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.EXITING,
        )
        assert self.action_run.retries_remaining == 0
        assert not self.action_run.is_unknown
        assert self.action_run.start.call_count == 1

    def test_handler_exiting_failstart_failed(self):
        self.action_run.action_command = mock.create_autospec(
            actioncommand.ActionCommand,
            exit_status=1,
        )
        self.action_run.machine.transition('start')
        assert self.action_run.handler(
            self.action_run.action_command,
            ActionCommand.FAILSTART,
        )
        assert self.action_run.is_failed
