from __future__ import absolute_import
from __future__ import unicode_literals

import mock
from testify import assert_equal
from testify import setup_teardown
from testify import TestCase

from tron.mesos import MesosCluster
from tron.mesos import MesosTask


def mock_task_event(
    task_id, platform_type, raw=None, terminal=False, success=False, **kwargs
):
    return mock.MagicMock(
        kind='task',
        task_id=task_id,
        platform_type=platform_type,
        raw=raw or {},
        terminal=terminal,
        success=success,
        **kwargs
    )


class MesosTaskTestCase(TestCase):
    @setup_teardown
    def setup(self):
        self.action_run_id = 'my_service.job.1.action'
        self.task_id = '123abcuuid'
        self.task = MesosTask(
            id=self.action_run_id,
            task_config=mock.Mock(
                cmd='echo hello world',
                task_id=self.task_id,
            ),
        )
        # Suppress logging
        with mock.patch.object(self.task, 'log'):
            yield

    def test_handle_staging(self):
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='staging',
        )
        self.task.handle_event(event)
        assert self.task.state == MesosTask.PENDING

    def test_handle_running(self):
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='running',
        )
        self.task.handle_event(event)
        assert self.task.state == MesosTask.RUNNING

    def test_handle_running_for_other_task(self):
        event = mock_task_event(
            task_id='other321',
            platform_type='running',
        )
        self.task.handle_event(event)
        assert self.task.state == MesosTask.PENDING

    def test_handle_finished(self):
        self.task.started()
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='finished',
            terminal=True,
            success=True,
        )
        self.task.handle_event(event)
        assert self.task.is_complete

    def test_handle_failed(self):
        self.task.started()
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='failed',
            terminal=True,
            success=False,
        )
        self.task.handle_event(event)
        assert self.task.is_failed
        assert self.task.is_done

    def test_handle_killed(self):
        self.task.started()
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='killed',
            terminal=True,
            success=False,
        )
        self.task.handle_event(event)
        assert self.task.is_failed
        assert self.task.is_done

    def test_handle_lost(self):
        self.task.started()
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='lost',
            terminal=True,
            success=False,
        )
        self.task.handle_event(event)
        assert self.task.is_failed
        assert self.task.is_done

    def test_handle_error(self):
        self.task.started()
        event = mock_task_event(
            task_id=self.task_id,
            platform_type='error',
            terminal=True,
            success=False,
        )
        self.task.handle_event(event)
        assert self.task.is_failed
        assert self.task.is_done

    def test_handle_unknown_terminal_event(self):
        self.task.started()
        event = mock_task_event(
            task_id=self.task_id,
            platform_type=None,
            terminal=True,
            success=False,
        )
        self.task.handle_event(event)
        assert self.task.is_failed
        assert self.task.is_done

    def test_handle_success_sequence(self):
        self.task.handle_event(
            mock_task_event(
                task_id=self.task_id,
                platform_type='staging',
            )
        )
        self.task.handle_event(
            mock_task_event(
                task_id=self.task_id,
                platform_type='running',
            )
        )
        self.task.handle_event(
            mock_task_event(
                task_id=self.task_id,
                platform_type='finished',
                terminal=True,
                success=True,
            )
        )
        assert self.task.is_complete

    def test_log_event_error(self):
        with mock.patch.object(self.task, 'log_event_info') as mock_log_event:
            mock_log_event.side_effect = Exception
            self.task.handle_event(
                mock_task_event(
                    task_id=self.task_id,
                    platform_type='running',
                )
            )
            assert mock_log_event.called
        assert self.task.state == MesosTask.RUNNING


class MesosClusterTestCase(TestCase):
    @setup_teardown
    def setup_mocks(self):
        with mock.patch(
            'tron.mesos.PyDeferredQueue',
            autospec=True,
        ) as queue_cls, mock.patch(
            'tron.mesos.TaskProcessor',
            autospec=True,
        ) as processor_cls, mock.patch(
            'tron.mesos.Subscription',
            autospec=True,
        ) as runner_cls, mock.patch(
            'tron.mesos.get_mesos_leader',
            autospec=True,
        ) as mock_get_leader:
            self.mock_queue = queue_cls.return_value
            self.mock_processor = processor_cls.return_value
            self.mock_runner_cls = runner_cls
            self.mock_runner_cls.return_value.stopping = False
            self.mock_get_leader = mock_get_leader
            yield

    def test_mesos_cluster(self):
        self._test_init_disabled()
        self._test_submit_disabled()
        self._test_create_task_disabled()
        self._test_stop_disabled()

        # Configure mesos cluster to enable it
        self._test_configure()

        self._test_submit_enabled()
        self._test_create_task_defaults()

        self._test_kill()
        self._test_stop()

    def _test_init_disabled(self):
        assert_equal(MesosCluster.mesos_enabled, False)

    def _test_submit_enabled(self):
        mock_task = mock.MagicMock(spec_set=MesosTask)
        mock_task.get_mesos_id.return_value = 'this_task'
        MesosCluster.submit(mock_task)

        assert 'this_task' in MesosCluster.tasks
        assert_equal(MesosCluster.tasks['this_task'], mock_task)
        MesosCluster.runner.run.assert_called_once_with(
            mock_task.get_config.return_value,
        )

    def _test_submit_disabled(self):
        mock_task = mock.MagicMock()
        mock_task.get_mesos_id.return_value = 'this_task'
        MesosCluster.submit(mock_task)

        assert 'this_task' not in MesosCluster.tasks
        mock_task.exited.assert_called_once_with(1)

    def _test_configure(self):
        mock_volume = mock.MagicMock()
        options = mock.Mock(
            master_address="master-b.com",
            master_port=5555,
            secret='my_secret',
            role='tron',
            enabled=True,
            default_volumes=[mock_volume],
            dockercfg_location='auth',
            offer_timeout=1000,
        )
        MesosCluster.configure(options)

        expected_volume = mock_volume._asdict.return_value

        assert_equal(MesosCluster.mesos_master_address, "master-b.com")
        assert_equal(MesosCluster.mesos_master_port, 5555)
        assert_equal(MesosCluster.mesos_secret, 'my_secret')
        assert_equal(MesosCluster.mesos_role, 'tron')
        assert_equal(MesosCluster.mesos_enabled, True)
        assert_equal(MesosCluster.default_volumes, [expected_volume])
        assert_equal(MesosCluster.dockercfg_location, 'auth')
        assert_equal(MesosCluster.offer_timeout, 1000)

    @mock.patch('tron.mesos.MesosTask', autospec=True)
    def _test_create_task_defaults(self, mock_task):
        mock_serializer = mock.MagicMock()
        mock_volume = MesosCluster.default_volumes[0]
        task = MesosCluster.create_task(
            action_run_id='action_c',
            command='echo hi',
            cpus=1,
            mem=10,
            constraints=[],
            docker_image='container:latest',
            docker_parameters=[],
            env={'TESTING': 'true'},
            extra_volumes=[],
            serializer=mock_serializer,
        )
        MesosCluster.runner.TASK_CONFIG_INTERFACE.assert_called_once_with(
            name='action_c',
            cmd='echo hi',
            cpus=1,
            mem=10,
            constraints=[],
            image='container:latest',
            docker_parameters=[],
            environment={'TESTING': 'true'},
            volumes=[mock_volume],
            uris=['auth'],
            offer_timeout=1000,
        )
        assert_equal(task, mock_task.return_value)
        mock_task.assert_called_once_with(
            'action_c',
            MesosCluster.runner.TASK_CONFIG_INTERFACE.return_value,
            mock_serializer,
        )

    @mock.patch('tron.mesos.MesosTask', autospec=True)
    def _test_create_task_disabled(self, mock_task):
        # If Mesos is disabled, should return None
        mock_serializer = mock.MagicMock()
        task = MesosCluster.create_task(
            action_run_id='action_c',
            command='echo hi',
            cpus=1,
            mem=10,
            constraints=[],
            docker_image='container:latest',
            docker_parameters=[],
            env={'TESTING': 'true'},
            extra_volumes=[],
            serializer=mock_serializer,
        )
        assert task is None

    def test_process_event_task(self):
        event = mock_task_event('this_task', 'some_platform_type')
        mock_task = mock.MagicMock(spec_set=MesosTask)
        mock_task.get_mesos_id.return_value = 'this_task'
        MesosCluster.tasks['this_task'] = mock_task

        MesosCluster._process_event(event)
        mock_task.handle_event.assert_called_once_with(event)

    def test_process_event_task_id_invalid(self):
        event = mock_task_event('other_task', 'some_platform_type')
        mock_task = mock.MagicMock(spec_set=MesosTask)
        mock_task.get_mesos_id.return_value = 'this_task'
        MesosCluster.tasks['this_task'] = mock_task

        MesosCluster._process_event(event)
        assert_equal(mock_task.handle_event.call_count, 0)

    def _test_stop(self):
        mock_task = mock.MagicMock()
        MesosCluster.tasks = {'task_id': mock_task}

        with mock.patch('tron.mesos.MesosCluster.runner', autospec=None), \
                mock.patch('tron.mesos.MesosCluster.deferred', autospec=None):
            MesosCluster.stop()
            assert_equal(MesosCluster.runner.stop.call_count, 1)
            assert_equal(MesosCluster.deferred.cancel.call_count, 1)
            mock_task.exited.assert_called_once_with(None)
            assert_equal(len(MesosCluster.tasks), 0)

    def _test_stop_disabled(self):
        # Shouldn't raise an error
        MesosCluster.stop()

    def _test_kill(self):
        MesosCluster.kill('fake_task_id')
        MesosCluster.runner.kill.assert_called_once_with('fake_task_id')
