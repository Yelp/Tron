from typing import Any
from typing import Dict
from unittest import mock

import pytest
from task_processing.interfaces.event import Event
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig

from tron.kubernetes import KubernetesCluster
from tron.kubernetes import KubernetesTask


@pytest.fixture
def mock_kubernetes_task():
    with mock.patch(
        "tron.kubernetes.logging.getLogger", return_value=mock.Mock(handlers=[mock.Mock()]), autospec=None,
    ):
        yield KubernetesTask(
            action_run_id="mock_service.mock_job.1.mock_action",
            task_config=KubernetesTaskConfig(name="mock--service-mock-job-mock--action", uuid="123456",),
        )


@pytest.fixture
def mock_kubernetes_cluster():
    with mock.patch("tron.kubernetes.PyDeferredQueue", autospec=True,), mock.patch(
        "tron.kubernetes.TaskProcessor", autospec=True,
    ):
        yield KubernetesCluster("kube-cluster-a:1234")


def mock_event_factory(
    task_id: str,
    platform_type: str,
    message: str = None,
    raw: Dict[str, Any] = None,
    success: bool = False,
    terminal: bool = False,
) -> Event:
    return Event(
        kind="task",
        task_id=task_id,
        platform_type=platform_type,
        raw=raw or {},
        terminal=terminal,
        success=success,
        message=message,
    )


def test_get_event_logger_add_unique_handlers(mock_kubernetes_task):
    """
    Ensures that only a single handler (for stderr) is added to the
    Kubernetes Taskevent logger, to prevent duplicate log output.
    """
    # Call 2 times to make sure 2nd call doesn't add another handler
    logger = mock_kubernetes_task.get_event_logger()
    logger = mock_kubernetes_task.get_event_logger()

    assert len(logger.handlers) == 1


def test_handle_event_log_event_info_exception(mock_kubernetes_task):
    with mock.patch.object(
        mock_kubernetes_task, "log_event_info", autospec=True, side_effect=Exception
    ) as mock_log_event_info:
        mock_kubernetes_task.handle_event(
            mock_event_factory(task_id=mock_kubernetes_task.get_kubernetes_id(), platform_type="running")
        )

    # TODO: should also assert that the task is in the expected state once that's hooked up
    assert mock_log_event_info.called


def test_handle_event_exit_early_on_misrouted_event(mock_kubernetes_task):
    with mock.patch.object(mock_kubernetes_task, "log_event_info", autospec=True,) as mock_log_event_info:
        mock_kubernetes_task.handle_event(
            mock_event_factory(task_id="not-the-pods-youre-looking-for", platform_type="finished")
        )

    # TODO: should also assert that the task is in the expected state once that's hooked up
    # we log before actually doing anything with an event, so this not being called means
    # we exited early
    assert not mock_log_event_info.called


def test_create_task_disabled():
    cluster = KubernetesCluster("kube-cluster-a:1234", enabled=False)
    mock_serializer = mock.MagicMock()

    task = cluster.create_task(action_run_id="action_a", serializer=mock_serializer,)

    assert task is None


def test_create_task(mock_kubernetes_cluster):
    mock_serializer = mock.MagicMock()

    task = mock_kubernetes_cluster.create_task(action_run_id="action_a", serializer=mock_serializer,)

    assert task is not None


def test_create_task_with_task_id(mock_kubernetes_cluster):
    mock_serializer = mock.MagicMock()

    task = mock_kubernetes_cluster.create_task(action_run_id="action_a", serializer=mock_serializer, task_id="yay.1234")

    mock_kubernetes_cluster.runner.TASK_CONFIG_INTERFACE().set_pod_name.assert_called_once_with("yay.1234")
    assert task is not None


def test_create_task_with_invalid_task_id(mock_kubernetes_cluster):
    mock_serializer = mock.MagicMock()

    with mock.patch.object(mock_kubernetes_cluster, "runner") as mock_runner:
        mock_runner.TASK_CONFIG_INTERFACE.return_value.set_pod_name = mock.MagicMock(side_effect=ValueError)
        task = mock_kubernetes_cluster.create_task(action_run_id="action_a", serializer=mock_serializer, task_id="boo")

    assert task is None


def test_process_event_task(mock_kubernetes_cluster):
    event = mock_event_factory(task_id="abc.123", platform_type="mock_type")
    mock_kubernetes_task = mock.MagicMock(spec_set=KubernetesTask)
    mock_kubernetes_task.get_kubernetes_id.return_value = "abc.123"
    mock_kubernetes_cluster.tasks["abc.123"] = mock_kubernetes_task

    mock_kubernetes_cluster.process_event(event)

    mock_kubernetes_task.handle_event.assert_called_once_with(event)


def test_process_event_task_invalid_id(mock_kubernetes_cluster):
    event = mock_event_factory(task_id="hwat.dis", platform_type="mock_type")
    mock_kubernetes_task = mock.MagicMock(spec_set=KubernetesTask)
    mock_kubernetes_task.get_kubernetes_id.return_value = "abc.123"
    mock_kubernetes_cluster.tasks["abc.123"] = mock_kubernetes_task

    mock_kubernetes_cluster.process_event(event)

    assert mock_kubernetes_task.handle_event.call_count == 0


def test_stop_default(mock_kubernetes_cluster):
    # When stopping, tasks should not exit. They will be recovered
    mock_task = mock.MagicMock()
    mock_kubernetes_cluster.tasks = {"task_id": mock_task}
    mock_kubernetes_cluster.stop()
    assert mock_kubernetes_cluster.deferred is None
    assert mock_task.exited.call_count == 0
    assert len(mock_kubernetes_cluster.tasks) == 1


def test_stop_disabled():
    # Shouldn't raise an error
    mock_kubernetes_cluster = KubernetesCluster("kube-cluster-a:1234", enabled=False)
    mock_kubernetes_cluster.stop()


def test_set_enabled_enable(mock_kubernetes_cluster):
    with mock.patch.object(mock_kubernetes_cluster, "connect", autospec=True,) as mock_connect, mock.patch.object(
        mock_kubernetes_cluster, "stop", autospec=True,
    ) as mock_stop:
        mock_kubernetes_cluster.set_enabled(is_enabled=True)

        assert mock_kubernetes_cluster.enabled is True
        mock_connect.assert_called_once()
        mock_stop.assert_not_called()


def test_set_enabled_disable(mock_kubernetes_cluster):
    with mock.patch.object(mock_kubernetes_cluster, "connect", autospec=True,) as mock_connect, mock.patch.object(
        mock_kubernetes_cluster, "stop", autospec=True,
    ) as mock_stop:
        mock_kubernetes_cluster.set_enabled(is_enabled=False)

        assert mock_kubernetes_cluster.enabled is False
        mock_connect.assert_not_called()
        mock_stop.assert_called_once()
