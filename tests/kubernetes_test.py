from typing import Any
from typing import Dict
from unittest import mock

import pytest
from task_processing.interfaces.event import Event
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig

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
