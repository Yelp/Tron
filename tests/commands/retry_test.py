import random
from unittest import mock

import pytest

from tron.commands import client
from tron.commands import retry


async def _empty_coro(*args, **kwargs):
    return None


@pytest.fixture(autouse=True)
def mock_sleep():
    with mock.patch("asyncio.sleep", _empty_coro, autospec=None):
        yield


@pytest.fixture(autouse=True)
def mock_client():
    with mock.patch.object(client, "Client", autospec=True) as m:
        m.return_value.url_base = "http://localhost"
        yield m


@pytest.fixture(autouse=True)
def mock_urlopen():  # prevent any requests from being made
    with mock.patch("urllib.request.urlopen", autospec=True) as m:
        yield m


@pytest.fixture
def mock_client_request():
    with mock.patch.object(client, "request", autospec=True) as m:
        m.return_value = mock.Mock(error=False, content={})  # response
        yield m


@mock.patch.object(
    client,
    "get_object_type_from_identifier",
    return_value=client.TronObjectIdentifier("JOB_RUN", "/a_job_run"),
    autospec=True,
)
def test_retry_action_init_not_an_action(mock_get_obj_type, mock_client):
    tron_client = mock_client.return_value
    with pytest.raises(ValueError):
        retry.RetryAction(tron_client, "a_fake_action_run")


@pytest.fixture
def fake_retry_action(mock_client):
    tron_client = mock_client.return_value
    tron_client.action_runs.return_value = dict(
        action_name="a_fake_action",
        requirements=["required_action_0", "required_action_1"],
        triggered_by="a_fake_trigger_0 (done), a_fake_trigger_1",
    )
    tron_client.job_runs.return_value = dict(
        job_name="a_fake_job",
        run_num=1234,
        runs=[
            dict(action_name="required_action_0", state="succeeded"),
            dict(action_name="non_required_action", state="succeeded"),
            dict(action_name="required_action_1", state="failed"),
            dict(action_name="upstream_action_0", trigger_downstreams="a_fake_trigger_0"),
            dict(action_name="upstream_action_1", trigger_downstreams="a_fake_trigger_1"),
            tron_client.action_runs.return_value,
        ],
    )

    with mock.patch.object(
        client,
        "get_object_type_from_identifier",
        side_effect=[
            client.TronObjectIdentifier("ACTION_RUN", "/a_fake_job/0/a_fake_action"),
            client.TronObjectIdentifier("JOB_RUN", "/a_fake_job/0"),
        ],
        autospec=True,
    ):
        yield retry.RetryAction(tron_client, "a_fake_job.0.a_fake_action", use_latest_command=True)


def test_retry_action_init_ok(fake_retry_action):
    assert fake_retry_action.retry_params == dict(command="retry", use_latest_command=1)
    assert fake_retry_action.full_action_name == "a_fake_job.0.a_fake_action"
    fake_retry_action.tron_client.action_runs.assert_called_once_with(
        "/a_fake_job/0/a_fake_action",
        num_lines=0,
    )
    assert fake_retry_action.action_name == "a_fake_action"
    assert fake_retry_action.action_run_id.url == "/a_fake_job/0/a_fake_action"
    fake_retry_action.tron_client.job_runs.assert_called_once_with("/a_fake_job/0")
    assert fake_retry_action.job_run_name == "a_fake_job.0"
    assert fake_retry_action.job_run_id.url == "/a_fake_job/0"
    assert fake_retry_action._required_action_indices == {"required_action_0": 0, "required_action_1": 2}


def test_check_trigger_statuses(fake_retry_action, event_loop):
    expected = dict(a_fake_trigger_0=True, a_fake_trigger_1=False)
    assert expected == event_loop.run_until_complete(fake_retry_action.check_trigger_statuses())
    assert fake_retry_action.tron_client.action_runs.call_args_list[1] == mock.call(  # 0th call is in init
        "/a_fake_job/0/a_fake_action",
        num_lines=0,
    )


def test_check_required_actions_statuses(fake_retry_action, event_loop):
    expected = dict(required_action_0=True, required_action_1=False)
    assert expected == event_loop.run_until_complete(fake_retry_action.check_required_actions_statuses())
    assert fake_retry_action.tron_client.job_runs.call_args_list[1] == mock.call("/a_fake_job/0")  # 0th call is in init


@pytest.mark.parametrize(
    "expected,triggered_by,required_action_1_state",
    [
        (False, "a_fake_trigger_0 (done), a_fake_trigger_1", "skipped"),  # unpublished triggers
        (False, "a_fake_trigger_0 (done), a_fake_trigger_1 (done)", "failed"),  # required not succeeded
        (True, "a_fake_trigger_0 (done), a_fake_trigger_1 (done)", "succeeded"),  # all done
    ],
)
def test_can_retry(fake_retry_action, event_loop, expected, triggered_by, required_action_1_state):
    fake_retry_action.tron_client.action_runs.return_value["triggered_by"] = triggered_by
    fake_retry_action.tron_client.job_runs.return_value["runs"][2]["state"] = required_action_1_state
    assert expected == event_loop.run_until_complete(fake_retry_action.can_retry())


def test_wait_for_deps_timeout(fake_retry_action, event_loop):
    assert not event_loop.run_until_complete(fake_retry_action.wait_for_deps(deps_timeout_s=3, poll_interval_s=1))
    assert fake_retry_action._elapsed.seconds == 3
    assert fake_retry_action.tron_client.action_runs.call_count == 5  # 1 in init, 4 in this test


def test_wait_for_deps_all_deps_done(fake_retry_action, event_loop):
    fake_retry_action.tron_client.job_runs.return_value["runs"][2]["state"] = "skipped"
    fake_retry_action.tron_client.action_runs.return_value = None
    triggered_by_results = [
        "a_fake_trigger_0 (done), a_fake_trigger_1",
        "a_fake_trigger_0 (done), a_fake_trigger_1",
        "a_fake_trigger_0 (done), a_fake_trigger_1 (done)",
    ]
    fake_retry_action.tron_client.action_runs.side_effect = [
        dict(
            action_name="a_fake_action",
            requirements=["required_action_0", "required_action_1"],
            triggered_by=r,
        )
        for r in triggered_by_results
    ]

    assert event_loop.run_until_complete(fake_retry_action.wait_for_deps(deps_timeout_s=3, poll_interval_s=1))
    # 3rd triggered_by result returned on check at 2nd second
    assert fake_retry_action._elapsed.seconds == 2
    assert fake_retry_action.tron_client.action_runs.call_count == 4  # 1 in init, 3 in this test


@pytest.mark.parametrize("expected,error", [(False, True), (True, False)])
def test_issue_retry(fake_retry_action, mock_client_request, event_loop, expected, error):
    mock_client_request.return_value.error = error
    assert expected == event_loop.run_until_complete(fake_retry_action.issue_retry())
    assert expected == fake_retry_action.succeeded


def test_wait_for_retry_deps_not_done(fake_retry_action, mock_client_request, event_loop):
    assert not event_loop.run_until_complete(
        fake_retry_action.wait_and_retry(deps_timeout_s=10, poll_interval_s=1, jitter=True),
    )
    assert fake_retry_action._elapsed.seconds == 10  # timeout
    mock_client_request.assert_not_called()  # retry not attempted


def test_wait_for_retry_deps_done(fake_retry_action, mock_client_request, event_loop):
    fake_retry_action.tron_client.job_runs.return_value["runs"][2]["state"] = "skipped"
    fake_retry_action.tron_client.action_runs.return_value["triggered_by"] = (
        "a_fake_trigger_0 (done), a_fake_trigger_1 (done)"
    )
    mock_client_request.return_value.error = False
    random.seed(1)  # init delay is 1s

    assert event_loop.run_until_complete(
        fake_retry_action.wait_and_retry(deps_timeout_s=10, poll_interval_s=5, jitter=True),
    )
    assert fake_retry_action._elapsed.seconds == 1  # init delay only
    mock_client_request.assert_called_once_with(
        "http://localhost/a_fake_job/0/a_fake_action",
        data=dict(command="retry", use_latest_command=1),
        user_attribution=True,
    )


@mock.patch.object(retry, "RetryAction", autospec=True)
def test_retry_actions(mock_retry_action, mock_client, event_loop):
    mock_wait_and_retry = mock_retry_action.return_value.wait_and_retry
    mock_wait_and_retry.return_value = _empty_coro()

    r_actions = retry.retry_actions(
        "http://localhost",
        ["a_job.0.an_action_0", "another_job.1.an_action_1"],
        use_latest_command=True,
        deps_timeout_s=4,
    )

    assert r_actions == [mock_retry_action.return_value] * 2
    assert mock_retry_action.call_args_list == [
        mock.call(mock_client.return_value, "a_job.0.an_action_0", use_latest_command=True),
        mock.call(mock_client.return_value, "another_job.1.an_action_1", use_latest_command=True),
    ]
    assert mock_wait_and_retry.call_args_list == [
        mock.call(deps_timeout_s=4, jitter=False),
        mock.call(deps_timeout_s=4),
    ]
