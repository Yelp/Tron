import datetime

import mock
import pytest

from tron.commands import backfill
from tron.commands import client

TEST_DATETIME_1 = datetime.datetime.strptime("2004-07-01", "%Y-%m-%d")
TEST_DATETIME_2 = datetime.datetime.strptime("2004-07-02", "%Y-%m-%d")
TEST_DATETIME_3 = datetime.datetime.strptime("2004-07-03", "%Y-%m-%d")


@pytest.fixture(autouse=True)
def mock_sleep():
    async def empty_coro(*args, **kwargs):
        return None

    with mock.patch("asyncio.sleep", empty_coro, autospec=None):
        yield


@pytest.fixture(autouse=True)
def mock_client():
    with mock.patch.object(client, "Client", autospec=True) as m:
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


@pytest.fixture
def fake_backfill_run(mock_client):
    tron_client = mock_client.return_value
    tron_client.url_base = "http://localhost"
    yield backfill.BackfillRun(
        tron_client, client.TronObjectIdentifier("JOB", "/a_job"), TEST_DATETIME_1,
    )


@pytest.mark.parametrize(
    "is_error,result,expected",
    [
        (True, "an_error_msg", None),  # tron api failed
        (False, "weird_resp_msg", None),  # bad response, can't get job run name
        (False, "Created JobRun:real_job_run_name", "real_job_run_name"),  # ok
    ],
)
def test_backfill_run_create(mock_client_request, fake_backfill_run, event_loop, is_error, result, expected):
    mock_client_request.return_value.error = is_error
    mock_client_request.return_value.content["result"] = result
    assert expected == event_loop.run_until_complete(fake_backfill_run.create())


@pytest.mark.parametrize(
    "obj_type,expected",
    [
        (client.RequestError(""), None),
        ([client.TronObjectIdentifier("JOB_RUN", "/a_run")], client.TronObjectIdentifier("JOB_RUN", "/a_run")),
    ],
)
@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_backfill_run_get_run_id(mock_get_obj_type, fake_backfill_run, event_loop, obj_type, expected):
    mock_get_obj_type.side_effect = obj_type
    assert expected == event_loop.run_until_complete(fake_backfill_run.get_run_id())
    assert expected == fake_backfill_run.run_id


@pytest.mark.parametrize(
    "job_run_resp,expected",
    [
        (client.RequestError, "unknown"),  # polling failed
        ([{}], "unknown"),  # default to unknown
        ([{"state": "failed"}], "failed"),  # ok
    ],
)
def test_backfill_run_sync_state(fake_backfill_run, event_loop, job_run_resp, expected):
    fake_backfill_run.run_id = client.TronObjectIdentifier("JOB_RUN", "/a_run")
    fake_backfill_run.tron_client.job_runs.side_effect = job_run_resp
    assert expected == event_loop.run_until_complete(fake_backfill_run.sync_state())


def test_backfill_run_watch_until_completion(fake_backfill_run, event_loop):
    async def change_run_state():
        fake_backfill_run.run_state = "cancelled"

    fake_backfill_run.sync_state = change_run_state
    assert "cancelled" == event_loop.run_until_complete(fake_backfill_run.watch_until_completion())


@pytest.mark.parametrize(
    "run_id,response,expected",
    [
        (None, mock.Mock(error=False), False),  # no run_id
        (client.TronObjectIdentifier("JOB_RUN", "/a_run"), mock.Mock(error=True), False),  # api error
        (client.TronObjectIdentifier("JOB_RUN", "/a_run"), mock.Mock(error=False), True),  # ok
    ],
)
def test_backfill_run_cancel(
    mock_client_request, fake_backfill_run, event_loop, run_id, response, expected,
):
    fake_backfill_run.run_id = run_id
    mock_client_request.return_value = response
    assert expected == event_loop.run_until_complete(fake_backfill_run.cancel())


@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_run_backfill_for_date_range_job_dne(mock_get_obj_type, event_loop):
    mock_get_obj_type.side_effect = ValueError
    with pytest.raises(ValueError):
        event_loop.run_until_complete(backfill.run_backfill_for_date_range("a_server", "a_job", []),)


@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_run_backfill_for_date_range_not_a_job(mock_get_obj_type, event_loop):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB_RUN", "a_url")
    with pytest.raises(ValueError):
        event_loop.run_until_complete(backfill.run_backfill_for_date_range("a_server", "a_job", []),)


@pytest.mark.parametrize(
    "ignore_errors,expected",
    [(True, {"succeeded", "failed", "unknown"}), (False, {"succeeded", "failed", "not started"}),],
)
@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_run_backfill_for_date_range_normal(mock_get_obj_type, event_loop, ignore_errors, expected):
    run_states = (state for state in ["succeeded", "failed", "unknown"])

    async def fake_run_until_completion(self):
        self.run_state = next(run_states)

    backfill.BackfillRun.run_until_completion = fake_run_until_completion
    dates = [TEST_DATETIME_1, TEST_DATETIME_2, TEST_DATETIME_3]
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB", "a_url")

    backfill_runs = event_loop.run_until_complete(
        backfill.run_backfill_for_date_range("a_server", "a_job", dates, max_parallel=2, ignore_errors=ignore_errors,)
    )

    assert {br.run_state for br in backfill_runs} == expected
