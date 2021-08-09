import asyncio
import datetime

import asynctest
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


@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_run_backfill_for_date_range_normal(mock_get_obj_type, event_loop):
    dates = [TEST_DATETIME_1, TEST_DATETIME_2, TEST_DATETIME_3]
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB", "a_url")
    mock_run_backfill_for_date = asynctest.CoroutineMock(
        side_effect=[
            ("job_run_1", TEST_DATETIME_1, True),
            ("job_run_2", TEST_DATETIME_2, True),
            ("job_run_3", TEST_DATETIME_3, False),
        ]
    )

    with mock.patch.object(backfill, "run_backfill_for_date", mock_run_backfill_for_date, autospec=None):
        all_successful = event_loop.run_until_complete(
            backfill.run_backfill_for_date_range("a_server", "a_job", dates, max_parallel=2)
        )

    assert all_successful


@pytest.mark.parametrize(
    "job_run_name,run_successful,expected",
    [
        (None, True, (None, TEST_DATETIME_1, False)),  # job run not created
        ("", True, ("", TEST_DATETIME_1, False)),  # don't know job run's name
        ("a_job_run", True, ("a_job_run", TEST_DATETIME_1, True)),  # job watched
        ("", asyncio.CancelledError(""), ("", TEST_DATETIME_1, False)),  # cancelled, but don't know name
        ("a_job_run", asyncio.CancelledError(""), ("a_job_run", TEST_DATETIME_1, False)),  # cancelled
    ],
)
@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_run_backfill_for_date(
    mock_get_obj_type, mock_client, mock_client_request, event_loop, job_run_name, run_successful, expected,
):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB_RUN", "a_job_run_url")
    mock_create = asynctest.CoroutineMock(return_value=job_run_name)
    mock_watch = asynctest.CoroutineMock(side_effect=[run_successful])
    tron_client = mock_client.return_value
    tron_client.url_base = "http://localhost"
    job_id = client.TronObjectIdentifier("JOB", "a_job_url")

    with mock.patch.object(backfill, "_create_job_run", mock_create, autospec=None,), mock.patch.object(
        backfill, "_watch_job_run", mock_watch, autospec=None,
    ):
        run_result = event_loop.run_until_complete(
            backfill.run_backfill_for_date(tron_client, job_id, TEST_DATETIME_1),
        )

    assert expected == run_result
    assert mock_create.call_args_list == [
        mock.call(tron_client.url_base, "a_job_url", TEST_DATETIME_1),
    ]
    if job_run_name:
        assert mock_watch.call_args_list == [
            mock.call(tron_client, "a_job_run", "a_job_run_url", "2004-07-01"),
        ]
        if isinstance(run_successful, asyncio.CancelledError):
            assert mock_client_request.call_args_list == [
                mock.call("/".join([tron_client.url_base, "a_job_run_url"]), data=dict(command="cancel")),
            ]


@pytest.mark.parametrize(
    "is_error,result,expected",
    [
        (True, "an_error_msg", None),  # tron api failed
        (False, "weird_resp_msg", ""),  # bad response, can't get job run name
        (False, "Created JobRun:real_job_run_name", "real_job_run_name"),  # ok
    ],
)
def test_create_job_run(mock_client_request, event_loop, is_error, result, expected):
    mock_client_request.return_value.error = is_error
    mock_client_request.return_value.content["result"] = result

    job_run_name = event_loop.run_until_complete(
        backfill._create_job_run("a_server", "http://localhost", TEST_DATETIME_1),
    )

    assert expected == job_run_name


@pytest.mark.parametrize(
    "job_run_resp,poll_cnt,expected",
    [
        (client.RequestError, 1, False),  # polling failed
        ([{}], 1, False),  # default to unknown
        ([{"state": "running"}, {"state": "failed"}], 2, False),  # job run fail
        ([{"state": "running"}, {"state": "succeeded"}], 2, True),  # ok
    ],
)
@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_watch_job_run(mock_get_obj_type, mock_client, event_loop, job_run_resp, poll_cnt, expected):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB_RUN", "a_url")
    tron_client = mock_client.return_value
    tron_client.url_base = "http://localhost"
    tron_client.job_runs.side_effect = job_run_resp

    run_successful = event_loop.run_until_complete(
        backfill._watch_job_run(tron_client, "a_job_run", "a_url", "2004-07-01"),
    )

    assert expected == run_successful
    assert poll_cnt == len(tron_client.job_runs.call_args_list)
