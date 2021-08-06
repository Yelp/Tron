import datetime

import mock
import pytest

from tron.commands import backfill
from tron.commands import client

TEST_DATETIME_1 = datetime.datetime.strptime("2004-07-01", "%Y-%m-%d")
TEST_DATETIME_2 = datetime.datetime.strptime("2004-07-02", "%Y-%m-%d")


@pytest.fixture(autouse=True)
def mock_sleep():
    with mock.patch("time.sleep", autospec=None, return_value=None):
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
def test_run_backfill_for_date_range_job_dne(mock_get_obj_type):
    mock_get_obj_type.side_effect = ValueError
    with pytest.raises(ValueError):
        next(backfill.run_backfill_for_date_range("a_server", "a_job", []))


@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
def test_run_backfill_for_date_range_not_a_job(mock_get_obj_type):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB_RUN", "a_url")
    with pytest.raises(ValueError):
        next(backfill.run_backfill_for_date_range("a_server", "a_job", []))


@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
@mock.patch.object(backfill, "_create_job_run", autospec=True)
def test_run_backfill_for_date_range_create_only(
    mock_create_job_run, mock_get_obj_type,
):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB", "a_url")
    mock_create_job_run.side_effect = [None, ""]

    results = list(
        backfill.run_backfill_for_date_range(
            "a_server", "a_job", [TEST_DATETIME_1, TEST_DATETIME_2], ignore_errors=False,
        ),
    )

    assert results == [False, True]
    assert mock_create_job_run.call_args_list == [
        mock.call("a_server", "a_url", TEST_DATETIME_1),
        mock.call("a_server", "a_url", TEST_DATETIME_2),
    ]


@pytest.mark.parametrize("ignore_errors,expected", [(False, [False, True]), (True, [True, True])])
@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
@mock.patch.object(backfill, "_create_job_run", autospec=True)
@mock.patch.object(backfill, "_watch_job_run", autospec=True)
def test_run_backfill_for_date_range_watch(
    mock_watch_job_run, mock_create_job_run, mock_get_obj_type, mock_client, ignore_errors, expected,
):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB", "a_url")
    mock_create_job_run.return_value = "a_job_run"
    mock_watch_job_run.side_effect = [False, True]

    results = list(
        backfill.run_backfill_for_date_range(
            "a_server", "a_job", [TEST_DATETIME_1, TEST_DATETIME_2], ignore_errors=ignore_errors,
        ),
    )

    assert results == expected
    assert mock_create_job_run.call_args_list == [
        mock.call("a_server", "a_url", TEST_DATETIME_1),
        mock.call("a_server", "a_url", TEST_DATETIME_2),
    ]
    assert mock_watch_job_run.call_args_list == [
        mock.call(mock_client.return_value, "a_job_run", "2004-07-01"),
        mock.call(mock_client.return_value, "a_job_run", "2004-07-02"),
    ]


@pytest.mark.parametrize(
    "is_error,result,expected",
    [
        (True, "an_error_msg", None),  # tron api failed
        (False, "weird_resp_msg", ""),  # bad response, can't get job run name
        (False, "Created JobRun:real_job_run_name", "real_job_run_name"),  # ok
    ],
)
def test_create_job_run(mock_client_request, is_error, result, expected):
    mock_client_request.return_value.error = is_error
    mock_client_request.return_value.content["result"] = result

    assert expected == backfill._create_job_run("a_server", "http://localhost", TEST_DATETIME_1)


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
def test_watch_job_run(mock_get_obj_type, mock_client, job_run_resp, poll_cnt, expected):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB_RUN", "a_url")
    tron_client = mock_client.return_value
    tron_client.url_base = "http://localhost"
    tron_client.job_runs.side_effect = job_run_resp

    run_successful = backfill._watch_job_run(tron_client, "a_job_run", "2004-07-01")

    assert expected == run_successful
    assert poll_cnt == len(tron_client.job_runs.call_args_list)
