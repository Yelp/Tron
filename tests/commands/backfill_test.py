import datetime

import mock
import pytest

from tron.commands import backfill
from tron.commands import client

TEST_DATETIME_1 = datetime.datetime.strptime("2004-07-01", "%Y-%m-%d")
TEST_DATETIME_2 = datetime.datetime.strptime("2004-07-02", "%Y-%m-%d")


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

    results = list(backfill.run_backfill_for_date_range("a_server", "a_job", [TEST_DATETIME_1, TEST_DATETIME_2]))

    assert results == [False, True]
    assert mock_create_job_run.call_args_list == [
        mock.call("a_server", "a_job", "a_url", TEST_DATETIME_1),
        mock.call("a_server", "a_job", "a_url", TEST_DATETIME_2),
    ]


@mock.patch.object(client, "get_object_type_from_identifier", autospec=True)
@mock.patch.object(backfill, "_create_job_run", autospec=True)
def test_run_backfill_for_date_range_no_job_run_name(
    mock_create_job_run, mock_get_obj_type,
):
    mock_get_obj_type.return_value = client.TronObjectIdentifier("JOB", "a_url")
    mock_create_job_run.return_value = ""

    results = list(backfill.run_backfill_for_date_range("a_server", "a_job", [TEST_DATETIME_1]))

    assert results == [True]
    assert mock_create_job_run.call_args_list == [
        mock.call("a_server", "a_job", "a_url", TEST_DATETIME_1),
    ]


@pytest.mark.parametrize(
    "is_error,result,expected",
    [
        (True, "an_error_msg", None),
        (False, "weird_resp_msg", ""),
        (False, "Created JobRun:real_job_run_name", "real_job_run_name"),
    ],
)
def test_create_job_run(mock_client_request, is_error, result, expected):
    mock_client_request.return_value.error = is_error
    mock_client_request.return_value.content["result"] = result
    assert backfill._create_job_run("a_server", "a_job", "http://localhost", TEST_DATETIME_1) == expected
