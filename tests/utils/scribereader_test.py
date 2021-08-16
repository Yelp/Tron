import datetime
from unittest import mock

from tron.utils.scribereader import read_log_stream_for_action_run


def test_read_log_stream_for_action_run_min_date_and_max_date_today():
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer:
        # in this case, we shouldn't even try to check the reader, so lets set an exception
        # to make sure we didn't try
        mock_stream_reader.return_value.__enter__.side_effect = Exception
        # but the tailer should have some data and actually be used
        mock_stream_tailer.return_value.__iter__.return_value = iter(
            [
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 1",
                "timestamp": "2021-01-02T18:10:09.169421619Z"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 2",
                "timestamp": "2021-01-02T18:11:09.169421619Z"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stderr",
                "message": "line 3",
                "timestamp": "2021-01-02T18:12:09.169421619Z"
            }""",
            ]
        )
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.1234.action",
            component="stdout",
            min_date=datetime.datetime.now(),
            max_date=datetime.datetime.now() + datetime.timedelta(hours=1),
        )

    mock_stream_reader.assert_not_called()
    mock_stream_tailer.assert_called_once()
    assert output == ["line 1", "line 2"]


def test_read_log_stream_for_action_run_min_date_and_max_date_different_days():
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer:
        # we should check the reader for data from a previous day
        mock_stream_reader.return_value.__enter__.return_value = iter(
            [
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 0",
                "timestamp": "2021-01-02T18:10:09.169421619Z"
            }""",
            ]
        )
        # but then check the tailer for todays data
        mock_stream_tailer.return_value.__iter__.return_value = iter(
            [
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 1",
                "timestamp": "2021-01-02T18:10:09.169421619Z"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 2",
                "timestamp": "2021-01-02T18:11:09.169421619Z"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stderr",
                "message": "line 3",
                "timestamp": "2021-01-02T18:12:09.169421619Z"
            }""",
            ]
        )
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.1234.action",
            component="stdout",
            min_date=datetime.datetime.now() - datetime.timedelta(days=5),
            max_date=datetime.datetime.now(),
        )

    mock_stream_reader.assert_called_once()
    mock_stream_tailer.assert_called_once()
    assert output == ["line 0", "line 1", "line 2"]


def test_read_log_stream_for_action_run_min_date_and_max_date_in_past():
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer:
        # all the data we want is from the past, so we should only check the reader
        mock_stream_reader.return_value.__enter__.return_value = iter(
            [
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 0",
                "timestamp": "2021-01-02T18:10:09.169421619Z"
            }""",
            ]
        )
        # so lets make sure we don't call the tailer
        mock_stream_tailer.return_value.__iter__.side_effect = Exception
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.1234.action",
            component="stdout",
            min_date=datetime.datetime.now() - datetime.timedelta(days=5),
            max_date=datetime.datetime.now() - datetime.timedelta(days=4),
        )

    mock_stream_reader.assert_called_once()
    mock_stream_tailer.assert_not_called()
    assert output == ["line 0"]
