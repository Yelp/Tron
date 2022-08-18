import datetime
from unittest import mock

import pytest

from tron.utils.scribereader import read_log_stream_for_action_run

try:
    import scribereader  # noqa: F401
except ImportError:
    pytest.skip("scribereader not available, skipping tests", allow_module_level=True)


def test_read_log_stream_for_action_run_min_date_and_max_date_today():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now()
    max_date = datetime.datetime.now() + datetime.timedelta(hours=1)
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion", autospec=True, return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration", autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, return_value=1000
    ):
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
                "timestamp": "2021-01-02T18:10:09.169421619Z",
                "cluster": "fake"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 2",
                "timestamp": "2021-01-02T18:11:09.169421619Z",
                "cluster": "fake"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stderr",
                "message": "line 3",
                "timestamp": "2021-01-02T18:12:09.169421619Z",
                "cluster": "fake"
            }""",
            ]
        )
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.1234.action",
            component="stdout",
            min_date=min_date,
            max_date=max_date,
            paasta_cluster="fake",
        )

    mock_stream_reader.assert_not_called()
    mock_stream_tailer.assert_called_once_with(
        stream_name="stream_paasta_app_output_namespace_job__action", lines=-1, tailing_host="host", tailing_port=1234,
    )
    assert output == ["line 1", "line 2"]


def test_read_log_stream_for_action_run_min_date_and_max_date_different_days():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now() - datetime.timedelta(days=5)
    max_date = datetime.datetime.now()
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion", autospec=True, return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration", autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, return_value=1000
    ):
        # we should check the reader for data from a previous day
        mock_stream_reader.return_value.__enter__.return_value = iter(
            [
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 0",
                "timestamp": "2021-01-02T18:10:09.169421619Z",
                "cluster": "fake"
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
                "timestamp": "2021-01-02T18:10:09.169421619Z",
                "cluster": "fake"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 2",
                "timestamp": "2021-01-02T18:11:09.169421619Z",
                "cluster": "fake"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stderr",
                "message": "line 3",
                "timestamp": "2021-01-02T18:12:09.169421619Z",
                "cluster": "fake"
            }""",
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 4",
                "timestamp": "2021-01-02T18:12:09.169421619Z",
                "cluster": "bad_fake"
            }""",
            ]
        )
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.1234.action",
            component="stdout",
            min_date=min_date,
            max_date=max_date,
            paasta_cluster="fake",
        )

    mock_stream_reader.assert_called_once_with(
        stream_name="stream_paasta_app_output_namespace_job__action",
        min_date=min_date,
        max_date=max_date,
        reader_host="host",
        reader_port=1234,
    )
    mock_stream_tailer.assert_called_once_with(
        stream_name="stream_paasta_app_output_namespace_job__action", lines=-1, tailing_host="host", tailing_port=1234,
    )
    assert output == ["line 0", "line 1", "line 2"]


def test_read_log_stream_for_action_run_min_date_and_max_date_in_past():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now() - datetime.timedelta(days=5)
    max_date = datetime.datetime.now() - datetime.timedelta(days=4)
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion", autospec=True, return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration", autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, return_value=1000
    ):
        # all the data we want is from the past, so we should only check the reader
        mock_stream_reader.return_value.__enter__.return_value = iter(
            [
                """{
                "tron_run_number": 1234,
                "component": "stdout",
                "message": "line 0",
                "timestamp": "2021-01-02T18:10:09.169421619Z",
                "cluster": "fake"
            }""",
            ]
        )
        # so lets make sure we don't call the tailer
        mock_stream_tailer.return_value.__iter__.side_effect = Exception
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.1234.action",
            component="stdout",
            min_date=min_date,
            max_date=max_date,
            paasta_cluster="fake",
        )

    mock_stream_reader.assert_called_once_with(
        stream_name="stream_paasta_app_output_namespace_job__action",
        min_date=min_date,
        max_date=max_date,
        reader_host="host",
        reader_port=1234,
    )
    mock_stream_tailer.assert_not_called()
    assert output == ["line 0"]


def test_read_log_stream_for_action_run_min_date_and_max_date_for_long_output():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now() - datetime.timedelta(days=5)
    max_date = datetime.datetime.now() - datetime.timedelta(days=4)
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port", autospec=True, return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader", autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer", autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion", autospec=True, return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration", autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, return_value=1000
    ):

        with open("./tests/utils/shortOutputTest.txt") as f:
            content_list = f.readlines()

        mock_stream_reader.return_value.__enter__.return_value = iter(content_list)

        # so lets make sure we don't call the tailer
        mock_stream_tailer.return_value.__iter__.side_effect = Exception
        output = read_log_stream_for_action_run(
            action_run_id="namespace.job.228.action",
            component="stdout",
            min_date=min_date,
            max_date=max_date,
            paasta_cluster="infrastage",
        )
    mock_stream_reader.assert_called_once_with(
        stream_name="stream_paasta_app_output_namespace_job__action",
        min_date=min_date,
        max_date=max_date,
        reader_host="host",
        reader_port=1234,
    )
    mock_stream_tailer.assert_not_called()
    assert len(output) == 1000 + 1
