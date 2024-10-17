import datetime
from unittest import mock

import pytest
import yaml

import tron.utils.scribereader
from tron.utils.scribereader import get_log_namespace
from tron.utils.scribereader import read_log_stream_for_action_run

try:
    import scribereader  # noqa: F401
    from clog.readers import S3LogsReader  # noqa: F401
except ImportError:
    pytest.skip("yelp logs readers not available, skipping tests", allow_module_level=True)


# used for an explicit patch of staticconf.read return value for an arbitrary namespace
def static_conf_patch(args):
    return lambda arg, namespace, default=None: args.get(arg)


def test_read_log_stream_for_action_run_not_available():
    with mock.patch("tron.utils.scribereader.scribereader_available", False), mock.patch(
        "tron.utils.scribereader.s3reader_available", False
    ):
        output = tron.utils.scribereader.read_log_stream_for_action_run(
            "namespace.job.1234.action",
            component="stdout",
            min_date=datetime.datetime.now(),
            max_date=datetime.datetime.now(),
            paasta_cluster="fake",
        )
    assert "unable to display logs" in output[0]


def test_read_log_stream_for_action_run_yelp_clog():
    with mock.patch(
        "staticconf.read",
        autospec=True,
        side_effect=static_conf_patch({"logging.use_s3_reader": True, "logging.max_lines_to_display": 1000}),
    ), mock.patch("tron.config.static_config.build_configuration_watcher", autospec=True,), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
    ), mock.patch(
        "tron.utils.scribereader.get_ecosystem", autospec=True, return_value="fake"
    ), mock.patch(
        "tron.utils.scribereader.get_superregion", autospec=True, return_value="fake"
    ), mock.patch(
        "tron.utils.scribereader.S3LogsReader", autospec=True
    ) as mock_s3_reader:

        mock_s3_reader.return_value.get_log_reader.return_value = iter(
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
            "namespace.job.1234.action",
            component="stdout",
            min_date=datetime.datetime.now(),
            max_date=datetime.datetime.now(),
            paasta_cluster="fake",
        )
    assert output == ["line 1", "line 2"]


@pytest.mark.parametrize(
    "local_datetime, expected_datetime",
    [
        (
            datetime.datetime(2024, 2, 29, 23, 59, 59, tzinfo=datetime.timezone(datetime.timedelta(hours=+3))),
            datetime.datetime(2024, 2, 29, 20, 59, 59, tzinfo=datetime.timezone.utc),
        ),
        (
            datetime.datetime(2024, 2, 29, 23, 59, 59, tzinfo=datetime.timezone(datetime.timedelta(hours=-3))),
            datetime.datetime(2024, 3, 1, 2, 59, 59, tzinfo=datetime.timezone.utc),
        ),
    ],
)
def test_read_log_stream_for_action_run_yelp_clog_tz(local_datetime, expected_datetime):
    with mock.patch(
        "staticconf.read",
        autospec=True,
        side_effect=static_conf_patch({"logging.use_s3_reader": True, "logging.max_lines_to_display": 1000}),
    ), mock.patch("tron.config.static_config.build_configuration_watcher", autospec=True,), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
    ), mock.patch(
        "tron.utils.scribereader.get_ecosystem", autospec=True, return_value="fake"
    ), mock.patch(
        "tron.utils.scribereader.get_superregion", autospec=True, return_value="fake"
    ), mock.patch(
        "tron.utils.scribereader.S3LogsReader", autospec=True
    ) as mock_s3_log_reader:

        read_log_stream_for_action_run(
            "namespace.job.1234.action",
            component="stdout",
            min_date=local_datetime,
            max_date=local_datetime,
            paasta_cluster="fake",
        )
    mock_s3_log_reader.return_value.get_log_reader.assert_called_once_with(
        log_name=mock.ANY, start_datetime=expected_datetime, end_datetime=expected_datetime
    )


def test_read_log_stream_for_action_run_min_date_and_max_date_today():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now()
    max_date = datetime.datetime.now() + datetime.timedelta(hours=1)
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port",
        autospec=True,
        return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader",
        autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer",
        autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion",
        autospec=True,
        return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration_watcher",
        autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, side_effect=static_conf_patch({"logging.max_lines_to_display": 1000})
    ), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
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
        stream_name="stream_paasta_app_output_namespace_job__action",
        lines=-1,
        tailing_host="host",
        tailing_port=1234,
    )
    assert output == ["line 1", "line 2"]


def test_read_log_stream_for_action_run_min_date_and_max_date_different_days():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now() - datetime.timedelta(days=5)
    max_date = datetime.datetime.now()
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port",
        autospec=True,
        return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader",
        autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer",
        autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion",
        autospec=True,
        return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration_watcher",
        autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, side_effect=static_conf_patch({"logging.max_lines_to_display": 1000})
    ), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
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
        stream_name="stream_paasta_app_output_namespace_job__action",
        lines=-1,
        tailing_host="host",
        tailing_port=1234,
    )
    assert output == ["line 0", "line 1", "line 2"]


def test_read_log_stream_for_action_run_min_date_and_max_date_in_past():
    # NOTE: these tests don't actually depend on the current time apart from
    # today vs not-today and the args are forwarded to scribereader anyway
    # so using the current time is fine
    min_date = datetime.datetime.now() - datetime.timedelta(days=5)
    max_date = datetime.datetime.now() - datetime.timedelta(days=4)
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port",
        autospec=True,
        return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader",
        autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer",
        autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion",
        autospec=True,
        return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration_watcher",
        autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, side_effect=static_conf_patch({"logging.max_lines_to_display": 1000})
    ), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
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
    # 1000 represents the number of lines that are expected to be
    # outputted by the test, which is similar to the logging.max_lines_to_display
    # in tron.yaml in srv-configs
    max_lines = 1000
    with mock.patch(
        "tron.utils.scribereader.get_scribereader_host_and_port",
        autospec=True,
        return_value=("host", 1234),
    ), mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_reader",
        autospec=True,
    ) as mock_stream_reader, mock.patch(
        "tron.utils.scribereader.scribereader.get_stream_tailer",
        autospec=True,
    ) as mock_stream_tailer, mock.patch(
        "tron.utils.scribereader.get_superregion",
        autospec=True,
        return_value="fake",
    ), mock.patch(
        "tron.config.static_config.build_configuration_watcher",
        autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, side_effect=static_conf_patch({"logging.max_lines_to_display": 1000})
    ), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
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
    # The expected output should be max_lines plus the
    # extra line for 'This output is truncated.' message
    assert len(output) == max_lines + 1


def test_get_log_namespace_yml_file_found():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    config_content = """
    job:
        actions:
            action:
                service: test_service
    """
    with mock.patch("builtins.open", mock.mock_open(read_data=config_content)), mock.patch(
        "yaml.safe_load", return_value=yaml.safe_load(config_content)
    ):
        result = get_log_namespace(action_run_id, paasta_cluster)
        assert result == "test_service"


def test_get_log_namespace_file_not_found():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    with mock.patch("builtins.open", side_effect=FileNotFoundError):
        result = get_log_namespace(action_run_id, paasta_cluster)
        assert result == "namespace"


def test_get_log_namespace_yaml_error():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    with mock.patch("builtins.open", mock.mock_open(read_data="invalid_yaml")), mock.patch(
        "yaml.safe_load", side_effect=yaml.YAMLError
    ):
        result = get_log_namespace(action_run_id, paasta_cluster)
        assert result == "namespace"


def test_get_log_namespace_generic_error():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    with mock.patch("builtins.open", mock.mock_open(read_data="some_data")), mock.patch(
        "yaml.safe_load", side_effect=Exception
    ):
        result = get_log_namespace(action_run_id, paasta_cluster)
        assert result == "namespace"


def test_get_log_namespace_service_not_found():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    config_content = """
    job:
        actions:
            action:
                command: "sleep 10"
    """
    with mock.patch("builtins.open", mock.mock_open(read_data=config_content)), mock.patch(
        "yaml.safe_load", return_value=yaml.safe_load(config_content)
    ):
        result = get_log_namespace(action_run_id, paasta_cluster)
        assert result == "namespace"
