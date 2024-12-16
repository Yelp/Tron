import datetime
from unittest import mock

import pytest
import yaml

import tron.utils.logreader
from tron.utils.logreader import decompose_action_id
from tron.utils.logreader import read_log_stream_for_action_run

try:
    from clog.readers import S3LogsReader  # noqa: F401
except ImportError:
    pytest.skip("yelp logs readers not available, skipping tests", allow_module_level=True)


# used for an explicit patch of staticconf.read return value for an arbitrary namespace
def static_conf_patch(args):
    return lambda arg, namespace, default=None: args.get(arg)


def test_read_log_stream_for_action_run_not_available():
    with mock.patch("tron.utils.logreader.s3reader_available", False):
        output = tron.utils.logreader.read_log_stream_for_action_run(
            "namespace.job.1234.action",
            component="stdout",
            min_date=datetime.datetime.now(),
            max_date=datetime.datetime.now(),
            paasta_cluster="fake",
        )
    assert "unable to display logs" in output[0]


def test_read_log_stream_for_action_run():
    with mock.patch(
        "staticconf.read",
        autospec=True,
        side_effect=static_conf_patch({"logging.max_lines_to_display": 1000}),
    ), mock.patch("tron.config.static_config.build_configuration_watcher", autospec=True,), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
    ), mock.patch(
        "tron.utils.logreader.get_superregion", autospec=True, return_value="fake"
    ), mock.patch(
        "tron.utils.logreader.S3LogsReader", autospec=True
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

    mock_s3_reader.return_value.get_log_reader.assert_called_once_with(
        log_name="stream_paasta_app_output_namespace_job__action", start_datetime=mock.ANY, end_datetime=mock.ANY
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
def test_read_log_stream_for_action_run_tz(local_datetime, expected_datetime):
    with mock.patch(
        "staticconf.read",
        autospec=True,
        side_effect=static_conf_patch({"logging.max_lines_to_display": 1000}),
    ), mock.patch("tron.config.static_config.build_configuration_watcher", autospec=True,), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
    ), mock.patch(
        "tron.utils.logreader.get_superregion", autospec=True, return_value="fake"
    ), mock.patch(
        "tron.utils.logreader.S3LogsReader", autospec=True
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


def test_read_log_stream_for_action_run_for_long_output():
    # 1000 represents the number of lines that are expected to be
    # outputted by the test, which is similar to the logging.max_lines_to_display
    # in tron.yaml in srv-configs
    max_lines = 1000
    with mock.patch("tron.utils.logreader.get_superregion", autospec=True, return_value="fake",), mock.patch(
        "tron.config.static_config.build_configuration_watcher",
        autospec=True,
    ), mock.patch(
        "staticconf.read", autospec=True, side_effect=static_conf_patch({"logging.max_lines_to_display": 1000})
    ), mock.patch(
        "tron.config.static_config.load_yaml_file",
        autospec=True,
    ), mock.patch(
        "tron.utils.logreader.S3LogsReader", autospec=True
    ) as mock_s3_reader:

        with open("./tests/utils/shortOutputTest.txt") as f:
            content_list = f.readlines()

        mock_s3_reader.return_value.get_log_reader.return_value = iter(content_list)

        output = read_log_stream_for_action_run(
            "namespace.job.228.action",
            component="stdout",
            min_date=datetime.datetime.now(),
            max_date=datetime.datetime.now(),
            paasta_cluster="infrastage",
        )

    mock_s3_reader.return_value.get_log_reader.assert_called_once_with(
        log_name="stream_paasta_app_output_namespace_job__action", start_datetime=mock.ANY, end_datetime=mock.ANY
    )
    assert len(output) == max_lines + 1


def test_decompose_action_id_yml_file_found():
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
        namespace, job_name, run_num, action = decompose_action_id(action_run_id, paasta_cluster)
        assert namespace == "test_service"
        assert job_name == "job"
        assert run_num == "1234"
        assert action == "action"


def test_decompose_action_id_file_not_found():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    with mock.patch("builtins.open", side_effect=FileNotFoundError):
        namespace, job_name, run_num, action = decompose_action_id(action_run_id, paasta_cluster)
        assert namespace == "namespace"
        assert job_name == "job"
        assert run_num == "1234"
        assert action == "action"


def test_decompose_action_id_yaml_error():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    with mock.patch("builtins.open", mock.mock_open(read_data="invalid_yaml")), mock.patch(
        "yaml.safe_load", side_effect=yaml.YAMLError
    ):
        namespace, job_name, run_num, action = decompose_action_id(action_run_id, paasta_cluster)
        assert namespace == "namespace"
        assert job_name == "job"
        assert run_num == "1234"
        assert action == "action"


def test_decompose_action_id_generic_error():
    action_run_id = "namespace.job.1234.action"
    paasta_cluster = "fake_cluster"
    with mock.patch("builtins.open", mock.mock_open(read_data="some_data")), mock.patch(
        "yaml.safe_load", side_effect=Exception
    ):
        namespace, job_name, run_num, action = decompose_action_id(action_run_id, paasta_cluster)
        assert namespace == "namespace"
        assert job_name == "job"
        assert run_num == "1234"
        assert action == "action"


def test_decompose_action_id_service_not_found():
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
        namespace, job_name, run_num, action = decompose_action_id(action_run_id, paasta_cluster)
        assert namespace == "namespace"
        assert job_name == "job"
        assert run_num == "1234"
        assert action == "action"
