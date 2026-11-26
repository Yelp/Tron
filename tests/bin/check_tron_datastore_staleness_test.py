import datetime
import time
from types import SimpleNamespace
from unittest import mock

import pytest
import pytz

from tron.bin import check_tron_datastore_staleness

UTC = pytz.utc
PACIFIC = pytz.timezone("US/Pacific")


def test_dst_allowance_no_transition():
    last_run_time = PACIFIC.localize(datetime.datetime(2026, 11, 2, 12, 0))
    now = PACIFIC.localize(datetime.datetime(2026, 11, 2, 13, 0))
    assert check_tron_datastore_staleness.get_dst_allowance(last_run_time, now, PACIFIC) == 0


def test_dst_allowance_no_time_zone(pacific_local_time):
    last_run_time = datetime.datetime(2026, 11, 1, 8, 50, tzinfo=UTC)
    now = datetime.datetime(2026, 11, 1, 10, 0, tzinfo=UTC)
    assert check_tron_datastore_staleness.get_dst_allowance(last_run_time, now, None) == 3600


def test_dst_allowance_fall_back():
    # 2026-11-01: clocks fall back from 02:00 PDT to 01:00 PST at 09:00 UTC.
    last_run_time = datetime.datetime(2026, 11, 1, 8, 50, tzinfo=UTC)  # 01:50 PDT
    now = datetime.datetime(2026, 11, 1, 10, 0, tzinfo=UTC)  # 02:00 PST
    assert check_tron_datastore_staleness.get_dst_allowance(last_run_time, now, PACIFIC) == 3600


def test_dst_allowance_fall_back_transition_before_last_run():
    # Transition already accounted for: both timestamps are on the PST side.
    last_run_time = datetime.datetime(2026, 11, 1, 10, 30, tzinfo=UTC)  # 02:30 PST
    now = datetime.datetime(2026, 11, 1, 11, 0, tzinfo=UTC)  # 03:00 PST
    assert check_tron_datastore_staleness.get_dst_allowance(last_run_time, now, PACIFIC) == 0


def test_dst_allowance_spring_forward():
    # 2027-03-14: clocks spring forward from 02:00 PST to 03:00 PDT at 10:00 UTC.
    # Spring-forward compresses wall-clock time and leaves no gap, so no allowance.
    last_run_time = datetime.datetime(2027, 3, 14, 8, 50, tzinfo=UTC)  # 00:50 PST
    now = datetime.datetime(2027, 3, 14, 10, 0, tzinfo=UTC)  # 03:00 PDT
    assert check_tron_datastore_staleness.get_dst_allowance(last_run_time, now, PACIFIC) == 0


def test_dst_allowance_other_time_zone():
    # A transition in the job's own time zone is detected, not just Pacific's.
    sydney = pytz.timezone("Australia/Sydney")
    # 2026-04-05: clocks fall back from 03:00 AEDT to 02:00 AEST at 16:00 UTC Apr 4.
    last_run_time = datetime.datetime(2026, 4, 4, 15, 50, tzinfo=UTC)  # 02:50 AEDT
    now = datetime.datetime(2026, 4, 4, 17, 0, tzinfo=UTC)  # 03:00 AEST
    assert check_tron_datastore_staleness.get_dst_allowance(last_run_time, now, sydney) == 3600


@pytest.fixture
def pacific_local_time(monkeypatch):
    try:
        with monkeypatch.context() as env:
            env.setenv("TZ", "US/Pacific")
            time.tzset()
            yield
    finally:
        time.tzset()


@pytest.mark.parametrize(
    "master_zone,job_zone,last,now,naive,expected",
    [
        (None, None, "2026-11-01T08:50", "2026-11-01T09:23", False, 0),
        (None, None, "2026-11-01T08:50", "2026-11-01T09:23", True, 0),
        ("UTC", "US/Pacific", "2026-11-01T08:50", "2026-11-01T09:23", False, 0),
        ("US/Pacific", "UTC", "2026-11-01T08:50", "2026-11-01T09:23", False, 2),
        ("US/Pacific", None, "2026-11-02T08:50", "2026-11-02T09:23", False, 2),
        ("US/Pacific", None, "2026-11-01T08:50", "2026-11-01T10:20", False, 0),
        ("US/Pacific", None, "2026-11-01T08:50", "2026-11-01T10:21", False, 2),
        ("US/Pacific", None, "2026-11-01T09:10", "2026-11-01T09:23", False, 0),
        ("US/Pacific", None, "2027-03-14T09:50", "2027-03-14T10:23", False, 2),
        ("US/Pacific", None, "2026-11-01T08:20", "2026-11-01T08:51", False, 2),
    ],
    ids=[
        "host-local-fallback",
        "host-local-naive-action-time",
        "job-override-needs-allowance",
        "utc-job-must-still-page",
        "ordinary-stall",
        "allowance-boundary",
        "allowance-expired",
        "fresh-after-fallback",
        "spring-stall",
        "before-fallback",
    ],
)
def test_main_exit_code(pacific_local_time, master_zone, job_zone, last, now, naive, expected):
    last_run_time = datetime.datetime.fromisoformat(last).replace(tzinfo=UTC)
    now = datetime.datetime.fromisoformat(now).replace(tzinfo=UTC)
    if naive:
        last_run_time = last_run_time.astimezone().replace(tzinfo=None)
    persistence_config = SimpleNamespace(store_type="dynamodb")
    master_config = SimpleNamespace(
        state_persistence=persistence_config,
        time_zone=pytz.timezone(master_zone) if master_zone else None,
    )
    job_config = SimpleNamespace(time_zone=pytz.timezone(job_zone) if job_zone else None)
    config = mock.Mock()
    config.get_master.return_value = master_config
    config.get_jobs.return_value = {"monitor.k8s": job_config}
    args = SimpleNamespace(config_path="unused", job_name="monitor.k8s", staleness_threshold=1800)
    state = {"job_state": {args.job_name: {"runs": [{"runs": [{"start_time": last_run_time, "state": "succeeded"}]}]}}}
    with mock.patch.object(
        check_tron_datastore_staleness, "parse_cli", autospec=True, return_value=args
    ), mock.patch.object(
        check_tron_datastore_staleness.manager, "ConfigManager", autospec=True
    ) as config_manager, mock.patch.object(
        check_tron_datastore_staleness, "PersistenceManagerFactory", autospec=True
    ) as factory, mock.patch.object(
        check_tron_datastore_staleness, "datetime", autospec=True
    ) as clock, mock.patch.object(
        check_tron_datastore_staleness.time,
        "time",
        autospec=True,
        return_value=now.timestamp(),
    ):
        config_manager.return_value.load.return_value = config
        factory.from_config.return_value.restore.return_value = state
        clock.datetime.now.return_value = now
        with pytest.raises(SystemExit) as result:
            check_tron_datastore_staleness.main()
        assert result.value.code == expected
        factory.from_config.assert_called_once_with(persistence_config)
        factory.from_config.return_value.restore.assert_called_once_with(job_names=[args.job_name])


def test_shelve_does_not_require_job_config():
    config = mock.Mock()
    config.get_master.return_value = SimpleNamespace(
        state_persistence=SimpleNamespace(store_type="shelve"), time_zone=None
    )
    config.get_jobs.side_effect = AssertionError("Shelve should not look up job configuration")
    args = SimpleNamespace(config_path="unused", job_name="missing.job", staleness_threshold=1800)
    with mock.patch.object(
        check_tron_datastore_staleness, "parse_cli", autospec=True, return_value=args
    ), mock.patch.object(
        check_tron_datastore_staleness.manager, "ConfigManager", autospec=True
    ) as config_manager, mock.patch.object(
        check_tron_datastore_staleness.os,
        "execl",
        autospec=True,
        side_effect=SystemExit(99),
    ) as execl:
        config_manager.return_value.load.return_value = config
        with pytest.raises(SystemExit) as result:
            check_tron_datastore_staleness.main()
        assert result.value.code == 99
        execl.assert_called_once_with(
            "/usr/lib/nagios/plugins/check_file_age",
            "/nail/tron/tron_state",
            "-w",
            "1800",
            "-c",
            "1800",
        )
        config.get_jobs.assert_not_called()
