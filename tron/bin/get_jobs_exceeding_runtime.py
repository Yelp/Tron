#!/usr/bin/env python3.8
import argparse
import logging
import sys
from typing import Optional

import pytimeparse

from tron.commands import cmd_utils
from tron.commands.client import Client


log = logging.getLogger("check_exceeding_time")

STATES_TO_CHECK = {"queued", "scheduled", "cancelled", "skipped"}


def parse_args() -> argparse.Namespace:
    parser = cmd_utils.build_option_parser()
    parser.add_argument(
        "--job",
        default=None,
        help="Check if a particular job exceeded a time to run. If unset checks all jobs",
    )
    parser.add_argument(
        "--time",
        help="This is used to specify the time that if any job exceeds will show. Defaults to 5 hours",
        type=int,
        dest="time_limit",
        default=18000,
    )
    args = parser.parse_args()
    return args


def check_if_time_exceeded(job_runs, job_expected_runtime) -> list:
    result = []
    for job_run in job_runs:
        if job_run.get("state", "unknown") not in STATES_TO_CHECK:
            if is_job_run_exceeding_expected_runtime(
                job_run,
                job_expected_runtime,
            ):
                result.append(job_run["id"])
    return result


def is_job_run_exceeding_expected_runtime(job_run, job_expected_runtime) -> bool:
    if job_expected_runtime is not None:
        duration_seconds = pytimeparse.parse(job_run.get("duration", ""))
        return duration_seconds and duration_seconds > job_expected_runtime
    return False


def check_job_time(job, time_limit) -> list:
    job_runs = job.get("runs", [])
    return check_if_time_exceeded(job_runs, time_limit)


def main() -> Optional[int]:
    args = parse_args()
    cmd_utils.setup_logging(args)
    cmd_utils.load_config(args)
    client = Client(args.server, args.cluster_name)
    results = []

    if args.job is None:
        jobs = client.jobs(include_job_runs=True)
        for job in jobs:
            job_url = client.get_url(job["name"])
            job = client.job_runs(job_url)
            results.extend(check_job_time(job=job, time_limit=args.time_limit))
    else:
        job_url = client.get_url(args.job)
        job = client.job_runs(job_url)
        results.extend(check_job_time(job=job, time_limit=args.time_limit))

    if not results:
        print("All jobs ran within the time limit")
    else:
        print(f"These are the runs that took longer than {args.time_limit} to run: {sorted(results)}")
    return


if __name__ == "__main__":
    sys.exit(main())
