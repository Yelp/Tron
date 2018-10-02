#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import sys
import time
from enum import Enum

import pytimeparse
from pysensu_yelp import send_event

from tron.commands import cmd_utils
from tron.commands import display
from tron.commands.client import Client
from tron.commands.client import get_object_type_from_identifier

log = logging.getLogger('check_tron_jobs')

_run_interval = None


class State(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    STUCK = "stuck"
    NO_RUN_YET = "no_run_yet"
    NOT_SCHEDULED = "not_scheduled"
    WAITING_FOR_FIRST_RUN = "waiting_for_first_run"
    UNKNOWN = "unknown"


def parse_cli():
    parser = cmd_utils.build_option_parser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Don't actually send alerts out. Defaults to %(default)s",
    )
    parser.add_argument(
        "--job",
        default=None,
        help="Check a particular job. If unset checks all jobs",
    )
    parser.add_argument(
        "--run-interval",
        help="Run interval for this monitoring script. This is used to "
        "calculate realert and alert_after setting. "
        "Default to %(default)s (seconds)",
        type=int,
        dest="run_interval",
        default=300,
    )
    args = parser.parse_args()
    return args


def _timestamp_to_timeobj(timestamp):
    return time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')


def compute_check_result_for_job_runs(client, job, job_content):
    url_index = client.index()
    kwargs = {}
    if job_content is None:
        kwargs["output"] = "OK: {} was just added and hasn't run yet.".format(
            job['name'],
        )
        kwargs["status"] = 0
        return kwargs

    relevant_job_run, last_state = get_relevant_run_and_state(job_content)
    if relevant_job_run is None:
        kwargs["output"] = f"CRIT: {job['name']} hasn't had a successful " \
            f"run yet.\n{pretty_print_job(job_content)}"
        kwargs["status"] = 2
        return kwargs

    # A job_run is like MASTER.foo.1
    job_run_id = get_object_type_from_identifier(
        url_index,
        relevant_job_run['id'],
    )
    action_runs = client.job(job_run_id.url, include_action_runs=True)
    # A job action is like MASTER.foo.1.step1
    actions_expected_runtime = job_content.get('actions_expected_runtime', {})
    relevant_action = get_relevant_action(
        action_runs=action_runs["runs"],
        last_state=last_state,
        actions_expected_runtime=actions_expected_runtime
    )
    action_run_id = get_object_type_from_identifier(
        url_index,
        relevant_action['id'],
    )
    action_run_details = client.action_runs(action_run_id.url, num_lines=10)

    if last_state == State.SUCCEEDED:
        prefix = "OK: The last job run succeeded"
        status = 0
    elif last_state == State.WAITING_FOR_FIRST_RUN:
        prefix = "OK: The job is 'new' and waiting for the first run"
        status = 0
    elif last_state == State.STUCK:
        prefix = "WARN: Job exceeded expected runtime or still running when next job is scheduled"
        status = 1
    elif last_state == State.FAILED:
        prefix = "CRIT: The last job run failed!"
        status = 2
    elif last_state == State.NOT_SCHEDULED:
        prefix = "CRIT: Job is not scheduled at all!"
        status = 2
    elif last_state == State.UNKNOWN:
        prefix = "CRIT: Job has gone 'unknown' and might need manual intervention"
        status = 2
    else:
        prefix = "UNKNOWN: The job is in a state that check_tron_jobs doesn't understand"
        status = 3

    kwargs["output"] = (
        "{}\n"
        "{}'s last relevant run (run {}) {}.\n\n"
        "Here is the last action:"
        "{}\n\n"
        "And the job run view:\n"
        "{}\n\n"
        "Here is the whole job view for context:\n"
        "{}"
    ).format(
        prefix,
        job['name'],
        relevant_job_run['id'],
        relevant_job_run['state'],
        pretty_print_actions(action_run_details),
        pretty_print_job_run(relevant_job_run),
        pretty_print_job(job_content),
    )
    kwargs["status"] = status
    return kwargs


def pretty_print_job(job_content):
    return display.format_job_details(job_content)


def pretty_print_job_run(job_run):
    display_action = display.DisplayActionRuns()
    return display_action.format(job_run)


def pretty_print_actions(action_run):
    return display.format_action_run_details(action_run)


def get_relevant_run_and_state(job_content):
    # The order of job run to check is as follows:
    #   1. The scheduled but hasn't run one checked first
    #   2. Then currently running ones are always checked (in case an action is failed/unknown)
    #   3. If there are multiple running ones, then most recent run_time wins
    #   4. If nothing is currently running, then most recent end_time wins
    job_runs = sorted(
        job_content.get('runs', []),
        key=lambda k: (k['end_time'] is None, k['end_time'], k['run_time']),
        reverse=True,
    )
    if len(job_runs) == 0:
        return None, State.NO_RUN_YET
    run = is_job_scheduled(job_runs)
    if run is None:
        return job_runs[0], State.NOT_SCHEDULED
    job_expected_runtime = job_content.get('expected_runtime', None)
    actions_expected_runtime = job_content.get('actions_expected_runtime', {})
    run = is_job_stuck(
        job_runs=job_runs,
        job_expected_runtime=job_expected_runtime,
        actions_expected_runtime=actions_expected_runtime
    )
    if run is not None:
        return run, State.STUCK
    for run in job_runs:
        state = run.get('state', 'unknown')
        if state in ["failed", "succeeded", "unknown"]:
            return run, State(state)
        if state in ["running"]:
            action_state = is_action_failed_or_unknown(run)
            if action_state != State.SUCCEEDED:
                return run, action_state
    return job_runs[0], State.WAITING_FOR_FIRST_RUN


def is_action_failed_or_unknown(job_run):
    for run in job_run.get('runs', []):
        if run.get('state', None) in ["failed", "unknown"]:
            return State(run.get('state'))
    return State.SUCCEEDED


def is_job_scheduled(job_runs):
    for job_run in job_runs:
        if job_run.get('state', 'unknown') in ["scheduled", "queued"]:
            return job_run
    return None


def is_job_stuck(job_runs, job_expected_runtime, actions_expected_runtime):
    next_run_time = None
    for job_run in job_runs:
        if job_run.get('state', 'unknown') == "running":
            if is_job_run_exceeding_expected_runtime(
                job_run, job_expected_runtime
            ):
                return job_run
            # check if it is still running at next scheduled job run time
            if next_run_time:
                difftime = _timestamp_to_timeobj(next_run_time)
                if time.time() > time.mktime(difftime):
                    return job_run
            for action_run in job_run.get('runs', []):
                if is_action_run_exceeding_expected_runtime(
                    action_run, actions_expected_runtime
                ):
                    return job_run

        next_run_time = job_run.get('run_time', None)
    return None


def is_job_run_exceeding_expected_runtime(job_run, job_expected_runtime):
    if job_expected_runtime is not None and job_run.get(
        'state', 'unknown'
    ) == "running":
        duration_seconds = pytimeparse.parse(job_run.get('duration', ''))
        if duration_seconds > job_expected_runtime:
            return True
    return False


def is_action_run_exceeding_expected_runtime(
    action_run, actions_expected_runtime
):
    if action_run.get('state', 'unknown') == 'running':
        action_name = action_run.get('action_name', None)
        if action_name in actions_expected_runtime and actions_expected_runtime[
            action_name
        ] is not None:
            duration_seconds = pytimeparse.parse(
                action_run.get('duration', '')
            )
            if duration_seconds > actions_expected_runtime[action_name]:
                return True
    return False


def get_relevant_action(*, action_runs, last_state, actions_expected_runtime):
    stuck_action_run_candidate = None
    for action_run in reversed(action_runs):
        action_state = action_run.get('state', 'unknown')
        try:
            if State(action_state) == last_state:
                return action_run
        except ValueError:
            if last_state == State.STUCK:
                if is_action_run_exceeding_expected_runtime(
                    action_run, actions_expected_runtime
                ):
                    return action_run
                if action_state == 'running':
                    stuck_action_run_candidate = action_run
    return stuck_action_run_candidate or action_runs[-1]


def guess_realert_every(job):
    try:
        job_next_run = job.get('next_run', None)
        if job_next_run is None:
            return -1
        job_runs = job.get('runs', [])
        job_runs_started = [
            run for run in job_runs if run['start_time'] is not None
        ]
        if len(job_runs_started) == 0:
            return -1
        job_previous_run = max(
            job_runs_started,
            key=lambda k: k['start_time'],
        ).get('start_time')
        time_diff = (
            time.mktime(_timestamp_to_timeobj(job_next_run)) -
            time.mktime(_timestamp_to_timeobj(job_previous_run))
        )
        realert_every = max(int(time_diff / _run_interval), 1)
    except Exception as e:
        log.warning("guess_realert_every failed: {}".format(e))
        return -1
    return realert_every


def compute_check_result_for_job(client, job):
    kwargs = {
        "name": "check_tron_job.{}".format(job['name']),
        "source": "tron",
    }
    kwargs.update(job['monitoring'])
    if 'realert_every' not in kwargs:
        kwargs['realert_every'] = guess_realert_every(job)
    kwargs['check_every'] = "{}s".format(_run_interval)

    status = job["status"]
    if status == "disabled":
        kwargs["output"] = "OK: {} is disabled and won't be checked.".format(
            job['name'],
        )
        kwargs["status"] = 0
        log.info(kwargs["output"])
        return kwargs
    else:
        # The job is not disabled, therefore we have to look at its run history
        url_index = client.index()
        tron_id = get_object_type_from_identifier(url_index, job["name"])
        job_content = client.job(
            tron_id.url,
            count=10,
            include_action_runs=True,
        )
        results = compute_check_result_for_job_runs(
            job=job,
            job_content=job_content,
            client=client,
        )
        kwargs.update(results)
        log.info(kwargs["output"].split("\n")[0])
        return kwargs


def check_job(job, client):
    if job.get('monitoring', {}) == {}:
        log.debug(
            "Not checking {}, no monitoring metadata setup.".format(
                job['name'],
            )
        )
        return
    if job.get('monitoring').get('team', None) is None:
        log.debug("Not checking {}, no team specified".format(job['name']))
        return
    log.info("Checking {}".format(job['name']))
    return compute_check_result_for_job(job=job, client=client)


def check_job_result(job, client, dry_run):
    result = check_job(job, client)
    if result is None:
        return
    if dry_run:
        log.info("Would have sent this event to sensu: ")
        log.info(result)
    else:
        log.debug("Sending event: {}".format(result))
        if 'runbook' not in result:
            result[
                'runbook'
            ] = "No runbook specified. Please specify a runbook in the monitoring section of the job definition."
        send_event(**result)


def main():
    args = parse_cli()
    cmd_utils.setup_logging(args)
    cmd_utils.load_config(args)
    client = Client(args.server)

    error_code = 0
    global _run_interval
    _run_interval = args.run_interval
    if args.job is None:
        jobs = client.jobs(include_job_runs=True)
        for job in jobs:
            try:
                check_job_result(job=job, client=client, dry_run=args.dry_run)
            except Exception as e:
                log.warning(
                    "check job result fails for job {}: {}".format(
                        job.get('name', ''),
                        e,
                    )
                )
                error_code = 1
    else:
        job_url = client.get_url(args.job)
        job = client.job_runs(job_url)
        check_job_result(job=job, client=client, dry_run=args.dry_run)

    return error_code


if __name__ == '__main__':
    sys.exit(main())
