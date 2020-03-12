#!/usr/bin/env python3.6
import datetime
import logging
import pprint
import sys
import time
from collections import defaultdict
from enum import Enum

import pytimeparse
from pyrsistent import m
from pyrsistent import pmap
from pysensu_yelp import send_event

from tron.commands import cmd_utils
from tron.commands import display
from tron.commands.client import Client
from tron.commands.client import get_object_type_from_identifier

PRECIOUS_JOB_ATTR = 'check_that_every_day_has_a_successful_run'
NUM_PRECIOUS = 7

log = logging.getLogger('check_tron_jobs')

_run_interval = None


class State(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    STUCK = "stuck"
    NO_RUN_YET = "no_run_yet"
    NO_RUNS_TO_CHECK = "no_runs_to_check"
    UNKNOWN = "unknown"
    SKIPPED = "skipped"


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


def _timestamp_to_shortdate(timestamp, separator='.'):
    return time.strftime(
        '%Y{0}%m{0}%d'.format(separator),
        _timestamp_to_timeobj(timestamp),
    )


def compute_check_result_for_job_runs(client, job, job_content, url_index, hide_stderr=False):
    cluster = client.cluster_name
    kwargs = {}
    if job_content is None:
        kwargs["output"] = f"OK: {job['name']} was just added and hasn't run yet on {cluster}."
        kwargs["status"] = 0
        return kwargs

    relevant_job_run, last_state = get_relevant_run_and_state(job_content)
    if relevant_job_run is None:
        kwargs["output"] = f"CRIT: {job['name']} hasn't had a successful " \
            f"run yet on {cluster}.\n{pretty_print_job(job_content)}"
        kwargs["status"] = 2
        return kwargs
    else:  # if no run scheduled, no run_time available
        relevant_job_run_date = _timestamp_to_shortdate(
            relevant_job_run['run_time']
        )

    # A job_run is like MASTER.foo.1
    job_run_id = relevant_job_run['id']

    # A job action is like MASTER.foo.1.step1
    actions_expected_runtime = job_content.get('actions_expected_runtime', {})
    relevant_action = get_relevant_action(
        action_runs=relevant_job_run["runs"],
        last_state=last_state,
        actions_expected_runtime=actions_expected_runtime
    )
    action_run_id = get_object_type_from_identifier(
        url_index,
        relevant_action['id'],
    )

    if last_state in (State.STUCK, State.FAILED, State.UNKNOWN):
        action_run_details = client.action_runs(action_run_id.url, num_lines=10)
    else:
        action_run_details = {}

    if last_state == State.SUCCEEDED:
        prefix = f"OK: The last job ({job_run_id}) run succeeded on {cluster}. Will watch future or in progress runs for the next failure"
        status = 0
        stderr = ""
    elif last_state == State.NO_RUNS_TO_CHECK:
        prefix = f"OK: The job {job['name']} is new and/or has no runs to check on {cluster}"
        status = 0
        stderr = ""
    elif last_state == State.SKIPPED:
        prefix = f"OK: The last job ({job_run_id}) run was skipped on {cluster}. Will watch future or in progress runs for the next failure"
        status = 0
        stderr = ""
    elif last_state == State.STUCK:
        if job['monitoring'].get("page_for_expected_runtime", False):
            level = "CRIT"
            status = 2
        else:
            level = "WARN"
            status = 1
        prefix = f"{level}: Job {job_run_id} exceeded expected runtime or still running when next job is scheduled on {cluster}"
        stderr = '\n'.join(action_run_details.get('stderr', ["(No stderr available)"]))
    elif last_state == State.FAILED:
        prefix = f"CRIT: The last job run ({job_run_id}) failed on {cluster}!"
        status = 2
        stderr = '\n'.join(action_run_details.get('stderr', ["(No stderr available)"]))
    elif last_state == State.UNKNOWN:
        prefix = f"CRIT: Job {job_run_id} has gone 'unknown' and might need manual intervention on {cluster}"
        status = 2
        stderr = ""
    else:
        prefix = f"UNKNOWN: Job {job_run_id} is in a state that check_tron_jobs doesn't understand"
        status = 3
        stderr = ""

    if hide_stderr:
        stderr = ""

    precious_runs_note = ''
    if job['monitoring'].get(PRECIOUS_JOB_ATTR, False) and status != 0:
        precious_runs_note = f"Note: This alert is the run for {relevant_job_run_date}. A resolve event will not occur until a job run for this date succeeds.\n"

    kwargs["output"] = (
        f"{prefix}\n"
        f"{stderr}\n"
        f"The latest run, {relevant_job_run['id']} {relevant_job_run['state']}\n"
        f"{precious_runs_note}"
    )
    if action_run_details:
        kwargs["output"] += (
            "\nHere is the last action:\n"
            f"{pretty_print_actions(action_run_details)}\n\n"
        )
    kwargs["output"] += (
        "And the job run view:\n"
        f"{pretty_print_job_run(relevant_job_run)}\n\n"
        "Here is the whole job view for context:\n"
        f"{pretty_print_job(job_content)}"
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

    job_expected_runtime = job_content.get('expected_runtime', None)
    actions_expected_runtime = job_content.get('actions_expected_runtime', {})
    stuck_run = is_job_stuck(
        job_runs=job_runs,
        job_expected_runtime=job_expected_runtime,
        actions_expected_runtime=actions_expected_runtime,
        allow_overlap=job_content.get('allow_overlap'),
    )
    for run in job_runs:
        state = run.get('state', 'unknown')
        if state in ["failed", "succeeded", "unknown", "skipped"]:
            return run, State(state)
        elif state in ["running", "waiting"]:
            action_state = is_action_failed_or_unknown(run)
            if action_state != State.SUCCEEDED:
                return run, action_state
            elif stuck_run is not None:
                return stuck_run, State.STUCK
    return job_runs[0], State.NO_RUNS_TO_CHECK


def is_action_failed_or_unknown(job_run):
    for run in job_run.get('runs', []):
        if run.get('state', None) in ["failed", "unknown"]:
            return State(run.get('state'))
    return State.SUCCEEDED


def is_job_stuck(
    job_runs, job_expected_runtime, actions_expected_runtime, allow_overlap
):
    next_run_time = None
    for job_run in job_runs:
        if job_run.get('state', 'unknown') == "running":
            if is_job_run_exceeding_expected_runtime(
                job_run, job_expected_runtime
            ):
                return job_run
            # check if it is still running at next scheduled job run time
            if not allow_overlap and next_run_time:
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
        # TODO: duration_seconds will be None for a running job if it's root
        # action is waiting for external dependency. Maybe fix by setting
        # job's start_time to run_time when that happens.
        if duration_seconds and duration_seconds > job_expected_runtime:
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
            run.get('start_time') or run.get('run_time') for run in job_runs
            if run.get('start_time') or run.get('run_time') and run.get('run_time') != job_next_run
        ]
        if len(job_runs_started) == 0:
            return -1
        job_previous_run = max(
            job_runs_started
        )
        time_diff = (
            time.mktime(_timestamp_to_timeobj(job_next_run)) -
            time.mktime(_timestamp_to_timeobj(job_previous_run))
        )
        realert_every = max(int(time_diff / _run_interval), 1)
    except Exception as e:
        log.warning(f"guess_realert_every failed: {e}")
        return -1
    return realert_every


def get_earliest_run_time_to_check(job_content, interval):
    if not job_content['runs']:
        return None

    earliest_run_time = min([
        time.mktime(_timestamp_to_timeobj(run['run_time']))
        for run in job_content['runs']
    ])
    return max(
        earliest_run_time,
        time.time() - datetime.timedelta(**{f'{interval}s': NUM_PRECIOUS - 1}).total_seconds()
    )


def sort_runs_by_interval(job_content, interval='day', until=None):
    """ Sorts a job's runs by a time interval (day, hour, minute, or second),
    according to a job run's run time.
    """
    interval_formats = {
        'day': '%Y.%m.%d',
        'hour': '%Y.%m.%d-%H',
        'minute': '%Y.%m.%d-%H.%M',
        'second': '%Y.%m.%d-%H.%M.%S',
    }
    fmt = interval_formats[interval]

    run_buckets = defaultdict(list)
    if job_content is not None:
        if not until:
            until = time.time()  # can't set in default arg
        earliest_run_time = get_earliest_run_time_to_check(job_content, interval) or until

        # We add all dates by interval between our earliest run_time and now,
        # allowing functions downstream to see if some dates had no runs
        start = datetime.datetime.fromtimestamp(earliest_run_time)
        end = datetime.datetime.fromtimestamp(until)
        step = datetime.timedelta(**{f'{interval}s': 1})

        # We compare the strings _after_ we've converted to the final format to make
        # sure we don't miss something due to off-by-one/weird DST bugs, etc
        while start.strftime(fmt) <= end.strftime(fmt):
            run_buckets[start.strftime(fmt)] = []
            start += step

        # Bucket runs by interval
        for run in job_content['runs']:
            run_time = time.strftime(
                interval_formats[interval],
                _timestamp_to_timeobj(run['run_time']),
            )
            if run_time not in run_buckets:
                continue
            run_buckets[run_time].append(run)
    return dict(run_buckets)


def compute_check_result_for_job(client, job, url_index):
    kwargs = m(
        name=f"check_tron_job.{job['name']}",
        source=client.cluster_name,
    )
    if 'realert_every' not in kwargs:
        kwargs = kwargs.set('realert_every', guess_realert_every(job))
    kwargs = kwargs.set('check_every', f"{_run_interval}s")

    # We want to prevent a monitoring config from setting the check_every
    # attribute, since one config should not dictate how often this script runs
    sensu_kwargs = (
        pmap(job['monitoring']).discard(PRECIOUS_JOB_ATTR)
        .discard('check_every')
        .discard('page_for_expected_runtime')
    )
    kwargs = kwargs.update(sensu_kwargs)
    hide_stderr = kwargs.get('hide_stderr', False)
    kwargs_list = []
    if job["status"] == "disabled":
        kwargs = kwargs.set(
            'output',
            f"OK: {job['name']} is disabled and won't be checked."
        )
        kwargs = kwargs.set('status', 0)
        kwargs_list.append(kwargs)
    else:
        # The job is not disabled, therefore we have to look at its run history
        tron_id = get_object_type_from_identifier(url_index, job["name"])
        job_content = pmap(
            client.job(
                tron_id.url,
                include_action_runs=True,
            )
        )

        if job['monitoring'].get(PRECIOUS_JOB_ATTR, False):
            dated_runs = sort_runs_by_interval(job_content, interval='day')
        else:
            dated_runs = {'': job_content['runs']}

        for date, runs in dated_runs.items():
            results = compute_check_result_for_job_runs(
                job=job,
                job_content=job_content.set('runs', runs),
                client=client,
                url_index=url_index,
                hide_stderr=hide_stderr,
            )
            dated_kwargs = kwargs.update(results)
            if date:  # if empty date, leave job name alone
                dated_kwargs = dated_kwargs.set(
                    'name', f"{kwargs['name']}-{date}"
                )
            kwargs_list.append(dated_kwargs)

    return [dict(kws) for kws in kwargs_list]


def check_job(job, client, url_index):
    if job.get('monitoring', {}) == {}:
        log.debug(f"Not checking {job['name']}, no monitoring metadata setup.")
        return
    if job.get('monitoring').get('team', None) is None:
        log.debug(f"Not checking {job['name']}, no team specified")
        return
    log.info(f"Checking {job['name']}")
    return compute_check_result_for_job(job=job, client=client, url_index=url_index)


def check_job_result(job, client, url_index, dry_run):
    results = check_job(job, client, url_index)
    if not results:
        return

    for result in results:
        if dry_run:
            log.info("Would have sent this event to sensu: ")
            log.info(pprint.pformat(result))
        else:
            log.debug(f"Sending event: {pprint.pformat(result)}")
            if 'runbook' not in result:
                result[
                    'runbook'
                ] = "No runbook specified. Please specify a runbook in the monitoring section of the job definition.",
            send_event(**result)


def main():
    args = parse_cli()
    cmd_utils.setup_logging(args)
    cmd_utils.load_config(args)
    client = Client(args.server, args.cluster_name)

    error_code = 0
    global _run_interval
    _run_interval = args.run_interval
    url_index = client.index()
    if args.job is None:
        jobs = client.jobs(include_job_runs=True)
        for job in jobs:
            try:
                check_job_result(job=job, client=client, url_index=url_index, dry_run=args.dry_run)
            except Exception as e:
                log.warning(f"check job result fails for job {job.get('name', '')}: {e}")
                error_code = 1
    else:
        job_url = client.get_url(args.job)
        job = client.job_runs(job_url)
        check_job_result(job=job, client=client, url_index=url_index, dry_run=args.dry_run)

    return error_code


if __name__ == '__main__':
    sys.exit(main())
