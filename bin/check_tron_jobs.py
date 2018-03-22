#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import sys
import time

from enum import Enum
from pysensu_yelp import send_event

from tron.commands import cmd_utils
from tron.commands import display
from tron.commands.client import Client
from tron.commands.client import get_object_type_from_identifier

log = logging.getLogger('check_tron_jobs')


class State(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    STUCK = "stuck"
    NO_RUN_YET = "no_run_yet"
    NOT_SCHEDULED = "not_scheduled"
    WAITING_FOR_FIRST_RUN = "waiting_for_first_run"
    UNKNOWN = "UNKNOWN"


def parse_cli():
    parser = cmd_utils.build_option_parser()
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Don't actually send alerts out. Defaults to %(default)s",
    )
    parser.add_argument(
        "--job", default=None,
        help="Check a particular job. If unset checks all jobs",
    )
    args = parser.parse_args()
    return args


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
        kwargs["output"] = "CRIT: {} hasn't had a successful run yet.\n{}".format(
            job['name'], pretty_print_job(job_content),
        )
        kwargs["status"] = 2
        return kwargs

    # A job_run is like MASTER.foo.1
    job_run_id = get_object_type_from_identifier(
        url_index, relevant_job_run['id'],
    )
    action_runs = client.job(job_run_id.url, include_action_runs=True)
    # A job action is like MASTER.foo.1.step1
    relevant_action = get_relevant_action(action_runs["runs"], last_state)
    action_run_id = get_object_type_from_identifier(
        url_index, relevant_action['id'],
    )
    action_run_details = client.action_runs(action_run_id.url, num_lines=10)

    if last_state == State.SUCCEEDED or last_state == State.WAITING_FOR_FIRST_RUN:
        prefix = "OK"
        annotation = ""
        status = 0
    elif last_state == State.STUCK:
        prefix = "WARN"
        annotation = "Job still running when next job is scheduled to run (stuck?)"
        status = 1
    elif last_state == State.FAILED:
        prefix = "CRIT"
        annotation = ""
        status = 2
    elif last_state == State.NOT_SCHEDULED:
        prefix = "CRIT"
        annotation = "Job is not scheduled at all"
        status = 2
    else:
        prefix = "UNKNOWN"
        annotation = ""
        status = 3

    kwargs["output"] = (
        "{}: {}\n"
        "{}'s last relevant run (run {}) {}.\n\n"
        "Here is the last action:"
        "{}\n\n"
        "And the job run view:\n"
        "{}\n\n"
        "Here is the whole job view for context:\n"
        "{}"
    ).format(
        prefix, annotation, job['name'], relevant_job_run['id'], relevant_job_run['state'],
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


def get_relevant_run_and_state(job_runs):
    if len(job_runs['runs']) == 0:
        return None, State.NO_RUN_YET
    run = is_job_scheduled(job_runs)
    if run is None:
        return job_runs['runs'][0], State.NOT_SCHEDULED
    run = is_job_stuck(job_runs)
    if run is not None:
        return run, State.STUCK
    for run in job_runs['runs']:
        if run.get('state', 'unknown') in ["failed", "succeeded"]:
            return run, State(run.get('state', 'unknown'))
    return job_runs['runs'][0], State.WAITING_FOR_FIRST_RUN


def is_job_scheduled(job_runs):
    for run in job_runs['runs']:
        if run.get('state', 'unknown') in ["scheduled", "queued"]:
            return run
    return None


def is_job_stuck(job_runs):
    next_run_time = None
    for run in sorted(job_runs['runs'], key=lambda k: k['run_time'], reverse=True):
        if run.get('state', 'unknown') == "running":
            if next_run_time:
                difftime = time.strptime(next_run_time, '%Y-%m-%d %H:%M:%S')
                if time.time() > time.mktime(difftime):
                    return run
        next_run_time = run.get('run_time', None)
    return None


def get_relevant_action(action_runs, last_state):
    for action in reversed(action_runs):
        action_state = action.get('state', 'unknown')
        try:
            if State(action_state) == last_state:
                return action
        except ValueError:
            if action_state == 'running' and last_state == State.STUCK:
                return action
    return action_runs[-1]


def compute_check_result_for_job(client, job):
    kwargs = {
        "name": "check_tron_job.{}".format(job['name']),
        "source": "tron",
    }
    kwargs.update(job['monitoring'])
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
            tron_id.url, count=10, include_action_runs=True,
        )
        results = compute_check_result_for_job_runs(
            job=job, job_content=job_content, client=client,
        )
        kwargs.update(results)
        log.info(kwargs["output"].split("\n")[0])
        return kwargs


def check_job(job, client):
    if job.get('monitoring', {}) == {}:
        log.debug("Not checking {}, no monitoring metadata setup.".format(
            job['name'],
        ))
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
            result['runbook'] = "No runbook specified. Please specify a runbook in the monitoring section of the job definition."
        send_event(**result)


def main():
    args = parse_cli()
    cmd_utils.setup_logging(args)
    cmd_utils.load_config(args)
    client = Client(args.server)

    error_code = 0
    if args.job is None:
        jobs = client.jobs(include_job_runs=True)
        for job in jobs:
            try:
                check_job_result(job=job, client=client, dry_run=args.dry_run)
            except Exception as e:
                log.info("check job result fails for job {}: {}".format(job, e))
                error_code = 1
    else:
        job_url = client.get_url(args.job)
        job = client.job_runs(job_url)
        check_job_result(job=job, client=client, dry_run=args.dry_run)

    return error_code


if __name__ == '__main__':
    sys.exit(main())
