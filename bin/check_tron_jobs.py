#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

from pysensu_yelp import send_event

from tron.commands import cmd_utils
from tron.commands import display
from tron.commands.client import Client
from tron.commands.client import get_object_type_from_identifier


log = logging.getLogger('check_tron_jobs')


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

    relevant_job_run = get_relevant_run(job_content)
    if relevant_job_run is None:
        kwargs["output"] = "CRIT: {} hasn't had a successful run yet.\n{}".format(
            job['name'], pretty_print_job(job_content),
        )
        kwargs["status"] = 2
        return kwargs

    # A job_run is like MASTER.foo.1
    last_state = relevant_job_run.get('state', 'unknown')
    job_run_id = get_object_type_from_identifier(
        url_index, relevant_job_run['id'],
    )
    action_runs = client.job(job_run_id.url, include_action_runs=True)
    # A job action is like MASTER.foo.1.step1
    relevant_action = get_relevant_action(action_runs["runs"])
    action_run_id = get_object_type_from_identifier(
        url_index, relevant_action['id'],
    )
    action_run_details = client.action_runs(action_run_id.url, num_lines=10)

    if is_job_stuck(job_content):
        prefix = "STUCK"
        status = 1
    elif last_state == "succeeded":
        prefix = "OK"
        status = 0
    elif last_state == "failed":
        prefix = "CRIT"
        status = 2
    else:
        prefix = "UNKNOWN"
        status = 3

    kwargs["output"] = (
        "{}: {}'s last relevant run (run {}) {}.\n\n"
        "Here is the last action:"
        "{}\n\n"
        "And the job run view:\n"
        "{}\n\n"
        "Here are is the whole job view for context:\n"
        "{}"
    ).format(
        prefix, job['name'], relevant_job_run['id'], last_state,
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


def get_relevant_run(job_runs):
    for run in job_runs['runs']:
        if run.get('state', 'unknown') in ["failed", "succeeded"]:
            return run
    return None


def is_job_stuck(job_runs):
    next_run_time = None
    for run in job_runs['runs']:
        if run.get('state', 'unknown') == "running":
            if next_run_time:
                difftime = time.strptime(next_run_time, '%Y-%m-%d %H:%M:%S')
                if time.time() > time.mktime(difftime):
                    return True
        next_run_time = run.get('run_time', None)
    return False


def get_relevant_action(action_runs):
    for action in reversed(action_runs):
        if action.get('state', 'unknown') == "failed":
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
        send_event(**result)


def main():
    args = parse_cli()
    cmd_utils.setup_logging(args)
    cmd_utils.load_config(args)
    client = Client(args.server)

    if args.job is None:
        jobs = client.jobs(include_job_runs=True)
        for job in jobs:
            check_job_result(job=job, client=client, dry_run=args.dry_run)
    else:
        job_url = client.get_url(args.job)
        job = client.job_runs(job_url)
        check_job_result(job=job, client=client, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
