import asyncio
import datetime
import functools
import pprint
import re
from typing import List
from typing import Optional
from typing import Tuple
from urllib.parse import urljoin

from tron.commands import client
from tron.core.actionrun import ActionRun

DEFAULT_MAX_PARALLEL_RUNS = 10
DEFAULT_POLLING_INTERVAL = 10  # seconds


def get_date_range(
    start_date: datetime.datetime, end_date: datetime.datetime, descending: bool = False,
) -> List[datetime.datetime]:
    dates = []
    delta = end_date - start_date
    for days_to_add in range(delta.days + 1):
        dates.append(start_date + datetime.timedelta(days=days_to_add))
    if descending:
        dates.reverse()
    return dates


def print_backfill_cmds(job: str, date_strs: List[str]) -> bool:
    print(f"Please run the following {len(date_strs)} commands:")
    print("")
    for date in date_strs:
        print(f"tronctl start {job} --run-date {date}")
    print("")
    print("Note that many jobs operate on the previous day's data.")


def confirm_backfill(job: str, date_strs: List[str]):
    print(
        f"To backfill for the job '{job}', a job run will be created for each "
        f"of the following {len(date_strs)} dates:"
    )
    pprint.pprint(date_strs)
    print("")
    user_resp = input("Confirm? [y/n] ")

    if user_resp.lower() != "y":
        print("Aborted.")
        return False
    else:
        print("")  # just for clean separation
        return True


async def run_backfill_for_date_range(
    server: str,
    job_name: str,
    dates: List[datetime.datetime],
    max_parallel: int = DEFAULT_MAX_PARALLEL_RUNS,
    ignore_errors: bool = True,
) -> bool:
    """Creates and watches job runs over a range of dates for a given job. At
    most, max_parallel runs can run in parallel to prevent resource exhaustion.
    """
    tron_client = client.Client(server)
    url_index = tron_client.index()

    # check job_name identifies a valid tron object
    job_id = client.get_object_type_from_identifier(url_index, job_name)
    # check job_name identifies a job
    if job_id.type != client.TronObjectType.job:
        raise ValueError(f"'{job_name}' is a {job_id.type.lower()}, not a job")

    finished, running = {}, set()
    all_successful = True

    while len(finished) < len(dates):
        # start more runs if we still have some and tha parallel limit is not yet reached
        while len(finished) + len(running) < len(dates) and len(running) < max_parallel:
            next_run_time = dates[len(finished) + len(running)]
            running.add(asyncio.ensure_future(run_backfill_for_date(tron_client, job_id, next_run_time)))

        just_finished, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for future in just_finished:
            job_run_name, run_time, run_successful = future.result()
            finished[run_time] = job_run_name, run_successful
            all_successful &= run_successful

        if not ignore_errors and not all_successful:
            print("Error: encountered failing job run; aborting all runs and exiting.")
            for run in running:
                run.cancel()  # cancel running async tasks
            return False
    return True


async def run_backfill_for_date(
    tron_client: client.Client, job_id: client.TronObjectIdentifier, run_time: datetime.datetime,
) -> Tuple[str, datetime.datetime, bool]:
    """Creates and watches a backfill for a specific date.

    Returns the job_run_name, run_time, and whether or not the backfill was
    successful.
    """
    date_str = run_time.date().isoformat()
    job_run_name = None
    job_run_id = None
    run_successful = True

    try:
        job_run_name = await _create_job_run(tron_client.url_base, job_id.url, run_time)

        if job_run_name is None:
            run_successful = False
        elif job_run_name == "":
            print(
                f"Warning: Job run for {date_str} created, but couldn't determine "
                "its name, so it is considered to have failed."
            )
            run_successful = False
        else:
            job_run_id = client.get_object_type_from_identifier(tron_client.index(), job_run_name)
            print(f"Waiting for job run '{job_run_name}' for {date_str} to finish...")
            run_successful = await _watch_job_run(tron_client, job_run_name, job_run_id.url, date_str)

    except asyncio.CancelledError:
        if job_run_id:
            response = client.request(urljoin(tron_client.url_base, job_run_id.url), data=dict(command="cancel"),)
            if response.error:
                print(
                    f"Error: couldn't cancel '{job_run_name}' for {date_str}. "
                    "You should use tronview to check on it."
                )
            else:
                print(f"Backfill job run '{job_run_name}' for {date_str} cancelled")
        else:
            # accounts for the case where the job was created, but this coroutine
            # is cancelled before the name (and id) is returned to us
            print(
                f"Warning: attempted to cancel backfill for {date_str}, but we "
                "don't know if it was created initially. You should use tronview "
                "to check."
            )
        run_successful = False

    return job_run_name, run_time, run_successful


async def _create_job_run(server: str, job_url: str, run_time: datetime.datetime,) -> Optional[str]:
    """Creates job run for a specific date.

    Returns:
        None, if the job run couldn't be created
        "", if the job run was created, but its name couldn't be determined
        the job run's name, otherwise
    """
    # create the job run
    loop = asyncio.get_event_loop()
    data = dict(command="start", run_time=run_time)
    response = await loop.run_in_executor(None, functools.partial(client.request, urljoin(server, job_url), data=data),)
    if response.error:
        date_str = run_time.date().isoformat()
        print(f"Error: couldn't start job run for {date_str}: {response.content}")
        return None

    # determine name of job run so that we can watch it
    # from tron.api.controller.JobController and tron.core.jobrun, the format
    # of the response result will be: "Created JobRun:<job_run_name>"
    result = response.content.get("result")
    match = re.match(r"^Created JobRun:([-.\w]+)$", result)
    return match.groups(0)[0] if match else ""


async def _watch_job_run(
    tron_client: client.Client,
    job_run_name: str,
    job_run_url: str,
    date_str: str,
    poll_intv_s: int = DEFAULT_POLLING_INTERVAL,
) -> bool:
    """Watches a job run until it completes.

    Returns:
        True, if the job run completed successfully
        False, otherwise
    """
    loop = asyncio.get_event_loop()
    job_run_state = None
    # job run states are derived form action run states
    # see: tron.core.jobrun.JobRun.state
    while job_run_state not in ActionRun.END_STATES:
        await asyncio.sleep(poll_intv_s)
        try:
            # poll job status every `poll_intv_s` seconds
            resp_content = await loop.run_in_executor(
                None,
                functools.partial(
                    tron_client.job_runs,
                    urljoin(tron_client.url_base, job_run_url),
                    include_runs=False,
                    include_graph=False,
                ),
            )
        except client.RequestError as e:
            print(f"Error: couldn't get state for job run '{job_run_name}': {e}")
            return False
        job_run_state = resp_content.get("state", ActionRun.UNKNOWN)

    print(f"Job run '{job_run_name}' for {date_str} finished with state: {job_run_state}")
    return job_run_state == ActionRun.SUCCEEDED
