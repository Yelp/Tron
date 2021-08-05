import datetime
import pprint
import re
from typing import Generator
from typing import List
from urllib.parse import urljoin

from tron.commands import client

DEFAULT_MAX_PARALLEL_RUNS = 10


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


def run_backfill_for_date_range(
    server: str, job_name: str, dates: List[datetime.datetime], max_parallel: int = DEFAULT_MAX_PARALLEL_RUNS,
) -> Generator[bool, None, None]:
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

    for run_time in dates:
        date_str = run_time.date().isoformat()
        job_run_name = _create_job_run(server, job_name, job_id.url, run_time)
        if job_run_name is None:
            yield False
        elif job_run_name == "":
            print(
                f"Warning: Job run for {date_str} created, but couldn't determine "
                "its name, so it will not be watched for completion"
            )
            yield True
        else:
            print(f"Created for job run '{job_run_name}' for {date_str}")
            yield True


def _create_job_run(server: str, job_name: str, job_url: str, run_time: datetime.datetime,) -> bool:
    """Creates and watches a job run for a specific date.

    Returns True if the job run completed successfully, and False otherwise.
    """
    # create the job run
    data = dict(command="start", run_time=run_time)
    response = client.request(urljoin(server, job_url), data=data)
    if response.error:
        print(f"Error: {response.content}")
        return None

    # determine name of job run so that we can watch it
    # from tron.api.controller.JobController and tron.core.jobrun, the format
    # of the response result will be: "Created JobRun:<job_run_name>"
    result = response.content.get("result")
    match = re.match(r"^Created JobRun:([-.\w]+)$", result)
    return match.groups(0)[0] if match else ""
