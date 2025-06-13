import asyncio
import datetime
import functools
import os
import pprint
import re
import signal
from typing import List
from typing import Optional
from typing import Set
from urllib.parse import urljoin

from tron.commands import client
from tron.commands import display
from tron.commands.authentication import get_auth_token
from tron.core.actionrun import ActionRun

DEFAULT_MAX_PARALLEL_RUNS = 3
LIMIT_MAX_PARALLEL_RUNS = 10
DEFAULT_POLLING_INTERVAL_S = 10


def get_date_range(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    descending: bool = False,
) -> List[datetime.datetime]:
    dates = []
    delta = end_date - start_date
    for days_to_add in range(delta.days + 1):
        dates.append(start_date + datetime.timedelta(days=days_to_add))
    if descending:
        dates.reverse()
    return dates


def print_backfill_cmds(job: str, date_strs: List[str]) -> None:
    print(f"Please run the following {len(date_strs)} commands:")
    print("")
    for date in date_strs:
        print(f"tronctl start {job} --run-date {date}")
    print("")
    print("Note that many jobs operate on the previous day's data.")


def confirm_backfill(job: str, date_strs: List[str]) -> bool:
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


class BackfillRun:
    NOT_STARTED_STATE = "not started"
    SUCCESS_STATES = {ActionRun.SUCCEEDED, ActionRun.CANCELLED, ActionRun.SKIPPED}

    def __init__(self, tron_client: client.Client, job_id: client.TronObjectIdentifier, run_time: datetime.datetime):
        self.tron_client = tron_client
        self.job_id = job_id
        self.run_time = run_time
        self.run_name: Optional[str] = None
        self.run_id: Optional[client.TronObjectIdentifier] = None
        self.run_state = BackfillRun.NOT_STARTED_STATE

    @property
    def run_time_str(self) -> str:
        return self.run_time.date().isoformat()

    async def run_until_completion(self) -> str:
        """Runs this job run until it finishes (i.e. reaches a terminal state)."""
        try:
            if await self.create():
                await self.sync_state()
                await self.watch_until_completion()
        except asyncio.CancelledError:
            await self.cancel()
        return self.run_state

    async def create(self) -> Optional[str]:
        """Creates job run for a specific date.

        Returns the name of the run, if it was created with no issues.
        """
        # create the job run
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            functools.partial(
                client.request,
                urljoin(self.tron_client.url_base, self.job_id.url),
                data=dict(command="start", run_time=self.run_time),
                user_attribution=True,
            ),
        )

        # figure out its name
        if response.error:
            print(f"Error: couldn't start job run for {self.run_time_str}: {response.content}")
        else:
            # determine name of job run so that we can watch it
            # from tron.api.controller.JobController and tron.core.jobrun, the format
            # of the response result will be: "Created JobRun:<job_run_name>"
            result = response.content.get("result")
            match = re.match(r"^Created JobRun:([-.\w]+)$", result)

            if match:
                self.run_name = match.groups(0)[0]  # type: ignore[assignment] # mypy wrongly identifies self.run_name type as "Union[str, int]"
                self.run_state = ActionRun.STARTING
                print(f"Job run '{self.run_name}' for {self.run_time_str} created")
            else:
                print(
                    f"Warning: Job run for {self.run_time_str} created, but couldn't determine "
                    "its name, so its state is considered to be unknown."
                )
                self.run_state = ActionRun.UNKNOWN

        return self.run_name

    async def get_run_id(self) -> Optional[client.TronObjectIdentifier]:
        if not self.run_id:
            loop = asyncio.get_event_loop()
            try:
                self.run_id = await loop.run_in_executor(
                    None,
                    client.get_object_type_from_identifier,
                    self.tron_client.index(),
                    self.run_name,
                )

            except client.RequestError as e:
                print(f"Error: couldn't get resource URL for job run '{self.run_name}': {e}")

        return self.run_id

    async def sync_state(self) -> str:
        """Syncs the local run state with that of the Tron server's.

        Returns the updated state.
        """
        if not self.run_id:
            self.run_id = await self.get_run_id()

        if self.run_id:
            loop = asyncio.get_event_loop()
            try:
                # get the state of the run using the resource url
                resp_content = await loop.run_in_executor(
                    None,
                    functools.partial(
                        self.tron_client.job_runs,
                        urljoin(self.tron_client.url_base, self.run_id.url),
                        include_runs=False,
                        include_graph=False,
                    ),
                )
                self.run_state = resp_content.get("state", ActionRun.UNKNOWN)

            except (client.RequestError, AttributeError, ValueError) as e:
                print(f"Error: couldn't get state for job run '{self.run_name}': {e}")
                self.run_state = ActionRun.UNKNOWN

        return self.run_state

    async def watch_until_completion(self, poll_intv_s: int = DEFAULT_POLLING_INTERVAL_S) -> str:
        """Watches this job run until it finishes.

        Returns the end state of the run.
        """
        while self.run_state not in ActionRun.END_STATES:
            await asyncio.sleep(poll_intv_s)
            await self.sync_state()

        print(f"Job run '{self.run_name}' for {self.run_time_str} finished with state: {self.run_state}")
        return self.run_state

    async def cancel(self) -> bool:
        """Cancel this run if it is running.

        Returns whether or not the run was successfully cancelled
        """
        if self.run_id:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                functools.partial(
                    client.request,
                    urljoin(self.tron_client.url_base, self.run_id.url),
                    data=dict(command="cancel"),
                    user_attribution=True,
                ),
            )
            if response.error:
                print(
                    f"Error: couldn't cancel '{self.run_name}' for {self.run_time_str}. "
                    "You should use tronview to check on it."
                )
            else:
                print(f"Backfill job run '{self.run_name}' for {self.run_time_str} cancelled")
                self.run_state = ActionRun.CANCELLED
                return True
        else:
            # accounts for the case where the job was created, but this coroutine
            # is cancelled before the name (and id) is returned to us
            print(
                f"Warning: attempted to cancel backfill for {self.run_time_str}, but we "
                "don't know if it was created initially. You should use tronview "
                "to check."
            )
        return False


async def run_backfill_for_date_range(
    server: str,
    job_name: str,
    dates: List[datetime.datetime],
    max_parallel: int = DEFAULT_MAX_PARALLEL_RUNS,
    ignore_errors: bool = True,
) -> List[BackfillRun]:
    """Creates and watches job runs over a range of dates for a given job. At
    most, max_parallel runs can run in parallel to prevent resource exhaustion.
    """
    loop = asyncio.get_event_loop()

    # Trigger authentication before submitting all async jobs, so auth tokens
    # are cached and won't prompt the user in the individual API calls
    if os.getenv("TRONCTL_API_AUTH"):
        get_auth_token()

    tron_client = client.Client(server, user_attribution=True)
    url_index = tron_client.index()

    # check job_name identifies a valid tron object
    job_id = client.get_object_type_from_identifier(url_index, job_name)
    # check job_name identifies a job
    if job_id.type != client.TronObjectType.job:
        raise ValueError(f"'{job_name}' is a {job_id.type.lower()}, not a job")

    backfill_runs = [BackfillRun(tron_client, job_id, run_time) for run_time in dates]
    running: Set[asyncio.Future] = set()
    finished_cnt = 0
    all_successful = True

    # `current_task()` will always return a task here, but we need to account
    # for the None case for mypy
    current_task = asyncio.Task.current_task()
    if current_task:
        loop.add_signal_handler(signal.SIGINT, current_task.cancel)
    try:
        while finished_cnt < len(dates):
            # start more runs if we still have some and tha parallel limit is not yet reached
            while finished_cnt + len(running) < len(dates) and len(running) < max_parallel:
                next_run = backfill_runs[finished_cnt + len(running)]
                running.add(asyncio.ensure_future(next_run.run_until_completion()))

            just_finished, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for task in just_finished:
                finished_cnt += 1
                all_successful &= task.result() in BackfillRun.SUCCESS_STATES

            if not ignore_errors and not all_successful:
                print("Error: encountered failing job run; cancelling all in-progress runs and exiting.")
                for task in running:
                    task.cancel()  # cancel running async tasks
                    await task  # wait until it is done cancelling
                break

    except asyncio.CancelledError:  # caused by sigint handler
        print("Error: SIGINT detected; aborting all in-progress runs and exiting")
        for task in running:
            task.cancel()
            await task
    return backfill_runs


class DisplayBackfillRuns(display.TableDisplay):

    columns = ["Date", "Job Run Name", "Final State"]
    fields = ["run_time", "run_name", "run_state"]
    widths = [15, 60, 15]
    title = "Backfills Job Runs"
    resize_fields = {"run_time", "run_name", "run_state"}
    header_color = "hgray"


def print_backfill_runs_table(runs: List[BackfillRun]) -> None:
    """Prints backfill runs in a table"""
    with display.Color.enable():
        table = DisplayBackfillRuns().format(
            [
                dict(run_time=r.run_time.date().isoformat(), run_name=(r.run_name or "n/a"), run_state=r.run_state)
                for r in runs
            ]
        )
        print(table)
