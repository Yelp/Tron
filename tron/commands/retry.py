import argparse
import asyncio
import datetime
import functools
import random
from typing import Dict
from typing import List
from typing import Optional
from urllib.parse import urljoin

import pytimeparse  # type:ignore

from tron.commands import client
from tron.commands import display
from tron.commands.backfill import BackfillRun


DEFAULT_POLLING_INTERVAL_S = 10


def parse_deps_timeout(duration: str) -> int:
    if duration == "infinity":
        return RetryAction.WAIT_FOREVER
    elif duration.isnumeric():
        seconds = int(duration)
    else:
        seconds = pytimeparse.parse(duration)
        if seconds is None:
            raise argparse.ArgumentTypeError(
                f"'{duration}' is not a valid duration. Must be either number of seconds or pytimeparse-parsable string."
            )
    if seconds < 0:
        raise argparse.ArgumentTypeError(f"'{duration}' must not be negative")
    return seconds


class RetryAction:
    NO_TIMEOUT = 0
    WAIT_FOREVER = -1

    RETRY_NOT_ISSUED = None
    RETRY_SUCCESS = True
    RETRY_FAIL = False

    def __init__(
        self,
        tron_client: client.Client,
        full_action_name: str,
        use_latest_command: bool = False,
    ):
        self.tron_client = tron_client
        self.retry_params = dict(command="retry", use_latest_command=int(use_latest_command))

        self.full_action_name = full_action_name
        self.action_run_id = self._validate_action_name(full_action_name)
        self.job_run_id = client.get_object_type_from_identifier(self.tron_client.index(), self.job_run_name)

        self._required_action_indices = self._get_required_action_indices()
        self._elapsed = datetime.timedelta(seconds=0)
        self._triggers_done = False
        self._required_actions_done = False
        self._retry_request_result: Optional[bool] = RetryAction.RETRY_NOT_ISSUED

    @property
    def job_run_name(self) -> str:
        return self.full_action_name.rsplit(".", 1)[0]

    @property
    def action_name(self) -> str:
        return self.full_action_name.rsplit(".", 1)[1]

    @property
    def status(self) -> str:
        if not self._triggers_done:
            return "Upstream triggers not all published"
        elif not self._required_actions_done:
            return "Required actions not all successfully completed"
        elif self._retry_request_result == RetryAction.RETRY_NOT_ISSUED:
            return "Retry request not issued, but dependencies done"
        elif self._retry_request_result == RetryAction.RETRY_SUCCESS:
            return "Retry request issued successfully"
        else:
            return "Failed to issue retry request"

    @property
    def succeeded(self) -> bool:
        return bool(self._retry_request_result)

    def _validate_action_name(self, full_action_name: str) -> client.TronObjectIdentifier:
        action_run_id: client.TronObjectIdentifier = client.get_object_type_from_identifier(
            self.tron_client.index(), full_action_name
        )
        if action_run_id.type != client.TronObjectType.action_run:
            raise ValueError(f"'{full_action_name}' is a {action_run_id.type.lower()}, not an action")
        self.tron_client.action_runs(action_run_id.url, num_lines=0)  # verify action exists
        return action_run_id

    def _get_required_action_indices(self) -> Dict[str, int]:
        job_run = self.tron_client.job_runs(self.job_run_id.url)
        required_actions = set()
        action_indices = {}

        for i, action_run in enumerate(job_run["runs"]):
            if action_run["action_name"] == self.action_name:
                required_actions = set(action_run["requirements"])
            action_indices[action_run["action_name"]] = i

        return {action_name: i for action_name, i in action_indices.items() if action_name in required_actions}

    def _log(self, msg: str) -> None:
        print(f"[{self._elapsed}] {self.full_action_name}: {msg}")

    async def can_retry(self) -> bool:
        if not self._triggers_done:
            triggers = await self.check_trigger_statuses()
            self._triggers_done = all(triggers.values())
            if self._triggers_done:
                if len(triggers) > 0:
                    self._log("All upstream triggers published")
            else:
                remaining_triggers = [trigger for trigger, is_done in triggers.items() if not is_done]
                self._log(f"Upstream triggers not yet published: {remaining_triggers}")
        if not self._required_actions_done:
            required_actions = await self.check_required_actions_statuses()
            self._required_actions_done = all(required_actions.values())
            if self._required_actions_done:
                if len(required_actions) > 0:
                    self._log("All required actions finished")
            else:
                remaining_required_actions = [action for action, is_done in required_actions.items() if not is_done]
                self._log(f"Required actions not yet succeeded: {remaining_required_actions}")
        return self._triggers_done and self._required_actions_done

    async def check_trigger_statuses(self) -> Dict[str, bool]:
        action_run = await asyncio.get_event_loop().run_in_executor(
            None,
            functools.partial(
                self.tron_client.action_runs,
                self.action_run_id.url,
                num_lines=0,
            ),
        )
        # from tron.api.adapter:ActionRunAdapter.get_triggered_by:
        # triggered_by is a single string with this format:
        #   {trigger_1} (done), {trigger_2}, etc.
        # where trigger_1 has been published, and trigger_2 is still waiting
        trigger_states = {}
        for trigger_and_state in action_run["triggered_by"].split(", "):
            if trigger_and_state:
                trigger, *maybe_state = trigger_and_state.split(" ")
                # if len(parts) == 2, then parts is [{trigger}, "(done)"]
                # else, parts is [{trigger}]
                trigger_states[trigger] = len(maybe_state) == 1
        return trigger_states

    async def check_required_actions_statuses(self) -> Dict[str, bool]:
        action_runs = (
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.tron_client.job_runs,
                self.job_run_id.url,
            )
        )["runs"]
        return {
            action_runs[i]["action_name"]: action_runs[i]["state"] in BackfillRun.SUCCESS_STATES
            for i in self._required_action_indices.values()
        }

    async def wait_and_retry(
        self,
        deps_timeout_s: int = 0,
        poll_interval_s: int = DEFAULT_POLLING_INTERVAL_S,
        jitter: bool = True,
    ) -> bool:
        if deps_timeout_s != RetryAction.NO_TIMEOUT and jitter:
            init_delay_s = random.randint(1, min(deps_timeout_s, poll_interval_s)) - 1
            self._elapsed += datetime.timedelta(seconds=init_delay_s)
            await asyncio.sleep(init_delay_s)

        if await self.wait_for_deps(deps_timeout_s=deps_timeout_s, poll_interval_s=poll_interval_s):
            return await self.issue_retry()
        else:
            deps_timeout_td = datetime.timedelta(seconds=deps_timeout_s)
            msg = "Action will not be retried."
            if deps_timeout_s != RetryAction.NO_TIMEOUT:
                msg = f"Not all dependencies completed after waiting for {deps_timeout_td}. " + msg
            self._log(msg)
            return False

    async def wait_for_deps(
        self,
        deps_timeout_s: int = 0,
        poll_interval_s: int = DEFAULT_POLLING_INTERVAL_S,
    ) -> bool:
        """Wait for all upstream dependencies to finished up to a timeout. Once the
        timeout has expired, one final check is always conducted.

        Returns whether or not deps successfully finished.
        """
        while deps_timeout_s == RetryAction.WAIT_FOREVER or self._elapsed.seconds < deps_timeout_s:
            if await self.can_retry():
                return True
            wait_for = poll_interval_s
            if deps_timeout_s != RetryAction.WAIT_FOREVER:
                wait_for = min(wait_for, int(deps_timeout_s - self._elapsed.seconds))
            await asyncio.sleep(wait_for)
            self._elapsed += datetime.timedelta(seconds=wait_for)

        return await self.can_retry()

    async def issue_retry(self) -> bool:
        self._log("Issuing retry request")
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            functools.partial(
                client.request,
                urljoin(self.tron_client.url_base, self.action_run_id.url),
                data=self.retry_params,
                user_attribution=True,
            ),
        )
        if response.error:
            self._log(f"Error: couldn't issue retry request: {response.content}")
            self._retry_request_result = RetryAction.RETRY_FAIL
        else:
            self._log(f"Got result: {response.content.get('result')}")
            self._log(f"Check the status of the retry run using: `tronview {self.full_action_name}`")
            self._retry_request_result = RetryAction.RETRY_SUCCESS
        return self._retry_request_result


def retry_actions(
    tron_server: str,
    full_action_names: List[str],
    use_latest_command: bool = False,
    deps_timeout_s: int = RetryAction.NO_TIMEOUT,
) -> List[RetryAction]:
    tron_client = client.Client(tron_server, user_attribution=True)
    r_actions = [RetryAction(tron_client, name, use_latest_command=use_latest_command) for name in full_action_names]

    loop = asyncio.get_event_loop()
    try:
        # first action starts checking immediately, rest have a jitter
        loop.run_until_complete(
            asyncio.gather(
                r_actions[0].wait_and_retry(deps_timeout_s=deps_timeout_s, jitter=False),
                *[ra.wait_and_retry(deps_timeout_s=deps_timeout_s) for ra in r_actions[1:]],
            )
        )
    finally:
        loop.close()
    return r_actions


class DisplayRetries(display.TableDisplay):
    columns = ["Action Name", "Final Status"]
    fields = ["full_action_name", "status"]
    widths = [60, 60]
    title = "Retries"
    resize_fields = {"full_action_name", "status"}
    header_color = "hgray"


def print_retries_table(retries: List[RetryAction]) -> None:
    """Prints retry runs in a table"""
    with display.Color.enable():
        table = DisplayRetries().format([dict(full_action_name=r.full_action_name, status=r.status) for r in retries])
        print(table)
