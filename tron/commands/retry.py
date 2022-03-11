import argparse
import datetime
import time
from typing import Dict
from typing import Optional
from urllib.parse import urljoin

import pytimeparse

from tron.commands import client
from tron.commands.backfill import BackfillRun


DEFAULT_POLLING_INTERVAL_S = 10


def parse_deps_timeout(d_str):
    if d_str == "infinity":
        return -1
    elif d_str.isnumeric():
        seconds = int(d_str)
    else:
        seconds = pytimeparse.parse(d_str)
        if seconds is None:
            raise argparse.ArgumentTypeError(
                f"'{d_str}' is not a valid duration. Must be either number of seconds or pytimeparse-parsable string."
            )
    if seconds < 0:
        raise argparse.ArgumentTypeError(f"'{d_str}' must not be negative")
    return seconds


def retry_action(
    tron_client, full_action_name, use_latest_command=False, deps_timeout_s=None,
):

    r_action = RetryAction(tron_client, full_action_name, use_latest_command=use_latest_command)
    return r_action.wait_and_retry(deps_timeout_s=deps_timeout_s, use_latest_command=use_latest_command)


class RetryAction:
    def __init__(
        self, tron_client: client.Client, full_action_name: str, use_latest_command: bool = False,
    ):
        self.tron_client = tron_client
        self.retry_params = dict(command="retry", use_latest_command=use_latest_command,)

        self.full_action_name = full_action_name
        self.action_run_id = self._validate_action_name(full_action_name)
        self.job_run_id = client.get_object_type_from_identifier(self.tron_client.index(), self.job_run_name)

        self._required_action_indices = self._get_required_action_indices()
        self._triggers_done = False
        self._required_actions_done = False

    @property
    def job_run_name(self):
        return self.full_action_name.rsplit(".", 1)[0]

    @property
    def action_name(self):
        return self.full_action_name.rsplit(".", 1)[1]

    def can_retry(self, elapsed: Optional[datetime.timedelta] = None) -> bool:
        def log(s):
            prefix = f"[{elapsed}]" if elapsed is not None else ""
            print(f"{prefix} {s}")

        if not self._triggers_done:
            triggers = self.get_triggers()
            self._triggers_done = all(triggers.values())
            if self._triggers_done:
                if len(triggers) > 0:
                    log("All upstream triggers published")
            else:
                remaining_triggers = [trigger for trigger, is_done in triggers.items() if not is_done]
                log(f"Still waiting on the following triggers to publish: {remaining_triggers}")
        if not self._required_actions_done:
            required_actions = self.get_required_actions()
            self._required_actions_done = all(required_actions.values())
            if self._required_actions_done:
                if len(required_actions) > 0:
                    log("All required actions finished")
            else:
                remaining_required_actions = [action for action, is_done in required_actions.items() if not is_done]
                log(
                    "Still waiting on the following required actions to "
                    f"complete successfully: {remaining_required_actions}"
                )
        return self._triggers_done and self._required_actions_done

    def get_triggers(self) -> Dict[str, bool]:
        action_run = self.tron_client.action_runs(self.action_run_id.url, num_lines=0)
        # from tron.api.adapter:ActionRunAdapter.get_triggered_by:
        # triggered_by is a single string with this format:
        #   {trigger_1} (done), {trigger_2}, etc.
        # where trigger_1 has been published, and trigger_2 is still waiting
        trigger_states = {}
        for trigger_and_state in action_run["triggered_by"].split(", "):
            if trigger_and_state:
                parts = trigger_and_state.split(" ")
                # if len(parts) == 2, then parts is [{trigger}, "(done)"]
                # else, parts is [{trigger}]
                trigger_states[parts[0]] = len(parts) == 2
        return trigger_states

    def get_required_actions(self) -> Dict[str, bool]:
        action_runs = self.tron_client.job_runs(self.job_run_id.url)["runs"]
        return {
            action_runs[i]["action_name"]: action_runs[i]["state"] in BackfillRun.SUCCESS_STATES
            for i in self._required_action_indices.values()
        }

    def wait_and_retry(self, deps_timeout_s: Optional[int] = None, use_latest_command: bool = False,) -> bool:
        if self.wait_for_deps(deps_timeout_s=deps_timeout_s):
            return self.issue_retry(use_latest_command=use_latest_command)
        else:
            deps_timeout_td = datetime.timedelta(seconds=deps_timeout_s)
            print(f"Not all triggers published after {deps_timeout_td}. Action will not be retried.")
            return False

    def wait_for_deps(
        self, deps_timeout_s: Optional[int] = None, poll_intv_s: int = DEFAULT_POLLING_INTERVAL_S,
    ) -> bool:
        """Wait for all upstream dependencies to finished up to a timeout. Once the
        timeout has expired, one final check is always conducted.

        Returns whether or not deps successfully finished.
        """
        elapsed = datetime.timedelta(seconds=0)

        while deps_timeout_s == -1 or elapsed.seconds < deps_timeout_s:
            if self.can_retry(elapsed=elapsed):
                return True
            wait_for = poll_intv_s
            if deps_timeout_s != -1:
                wait_for = min(wait_for, int(deps_timeout_s - elapsed.seconds))
            time.sleep(wait_for)
            elapsed += datetime.timedelta(seconds=wait_for)

        return self.can_retry(elapsed=elapsed)

    def issue_retry(self, use_latest_command: bool = False) -> bool:
        print(f"Issuing retry request for {self.full_action_name}")
        response = client.request(
            urljoin(self.tron_client.url_base, self.action_run_id.url),
            data=dict(command="retry", use_latest_command=int(use_latest_command),),
        )
        if response.error:
            print(f"Error: couldn't issue retry request: {response.content}")
        else:
            print(f"Got result: {response.content.get('result')}")
            print("Check the status of the retry run using:")
            print(f"   tronview {self.full_action_name}")
        return bool(response.error)

    def _validate_action_name(self, full_action_name: str) -> client.TronObjectIdentifier:
        action_run_id = client.get_object_type_from_identifier(self.tron_client.index(), full_action_name)
        if action_run_id.type != client.TronObjectType.action_run:
            raise ValueError(f"Unknown action name: '{full_action_name}'")
        return action_run_id

    def _get_required_action_indices(self) -> Dict[str, bool]:
        job_run = self.tron_client.job_runs(self.job_run_id.url)
        required_actions = set()
        action_indices = {}

        for i, action_run in enumerate(job_run["runs"]):
            if action_run["action_name"] == self.action_name:
                required_actions = set(action_run["requirements"])
            action_indices[action_run["action_name"]] = i

        return dict(filter(lambda e: e[0] in required_actions, action_indices.items()))
