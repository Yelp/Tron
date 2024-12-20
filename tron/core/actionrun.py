"""
 tron.core.actionrun
"""
import datetime
import json
import logging
import os
from dataclasses import dataclass
from dataclasses import fields
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

from twisted.internet import reactor
from twisted.internet.base import DelayedCall

from tron import command_context
from tron import node
from tron.actioncommand import ActionCommand
from tron.actioncommand import NoActionRunnerFactory
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.bin.action_runner import build_environment
from tron.bin.action_runner import build_labels
from tron.command_context import CommandContext
from tron.config import schema
from tron.config.config_utils import StringFormatter
from tron.config.schema import ExecutorTypes
from tron.core import action
from tron.core.action import ActionCommandConfig
from tron.eventbus import EventBus
from tron.kubernetes import KubernetesClusterRepository
from tron.kubernetes import KubernetesTask
from tron.mesos import MesosClusterRepository
from tron.serialize import filehandler
from tron.utils import exitcode
from tron.utils import maybe_decode
from tron.utils import proxy
from tron.utils import timeutils
from tron.utils.observer import Observable
from tron.utils.observer import Observer
from tron.utils.persistable import Persistable
from tron.utils.state import Machine


log = logging.getLogger(__name__)
MAX_RECOVER_TRIES = 5
INITIAL_RECOVER_DELAY = 3
KUBERNETES_ACTIONRUN_EXECUTORS: Set[str] = {ExecutorTypes.kubernetes.value, ExecutorTypes.spark.value}  # type: ignore # mypy can't seem to inspect this enum


class ActionRunFactory:
    """Construct ActionRuns and ActionRunCollections for a JobRun and
    ActionGraph.
    """

    @classmethod
    def build_action_run_collection(cls, job_run, action_runner):
        """Create an ActionRunCollection from an ActionGraph and JobRun."""
        action_run_map = {
            maybe_decode(
                name
            ): cls.build_run_for_action(  # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
                job_run,
                action_inst,
                action_runner,
            )
            for name, action_inst in job_run.action_graph.action_map.items()
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def action_run_collection_from_state(
        cls,
        job_run,
        runs_state_data,
        cleanup_action_state_data,
    ):
        action_runs = [cls.action_run_from_state(job_run, state_data) for state_data in runs_state_data]
        if cleanup_action_state_data:
            action_runs.append(
                cls.action_run_from_state(
                    job_run,
                    cleanup_action_state_data,
                    cleanup=True,
                ),
            )

        # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
        action_run_map = {maybe_decode(action_run.action_name): action_run for action_run in action_runs}
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def build_run_for_action(cls, job_run, action, action_runner):
        """Create an ActionRun for a JobRun and Action."""
        run_node = action.node_pool.next() if action.node_pool else job_run.node

        if action.trigger_timeout:
            trigger_timeout = job_run.run_time + action.trigger_timeout
        else:
            trigger_timeout = job_run.run_time + datetime.timedelta(days=1)

        args = {
            "job_run_id": job_run.id,
            "name": action.name,
            "node": run_node,
            "command_config": action.command_config,
            "parent_context": job_run.context,
            "output_path": job_run.output_path.clone(),
            "cleanup": action.is_cleanup,
            "action_runner": action_runner,
            "retries_remaining": action.retries,
            "retries_delay": action.retries_delay,
            "executor": action.executor,
            "trigger_downstreams": action.trigger_downstreams,
            "triggered_by": action.triggered_by,
            "on_upstream_rerun": action.on_upstream_rerun,
            "trigger_timeout_timestamp": trigger_timeout.timestamp(),
        }
        if action.executor == ExecutorTypes.mesos.value:
            return MesosActionRun(**args)
        elif action.executor in KUBERNETES_ACTIONRUN_EXECUTORS:
            return KubernetesActionRun(**args)
        return SSHActionRun(**args)

    @classmethod
    def action_run_from_state(cls, job_run, state_data, cleanup=False):
        """Restore an ActionRun for this JobRun from the state data."""
        args = {
            "state_data": state_data,
            "parent_context": job_run.context,
            "output_path": job_run.output_path.clone(),
            "job_run_node": job_run.node,
            "cleanup": cleanup,
            "action_graph": job_run.action_graph,
        }

        if state_data.get("executor") == ExecutorTypes.mesos.value:
            return MesosActionRun.from_state(**args)
        if state_data.get("executor") in KUBERNETES_ACTIONRUN_EXECUTORS:
            return KubernetesActionRun.from_state(**args)
        return SSHActionRun.from_state(**args)


@dataclass
class ActionRunAttempt(Persistable):
    """Stores state about one try of an action run."""

    command_config: action.ActionCommandConfig
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    rendered_command: Optional[str] = None
    exit_status: Optional[int] = None
    mesos_task_id: Optional[str] = None
    kubernetes_task_id: Optional[str] = None

    def exit(self, exit_status, end_time=None):
        if self.end_time is None:
            self.exit_status = exit_status
            self.end_time = end_time or timeutils.current_time()

    @property
    def display_command(self):
        return self.rendered_command or self.command_config.command

    @property
    def state_data(self):
        state_data = {
            "command_config": self.command_config.state_data,
        }
        for field in fields(self):
            if field.name not in state_data:
                state_data[field.name] = getattr(self, field.name)
        return state_data

    @staticmethod
    def to_json(state_data: dict) -> Optional[str]:
        """Serialize the ActionRunAttempt instance to a JSON string."""
        try:
            return json.dumps(
                {
                    "command_config": ActionCommandConfig.to_json(state_data["command_config"]),
                    "start_time": state_data["start_time"].isoformat() if state_data["start_time"] else None,
                    "end_time": state_data["end_time"].isoformat() if state_data["end_time"] else None,
                    "rendered_command": state_data["rendered_command"],
                    "exit_status": state_data["exit_status"],
                    "mesos_task_id": state_data["mesos_task_id"],
                    "kubernetes_task_id": state_data["kubernetes_task_id"],
                }
            )
        except KeyError:
            log.exception("Missing key in state_data:")
            raise
        except Exception:
            log.exception("Error serializing ActionRunAttempt to JSON:")
            raise

    @staticmethod
    def from_json(state_data: str):
        """Deserialize the ActionRunAttempt instance from a JSON string."""
        try:
            json_data = json.loads(state_data)
            deserialized_data = {
                "command_config": ActionCommandConfig.from_json(json_data["command_config"]),
                "start_time": (
                    datetime.datetime.fromisoformat(json_data["start_time"]) if json_data["start_time"] else None
                ),
                "end_time": datetime.datetime.fromisoformat(json_data["end_time"]) if json_data["end_time"] else None,
                "rendered_command": json_data["rendered_command"],
                "exit_status": json_data["exit_status"],
                "mesos_task_id": json_data["mesos_task_id"],
                "kubernetes_task_id": json_data["kubernetes_task_id"],
            }
        except Exception:
            log.exception("Error deserializing ActionRunAttempt from JSON")
            raise
        return deserialized_data

    @classmethod
    def from_state(cls, state_data):
        # it's possible that we've rolled back to an older Tron version that doesn't support data that we've persisted
        # (e.g., new fields for an ActionCommandConfig) so ensure that we only load what we currently support
        valid_command_config_entries_from_state = {
            field.name: state_data["command_config"][field.name]
            for field in fields(action.ActionCommandConfig)
            if field.name in state_data["command_config"]
        }
        state_data["command_config"] = action.ActionCommandConfig(**valid_command_config_entries_from_state)

        valid_actionrun_attempt_entries_from_state = {
            field.name: state_data[field.name] for field in fields(cls) if field.name in state_data
        }
        return cls(**valid_actionrun_attempt_entries_from_state)


class ActionRun(Observable, Persistable):
    """Base class for tracking the state of a single run of an Action.

    ActionRun's state machine is observed by a parent JobRun.
    """

    CANCELLED = "cancelled"
    FAILED = "failed"
    QUEUED = "queued"
    RUNNING = "running"
    SCHEDULED = "scheduled"
    SKIPPED = "skipped"
    STARTING = "starting"
    SUCCEEDED = "succeeded"
    WAITING = "waiting"
    UNKNOWN = "unknown"

    default_transitions = dict(fail=FAILED, success=SUCCEEDED)
    STATE_MACHINE = Machine(
        SCHEDULED,
        **{
            CANCELLED: dict(skip=SKIPPED),
            FAILED: dict(skip=SKIPPED),
            RUNNING: dict(
                cancel=CANCELLED,
                fail_unknown=UNKNOWN,
                **default_transitions,
            ),
            STARTING: dict(
                started=RUNNING,
                fail=FAILED,
                fail_unknown=UNKNOWN,
                cancel=CANCELLED,
            ),
            UNKNOWN: dict(
                running=RUNNING,
                fail_unknown=UNKNOWN,
                **default_transitions,
            ),
            WAITING: dict(
                cancel=CANCELLED,
                start=STARTING,
                **default_transitions,
            ),
            QUEUED: dict(
                ready=WAITING,
                cancel=CANCELLED,
                start=STARTING,
                schedule=SCHEDULED,
                **default_transitions,
            ),
            SCHEDULED: dict(
                ready=WAITING,
                queue=QUEUED,
                cancel=CANCELLED,
                start=STARTING,
                **default_transitions,
            ),
        },
    )

    # The set of states that are considered end states. Technically some of
    # these states can be manually transitioned to other states.
    END_STATES = {FAILED, SUCCEEDED, CANCELLED, SKIPPED, UNKNOWN}

    # Failed render command is false to ensure that it will fail when run
    FAILED_RENDER = "false # Command failed to render correctly. See the Tron error log."
    NOTIFY_TRIGGER_READY = "trigger_ready"

    # This is a list of "alternate locations" that we can look for stdout/stderr in
    # The PR in question is https://github.com/Yelp/Tron/pull/735/files, which changed
    # the format of the stdout/stderr paths
    STDOUT_PATHS = [
        os.path.join(
            "{namespace}.{jobname}",
            "{namespace}.{jobname}.{run_num}",
            "{namespace}.{jobname}.{run_num}.{action}",
        ),  # old style paths (pre-#735 PR)
        os.path.join(
            "{namespace}.{jobname}",
            "{namespace}.{jobname}.{run_num}",
            "{namespace}.{jobname}.{run_num}.{action}",
            "{namespace}.{jobname}.{run_num}.recovery-{namespace}.{jobname}.{run_num}.{action}",
        ),  # old style recovery paths (pre-#735 PR)
        os.path.join(
            "{namespace}",
            "{jobname}",
            "{run_num}",
            "{action}-recovery",
        ),  # new style recovery paths (post-#735 PR)
    ]

    context_class = command_context.ActionRunContext

    # TODO: create a class for ActionRunId, JobRunId, Etc
    def __init__(
        self,
        job_run_id: str,
        name: str,
        node: node.Node,
        command_config: action.ActionCommandConfig,
        parent_context: Optional[CommandContext] = None,
        output_path: Optional[filehandler.OutputPath] = None,
        cleanup: bool = False,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        run_state: str = SCHEDULED,
        exit_status: Optional[int] = None,
        attempts: Optional[List[ActionRunAttempt]] = None,
        action_runner: Optional[Union[NoActionRunnerFactory, SubprocessActionRunnerFactory]] = None,
        retries_remaining: Optional[int] = None,
        retries_delay: Optional[datetime.timedelta] = None,
        machine: Optional[Machine] = None,
        executor: Optional[str] = None,
        trigger_downstreams: Optional[Union[bool, dict]] = None,
        triggered_by: Optional[List[str]] = None,
        on_upstream_rerun: Optional[schema.ActionOnRerun] = None,
        trigger_timeout_timestamp: Optional[float] = None,
        original_command: Optional[str] = None,
    ):
        super().__init__()
        self.job_run_id = maybe_decode(
            job_run_id
        )  # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
        self.action_name = maybe_decode(
            name
        )  # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
        self.node = node
        self.start_time = start_time
        self.end_time = end_time
        self.exit_status = exit_status
        self.action_runner = action_runner or NoActionRunnerFactory()
        self.machine = machine or Machine.from_machine(
            ActionRun.STATE_MACHINE,
            None,
            run_state,
        )
        self.is_cleanup = cleanup

        self.executor = executor
        self.command_config = command_config
        self.original_command = original_command or command_config.command
        self.attempts = attempts or []
        self.output_path = output_path or filehandler.OutputPath()
        self.output_path.append(self.action_name)
        self.context = command_context.build_context(self, parent_context)
        self.retries_remaining = retries_remaining
        self.retries_delay = retries_delay
        self.trigger_downstreams = trigger_downstreams
        self.triggered_by = triggered_by
        self.on_upstream_rerun = on_upstream_rerun
        self.trigger_timeout_timestamp = trigger_timeout_timestamp
        self.trigger_timeout_call = None

        self.action_command = None
        self.in_delay = None  # type: Optional[DelayedCall]

    @property
    def state(self):
        return self.machine.state

    @property
    def id(self):
        return f"{self.job_run_id}.{self.action_name}"

    @property
    def name(self):
        return self.action_name

    @property
    def last_attempt(self):
        if self.attempts:
            return self.attempts[-1]
        return None

    @property
    def exit_statuses(self):
        if self.attempts:
            return [a.exit_status for a in self.attempts if a.end_time]
        return []

    @property
    def command(self):
        if self.attempts:
            return self.attempts[-1].display_command
        else:
            return self.command_config.command

    @property
    def rendered_command(self):
        if self.attempts:
            return self.attempts[-1].rendered_command
        return None

    @classmethod
    def attempts_from_state(cls, state_data, command_config):
        attempts = []
        if "attempts" in state_data:
            attempts = [ActionRunAttempt.from_state(a) for a in state_data["attempts"]]
        else:
            rendered_command = maybe_decode(
                state_data.get("rendered_command")
            )  # TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
            exit_statuses = state_data.get("exit_statuses", [])
            # If the action has started, add an attempt for the final try
            if state_data.get("start_time"):
                exit_statuses = exit_statuses + [state_data.get("exit_status")]
            for exit_status in exit_statuses:
                attempts.append(
                    ActionRunAttempt(
                        command_config=command_config,
                        rendered_command=rendered_command,
                        exit_status=exit_status,
                        start_time="unknown",
                        end_time="unknown",
                    ),
                )
            if attempts:
                # only one of these should ever be valid - and we'll want to clean
                # this up once we're off of mesos such that we only restore the k8s
                # task id
                attempts[-1].mesos_task_id = state_data.get("mesos_task_id")
                attempts[-1].kubernetes_task_id = state_data.get("kubernetes_task_id")
        return attempts

    @classmethod
    def from_state(
        cls,
        state_data,
        parent_context,
        output_path,
        job_run_node,
        action_graph,
        cleanup=False,
    ):
        """Restore the state of this ActionRun from a serialized state."""
        pool_repo = node.NodePoolRepository.get_instance()

        # Support state from older version
        if "id" in state_data:
            job_run_id, action_name = state_data["id"].rsplit(".", 1)
        else:
            job_run_id = state_data["job_run_id"]
            action_name = state_data["action_name"]

        job_run_node = pool_repo.get_node(
            state_data.get("node_name"),
            job_run_node,
        )

        action_runner_data = state_data.get("action_runner")
        if action_runner_data:
            action_runner = SubprocessActionRunnerFactory(**action_runner_data)
        else:
            action_runner = NoActionRunnerFactory()

        action_config = action_graph.action_map.get(action_name)
        if action_config:
            command_config = action_config.command_config
        else:
            command_config = action.ActionCommandConfig(command="")

        attempts = cls.attempts_from_state(state_data, command_config)
        run = cls(
            job_run_id=job_run_id,
            name=action_name,
            node=job_run_node,
            parent_context=parent_context,
            output_path=output_path,
            command_config=command_config,
            original_command=state_data.get("original_command"),
            cleanup=cleanup,
            start_time=state_data["start_time"],
            end_time=state_data["end_time"],
            run_state=state_data["state"],
            exit_status=state_data.get("exit_status"),
            attempts=attempts,
            retries_remaining=state_data.get("retries_remaining"),
            retries_delay=state_data.get("retries_delay"),
            action_runner=action_runner,
            executor=state_data.get("executor", ExecutorTypes.ssh.value),
            trigger_downstreams=state_data.get("trigger_downstreams"),
            triggered_by=state_data.get("triggered_by"),
            on_upstream_rerun=state_data.get("on_upstream_rerun"),
            trigger_timeout_timestamp=state_data.get("trigger_timeout_timestamp"),
        )

        # Transition running to fail unknown because exit status was missed
        # Recovery will look for unknown runs
        if run.is_active:
            run.transition_and_notify("fail_unknown")
        return run

    def start(self, original_command=True) -> Optional[Union[bool, ActionCommand]]:
        """Start this ActionRun."""
        if self.in_delay is not None:
            log.warning(f"{self} cancelling suspend timer")
            self.in_delay.cancel()
            self.in_delay = None

        if not self.machine.check("start"):
            return False

        if len(self.attempts) == 0:
            log.info(f"{self} starting")
        else:
            log.info(f"{self} restarting, retry {len(self.attempts)}")

        new_attempt = self.create_attempt(original_command=original_command)
        self.start_time = new_attempt.start_time
        self.transition_and_notify("start")

        if not self.command_config.command:
            log.error(f"{self} no longer configured in tronfig, cannot run")
            self.fail(exitcode.EXIT_INVALID_COMMAND)

        if not self.is_valid_command(new_attempt.rendered_command):
            log.error(f"{self} invalid command: {new_attempt.command_config.command}")
            self.fail(exitcode.EXIT_INVALID_COMMAND)
            return None

        return self.submit_command(new_attempt)

    def create_attempt(self, original_command=True):
        current_time = timeutils.current_time()
        command_config = self.command_config.copy()
        if original_command:
            command_config.command = self.original_command
        rendered_command = self.render_command(command_config.command)
        new_attempt = ActionRunAttempt(
            command_config=command_config,
            start_time=current_time,
            rendered_command=rendered_command,
        )
        self.attempts.append(new_attempt)
        return new_attempt

    def submit_command(self, attempt) -> Optional[Union[bool, ActionCommand]]:
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def kill(self, final=True):
        raise NotImplementedError()

    def recover(self):
        raise NotImplementedError()

    def _done(self, target, exit_status=0) -> Optional[bool]:
        if self.machine.check(target):
            if self.triggered_by:
                EventBus.clear_subscriptions(self.__hash__())
            self.clear_trigger_timeout()
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            if self.last_attempt is not None and self.last_attempt.end_time is None:
                self.last_attempt.exit(exit_status, self.end_time)
            log.info(
                f"{self} completed with {target}, transitioned to " f"{self.state}, exit status: {exit_status}",
            )
            return self.transition_and_notify(target)
        else:
            log.debug(
                f"{self} cannot transition from {self.state} via {target}",
            )
        return None

    def retry(self, original_command=True):
        """Invoked externally (via API) when action needs to be re-tried
        manually.
        """

        # Manually retrying means we force the retries to be 1 and
        # Cancel any delay, so the retry is kicked off asap
        if self.retries_remaining is None or self.retries_remaining <= 0:
            self.retries_remaining = 1
        if self.in_delay is not None:
            self.in_delay.cancel()
            self.in_delay = None
        self.retries_delay = None

        if self.is_done:
            self.machine.reset()
            return self._exit_unsuccessful(self.exit_status, retry_original_command=original_command)
        else:
            log.info(f"{self} getting killed for a retry")
            return self.kill(final=False)

    def start_after_delay(self):
        log.info(f"{self} resuming after retry delay")
        self.machine.reset()
        self.in_delay = None
        self.start()

    def restart(self, original_command=True) -> Optional[Union[bool, ActionCommand]]:
        """Used by `fail` when action run has to be re-tried"""
        if self.retries_delay is not None:
            self.in_delay = reactor.callLater(  # type: ignore  # no twisted stubs
                self.retries_delay.total_seconds(),
                self.start_after_delay,
            )
            log.info(f"{self} delaying for a retry in {self.retries_delay}s")
            return True
        else:
            self.machine.reset()
            return self.start(original_command=original_command)

    def fail(self, exit_status=None):
        if self.retries_remaining:
            self.retries_remaining = -1

        return self._done("fail", exit_status)

    def _exit_unsuccessful(
        self, exit_status=None, retry_original_command=True, non_retryable_exit_codes=[]
    ) -> Optional[Union[bool, ActionCommand]]:
        if self.is_done:
            log.info(
                f"{self} got exit code {exit_status} but already in terminal " f'state "{self.state}", not retrying',
            )
            return None
        if self.last_attempt is not None:
            self.last_attempt.exit(exit_status)
        if self.retries_remaining is not None:
            if exit_status in non_retryable_exit_codes:
                self.retries_remaining = 0
                log.info(f"{self} skipping auto-retries, received non-retryable exit code ({exit_status}).")
            else:
                if self.retries_remaining > 0:
                    self.retries_remaining -= 1
                    return self.restart(original_command=retry_original_command)
                else:
                    log.info(
                        f"Reached maximum number of retries: {len(self.attempts)}",
                    )
        if exit_status is None or exit_status in non_retryable_exit_codes:
            return self._done("fail_unknown", exit_status)
        else:
            return self._done("fail", exit_status)

    def triggers_to_emit(self) -> List[str]:
        if not self.trigger_downstreams:
            return []

        if isinstance(self.trigger_downstreams, bool):
            templates = ["shortdate.{shortdate}"]
        elif isinstance(self.trigger_downstreams, dict):
            templates = [f"{k}.{v}" for k, v in self.trigger_downstreams.items()]

        return [self.render_template(trig) for trig in templates]

    def emit_triggers(self):
        triggers = self.triggers_to_emit()
        if not triggers:
            return

        log.info(f"{self} publishing triggers: [{', '.join(triggers)}]")
        job_id = ".".join(self.job_run_id.split(".")[:-1])
        for trigger in triggers:
            EventBus.publish(f"{job_id}.{self.action_name}.{trigger}")

    # TODO: cache if safe
    @property
    def rendered_triggers(self) -> List[str]:
        return [self.render_template(trig) for trig in self.triggered_by or []]

    # TODO: subscribe for events and maintain a list of remaining triggers
    @property
    def remaining_triggers(self):
        return [trig for trig in self.rendered_triggers if not EventBus.has_event(trig)]

    def success(self) -> Optional[bool]:
        transition_valid = self._done("success")
        if transition_valid:
            if self.trigger_downstreams:
                self.emit_triggers()

        return transition_valid

    def fail_unknown(self):
        """Failed with unknown reason."""
        log.warning(f"{self} failed with no exit code")
        return self._done("fail_unknown", None)

    def cancel_delay(self):
        if self.in_delay is not None:
            self.in_delay.cancel()
            self.in_delay = None
            self.fail(exitcode.EXIT_STOP_KILL)
            return True

    @property
    def state_data(self):
        """This data is used to serialize the state of this action run."""

        if isinstance(self.action_runner, NoActionRunnerFactory):
            action_runner = None
        else:
            action_runner = dict(
                status_path=self.action_runner.status_path,
                exec_path=self.action_runner.exec_path,
            )

        return {
            "job_run_id": self.job_run_id,
            "action_name": self.action_name,
            "state": self.state,
            "original_command": self.original_command,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "node_name": self.node.get_name() if self.node else None,
            "exit_status": self.exit_status,
            "attempts": [a.state_data for a in self.attempts],
            "retries_remaining": self.retries_remaining,
            "retries_delay": self.retries_delay,
            "action_runner": action_runner,
            "executor": self.executor,
            "trigger_downstreams": self.trigger_downstreams,
            "triggered_by": self.triggered_by,
            "on_upstream_rerun": self.on_upstream_rerun,
            "trigger_timeout_timestamp": self.trigger_timeout_timestamp,
        }

    @staticmethod
    def from_json(state_data: str):
        """Deserialize the ActionRun instance from a JSON Dictionary."""
        try:
            json_data = json.loads(state_data)
            if json_data.get("action_runner") is None:
                action_runner_json = NoActionRunnerFactory.from_json()
            else:
                action_runner_json = SubprocessActionRunnerFactory.from_json(json_data["action_runner"])
            deserialized_data = {
                "job_run_id": json_data["job_run_id"],
                "action_name": json_data["action_name"],
                "state": json_data["state"],
                "original_command": json_data["original_command"],
                "start_time": (
                    datetime.datetime.fromisoformat(json_data["start_time"]) if json_data["start_time"] else None
                ),
                "end_time": datetime.datetime.fromisoformat(json_data["end_time"]) if json_data["end_time"] else None,
                "node_name": json_data["node_name"],
                "exit_status": json_data["exit_status"],
                "attempts": [ActionRunAttempt.from_json(a) for a in json_data["attempts"]],
                "retries_remaining": json_data["retries_remaining"],
                "retries_delay": (
                    datetime.timedelta(seconds=json_data["retries_delay"]) if json_data["retries_delay"] else None
                ),
                "executor": json_data["executor"],
                "trigger_downstreams": json_data["trigger_downstreams"],
                "triggered_by": json_data["triggered_by"],
                "on_upstream_rerun": json_data["on_upstream_rerun"],
                "trigger_timeout_timestamp": json_data["trigger_timeout_timestamp"],
                "action_runner": action_runner_json,
            }
        except Exception:
            log.exception("Error deserializing ActionRun from JSON")
            raise
        return deserialized_data

    @staticmethod
    def to_json(state_data: dict) -> Optional[str]:
        """Serialize the ActionRun instance to a JSON string."""

        action_runner = state_data.get("action_runner")
        if action_runner is None:
            action_runner_json = NoActionRunnerFactory.to_json()
        else:
            action_runner_json = SubprocessActionRunnerFactory.to_json(action_runner)

        try:
            return json.dumps(
                {
                    "job_run_id": state_data["job_run_id"],
                    "action_name": state_data["action_name"],
                    "state": state_data["state"],
                    "original_command": state_data["original_command"],
                    "start_time": state_data["start_time"].isoformat() if state_data["start_time"] else None,
                    "end_time": state_data["end_time"].isoformat() if state_data["end_time"] else None,
                    "node_name": state_data["node_name"],
                    "exit_status": state_data["exit_status"],
                    "attempts": [ActionRunAttempt.to_json(attempt) for attempt in state_data["attempts"]],
                    "retries_remaining": state_data["retries_remaining"],
                    "retries_delay": (
                        state_data["retries_delay"].total_seconds() if state_data["retries_delay"] is not None else None
                    ),
                    "action_runner": action_runner_json,
                    "executor": state_data["executor"],
                    "trigger_downstreams": state_data["trigger_downstreams"],
                    "triggered_by": state_data["triggered_by"],
                    "on_upstream_rerun": state_data["on_upstream_rerun"],
                    "trigger_timeout_timestamp": state_data["trigger_timeout_timestamp"],
                }
            )
        except KeyError:
            log.exception("Missing key in state_data:")
            raise
        except Exception:
            log.exception("Error serializing ActionRun to JSON:")
            raise

    def render_template(self, template):
        """Render our configured command using the command context."""
        return StringFormatter(self.context).format(template)

    def render_command(self, command):
        """Render our configured command using the command context."""
        try:
            return self.render_template(command)
        except Exception as e:
            log.error(f"{self} failed rendering command: {e}")
            # Return a command string that will always fail
            return self.FAILED_RENDER

    def is_valid_command(self, command):
        return command != self.FAILED_RENDER

    @property
    def is_done(self):
        return self.state in self.END_STATES

    @property
    def is_complete(self):
        return self.is_succeeded or self.is_skipped

    @property
    def is_broken(self):
        return self.is_failed or self.is_cancelled or self.is_unknown

    @property
    def is_active(self):
        return self.is_starting or self.is_running

    def cleanup(self):
        self.clear_observers()
        if self.triggered_by:
            EventBus.clear_subscriptions(self.__hash__())
        self.clear_trigger_timeout()
        self.cancel()

    def clear_trigger_timeout(self):
        if self.trigger_timeout_call:
            self.trigger_timeout_call.cancel()
            self.trigger_timeout_call = None

    def setup_subscriptions(self):
        remaining_triggers = self.remaining_triggers
        if not remaining_triggers:
            return

        if self.trigger_timeout_timestamp:
            now = timeutils.current_time().timestamp()
            delay = max(self.trigger_timeout_timestamp - now, 1)
            self.trigger_timeout_call = reactor.callLater(
                delay,
                self.trigger_timeout_reached,
            )
        else:
            log.error(f"{self} has no trigger_timeout_timestamp")

        for trigger in remaining_triggers:
            EventBus.subscribe(trigger, self.__hash__(), self.trigger_notify)

    def trigger_timeout_reached(self):
        if self.remaining_triggers:
            self.trigger_timeout_call = None
            log.warning(
                f"{self} reached timeout waiting for: {self.remaining_triggers}",
            )
            self.fail(exitcode.EXIT_TRIGGER_TIMEOUT)
        else:
            self.notify(ActionRun.NOTIFY_TRIGGER_READY)

    def trigger_notify(self, *_):
        if not self.remaining_triggers:
            self.clear_trigger_timeout()
            self.notify(ActionRun.NOTIFY_TRIGGER_READY)

    @property
    def is_blocked_on_trigger(self):
        return not self.is_done and bool(self.remaining_triggers)

    def clear_end_state(self):
        self.exit_status = None
        self.end_time = None
        last_attempt = self.last_attempt
        if last_attempt:
            last_attempt.exit_status = None
            last_attempt.end_time = None

    def __getattr__(self, name: str):
        """Support convenience properties for checking if this ActionRun is in
        a specific state (Ex: self.is_running would check if self.state is
        STATE_RUNNING) or for transitioning to a new state (ex: ready).
        """
        if name in self.machine.transition_names:
            return lambda: self.transition_and_notify(name)

        if name.startswith("is_"):
            state_name = name.replace("is_", "")
            if state_name not in self.machine.states:
                raise AttributeError(f"{name} is not a state")
            return self.state == state_name
        else:
            raise AttributeError(name)

    def __str__(self):
        return f"ActionRun: {self.id}"

    def transition_and_notify(self, target) -> Optional[bool]:
        if self.machine.transition(target):
            self.notify(self.state)
            return True
        return None


class SSHActionRun(ActionRun, Observer):
    """An ActionRun that executes the command on a node through SSH."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.recover_tries = 0

    def submit_command(self, attempt):
        action_command = self.build_action_command(attempt)
        try:
            self.node.submit_command(action_command)
        except node.Error as e:
            log.warning("Failed to start %s: %r", self.id, e)
            self._exit_unsuccessful(exitcode.EXIT_NODE_ERROR)
            return
        return True

    def stop(self):
        if self.retries_remaining is not None:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        stop_command = self.action_runner.build_stop_action_command(
            self.id,
            "terminate",
        )
        self.node.submit_command(stop_command)

    def kill(self, final=True):
        if self.retries_remaining is not None and final:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        kill_command = self.action_runner.build_stop_action_command(
            self.id,
            "kill",
        )
        self.node.submit_command(kill_command)

    def build_action_command(self, attempt):
        """Create a new ActionCommand instance to send to the node."""
        serializer = filehandler.OutputStreamSerializer(self.output_path)
        self.action_command = self.action_runner.create(
            id=self.id,
            command=attempt.rendered_command,
            serializer=serializer,
        )
        self.watch(self.action_command)
        return self.action_command

    def handle_unknown(self):
        if isinstance(self.action_runner, NoActionRunnerFactory):
            log.info(
                f"Unable to recover action_run {self.id}: " "action_run has no action_runner",
            )
            return self.fail_unknown()

        if self.recover_tries >= MAX_RECOVER_TRIES:
            log.info(f"Reached maximum tries {MAX_RECOVER_TRIES} for recovering {self.id}")
            return self.fail_unknown()

        desired_delay = INITIAL_RECOVER_DELAY * (3**self.recover_tries)
        self.recover_tries += 1
        log.info(f"Starting try #{self.recover_tries} to recover {self.id}, waiting {desired_delay}")
        return self.do_recover(delay=desired_delay)

    def recover(self):
        log.info(f"Creating recovery run for actionrun {self.id}")
        if isinstance(self.action_runner, NoActionRunnerFactory):
            log.info(
                f"Unable to recover action_run {self.id}: " "action_run has no action_runner",
            )
            return None

        if not self.machine.check("running"):
            log.error(
                f"Unable to transition action run {self.id} "
                f"from {self.machine.state} to running. "
                f"Only UNKNOWN actions can be recovered. ",
            )
            return None

        return self.do_recover(delay=0)

    def do_recover(self, delay):
        recovery_command = (
            f"{self.action_runner.exec_path}/recover_batch.py {self.action_runner.status_path}/{self.id}/status"
        )
        command_config = action.ActionCommandConfig(command=recovery_command)
        rendered_command = self.render_command(recovery_command)
        attempt = ActionRunAttempt(
            command_config=command_config,
            rendered_command=rendered_command,
        )

        # Put the "recovery" output at the same directory level as the original action_run's output
        self.output_path.parts = []

        # Might not need a separate action run
        # Using for the separate name
        recovery_run = SSHActionRun(
            job_run_id=self.job_run_id,
            name=f"{self.name}-recovery",
            node=self.node,
            command_config=command_config,
            output_path=self.output_path,
        )
        recovery_action_command = recovery_run.build_action_command(attempt)
        recovery_action_command.write_stdout(
            f"Recovering action run {self.id}",
        )
        # Put action command in "running" state so if it fails to connect
        # and exits with no exit code, the real action run will not retry.
        recovery_action_command.started()

        # this line is where the magic happens.
        # the action run watches another actioncommand,
        # and updates its internal state according to its result.
        self.watch(recovery_action_command)

        self.clear_end_state()
        self.machine.transition("running")

        # Still want the action to appear running while we're waiting to submit the recovery
        # So we do the delay at the end, after the transition to 'running' above
        if not delay:
            return self.submit_recovery_command(recovery_run, recovery_action_command)
        else:
            return reactor.callLater(
                delay,
                self.submit_recovery_command,
                recovery_run,
                recovery_action_command,
            )

    def submit_recovery_command(self, recovery_run, recovery_action_command):
        log.info(
            f"Submitting recovery job with command {recovery_action_command.command} " f"to node {recovery_run.node}",
        )
        try:
            deferred = recovery_run.node.submit_command(recovery_action_command)
            deferred.addCallback(
                lambda x: log.info(f"Completed recovery run {recovery_run.id}"),
            )
            return True
        except node.Error as e:
            log.warning(f"Failed to submit recovery for {self.id}: {e!r}")

    def handle_action_command_state_change(self, action_command, event, event_data=None):
        """Observe ActionCommand state changes."""
        log.debug(
            f"{self} action_command state change: {action_command.state}",
        )

        if event == ActionCommand.RUNNING:
            return self.transition_and_notify("started")

        if event == ActionCommand.FAILSTART:
            return self._exit_unsuccessful(exitcode.EXIT_NODE_ERROR)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                return self.handle_unknown()

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)

    handler = handle_action_command_state_change


class MesosActionRun(ActionRun, Observer):
    """An ActionRun that executes the command on a Mesos cluster."""

    def _create_mesos_task(self, mesos_cluster, serializer, attempt, task_id=None):
        command_config = attempt.command_config
        return mesos_cluster.create_task(
            action_run_id=self.id,
            command=attempt.rendered_command,
            cpus=command_config.cpus,
            mem=command_config.mem,
            disk=1024.0 if command_config.disk is None else command_config.disk,
            constraints=[[c.attribute, c.operator, c.value] for c in command_config.constraints],
            docker_image=command_config.docker_image,
            docker_parameters=[e._asdict() for e in command_config.docker_parameters],
            env=build_environment(original_env=command_config.env, run_id=self.id),
            extra_volumes=[e._asdict() for e in command_config.extra_volumes],
            serializer=serializer,
            task_id=task_id,
        )

    def submit_command(self, attempt):
        serializer = filehandler.OutputStreamSerializer(self.output_path)
        mesos_cluster = MesosClusterRepository.get_cluster()
        task = self._create_mesos_task(mesos_cluster, serializer, attempt)
        if not task:  # Mesos is disabled
            self.fail(exitcode.EXIT_MESOS_DISABLED)
            return

        attempt.mesos_task_id = task.get_mesos_id()

        # Watch before submitting, in case submit causes a transition
        self.watch(task)
        mesos_cluster.submit(task)
        return task

    def recover(self):
        if not self.machine.check("running"):
            log.error(
                f"{self} unable to transition from {self.machine.state}" "to running for recovery",
            )
            return

        if not self.attempts or self.attempts[-1].mesos_task_id is None:
            log.error(f"{self} no task ID, cannot recover")
            self.fail_unknown()
            return

        last_attempt = self.attempts[-1]

        log.info(f"{self} recovering Mesos run")

        serializer = filehandler.OutputStreamSerializer(self.output_path)
        mesos_cluster = MesosClusterRepository.get_cluster()
        task = self._create_mesos_task(
            mesos_cluster,
            serializer,
            last_attempt,
            last_attempt.mesos_task_id,
        )
        if not task:
            log.warning(
                f"{self} cannot recover, Mesos is disabled or " f"invalid task ID {last_attempt.mesos_task_id!r}",
            )
            self.fail_unknown()
            return

        self.watch(task)
        mesos_cluster.recover(task)

        # Reset status
        self.clear_end_state()
        self.transition_and_notify("running")

        return task

    def stop(self):
        if self.retries_remaining is not None:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        return self._kill_mesos_task()

    def kill(self, final=True):
        if self.retries_remaining is not None and final:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        return self._kill_mesos_task()

    def _kill_mesos_task(self):
        msgs = []
        if not self.is_active:
            msgs.append(
                f"Action is {self.state}, not running. Continuing anyway.",
            )

        mesos_cluster = MesosClusterRepository.get_cluster()
        last_attempt = self.last_attempt
        if last_attempt is None or last_attempt.mesos_task_id is None:
            msgs.append("Error: Can't find task id for the action.")
        else:
            msgs.append(f"Sending kill for {last_attempt.mesos_task_id}...")
            succeeded = mesos_cluster.kill(last_attempt.mesos_task_id)
            if succeeded:
                msgs.append(
                    "Sent! It can take up to docker_stop_timeout (current setting is 2 mins) to stop.",
                )
            else:
                msgs.append(
                    "Error while sending kill request. Please try again.",
                )

        return "\n".join(msgs)

    def handle_action_command_state_change(self, action_command, event, event_data=None):
        """Observe ActionCommand state changes."""
        log.debug(
            f"{self} action_command state change: {action_command.state}",
        )

        if event == ActionCommand.RUNNING:
            return self.transition_and_notify("started")

        if event == ActionCommand.FAILSTART:
            return self._exit_unsuccessful(action_command.exit_status)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                # This is different from SSHActionRun
                # Allows retries to happen, if configured
                return self._exit_unsuccessful(None)

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)

    handler = handle_action_command_state_change


class KubernetesActionRun(ActionRun, Observer):
    """An ActionRun that executes the command on a Kubernetes cluster."""

    def submit_command(self, attempt: ActionRunAttempt) -> Optional[KubernetesTask]:
        """
        Attempt to run a given ActionRunAttempt on the configured Kubernetes cluster.

        If k8s usage is not toggled off, a KubernetesTask representing what was scheduled
        onto the cluster will be returned - otherwise, None.
        """
        k8s_cluster = KubernetesClusterRepository.get_cluster()
        if not k8s_cluster:
            self.fail(exitcode.EXIT_KUBERNETES_NOT_CONFIGURED)
            return None

        if attempt.rendered_command is None:
            self.fail(exitcode.EXIT_INVALID_COMMAND)
            return None

        if attempt.command_config.docker_image is None:
            self.fail(exitcode.EXIT_KUBERNETES_TASK_INVALID)
            return None
        try:
            task = k8s_cluster.create_task(
                action_run_id=self.id,
                command=attempt.rendered_command,
                cpus=attempt.command_config.cpus,
                mem=attempt.command_config.mem,
                disk=attempt.command_config.disk,
                docker_image=attempt.command_config.docker_image,
                env=build_environment(original_env=attempt.command_config.env, run_id=self.id),
                secret_env=attempt.command_config.secret_env,
                secret_volumes=attempt.command_config.secret_volumes,
                projected_sa_volumes=attempt.command_config.projected_sa_volumes,
                field_selector_env=attempt.command_config.field_selector_env,
                serializer=filehandler.OutputStreamSerializer(self.output_path),
                volumes=attempt.command_config.extra_volumes,
                cap_add=attempt.command_config.cap_add,
                cap_drop=attempt.command_config.cap_drop,
                node_selectors=attempt.command_config.node_selectors,
                node_affinities=attempt.command_config.node_affinities,
                topology_spread_constraints=attempt.command_config.topology_spread_constraints,
                pod_labels=build_labels(run_id=self.id, original_labels=attempt.command_config.labels),
                pod_annotations=attempt.command_config.annotations,
                service_account_name=attempt.command_config.service_account_name,
                ports=attempt.command_config.ports,
            )
        except Exception:
            log.exception(f"Unable to create task for ActionRun {self.id}")
            self.fail(exitcode.EXIT_KUBERNETES_TASK_INVALID)
            return None

        if not task:
            # generally, if we didn't get a task back that means that k8s usage is disabled
            self.fail(exitcode.EXIT_KUBERNETES_DISABLED)
            return None

        attempt.kubernetes_task_id = task.get_kubernetes_id()

        # Watch before submitting, in case submit causes a transition
        self.watch(task)

        try:
            k8s_cluster.submit(task)
        except Exception:
            log.exception(f"Unable to submit task for ActionRun {self.id}")
            self.fail(exitcode.EXIT_KUBERNETES_TASK_INVALID)
            return None

        return task

    def recover(self) -> Optional[KubernetesTask]:
        """
        Called on Tron restart per previously running ActionRun to attempt to restart Tron's tracking
        of this run. See tron.core.recovery

        If we're able to successfully recover, a KubernetesTask representing what is currently being run
        will be returned - otherwise, None.
        """
        k8s_cluster = KubernetesClusterRepository.get_cluster()
        if not k8s_cluster:
            self.fail(exitcode.EXIT_KUBERNETES_NOT_CONFIGURED)
            return None

        # We cannot recover if we can't transition to running
        if not self.machine.check("running"):
            log.error(f"{self} unable to transition from {self.machine.state} to running for recovery")
            return None

        if not self.attempts or self.attempts[-1].kubernetes_task_id is None:
            log.error(f"{self} no task ID, cannot recover")
            self.fail_unknown()
            return None
        last_attempt = self.attempts[-1]

        if last_attempt.rendered_command is None:
            log.error(f"{self} rendered_command is None, cannot recover")
            self.fail(exitcode.EXIT_INVALID_COMMAND)
            return None

        if last_attempt.command_config.docker_image is None:
            log.error(f"{self} docker_image is None, cannot recover")
            self.fail(exitcode.EXIT_KUBERNETES_TASK_INVALID)
            return None

        log.info(f"{self} recovering Kubernetes run")
        # try/except block here is necessary cause if this fails, jobs will get resetted to 0 and we dont want that to happen
        try:
            task = k8s_cluster.create_task(
                action_run_id=self.id,
                command=last_attempt.rendered_command,
                cpus=last_attempt.command_config.cpus,
                mem=last_attempt.command_config.mem,
                disk=last_attempt.command_config.disk,
                docker_image=last_attempt.command_config.docker_image,
                env=build_environment(original_env=last_attempt.command_config.env, run_id=self.id),
                secret_env=last_attempt.command_config.secret_env,
                # the field_selector_env = {'PAASTA_POD_IP': ['status.podIP']} is in a diff format than
                # the field_selector_env in submit_command function.
                field_selector_env=last_attempt.command_config.field_selector_env,
                serializer=filehandler.OutputStreamSerializer(self.output_path),
                secret_volumes=last_attempt.command_config.secret_volumes,
                projected_sa_volumes=last_attempt.command_config.projected_sa_volumes,
                volumes=last_attempt.command_config.extra_volumes,
                cap_add=last_attempt.command_config.cap_add,
                cap_drop=last_attempt.command_config.cap_drop,
                task_id=last_attempt.kubernetes_task_id,
                node_selectors=last_attempt.command_config.node_selectors,
                node_affinities=last_attempt.command_config.node_affinities,
                topology_spread_constraints=last_attempt.command_config.topology_spread_constraints,
                pod_labels=build_labels(run_id=self.id, original_labels=last_attempt.command_config.labels),
                pod_annotations=last_attempt.command_config.annotations,
                service_account_name=last_attempt.command_config.service_account_name,
                ports=last_attempt.command_config.ports,
            )
        except Exception:
            log.exception(f"Unable to create task for ActionRun {self.id}")
            raise
        if not task:
            log.warning(
                f"{self} cannot recover, Kubernetes is disabled or "
                f"invalid task ID {last_attempt.kubernetes_task_id!r}",
            )
            self.fail_unknown()
            return None

        self.watch(task)
        k8s_cluster.recover(task)

        # Reset status
        self.clear_end_state()
        self.transition_and_notify("running")

        return task

    def stop(self) -> Optional[str]:
        """
        Compatibility alias for KubernetesActionRun::kill().

        Kills the Kubernetes Pod for this ActionRun and consumes a retry.
        May return an error/diagnostic message suitible for displaying to users.
        """
        return self.kill()

    def kill(self, final: bool = True) -> Optional[str]:
        """
        Kills the Kubernetes Pod for this ActionRun and consumes a retry.

        May return an error/diagnostic message suitible for displaying to users.
        """
        if self.retries_remaining is not None and final:
            self.retries_remaining = -1

        # it's possible that a user wants to kill an action that has delayed it's start
        # (e.g., they're killing a retry of a failed action that has a retry_delay set),
        # so let's check if there's such a delay present and cancel that since in this case
        # there's nothing actually running in k8s yet
        if self.cancel_delay():
            return None

        msgs = []
        if not self.is_active:
            msgs.append(f"Action is {self.state}, not running. Continuing anyway.")

        k8s_cluster = KubernetesClusterRepository.get_cluster()
        if not k8s_cluster:
            return f"Unable to kill action {self.action_name} - could not get Kubernetes cluster."
        last_attempt = self.last_attempt
        if last_attempt is None or last_attempt.kubernetes_task_id is None:
            msgs.append("Error: Can't find task id for the action.")
        else:
            msgs.append(f"Sending kill for {last_attempt.kubernetes_task_id}...")
            succeeded = k8s_cluster.kill(last_attempt.kubernetes_task_id)
            if succeeded:
                msgs.append("Sent! Note: the Docker container may not stop immediately.")
            else:
                msgs.append("Error while sending kill request. Please try again.")

        return "\n".join(msgs)

    def _exit_unsuccessful(
        self, exit_status=None, retry_original_command=True, non_retryable_exit_codes=[]
    ) -> Optional[Union[bool, ActionCommand]]:

        k8s_cluster = KubernetesClusterRepository.get_cluster()
        non_retryable_exit_codes = [] if not k8s_cluster else k8s_cluster.non_retryable_exit_codes

        return super()._exit_unsuccessful(
            exit_status=exit_status,
            retry_original_command=retry_original_command,
            non_retryable_exit_codes=non_retryable_exit_codes,
        )

    def handle_action_command_state_change(
        self, action_command: ActionCommand, event: str, event_data=None
    ) -> Optional[Union[bool, ActionCommand]]:
        """
        Observe ActionCommand state changes and transition the ActionCommand state machine to a new state.
        """
        log.debug(f"{self} action_command state change: {action_command.state} for event: {event}.")

        if event == ActionCommand.RUNNING:
            return self.transition_and_notify("started")

        if event == ActionCommand.FAILSTART:
            return self._exit_unsuccessful(action_command.exit_status)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                # This is different from SSHActionRun - allows retries to happen, if configured
                return self._exit_unsuccessful(None)

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)
        return None

    handler = handle_action_command_state_change


def min_filter(seq):
    seq = list(filter(None, seq))
    return min(seq) if any(seq) else None


def eager_all(seq):
    return all(list(seq))


class ActionRunCollection:
    """A collection of ActionRuns used by a JobRun."""

    def __init__(self, action_graph, run_map):
        self.action_graph = action_graph
        self.run_map: Dict[str, ActionRun] = run_map
        # Setup proxies
        self.proxy_action_runs_with_cleanup = proxy.CollectionProxy(
            self.get_action_runs_with_cleanup,
            [
                proxy.attr_proxy("is_running", any),
                proxy.attr_proxy("is_starting", any),
                proxy.attr_proxy("is_scheduled", any),
                proxy.attr_proxy("is_cancelled", any),
                proxy.attr_proxy("is_active", any),
                proxy.attr_proxy("is_waiting", any),
                proxy.attr_proxy("is_queued", all),
                proxy.attr_proxy("is_complete", all),
                proxy.func_proxy("queue", eager_all),
                proxy.func_proxy("cancel", eager_all),
                proxy.func_proxy("success", eager_all),
                proxy.func_proxy("fail", eager_all),
                proxy.func_proxy("ready", eager_all),
                proxy.func_proxy("cleanup", eager_all),
                proxy.func_proxy("stop", eager_all),
                proxy.attr_proxy("start_time", min_filter),
                proxy.attr_proxy("state_data", eager_all),
            ],
        )

    def action_runs_for_actions(self, actions):
        return (self.run_map[a.name] for a in actions if a.name in self.run_map)

    def get_action_runs_with_cleanup(self):
        return self.run_map.values()

    action_runs_with_cleanup = property(get_action_runs_with_cleanup)

    def get_action_runs(self):
        return (run for run in self.run_map.values() if not run.is_cleanup)

    action_runs = property(get_action_runs)

    def update_action_config(self, action_graph):
        # If there are new command configs that match the action name, update them
        # Do not update the actual action_graph
        updated = False
        for action_run in self.get_action_runs_with_cleanup():
            new_action = action_graph.action_map.get(action_run.action_name)
            if new_action and new_action.command_config != action_run.command_config:
                action_run.command_config = new_action.command_config
                updated = True
        return updated

    @property
    def cleanup_action_run(self) -> Optional[ActionRun]:
        return self.run_map.get(action.CLEANUP_ACTION_NAME)

    @property
    def state_data(self):
        return [run.state_data for run in self.action_runs]

    @property
    def cleanup_action_state_data(self):
        if self.cleanup_action_run:
            return self.cleanup_action_run.state_data

    def get_startable_action_runs(self):
        """Returns any actions that are scheduled or queued that can be run."""

        return [r for r in self.action_runs if r.machine.check("start") and not self._is_run_blocked(r)]

    @property
    def has_startable_action_runs(self):
        return any(self.get_startable_action_runs())

    def _is_run_blocked(self, action_run, in_job_only=False):
        """Returns True if the ActionRun is waiting on a required run to
        finish before it can run.

        If in_job_only is True, only considers required actions in this job,
        not triggers.
        """
        if action_run.is_done or action_run.is_active:
            return False

        required_actions = self.action_graph.get_dependencies(
            action_run.action_name,
        )

        if required_actions:
            required_runs = self.action_runs_for_actions(required_actions)
            if any(not run.is_complete for run in required_runs):
                return True

        if action_run.is_blocked_on_trigger and not in_job_only:
            return True

        return False

    @property
    def is_blocked_on_trigger(self):
        return any(r.is_blocked_on_trigger for r in self.action_runs)

    @property
    def is_done(self):
        """Returns True when there are no running ActionRuns and all
        non-blocked ActionRuns are done.
        """
        if self.is_running:
            return False

        def done_or_blocked(action_run):
            # Can't make progress if blocked by actions in the job, and other actions are done.
            # On the other hand, not necessarily done if still waiting for cross-job dependencies.
            return action_run.is_done or self._is_run_blocked(action_run, in_job_only=True)

        return all(done_or_blocked(run) for run in self.action_runs)

    @property
    def is_failed(self):
        """Return True if there are failed actions and all ActionRuns are
        done or blocked.
        """
        return self.is_done and any(run.is_failed for run in self.action_runs)

    @property
    def is_complete_without_cleanup(self):
        return all(run.is_complete for run in self.action_runs)

    @property
    def names(self):
        return self.run_map.keys()

    @property
    def end_time(self):
        if not self.is_done:
            return None
        end_times = list(run.end_time for run in self.get_action_runs_with_cleanup() if run.end_time)
        return max(end_times) if any(end_times) else None

    def __str__(self):
        def blocked_state(action_run):
            return ":blocked" if self._is_run_blocked(action_run) else ""

        run_states = ", ".join(f"{a.action_name}({a.state}{blocked_state(a)})" for a in self.run_map.values())
        return f"{self.__class__.__name__}[{run_states}]"

    def __getattr__(self, name):
        return self.proxy_action_runs_with_cleanup.perform(name)

    def __getitem__(self, name):
        return self.run_map[name]

    def __contains__(self, name):
        return name in self.run_map

    def __iter__(self):
        return iter(self.run_map.values())

    def get(self, name):
        return self.run_map.get(name)
