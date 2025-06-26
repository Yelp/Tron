"""
Web Controllers for the API.
"""
import logging
from typing import Dict
from typing import TYPE_CHECKING
from typing import TypedDict

from tron import yaml
from tron.config.manager import ConfigManager
from tron.eventbus import EventBus

if TYPE_CHECKING:
    from tron.mcp import MasterControlProgram

log = logging.getLogger(__name__)


class UnknownCommandError(Exception):
    """Exception raised when a controller received an unknown command."""


class InvalidCommandForActionState(Exception):
    """
    Exception raised when a controller attempts a command on an action in a state
    that does not support that command (e.g., skipping a successful run).
    """

    def __init__(self, command: str, action_name: str, action_state: str) -> None:
        self.command = command
        self.action_name = action_name
        self.action_state = action_state
        self.message = f"Failed to {command} on {action_name}. State is {action_state}."
        super().__init__()


class JobCollectionController:
    def __init__(self, job_collection):
        self.job_collection = job_collection

    def handle_command(self, command, old_name=None, new_name=None):
        if command == "move":
            if old_name not in self.job_collection.get_names():
                return f"Error: {old_name} doesn't exist"
            if new_name in self.job_collection.get_names():
                return f"Error: {new_name} exists already"
            return self.job_collection.move(old_name, new_name)

        raise UnknownCommandError(f"Unknown command {command}. Try running this on an individual job or action run id")


class ActionRunController:

    mapped_commands = {
        "start",
        "success",
        "cancel",
        "fail",
        "skip",
        "stop",
        "kill",
        "retry",
        "recover",
    }

    def __init__(self, action_run, job_run):
        self.action_run = action_run
        self.job_run = job_run

    def handle_command(self, command, **kwargs):
        if command not in self.mapped_commands:
            raise UnknownCommandError(
                f"Unknown command {command}. You can only do one of the following to Action runs: {self.mapped_commands}"
            )

        if command == "start" and self.job_run.is_scheduled:
            return "Action run cannot be started if its job run is still " "scheduled."

        if command == "recover" and not self.action_run.is_unknown:
            return "Action run cannot be recovered if its state is not unknown."

        if command in ("stop", "kill"):
            return self.handle_termination(command)

        if command == "retry":
            original_command = not kwargs.get("use_latest_command", False)
            return self.handle_retry(original_command)

        if getattr(self.action_run, command)():
            msg = "%s now in state %s"
            return msg % (self.action_run, self.action_run.state)

        raise InvalidCommandForActionState(
            command=command,
            action_name=self.action_run.name,
            action_state=self.action_run.state,
        )

    def handle_termination(self, command):
        try:
            # Extra message is only used for killing mesos action as warning so far.
            extra_msg = getattr(self.action_run, command)()
            msg = "Attempting to %s %s"
            if extra_msg is not None:
                msg = msg + "\n" + extra_msg
            return msg % (command, self.action_run)
        except NotImplementedError as e:
            msg = "Failed to %s: %s"
            return msg % (command, e)

    def handle_retry(self, original_command):
        cleanup_run = self.job_run.action_runs.cleanup_action_run
        if cleanup_run and cleanup_run.is_done:
            return "JobRun has run a cleanup action, use rerun instead"

        if self.action_run.retry(original_command=original_command):
            return "Retrying %s" % self.action_run
        else:
            return "Failed to schedule retry for %s" % self.action_run


class JobRunController:

    mapped_commands = {"start", "success", "cancel", "fail", "stop"}

    def __init__(self, job_run, job_scheduler):
        self.job_run = job_run
        self.job_scheduler = job_scheduler

    def handle_command(self, command):
        # as of TRON-1340, `tronctl backfill` depends on this response value
        # (i.e. "Created JobRun:<job_run_name>"). be careful when changing this!
        if command == "restart" or command == "rerun":
            runs = self.job_scheduler.manual_start(self.job_run.run_time)
            return "Created %s" % ",".join(str(run) for run in runs)

        if command in self.mapped_commands:
            if getattr(self.job_run, command)():
                return f"{self.job_run} now in state {self.job_run.state}"

            msg = "Failed to %s, %s in state %s"
            return msg % (command, self.job_run, self.job_run.state)

        if command == "retry":
            raise UnknownCommandError(
                "Error: Job runs cannot be retried, only individual actions can. Did you mean 'rerun'?"
            )
        else:
            raise UnknownCommandError(
                f"Unknown command {command}. Only one of the following applies to a Job run: {self.mapped_commands}"
            )


class JobController:
    def __init__(self, job_scheduler):
        self.job_scheduler = job_scheduler

    def handle_command(self, command, run_time=None):
        if command == "enable":
            self.job_scheduler.enable()
            return "%s is enabled" % self.job_scheduler.get_job()

        elif command == "disable":
            self.job_scheduler.disable()
            return "%s is disabled" % self.job_scheduler.get_job()

        elif command == "start":
            runs = self.job_scheduler.manual_start(run_time=run_time)
            return "Created %s" % ",".join(str(run) for run in runs)

        if command == "retry":
            raise UnknownCommandError(
                "Error: A whole Job cannot be retried, only individual actions for a specific job run id can."
            )
        elif command in ["stop", "success", "cancel", "fail", "stop"]:
            raise UnknownCommandError(
                f"Error: {command} doesn't apply to a whole Job. Please run this on an individual job run id. Hint: try '{self.job_scheduler.get_job()}.-1' for the latest job id"
            )
        else:
            raise UnknownCommandError(
                f"Unknown command {command}. Does it apply to a whole job? Try a specific Job id or individual action"
            )


class ConfigResponse(TypedDict):
    config: str
    hash: str


class ConfigController:
    """Control config. Return config contents and accept updated configuration
    from the API.
    """

    DEFAULT_NAMED_CONFIG = "\njobs:\n"

    def __init__(self, mcp: "MasterControlProgram") -> None:
        self.mcp = mcp
        self.config_manager: ConfigManager = mcp.get_config_manager()

    def _get_config_content(self, name) -> str:
        if name not in self.config_manager:
            return self.DEFAULT_NAMED_CONFIG
        return self.config_manager.read_raw_config(name)

    def read_config(self, name) -> ConfigResponse:
        config_content = self._get_config_content(name)
        config_hash = self.config_manager.get_hash(name)
        return {"config": config_content, "hash": config_hash}

    def read_all_configs(self) -> Dict[str, ConfigResponse]:
        configs = {}

        for service in self.config_manager.get_namespaces():
            config: ConfigResponse = {
                "config": self._get_config_content(service),
                "hash": self.config_manager.get_hash(service),
            }
            configs[service] = config

        return configs

    def check_config(self, name, content, config_hash):
        """Update a configuration fragment and reload the MCP."""
        if self.config_manager.get_hash(name) != config_hash:
            return "Configuration update will fail: config is stale, try again"

        try:
            content = yaml.load(content)
            self.config_manager.validate_with_fragment(name, content)
        except Exception as e:
            return "Configuration update will fail: %s" % str(e)

    def update_config(self, name, content, config_hash):
        """Update a configuration fragment and reload the MCP."""
        if self.config_manager.get_hash(name) != config_hash:
            return "Configuration has changed. Please try again."

        old_config = self.read_config(name)["config"]
        try:
            log.info(f"Reconfiguring namespace {name}")
            self.config_manager.write_config(name, content)
            self.mcp.reconfigure(namespace=name)
        except Exception as e:
            log.error(f"Configuration for {name} update failed: {e}")
            log.error("Reconfiguring with the previous good configuration")
            try:
                self.config_manager.write_config(name, old_config)
                self.mcp.reconfigure(namespace=name)
            except Exception as e:
                log.error("Could not restore old config: %s" % e)
                return str(e)
            return str(e)

    def delete_config(self, name, content, config_hash):
        """Delete a configuration fragment and reload the MCP."""
        if self.config_manager.get_hash(name) != config_hash:
            return "Configuration has changed. Please try again."

        if content != "":
            return "Configuration content is not empty, will not delete."

        try:
            log.info(f"Deleting namespace {name}")
            self.config_manager.delete_config(name)
            self.mcp.reconfigure(namespace=name)
        except Exception as e:
            log.error(f"Deleting configuration for {name} failed: {e}")
            return str(e)

    def get_namespaces(self):
        return self.config_manager.get_namespaces()


class EventsController:
    COMMANDS = {"publish", "discard"}

    def publish(self, event):
        if not EventBus.instance:
            return dict(error="EventBus disabled")

        if EventBus.has_event(event):
            msg = f"event {event} already published"
            log.warning(msg)
            return dict(response=msg)

        if not EventBus.publish(event):
            msg = f"could not publish {event}"
            log.error(msg)
            return dict(error=msg)

        return dict(response="OK")

    def discard(self, event):
        if not EventBus.instance:
            return dict(error="EventBus disabled")

        if not EventBus.discard(event):
            msg = f"could not discard {event}"
            log.error(msg)
            return dict(error=msg)

        return dict(response="OK")

    def info(self):
        if not EventBus.instance:
            return dict(error="EventBus disabled")

        return dict(response=EventBus.instance.event_log)
