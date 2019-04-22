"""
Web Controllers for the API.
"""
import logging

from tron import yaml
from tron.eventbus import EventBus

log = logging.getLogger(__name__)


class UnknownCommandError(Exception):
    """Exception raised when a controller received an unknown command."""


class JobCollectionController(object):
    def __init__(self, job_collection):
        self.job_collection = job_collection

    def handle_command(self, command, old_name=None, new_name=None):
        if command == 'move':
            if old_name not in self.job_collection.get_names():
                return f"Error: {old_name} doesn't exist"
            if new_name in self.job_collection.get_names():
                return f"Error: {new_name} exists already"
            return self.job_collection.move(old_name, new_name)

        raise UnknownCommandError("Unknown command %s" % command)


class ActionRunController(object):

    mapped_commands = {
        'start',
        'success',
        'cancel',
        'fail',
        'skip',
        'stop',
        'kill',
        'retry',
    }

    def __init__(self, action_run, job_run):
        self.action_run = action_run
        self.job_run = job_run

    def handle_command(self, command):
        if command not in self.mapped_commands:
            raise UnknownCommandError("Unknown command %s" % command)

        if command == 'start' and self.job_run.is_scheduled:
            return (
                "Action run can not be started if it's job run is still "
                "scheduled."
            )

        if command in ('stop', 'kill'):
            return self.handle_termination(command)

        if command == 'retry':
            return self.handle_retry()

        if getattr(self.action_run, command)():
            msg = "%s now in state %s"
            return msg % (self.action_run, self.action_run.state)

        msg = "Failed to %s on %s. State is %s."
        return msg % (command, self.action_run, self.action_run.state)

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

    def handle_retry(self):
        cleanup_run = self.job_run.action_runs.cleanup_action_run
        if cleanup_run and cleanup_run.is_done:
            return "JobRun has run a cleanup action, use rerun instead"

        if self.action_run.retry():
            return "Retrying %s" % self.action_run
        else:
            return "Failed to schedule retry for %s" % self.action_run


class JobRunController(object):

    mapped_commands = {'start', 'success', 'cancel', 'fail', 'stop'}

    def __init__(self, job_run, job_scheduler):
        self.job_run = job_run
        self.job_scheduler = job_scheduler

    def handle_command(self, command):
        if command == 'restart' or command == 'rerun':
            runs = self.job_scheduler.manual_start(self.job_run.run_time)
            return "Created %s" % ",".join(str(run) for run in runs)

        if command in self.mapped_commands:
            if getattr(self.job_run, command)():
                return "%s now in state %s" % (
                    self.job_run,
                    self.job_run.state,
                )

            msg = "Failed to %s, %s in state %s"
            return msg % (command, self.job_run, self.job_run.state)

        raise UnknownCommandError("Unknown command %s" % command)


class JobController(object):
    def __init__(self, job_scheduler):
        self.job_scheduler = job_scheduler

    def handle_command(self, command, run_time=None):
        if command == 'enable':
            self.job_scheduler.enable()
            return "%s is enabled" % self.job_scheduler.get_job()

        elif command == 'disable':
            self.job_scheduler.disable()
            return "%s is disabled" % self.job_scheduler.get_job()

        elif command == 'start':
            runs = self.job_scheduler.manual_start(run_time=run_time)
            return "Created %s" % ",".join(str(run) for run in runs)

        raise UnknownCommandError("Unknown command %s" % command)


class ConfigController(object):
    """Control config. Return config contents and accept updated configuration
    from the API.
    """

    DEFAULT_NAMED_CONFIG = "\njobs:\n"

    def __init__(self, mcp):
        self.mcp = mcp
        self.config_manager = mcp.get_config_manager()

    def _get_config_content(self, name):
        if name not in self.config_manager:
            return self.DEFAULT_NAMED_CONFIG
        return self.config_manager.read_raw_config(name)

    def read_config(self, name):
        config_content = self._get_config_content(name)
        config_hash = self.config_manager.get_hash(name)
        return dict(config=config_content, hash=config_hash)

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

        old_config = self.read_config(name)['config']
        try:
            self.config_manager.write_config(name, content)
            self.mcp.reconfigure()
        except Exception as e:
            log.error("Configuration update failed: %s" % e)
            log.error("Reconfiguring with the previous good configuration")
            try:
                self.config_manager.write_config(name, old_config)
                self.mcp.reconfigure()
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
            self.config_manager.delete_config(name)
            self.mcp.reconfigure()
        except Exception as e:
            log.error("Deleting configuration for %s failed: %s" % (name, e))
            return str(e)

    def get_namespaces(self):
        return self.config_manager.get_namespaces()


class EventsController:
    COMMANDS = {'publish', 'discard'}

    def publish(self, event):
        if not EventBus.instance:
            return dict(error='EventBus disabled')

        if EventBus.has_event(event):
            msg = f"event {event} already published"
            log.warning(msg)
            return dict(response=msg)

        if not EventBus.publish(event):
            msg = f'could not publish {event}'
            log.error(msg)
            return dict(error=msg)

        return dict(response='OK')

    def discard(self, event):
        if not EventBus.instance:
            return dict(error='EventBus disabled')

        if not EventBus.discard(event):
            msg = f"could not discard {event}"
            log.error(msg)
            return dict(error=msg)

        return dict(response='OK')

    def info(self):
        if not EventBus.instance:
            return dict(error='EventBus disabled')

        return dict(response=EventBus.instance.event_log)
