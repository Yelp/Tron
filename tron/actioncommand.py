from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from io import StringIO

from six.moves import shlex_quote

from tron.config import schema
from tron.serialize import filehandler
from tron.utils import timeutils
from tron.utils.observer import Observable
from tron.utils.state import Machine

log = logging.getLogger(__name__)


class ActionCommand(Observable):
    """An ActionCommand encapsulates a runnable task that is passed to a node
    for execution.

    A Node calls:
      started   (when the command starts)
      exited    (when the command exits)
      write_<channel> (when output is received)
      done      (when the command is finished)
    """

    PENDING = 'pending'
    RUNNING = 'running'
    EXITING = 'exiting'
    COMPLETE = 'complete'
    FAILSTART = 'failstart'

    STATE_MACHINE = Machine(
        PENDING, **{
            PENDING: {
                'start': RUNNING,
                'exit': FAILSTART
            },
            RUNNING: {
                'exit': EXITING
            },
            EXITING: {
                'close': COMPLETE
            },
        }
    )

    STDOUT = '.stdout'
    STDERR = '.stderr'

    def __init__(self, id, command, serializer=None):
        super().__init__()
        self.id = id
        self.command = command
        self.machine = Machine.from_machine(ActionCommand.STATE_MACHINE)
        self.exit_status = None
        self.start_time = None
        self.end_time = None
        if serializer:
            self.stdout = serializer.open(self.STDOUT)
            self.stderr = serializer.open(self.STDERR)
        else:
            self.stdout = filehandler.NullFileHandle
            self.stderr = filehandler.NullFileHandle

    @property
    def state(self):
        return self.machine.state

    def transition_and_notify(self, target):
        if self.machine.transition(target):
            self.notify(self.state)
            return True

    def started(self):
        if self.machine.check('start'):
            self.start_time = timeutils.current_timestamp()
            return self.transition_and_notify('start')

    def exited(self, exit_status):
        if self.machine.check('exit'):
            self.end_time = timeutils.current_timestamp()
            self.exit_status = exit_status
            return self.transition_and_notify('exit')

    def write_stderr(self, value):
        self.stderr.write(value)

    def write_stdout(self, value):
        self.stdout.write(value)

    def done(self):
        if self.machine.check('close'):
            self.stdout.close()
            self.stderr.close()
            return self.transition_and_notify('close')

    def handle_errback(self, result):
        """Handle an unexpected error while being run. This will likely be
        an interval error. Cleanup the state of this ActionCommand and log
        something useful for debugging.
        """
        log.error(f"Unknown failure for {self}, {str(result)}")
        self.exited(result)
        self.done()

    @property
    def is_unknown(self):
        return self.exit_status is None

    @property
    def is_failed(self):
        return bool(self.exit_status)

    @property
    def is_complete(self):
        """Complete implies done and success."""
        return self.machine.state == ActionCommand.COMPLETE

    @property
    def is_done(self):
        """Done implies no more work will be done, but might not be success."""
        return self.machine.state in (
            ActionCommand.COMPLETE, ActionCommand.FAILSTART
        )

    def __repr__(self):
        return f"ActionCommand {self.id} {self.command}: {self.state}"


class StringBufferStore(object):
    """A serializer object which can be passed to ActionCommand as a
    serializer, but stores streams in memory.
    """

    def __init__(self):
        self.buffers = {}

    def open(self, name):
        return self.buffers.setdefault(name, StringIO())

    def clear(self):
        self.buffers.clear()


class NoActionRunnerFactory(object):
    """Action runner factory that does not wrap the action run command."""

    @classmethod
    def create(cls, id, command, serializer):
        return ActionCommand(id, command, serializer)

    @classmethod
    def build_stop_action_command(cls, _id, _command):
        """It is not possible to stop action commands without a runner."""
        raise NotImplementedError("An action_runner is required to stop.")


class SubprocessActionRunnerFactory(object):
    """Run actions by wrapping them in `action_runner.py`."""

    runner_exec_name = "action_runner.py"
    status_exec_name = "action_status.py"

    def __init__(self, status_path, exec_path):
        self.status_path = status_path
        self.exec_path = exec_path

    @classmethod
    def from_config(cls, config):
        return cls(config.remote_status_path, config.remote_exec_path)

    def create(self, id, command, serializer):
        command = self.build_command(id, command, self.runner_exec_name)
        return ActionCommand(id, command, serializer)

    def build_command(self, id, command, exec_name):
        status_path = os.path.join(self.status_path, id)
        runner_path = os.path.join(self.exec_path, exec_name)
        return "%s %s %s %s" % (
            shlex_quote(runner_path),
            shlex_quote(status_path),
            shlex_quote(command),
            shlex_quote(id),
        )

    def build_stop_action_command(self, id, command):
        command = self.build_command(id, command, self.status_exec_name)
        run_id = '%s.%s' % (id, command)
        return ActionCommand(run_id, command, StringBufferStore())

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__ and
            self.status_path == other.status_path and
            self.exec_path == other.exec_path
        )

    def __ne__(self, other):
        return not self == other


def create_action_runner_factory_from_config(config):
    """A factory-factory method which returns a callable that can be used to
    create ActionCommand objects. The factory definition should match the
    constructor for ActionCommand.
    """
    if not config or config.runner_type == schema.ActionRunnerTypes.none:
        return NoActionRunnerFactory()
    elif config.runner_type == schema.ActionRunnerTypes.subprocess:
        return SubprocessActionRunnerFactory.from_config(config)
    else:
        raise ValueError("Unknown runner type: %s", config.runner_type)
