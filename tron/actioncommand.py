from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

from six.moves import shlex_quote

from tron.config import schema
from tron.serialize import filehandler
from tron.utils import state
from tron.utils import timeutils

log = logging.getLogger(__name__)


class ActionState(state.NamedEventState):
    pass


class CompletedActionCommand(object):
    """This is a null object for ActionCommand."""
    is_complete = True
    is_done = True
    is_failed = False
    is_unknown = False

    @staticmethod
    def write_stderr(_):
        pass


class ActionCommand(object):
    """An ActionCommand encapsulates a runnable task that is passed to a node
    for execution.

    A Node calls:
      started   (when the command starts)
      exited    (when the command exits)
      write_<channel> (when output is received)
      done      (when the command is finished)
    """

    COMPLETE = ActionState('complete')
    FAILSTART = ActionState('failstart')
    EXITING = ActionState('exiting', close=COMPLETE)
    RUNNING = ActionState('running', exit=EXITING)
    PENDING = ActionState('pending', start=RUNNING, exit=FAILSTART)

    STDOUT = '.stdout'
    STDERR = '.stderr'

    def __init__(self, id, command, serializer=None):
        self.id = id
        self.command = command
        self.machine = state.StateMachine(self.PENDING, delegate=self)
        self.exit_status = None
        self.start_time = None
        self.end_time = None
        self.stdout = filehandler.NullFileHandle
        self.stderr = filehandler.NullFileHandle
        if serializer:
            self.stdout = serializer.open(self.STDOUT)
            self.stderr = serializer.open(self.STDERR)

    @property
    def state(self):
        return self.machine.state

    @property
    def attach(self):
        return self.machine.attach

    def started(self):
        if not self.machine.check('start'):
            return False
        self.start_time = timeutils.current_timestamp()
        return self.machine.transition('start')

    def exited(self, exit_status):
        if not self.machine.check('exit'):
            return False
        self.end_time = timeutils.current_timestamp()
        self.exit_status = exit_status
        return self.machine.transition('exit')

    def write_stderr(self, value):
        self.stderr.write(value)

    def write_stdout(self, value):
        self.stdout.write(value)

    def done(self):
        if not self.machine.check('close'):
            return False
        self.stdout.close()
        self.stderr.close()
        return self.machine.transition('close')

    def handle_errback(self, result):
        """Handle an unexpected error while being run.  This will likely be
        an interval error. Cleanup the state of this ActionCommand and log
        something useful for debugging.
        """
        log.error(
            "Unknown failure for ActionCommand run %s: %s\n%s",
            self.id,
            self.command,
            str(result),
        )
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
        return self.machine.state == self.COMPLETE

    @property
    def is_done(self):
        """Done implies no more work will be done, but might not be success."""
        return self.machine.state in (self.COMPLETE, self.FAILSTART)

    def __repr__(self):
        return "ActionCommand %s %s: %s" % (self.id, self.command, self.state)


class StringBuffer(object):
    """An object which stores strings."""

    def __init__(self):
        self.buffer = []

    def write(self, msg):
        self.buffer.append(msg)

    def get_value(self):
        return ''.join(self.buffer).rstrip()

    def close(self):
        pass


class StringBufferStore(object):
    """A serializer object which can be passed to ActionCommand as a
    serializer, but stores streams in memory.
    """

    def __init__(self):
        self.buffers = {}

    def open(self, name):
        return self.buffers.setdefault(name, StringBuffer())

    def get_stream(self, name):
        return self.buffers[name].get_value()

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
    if not config:
        return NoActionRunnerFactory()

    if config.runner_type not in schema.ActionRunnerTypes:
        raise ValueError("Unknown runner type: %s", config.runner_type)

    if config.runner_type == schema.ActionRunnerTypes.none:
        return NoActionRunnerFactory()

    if config.runner_type == schema.ActionRunnerTypes.subprocess:
        return SubprocessActionRunnerFactory.from_config(config)
