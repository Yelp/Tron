import logging
from tron.serialize import filehandler

from tron.utils import state, timeutils

log = logging.getLogger(__name__)


class ActionState(state.NamedEventState):
    pass


class CompletedActionCommand(object):
    is_complete = True
    is_done = True
    is_failed = False


class ActionCommand(object):
    """An ActionCommand encapsulates a runnable task that is passed to a node
    for execution.

    A Node calls:
      started   (when the command starts)
      exited    (when the command exits)
      write_<channel> (when output is received)
      done      (when the command is finished)
    """

    COMPLETE    = ActionState('complete')
    FAILSTART   = ActionState('failstart')
    EXITING     = ActionState('exiting', close=COMPLETE)
    RUNNING     = ActionState('running', exit=EXITING)
    PENDING     = ActionState('pending', start=RUNNING, exit=FAILSTART)

    STDOUT      = '.stdout'
    STDERR      = '.stderr'

    def __init__(self, id, command, serializer=None):
        self.id             = id
        self.command        = command
        self.machine        = state.StateMachine(
                                initial_state=self.PENDING, delegate=self)
        self.exit_status    = None
        self.start_time     = None
        self.end_time       = None
        self.stdout         = filehandler.NullFileHandle
        self.stderr         = filehandler.NullFileHandle
        if serializer:
            self.stdout         = serializer.open(self.STDOUT)
            self.stderr         = serializer.open(self.STDERR)

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
        self.end_time    = timeutils.current_timestamp()
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
        an interval error. Cleanup the state of this AcctionCommand and log
        something useful for debugging.
        """
        log.error("Unknown failure for ActionCommand run %s: %s\n%s",
                self.id, self.command, str(result))
        self.exited(result)
        self.done()

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
