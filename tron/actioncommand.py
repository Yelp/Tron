import logging
from tron.serialize import filehandler

from tron.utils import state, timeutils

log = logging.getLogger(__name__)

class ActionCommand(object):
    """An ActionCommand encapsulates a runnable task that is passed to a node
    for execution.

    A Node calls:
      started   (when the command starts)
      exited    (when the command exits)
      write_<channel> (when output is received)
      done      (when the command is finished)
    """

    class ActionState(state.NamedEventState):
        pass

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
        if self.machine.transition("start"):
            self.start_time = timeutils.current_timestamp()
            return True

    def exited(self, exit_status):
        if self.machine.transition("exit"):
            self.end_time    = timeutils.current_timestamp()
            self.exit_status = exit_status
            return True

    def write_stderr(self, value):
        self.stderr.write(value)

    def write_stdout(self, value):
        self.stdout.write(value)

    def done(self):
        if self.machine.transition("close"):
            self.stdout.close()
            self.stderr.close()
            return True

    def handle_errback(self, result):
        """Handle an unexpected error while being run.  This will likely be
        an interval error. Cleanup the state of this AcctionCommand and log
        something useful for debugging.
        """
        log.error("Unknown failure for ActionCommand run %s: %s\n%s",
            self.id, self.command, str(result))
        self.exited(result)
        self.done()

    def __repr__(self):
        return "ActionCommand %s %s: %s" % (self.id, self.command, self.state)