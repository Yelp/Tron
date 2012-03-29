"""
 tron.core.actionrun
"""
import logging
import traceback
from tron import command_context
from tron.serialize import filehandler
from tron import node

from tron.utils import state, timeutils
from tron.utils.observer import Observer

log = logging.getLogger('tron.core.actionrun')


class Error(Exception):
    pass


class InvalidStartStateError(Error):
    """Indicates the action can't start in the state it's in"""
    pass


# TODO: this appears to actually be more coupled with Node then its name
# suggested. Consider moving to tron.node
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

    def __init__(self, id, command, serializer):
        self.id             = id
        self.command        = command
        self.machine        = state.StateMachine(
                                initial_state=self.PENDING, delegate=self)
        self.exit_status    = None
        self.start_time     = None
        self.end_time       = None
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

    def _write(self, stream, value):
        if not stream:
            return
        stream.write(value)

    def write_stderr(self, value):
        self._write(self.stderr, value)

    def write_stdout(self, value):
        self._write(self.stdout, value)

    def _close(self, stream):
        if stream:
            stream.close()

    def done(self):
        if self.machine.transition("close"):
            self._close(self.stdout)
            self._close(self.stderr)
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
        return "ActionCommand %s %s: %s" % (
            self.id, self.command, self.state)


class ActionRunContext(object):
    """Context object that gives us access to data about the action run."""

    def __init__(self, action_run):
        self.action_run = action_run

    @property
    def runid(self):
        # TODO: is this correct?
        return self.action_run.id

    # TODO: actionname

    @property
    def node(self):
        return self.action_run.node.hostname

    def __getitem__(self, name):
        """Attempt to parse date arithmetic syntax and apply to run_time."""
        run_time = self.action_run.run_time
        time_value = timeutils.DateArithmetic.parse(name, run_time)
        if time_value:
            return time_value

        raise KeyError(name)


class ActionRun(Observer):
    """Tracks the state of a single run of an Action.

    ActionRuns Observer ActionCommands they create and are observed by a
    parent JobRun.
    """
    STATE_CANCELLED     = state.NamedEventState('cancelled')
    STATE_UNKNOWN       = state.NamedEventState('unknown', short_name='UNKWN')
    STATE_FAILED        = state.NamedEventState('failed')
    STATE_SUCCEEDED     = state.NamedEventState('succeeded')
    STATE_RUNNING       = state.NamedEventState('running')
    STATE_STARTING      = state.NamedEventState('starting', short_chars=5)
    STATE_QUEUED        = state.NamedEventState('queued')
    STATE_SCHEDULED     = state.NamedEventState('scheduled')
    STATE_SKIPPED       = state.NamedEventState('skipped')

    STATE_SCHEDULED['ready']    = STATE_QUEUED
    STATE_SCHEDULED['queue']    = STATE_QUEUED
    STATE_SCHEDULED['cancel']   = STATE_CANCELLED
    STATE_SCHEDULED['start']    = STATE_STARTING

    STATE_QUEUED['cancel']      = STATE_CANCELLED
    STATE_QUEUED['start']       = STATE_STARTING
    STATE_QUEUED['schedule']    = STATE_SCHEDULED

    STATE_STARTING['started']   = STATE_RUNNING
    STATE_STARTING['fail']      = STATE_FAILED

    STATE_RUNNING['fail']       = STATE_FAILED
    STATE_RUNNING['fail_unknown'] = STATE_UNKNOWN
    STATE_RUNNING['success']    = STATE_SUCCEEDED

    STATE_FAILED['skip']        = STATE_SKIPPED
    STATE_CANCELLED ['skip']    = STATE_SKIPPED

    # We can force many states to be success or failure
    for event_state in (STATE_UNKNOWN, STATE_QUEUED, STATE_SCHEDULED):
        event_state['success']  = STATE_SUCCEEDED
        event_state['fail']     = STATE_FAILED

    # The set of states that are considered end states. Technically some of
    # these states can be manually transitioned to other states.
    END_STATES = set(
        (STATE_FAILED, STATE_SUCCEEDED, STATE_CANCELLED, STATE_SKIPPED))

    FAILED_RENDER = 'false'

    def __init__(self, id, node, run_time, bare_command,
        parent_context=None, output_path=None):
        self.id                 = id
        self.node               = node
        self.output_path        = output_path   # list of path parts
        self.bare_command       = bare_command
        self.run_time           = run_time      # parent JobRun start time
        self.start_time         = None          # ActionRun start time
        self.end_time           = None
        self.exit_status        = None
        self.rendered_command   = None
        self.machine            = state.StateMachine(
                                    ActionRun.STATE_SCHEDULED, delegate=self)
        context                 = ActionRunContext(self)
        self.context            = command_context.CommandContext(
                                    context, parent_context)

    @property
    def state(self):
        return self.machine.state

    @property
    def attach(self):
        return self.machine.attach

    def check_state(self, state):
        """Check if the state machine can be transitioned to state."""
        return self.machine.check(state)

    def attempt_start(self):
        if self.machine.transition('ready'):
            return self.start()

    def start(self):
        """Start this ActionRun."""
        if not self.machine.check('start'):
            raise InvalidStartStateError(self.state)

        log.info("Starting action run %s", self.id)
        self.start_time = timeutils.current_time()
        self.machine.transition('start')

        if not self.is_valid_command:
            log.error("Command for action run %s is invalid: %r",
                self.id, self.bare_command)
            return self.fail(-1)

        action_command = self.build_action_command()
        try:
            self.node.submit_command(action_command)
        except node.Error, e:
            log.warning("Failed to start %s: %r", self.id, e)
            return self.fail(-2)

        return True

    def build_action_command(self):
        """Create a new ActionCommand instance to send to the node."""
        self.action_command = action_command = ActionCommand(
            self.id,
            self.command,
            filehandler.OutputStreamSerializer(*self.output_path)
        )
        self.watch(action_command, True)
        return action_command

    def cancel(self):
        return self.machine.transition('cancel')

    def schedule(self):
        return self.machine.transition('schedule')

    def queue(self):
        return self.machine.transition('queue')

    def skip(self):
        """Mark the run as having been skipped."""
        return self.machine.transition('skip')

    def watcher(self, action_command, event):
        """Observe ActionCommand state changes."""
        log.debug("Action command state change: %s", action_command.state)

        if event == ActionCommand.RUNNING:
            return self.machine.transition('started')

        if event == ActionCommand.FAILSTART:
            return self.fail(None)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                return self.fail_unknown()

            if not action_command.exit_status:
                return self.success()

            return self.fail(action_command.exit_status)

    def _complete(self, target, exit_status=0):
        log.info("Action run %s completed with %s and exit status %r",
            self.id, target, exit_status)
        if self.machine.transition(target):
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            return True

    def fail(self, exit_status=0):
        return self._complete('fail', exit_status)

    def success(self):
        return self._complete('success')

    def fail_unknown(self):
        """Failed with unknown reason."""
        log.warn("Lost communication with action run %s", self.id)
        return self.machine.transition('fail_unknown')

    def restore_state(self, state_data):
        """Restore the state of this ActionRun from a serialized state."""
        self.id                 = state_data['id']
        self.machine.state      = state.named_event_by_name(
            self.STATE_SCHEDULED, state_data['state'])
        self.run_time           = state_data['run_time']
        self.start_time         = state_data['start_time']
        self.end_time           = state_data['end_time']
        self.rendered_command   = state_data['command']

        # We were running when the state file was built, so we have no idea
        # what happened now.
        if self.is_running:
            self.machine.transition('fail_unknown')

    @property
    def state_data(self):
        """This data is used to serialize the state of this action run."""
        return {
            'id':               self.id,
            'state':            str(self.state),
            'run_time':         self.run_time,
            'start_time':       self.start_time,
            'end_time':         self.end_time,
            'command':          self.command,
            }

    def repr_data(self, max_lines=None):
        """Return a dictionary that represents the external view of this
        action run.
        """
        data = {
            'id':               self.id,
            'state':            self.state.short_name,
            'node':             self.node.hostname,
            'command':          self.command,
            'raw_command':      self.bare_command,
            'run_time':         self.run_time,
            'start_time':       self.start_time,
            'end_time':         self.end_time,
            'exit_status':      self.exit_status,
        }
        return data

    def render_command(self):
        """Render our configured command using the command context."""
        return self.bare_command % self.context

    @property
    def command(self):
        if self.rendered_command:
            return self.rendered_command

        try:
            self.rendered_command = self.render_command()
            return self.rendered_command
        except Exception:
            log.error("Failed generating rendering command\n%s" %
                      traceback.format_exc())

            # Return a command string that will always fail
            return self.FAILED_RENDER

    @property
    def is_valid_command(self):
        try:
            rendered = self.render_command()
            return rendered != self.FAILED_RENDER
        except Exception:
            return False

    @property
    def is_done(self):
        return self.state in self.END_STATES

    def __getattr__(self, name):
        """Support convenience properties for checking if this ActionRun is in
        a specific state (Ex: self.is_running would check if self.state is
        STATE_RUNNING).
        """
        state_name = name.replace('is_', 'state_').upper()
        try:
            return self.state == self.__getattribute__(state_name)
        except AttributeError:
            raise AttributeError(name)
