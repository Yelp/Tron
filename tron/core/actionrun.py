"""
 tron.core.actionrun
"""
import logging
from tron import command_context
from tron.serialize import filehandler
from tron import node

from tron.utils import state, timeutils

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
        self.machine        = state.StateMachine(initial_state=self.PENDING)
        self.serializer     = serializer
        self.exit_status    = None
        self.start_time     = None
        self.end_time       = None

    @property
    def state(self):
        return self.machine.state

    def started(self):
        if self.machine.transition("start"):
            self.start_time = timeutils.current_timestamp()
            return True

    def exited(self, exit_status):
        if self.machine.transition("exit"):
            self.end_time    = timeutils.current_timestamp()
            self.exit_status = exit_status
            return True

    def _write(self, stream_name, value):
        if not self.serializer:
            return
        fh = self.serializer.open(stream_name)
        fh.write(value)

    def write_stderr(self, value):
        self._write(self.STDERR, value)

    def write_stdout(self, value):
        self._write(self.STDOUT, value)

    def done(self):
        if self.machine.transition("close"):
            # TODO: close and test closed fh for stdout/stderr
            pass

    def __repr__(self):
        return "ActionCommand %s %s: %s" % (
            self.id, self.command, self.state)


class ActionRunContext(object):
    """Context object that gives us access to data about the action run.
    """

    def __init__(self, action_run):
        self.action_run = action_run

    @property
    def runid(self):
        return self.action_run.id

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


class ActionRun(object):
    """Tracks the state of a single run of an Action."""
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
    END_STATES = set((
        STATE_FAILED,
        STATE_SUCCEEDED,
        STATE_CANCELLED,
        STATE_SKIPPED
    ))

    # TODO: work starts here
    def __init__(self, id, node, run_time, bare_command,
        parent_context=None, output_path=None):
        self.id                 = id
        self.node               = node
        self.output_path        = output_path
        self.bare_command       = bare_command
        self.run_time           = run_time      # parent JobRun start time
        self.start_time         = None          # ActionRun start time
        self.end_time           = None
        self.exit_status        = None
        self.rendered_command   = None
        self.machine            = state.StateMachine(ActionRun.STATE_SCHEDULED)
        context                 = ActionRunContext(self)
        self.context = command_context.CommandContext(context, parent_context)

    @property
    def state(self):
        return self.machine.state

    def check_state(self, state):
        """Check if the state machine can be transitioned to state."""
        return self.machine.check(state)

    def attempt_start(self):
        self.machine.transition('ready')

        # TODO: should be checked in JobRun or ActionRunCollection?
        if all(r.is_success or r.is_skipped for r in self.required_runs):
            return self.start()

    def start(self):
        if not self.machine.check('start'):
            raise InvalidStartStateError(self.state)

        log.info("Starting action run %s", self.id)
        self.start_time = timeutils.current_time()
        self.end_time = None
        self.machine.transition('start')
        assert self.state == self.STATE_STARTING, self.state

        if not self.is_valid_command:
            log.error("Command for action run %s is invalid: %r",
                self.id, self.action.command)
            return self.fail(-1)

        self.action_command = ActionCommand(
            self.id,
            self.command,
            filehandler.OutputStreamSerializer(self.output_path)
        )

        # TODO: observer
        self.action_command.machine.listen(True, self._handle_action_command)
        try:
            df = self.node.run(self.action_command)
            df.addErrback(self._handle_errback)
        except node.Error, e:
            log.warning("Failed to start %s: %r", self.id, e)
            return False
        return True

    def cancel(self):
        return self.machine.transition('cancel')

    def schedule(self):
        return self.machine.transition('schedule')

    def queue(self):
        return self.machine.transition('queue')

    def skip(self):
        """Mark the run as having been skipped."""
        return self.machine.transition('skip')


    def _handle_errback(self, result):
        """Handle an error on the action command deferred

        This isn't the primary way we get notified of failures, as most
        expected ones will come through us being a listener to the action
        command. However, if something internally goes wrong we'll catch it
        here, as well as getting more details on the cause of any exception
        generated.
        """
        log.info("Action error: %s", str(result))
        self._close_output_file()
        if isinstance(result.value, node.ConnectError):
            log.warning("Failed to connect to host %s for run %s",
                self.node.hostname, self.id)
        elif isinstance(result.value, node.ResultError):
            log.warning("Failed to retrieve exit for run %s after executing"
                        " command on host %s", self.id, self.node.hostname)
        else:
            log.warning("Unknown failure for run %s on host %s: %s",
                self.id, self.node.hostname, str(result))
            self.fail_unknown()

    def _handle_action_command(self):
        """Our hook for being a listener to a running action command.

        On any state change, the action command will call us back so we can
        evaluate if we need to change some state ourselves.
        """
        log.debug("Action command state change: %s", self.action_command.state)
        if self.action_command.state == ActionCommand.RUNNING:
            return self.machine.transition('started')

        if self.action_command.state == ActionCommand.FAILSTART:
            self._close_output_file()
            return self.fail(None)

        if self.action_command.state == ActionCommand.EXITING:
            if self.action_command.exit_status is None:
                return self.fail_unknown()
            if not self.action_command.exit_status:
                return self.succeed()
            return self.fail(self.action_command.exit_status)

        if self.action_command.state == ActionCommand.COMPLETE:
            self._close_output_file()
            return

        raise Error(
            "Invalid state for action command : %r" % self.action_command)

    def fail(self, exit_status=0):
        """Mark the run as having failed, providing an exit status"""
        log.info("Action run %s failed with exit status %r",
            self.id, exit_status)
        if self.machine.transition('fail'):
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            return True

    def fail_unknown(self):
        """Failed with unknown reason."""
        log.info("Lost communication with action run %s", self.id)

        if self.machine.transition('fail_unknown'):
            self.exit_status = None
            self.end_time = None
            return True

    def success(self):
        if self.machine.transition('success'):
            log.info("Action run %s succeeded", self.id)
            self.exit_status = 0
            self.end_time = timeutils.current_time()
            return True

    def restore_state(self, state_data):
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
            'id':           self.id,
            'state':        str(self.state),
            'run_time':     self.run_time,
            'start_time':   self.start_time,
            'end_time':     self.end_time,
            'command':      self.command,
            }

    def repr_data(self, max_lines=None):
        """Return a dictionary that represents the external view of this
        action run.
        """
        data = {
            'id':           self.id,
            'state':        self.state.short_name,
            'node':         self.node.hostname,
            'command':      self.command,
            'raw_command':  self.bare_command,
            'run_time':     self.run_time,
            'start_time':   self.start_time,
            'end_time':     self.end_time,
            'exit_status':  self.exit_status,
            'requirements': [req.name for req in self.action.required_actions],
        }
        return data

    def render_command(self):
        """Render our configured command under the command context.

        Note that this can fail in bad ways due to user input issues, so it's
        recommend that 'command' or 'is_valid_command' be used.
        """
        return self.bare_command % self.context

    @property
    def command(self):
        try:
            if not self.rendered_command or not self.is_done:
                self.rendered_command = self.render_command()
            return self.rendered_command
        except Exception:
            # TODO: full stack trace
            log.exception("Failed generating rendering command. Bad format")

            # If we can't properly build our command, we at least want to
            # ensure we run something that won't succeed. Ideally this will
            # be caught earlier. See also is_valid_command
            return "false"

    @property
    def is_valid_command(self):
        try:
            self.render_command()
            return True
        except Exception:
            return False

    @property
    def is_done(self):
        return self.state in self.END_STATES

    def __getattr__(self, name):
        """Check the state."""
        state_name = name.replace('in_', 'state_').upper()
        return self.state == getattr(self, state_name)