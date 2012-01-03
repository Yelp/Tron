import uuid
import logging
import re
import datetime
import os
import sys

from twisted.internet import defer

from tron import node, command_context
from tron.utils import timeutils
from tron.utils import state

log = logging.getLogger('tron.action')


class Error(Exception):
    pass


class InvalidStartStateError(Error):
    """Indicates the action can't start in the state it's in"""
    pass


class ActionRunContext(object):
    """Context object that gives us access to data about the action run
    itself
    """

    def __init__(self, action_run):
        self.action_run = action_run

    @property
    def actionname(self):
        return self.action_run.action.name

    @property
    def runid(self):
        return self.action_run.id

    @property
    def node(self):
        return self.action_run.node.hostname

    def __getitem__(self, name):
        # We've got a complex getitem implementaiton because we want to suport
        # crazy date arithmetic syntax for the run time of the action.
        # This allows features like running a job with an argument that is the
        # previous day by doing something like
        #   ./my_job --run-date=%(shortdate-1)s
        run_time = self.action_run.run_time

        match = re.match(r'([\w]+)([+-]*)(\d*)', name)
        attr, op, value = match.groups()
        if attr in ("shortdate", "year", "month", "day"):
            if value:
                int_value = int(value)
                if op == '-':
                    int_value = -int_value
                if attr == "year":
                    delta = timeutils.macro_timedelta(run_time,
                                                      years=int_value)
                elif attr == "month":
                    delta = timeutils.macro_timedelta(run_time,
                                                      months=int_value)
                else:
                    delta = timeutils.macro_timedelta(run_time,
                                                      days=int_value)
                run_date = run_time + delta
            else:
                run_date = run_time

            if attr == "year":
                return run_date.strftime("%Y")
            elif attr == "month":
                return run_date.strftime("%m")
            elif attr == "day":
                return run_date.strftime("%d")
            else:
                return run_date.strftime("%Y-%m-%d")
        elif attr == "unixtime":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return int(timeutils.to_timestamp(run_time)) + delta
        elif attr == "daynumber":
            delta = 0
            if value:
                delta = int(value)
            if op == "-":
                delta *= -1
            return run_time.toordinal() + delta
        else:
            raise KeyError(name)


# States where we have executed the run.
# ACTION_RUN_EXECUTED_STATES = [ACTION_RUN_FAILED, ACTION_RUN_SUCCEEDED]


class ActionRun(object):
    """An instance of running a action"""
    STATE_CANCELLED = state.NamedEventState('cancelled')
    STATE_UNKNOWN = state.NamedEventState('unknown')
    STATE_FAILED = state.NamedEventState('failed')
    STATE_SUCCEEDED = state.NamedEventState('succeeded')
    STATE_RUNNING = state.NamedEventState('running')
    STATE_STARTING = state.NamedEventState('starting')
    STATE_QUEUED = state.NamedEventState('queued')
    STATE_SCHEDULED = state.NamedEventState('scheduled')

    STATE_SCHEDULED['ready'] = STATE_QUEUED
    STATE_SCHEDULED['queue'] = STATE_QUEUED
    STATE_SCHEDULED['cancel'] = STATE_CANCELLED

    STATE_QUEUED['cancel'] = STATE_CANCELLED

    STATE_STARTING['started'] = STATE_RUNNING
    STATE_STARTING['fail'] = STATE_FAILED

    STATE_RUNNING['fail'] = STATE_FAILED
    STATE_RUNNING['fail_unknown'] = STATE_UNKNOWN
    STATE_RUNNING['success'] = STATE_SUCCEEDED

    # We only allowing starting from these two states.
    STATE_QUEUED['start'] = STATE_STARTING
    STATE_SCHEDULED['start'] = STATE_STARTING

    # We can force many states to be success or failure
    for event_state in (STATE_UNKNOWN, STATE_QUEUED, STATE_SCHEDULED):
        event_state['success'] = STATE_SUCCEEDED
        event_state['fail'] = STATE_FAILED

    STATE_QUEUED['schedule'] = STATE_SCHEDULED

    def __init__(self, action, context=None, output_path=None):
        self.action = action
        self.id = None

        self.run_time = None    # What time are we supposed to start
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end
        self.exit_status = None
        self.machine = state.StateMachine(ActionRun.STATE_SCHEDULED)

        self.node = None

        if context is None:
            # Provide dummy values for context variables that JobRun provides
            # but aren't available outside of a run. This is to avoid
            # meaningless KeyErrors in the logs.
            context = dict(cleanup_job_status='UNKNOWN')

        new_context = ActionRunContext(self)

        self.context = command_context.CommandContext(new_context, context)

        # If we ran the command, we'll store it for posterity.
        self.rendered_command = None

        self.output_path = output_path
        self.stdout_file = None
        self.stderr_file = None

        self.required_runs = []
        self.waiting_runs = []

    @property
    def state(self):
        return self.machine.state

    @property
    def stdout_path(self):
        if self.output_path is None:
            return None

        return os.path.join(self.output_path, self.id + '.stdout')

    @property
    def stderr_path(self):
        if self.output_path is None:
            return None

        return os.path.join(self.output_path, self.id + '.stderr')

    def tail_stdout(self, num_lines=0):
        return self.tail_file(self.stdout_path, num_lines)

    def tail_stderr(self, num_lines=0):
        return self.tail_file(self.stderr_path, num_lines)

    def tail_file(self, path, num_lines):
        try:
            out = open(path, 'r')
            out.close()
        except IOError:
            return []

        if not num_lines or num_lines <= 0:
            num_lines = sys.maxint

        tail = os.popen("tail -n %s %s" % (num_lines + 1, path))
        lines = tail.read().split('\n')[:-1]

        if len(lines) > num_lines:
            lines[0] = "..."

        return lines

    def attempt_start(self):
        self.machine.transition('ready')

        if all([r.is_success for r in self.required_runs]):
            self.start()

    def start(self):
        if self.state not in (self.STATE_QUEUED, self.STATE_SCHEDULED):
            raise InvalidStartStateError(self.state)

        log.info("Starting action run %s", self.id)
        self.start_time = timeutils.current_time()
        self.end_time = None
        self.machine.transition('start')
        assert self.state == self.STATE_STARTING, self.state
        self._open_output_file()

        if not self.is_valid_command:
            log.error("Command for action run %s is invalid: %r",
                      self.id, self.action.command)
            self.fail(-1)
            return

        # And now we try to actually start some work....
        self.action_command = ActionCommand(self.id,
                                            self.command,
                                            stdout=self.stdout_file,
                                            stderr=self.stderr_file)
        self.action_command.machine.listen(True, self._handle_action_command)
        try:
            df = self.node.run(self.action_command)
            df.addErrback(self._handle_errback)
        except node.Error, e:
            log.warning("Failed to start %s: %r", self.id, e)

    def cancel(self):
        self.machine.transition('cancel')

    def schedule(self):
        self.machine.transition('schedule')

    def queue(self):
        self.machine.transition('queue')

    def _open_output_file(self):
        try:
            log.info("Opening file %s for output", self.stdout_path)
            if self.stdout_path:
                self.stdout_file = open(self.stdout_path, 'a')
            if self.stderr_path:
                self.stderr_file = open(self.stderr_path, 'a')
        except IOError, e:
            log.error(str(e) + " - Not storing command output!")

    def _close_output_file(self):
        if self.stdout_file:
            self.stdout_file.close()
        if self.stderr_file:
            self.stderr_file.close()

        self.stdout_file = self.stderr_file = None

    def _handle_errback(self, result):
        """Handle an error on the action command deferred

        This isn't the primary way we get notified of failures, as most
        expected ones will come through us being a listener to the action
        command. However, if something internally goes wrong we'll catch it
        here, as well as getting more details on the cause of any exception
        generated.
        """
        log.info("Action error: %s", str(result))
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
            self.machine.transition('started')
        elif self.action_command.state == ActionCommand.FAILSTART:
            self._close_output_file()
            self.fail(None)
        elif self.action_command.state == ActionCommand.EXITING:
            if self.action_command.exit_status is None:
                self.fail_unknown()
            elif self.action_command.exit_status == 0:
                self.succeed()
            else:
                self.fail(self.action_command.exit_status)
        elif self.action_command.state == ActionCommand.COMPLETE:
            self._close_output_file()
        else:
            raise Error("Invalid state for action command : %r" %
                        self.action_command)

    def start_dependants(self):
        for run in self.waiting_runs:
            run.attempt_start()

    def ignore_dependants(self):
        for run in self.waiting_runs:
            log.info("Not running waiting run %s, the dependant action failed",
                     run.id)

    def fail(self, exit_status):
        """Mark the run as having failed, providing an exit status"""
        log.info("Action run %s failed with exit status %r",
                 self.id, exit_status)
        self.machine.transition('fail')
        self.exit_status = exit_status
        self.end_time = timeutils.current_time()

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know
        what result was
        """
        log.info("Lost communication with action run %s", self.id)

        self.machine.transition('fail_unknown')
        self.exit_status = None
        self.end_time = None

    def mark_success(self):
        self.exit_status = 0
        self.end_time = timeutils.current_time()
        self.machine.transition('success')

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Action run %s succeeded", self.id)

        self.mark_success()
        self.start_dependants()

    def restore_state(self, state_data):
        self.id = state_data['id']
        self.machine.state = state.named_event_by_name(self.STATE_SCHEDULED,
                                                       state_data['state'])
        self.run_time = state_data['run_time']
        self.start_time = state_data['start_time']
        self.end_time = state_data['end_time']
        self.rendered_command = state_data['command']

        # We were running when the state file was built, so we have no idea
        # what happened now.
        if self.is_running:
            self.machine.transition('fail_unknown')

    @property
    def data(self):
        return {'id': self.id,
                'state': str(self.state),
                'run_time': self.run_time,
                'start_time': self.start_time,
                'end_time': self.end_time,
                'command': self.command
        }

    def render_command(self):
        """Render our configured command under the command context.

        Note that this can fail in bad ways due to user input issues, so it's
        recommend that 'command' or 'is_valid_command' be used.
        """
        return self.action.command % self.context

    @property
    def command(self):
        try:
            if not self.rendered_command or not self.is_done:
                self.rendered_command = self.render_command()
            return self.rendered_command
        except Exception:
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
    def is_queued(self):
        return self.state == self.STATE_QUEUED

    @property
    def is_cancelled(self):
        return self.state == self.STATE_CANCELLED

    @property
    def is_scheduled(self):
        return self.state == self.STATE_SCHEDULED

    @property
    def is_done(self):
        return self.state in (self.STATE_FAILED, self.STATE_SUCCEEDED,
                              self.STATE_CANCELLED)

    @property
    def is_unknown(self):
        return self.state == self.STATE_UNKNOWN

    @property
    def is_running(self):
        return self.state == self.STATE_RUNNING

    @property
    def is_starting(self):
        return self.state == self.STATE_STARTING

    @property
    def is_failure(self):
        return self.state == self.STATE_FAILED

    @property
    def is_success(self):
        return self.state == self.STATE_SUCCEEDED


class Action(object):
    def __init__(self, name=None):
        self.name = name

        self.required_actions = []
        self.job = None
        self.command = None

    def __eq__(self, other):
        if not isinstance(other, Action) \
           or self.name != other.name \
           or self.command != other.command:
            return False

        return all([me == you for (me, you) in zip(self.required_actions,
                                                   other.required_actions)])

    def __ne__(self, other):
        return not self == other

    def build_run(self, job_run):
        """Build an instance of ActionRun for this action

        This is used by the scheduler when scheduling a run
        """
        new_run = ActionRun(self, context=job_run.context)

        new_run.id = "%s.%s" % (job_run.id, self.name)
        new_run.output_path = job_run.output_path

        return new_run


class ActionCommand(object):

    class ActionState(state.NamedEventState):
        pass

    COMPLETE = ActionState("complete")
    FAILSTART = ActionState("failstart")
    EXITING = ActionState("exiting", close=COMPLETE)
    RUNNING = ActionState("running", exit=EXITING)
    PENDING = ActionState("pending", start=RUNNING, exit=FAILSTART)

    def __init__(self, id, command, stdout=None, stderr=None):
        """An Action Command is what a node actually executes

        This object encapsulates everything necessary for a node to execute the
        command, collect results, and inform anyone who cares.

        A Node will call:
          started (when the command starts)
          exited (when the command exits)
          write_<channel> (when output is received)

        Clients should register as listeners for state changes by adding a
        callable to ActionCommand.listeners. The callable will be exected with
        a single argument of 'self' for convenience.
        """
        self.id = id
        self.command = command

        # Create our state machine
        # This guy is pretty simple, as we're just going to have string based
        # events passed to it, and there is no other external interaction
        self.machine = state.StateMachine(initial_state=self.PENDING)

        self.stdout_file = stdout
        self.stderr_file = stderr
        self.exit_status = None
        self.start_time = None
        self.end_time = None

    @property
    def state(self):
        return self.machine.state

    def started(self):
        self.start_time = timeutils.current_timestamp()
        self.machine.transition("start")

    def exited(self, exit_status):
        self.end_time = timeutils.current_timestamp()
        self.exit_status = exit_status
        self.machine.transition("exit")

    def write_stderr(self, value):
        if self.stderr_file:
            self.stderr_file.write(value)

    def write_stdout(self, value):
        if self.stdout_file:
            self.stdout_file.write(value)

    def write_done(self):
        self.machine.transition("close")

    def __repr__(self):
        return "[ActionCommand %s] %s : %s" % (self.id, self.command,
                                               self.state)
