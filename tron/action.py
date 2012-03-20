import logging
import re
import os
from subprocess import Popen, PIPE
import sys

from tron import node, command_context
from tron.filehandler import FileHandleManager
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


class ActionRun(object):
    """An instance of running a action"""
    STATE_CANCELLED = state.NamedEventState('cancelled')
    STATE_UNKNOWN = state.NamedEventState('unknown', short_name='UNKWN')
    STATE_FAILED = state.NamedEventState('failed')
    STATE_SUCCEEDED = state.NamedEventState('succeeded')
    STATE_RUNNING = state.NamedEventState('running')
    STATE_STARTING = state.NamedEventState('starting', short_chars=5)
    STATE_QUEUED = state.NamedEventState('queued')
    STATE_SCHEDULED = state.NamedEventState('scheduled')
    STATE_SKIPPED = state.NamedEventState('skipped')

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

    # We can tell tron to skip a failed step
    STATE_FAILED['skip'] = STATE_SKIPPED
    STATE_CANCELLED ['skip'] = STATE_SKIPPED

    STATE_QUEUED['schedule'] = STATE_SCHEDULED

    def __init__(self,
        action,
        context=None,
        output_path=None,
        node=None,
        id=None,
    ):
        self.action = action
        self.id = id

        self.run_time = None    # What time are we supposed to start
        self.start_time = None  # What time did we start
        self.end_time = None    # What time did we end
        self.exit_status = None
        self.machine = state.StateMachine(ActionRun.STATE_SCHEDULED)

        self.node = node

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
        if not path or not os.path.exists(path):
            return []
        if not num_lines or num_lines <= 0:
            num_lines = sys.maxint

        try:
            cmd = ('tail', '-n', str(num_lines), path)
            tail_sub = Popen(cmd, stdout=PIPE)
            lines = [line.rstrip() for line in tail_sub.stdout]
        except OSError:
            log.error("Could not tail %s." % path)
            return []

        if len(lines) > num_lines:
            lines[0] = "..."

        return lines

    def attempt_start(self):
        self.machine.transition('ready')

        if all(r.is_success or r.is_skipped for r in self.required_runs):
            return self.start()

    def check_state(self, state):
        """Check if the state machine can be transitioned to state."""
        return self.machine.check(state)

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

        # And now we try to actually start some work....
        self._setup_output_files()
        self.action_command = ActionCommand(
            self.id,
            self.command,
            stdout=self.stdout_file,
            stderr=self.stderr_file
        )
        self.action_command.machine.listen(True, self._handle_action_command)
        try:
            df = self.node.run(self.action_command)
            df.addErrback(self._handle_errback)
        except node.Error, e:
            log.warning("Failed to start %s: %r", self.id, e)
            return False
        return True

    def _setup_output_files(self):
        file_manager = FileHandleManager.get_instance()
        self.stdout_file = file_manager.open(self.stdout_path)
        self.stderr_file = file_manager.open(self.stderr_path)

    def cancel(self):
        return self.machine.transition('cancel')

    def schedule(self):
        return self.machine.transition('schedule')

    def queue(self):
        return self.machine.transition('queue')

    def skip(self):
        """Mark the run as having been skipped."""
        if self.machine.transition('skip'):
            self.start_dependants()
            return True
        return False

    def _close_output_file(self):
        """Attempt to close any open file handlers."""
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

    def start_dependants(self):
        for run in self.waiting_runs:
            run.attempt_start()

    def ignore_dependants(self):
        for run in self.waiting_runs:
            log.info("Not running waiting run %s, the dependant action failed",
                     run.id)

    def fail(self, exit_status=0):
        """Mark the run as having failed, providing an exit status"""
        log.info("Action run %s failed with exit status %r",
                 self.id, exit_status)
        if self.machine.transition('fail'):
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            return True

    def fail_unknown(self):
        """Mark the run as having failed, but note that we don't actually know
        what result was
        """
        log.info("Lost communication with action run %s", self.id)

        if self.machine.transition('fail_unknown'):
            self.exit_status = None
            self.end_time = None
            return True

    def mark_success(self):
        self.exit_status = 0
        self.end_time = timeutils.current_time()
        return self.machine.transition('success')

    def succeed(self):
        """Mark the run as having succeeded"""
        log.info("Action run %s succeeded", self.id)

        if self.mark_success():
            self.start_dependants()
            return True
        return False

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
            return self.machine.transition('fail_unknown')

    @property
    def data(self):
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
            'name':         self.action.name,
            'state':        self.state.short_name,
            'node':         self.node.hostname,
            'command':      self.command,
            'raw_command':  self.action.command,
            'run_time':     self.run_time,
            'start_time':   self.start_time,
            'end_time':     self.end_time,
            'exit_status':  self.exit_status,
            'requirements': [req.name for req in self.action.required_actions],
            'stdout':       [],
            'stderr':       []
        }
        if max_lines:
            data['stdout'] = self.tail_stdout(max_lines)
            data['stderr'] = self.tail_stderr(max_lines)
        return data

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
    def is_done(self):
        return self.state in (self.STATE_FAILED, self.STATE_SUCCEEDED,
                              self.STATE_CANCELLED, self.STATE_SKIPPED)

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

    @property
    def is_skipped(self):
        return self.state == self.STATE_SKIPPED


class Action(object):
    def __init__(self, name=None, command=None, node_pool=None,
                 required_actions=None):
        self.name = name
        self.command = command
        self.node_pool = node_pool

        self.required_actions = required_actions or []
        self.job = None

    @classmethod
    def from_config(cls, config, node_pools):
        return cls(
            name=config.name,
            command=config.command,
            node_pool=node_pools[config.node] if config.node else None
        )

    def __eq__(self, other):
        if (not isinstance(other, Action)
           or self.name != other.name
           or self.command != other.command):
            return False

        return all(me == you for (me, you) in zip(self.required_actions,
                                                   other.required_actions))

    def __ne__(self, other):
        return not self == other

    def build_run(self, job_run, cleanup=False):
        """Build an instance of ActionRun for this action. If cleanup=True
        we're building a cleanup action run.
        """
        callback = job_run.cleanup_action_run if cleanup else job_run.run_completed

        action_run = ActionRun(
            self,
            context=job_run.context,
            node=job_run.node,
            id="%s.%s" % (job_run.id, self.name),
            output_path=job_run.output_path)
        action_run.machine.listen(True, job_run.job.notify)
        action_run.machine.listen(ActionRun.STATE_SUCCEEDED, callback)
        action_run.machine.listen(ActionRun.STATE_FAILED,    callback)
        return action_run


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
        return self.machine.transition("start")

    def exited(self, exit_status):
        self.end_time = timeutils.current_timestamp()
        self.exit_status = exit_status
        return self.machine.transition("exit")

    def write_stderr(self, value):
        if self.stderr_file:
            self.stderr_file.write(value)

    def write_stdout(self, value):
        if self.stdout_file:
            self.stdout_file.write(value)

    def write_done(self):
        return self.machine.transition("close")

    def __repr__(self):
        return "[ActionCommand %s] %s : %s" % (self.id, self.command,
                                               self.state)
