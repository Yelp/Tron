"""
 tron.core.actionrun
"""
import logging
import traceback
import itertools
from tron import command_context
from tron.core import action
from tron.serialize import filehandler
from tron import node
from tron.actioncommand import ActionCommand

from tron.utils import state, timeutils, proxy
from tron.utils.observer import Observer

log = logging.getLogger(__name__)


class Error(Exception):
    pass


class InvalidStartStateError(Error):
    """Indicates the action can't start in the state it's in"""
    pass


class ActionRunContext(object):
    """Context object that gives us access to data about the action run."""

    def __init__(self, action_run):
        self.action_run = action_run

    @property
    def runid(self):
        return self.action_run.id

    @property
    def actionname(self):
        return self.action_run.action_name

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

    ActionRuns observers ActionCommands they create and are observed by a
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
        (STATE_FAILED,
         STATE_SUCCEEDED,
         STATE_CANCELLED,
         STATE_SKIPPED,
         STATE_UNKNOWN)
    )

    FAILED_RENDER = 'false'

    def __init__(self, job_run_id, name, node, run_time, bare_command=None,
            parent_context=None, output_path=None, cleanup=False,
            start_time=None, end_time=None, run_state=STATE_SCHEDULED,
            rendered_command=None):
        self.job_run_id         = job_run_id
        self.action_name        = name
        self.node               = node
        self.output_path        = output_path or filehandler.OutputPath()
        self.bare_command       = bare_command or rendered_command
        self.run_time           = run_time      # parent JobRun start time
        self.start_time         = start_time    # ActionRun start time
        self.end_time           = end_time
        self.exit_status        = None
        self.rendered_command   = rendered_command
        self.machine            = state.StateMachine(run_state, delegate=self)
        context                 = ActionRunContext(self)
        self.context            = command_context.CommandContext(
                                    context, parent_context)
        self.is_cleanup         = cleanup

    @property
    def state(self):
        return self.machine.state

    @property
    def attach(self):
        return self.machine.attach

    @property
    def id(self):
        return "%s.%s" % (self.job_run_id, self.action_name)

    def check_state(self, state):
        """Check if the state machine can be transitioned to state."""
        return self.machine.check(state)

    @classmethod
    def from_state(cls, state_data, parent_context, output_path, cleanup=False):
        """Restore the state of this ActionRun from a serialized state."""
        node_pools = node.NodePoolStore.get_instance()

        # Support state from older version
        if 'id' in state_data:
            job_run_id, action_name = state_data['id'].rsplit('.', 1)
        else:
            job_run_id = state_data['job_run_id']
            action_name = state_data['action_name']

        run = cls(
            job_run_id,
            action_name,
            node_pools.get(state_data.get('node_name')),
            state_data['run_time'],
            parent_context,
            output_path,
            rendered_command=state_data['command'],
            cleanup=cleanup,
            start_time=state_data['start_time'],
            end_time=state_data['end_time'],
            run_state=state.named_event_by_name(
                    cls.STATE_SCHEDULED, state_data['state'])
        )

        # Transition running to fail unknown because exit status was missed
        if run.is_running:
            run.machine.transition('fail_unknown')
        return run

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
            filehandler.OutputStreamSerializer(self.output_path)
        )
        self.watch(action_command)
        return action_command

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

    @property
    def state_data(self):
        """This data is used to serialize the state of this action run."""
        return {
            'job_run_id':       self.job_run_id,
            'action_name':      self.action_name,
            'state':            str(self.state),
            'run_time':         self.run_time,
            'start_time':       self.start_time,
            'end_time':         self.end_time,
            'command':          self.command,
            'node_name':        self.node.name if self.node else None
        }

    def repr_data(self, max_lines=None):
        """Return a dictionary that represents the external view of this
        action run.
        """
        return {
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
        STATE_RUNNING) or for transitioning to a new state (ex: ready).
        """
        if name in ['cancel', 'schedule', 'queue', 'skip', 'ready']:
            return lambda: self.machine.transition(name)

        state_name = name.replace('is_', 'state_').upper()
        try:
            return self.state == self.__getattribute__(state_name)
        except AttributeError:
            raise AttributeError(name)


class ActionRunCollection(object):
    """A collection of ActionRuns used by a JobRun."""

    def __init__(self, action_graph, run_map):
        self.action_graph       = action_graph
        self.run_map            = run_map
        self.cleanup_action_run = self.run_map.get(action.CLEANUP_ACTION_NAME)

        # Setup proxies
        self.proxy_action_runs_with_cleanup = proxy.CollectionProxy(
            self.action_runs_with_cleanup, [
#                ('is_failure',      any,    False),
#                ('is_starting',     any,    False),
#                ('is_running',      any,    False),
#                ('is_scheduled',    any,    False),
#                ('is_unknown',      any,    False),
#                ('is_queued',       all,    False),
#                ('is_cancelled',    all,    False),
#                ('is_skipped',      all,    False),
#                ('is_done',         all,    False),
#                ('check_state',     all,    True),
#                ('cancel',          all,    True),
#                ('succeed',         all,    True),
#                ('fail',            any,    True),
                ('ready',           all,    True),
            ])

        self.proxy_action_runs = proxy.CollectionProxy(
            self.action_runs, [
#                ('schedule',        all,    True),
#                ('queue',           all,    True),
            ])

    def action_runs_for_names(self, names):
        return (self.run_map[name] for name in names)

    def action_runs_for_actions(self, actions):
        return (self.run_map[a.name] for a in actions)

    @property
    def action_runs_with_cleanup(self):
        return self.run_map.itervalues()

    @property
    def action_runs(self):
        return (run for run in self.run_map.itervalues() if not run.is_cleanup)

    @property
    def state_data(self):
        return [run.state_data for run in self.action_runs]

    @property
    def cleanup_action_state_data(self):
        if self.cleanup_action_run:
            return self.cleanup_action_run.state_data

    def _get_runs_using(self, func, include_cleanup=False):
        """Return an iterator of all the ActionRuns which cause func to return
        True. func should be a callable that takes a single ActionRun and
        returns True or False.
        """
        if include_cleanup:
            action_runs = self.action_runs_with_cleanup
        else:
            action_runs = self.action_runs
        return itertools.ifilter(func, action_runs)

    def get_startable_actions(self):
        """Returns any actions that are scheduled or queued that can be run.
        """
        def startable(action_run):
            return (
                (action_run.is_scheduled or action_run.is_queued) and
                not self._is_blocked(action_run)
            )
        return self._get_runs_using(startable)


#    def get_unblocked_actions(self):
#        return self._get_runs_using(lambda ar: not self._is_blocked(ar))
#
#    def get_blocked_actions(self):
#        return self._get_runs_using(self._is_blocked)

    def _is_blocked(self, action_run):
        """Returns True if this ActionRun is waiting on a required run to
        finish before it can run.
        """
        if (action_run.is_done or
                action_run.is_running or action_run.is_starting):
            return False

        required_actions = self.action_graph[action_run.name].required
        if not required_actions:
            return False

        required_runs = self.action_runs_for_actions(required_actions)
        return any(
            self._is_blocked(run) or not run.is_done
            for run in required_runs
        )

    # TODO: is this needed?
    @property
    def all_but_cleanup_success(self):
        """Overloaded all_but_cleanup_success, because we can still succeed
        if some actions were skipped.
        """
        return all(r.is_success or r.is_skipped for r in self.action_runs)

    @property
    def is_success(self):
        """Overloaded is_success, because we can still succeed if some
        actions were skipped.
        """
        return all(
            r.is_success or r.is_skipped for r in self.action_runs_with_cleanup
        )

    @property
    def all_but_cleanup_done(self):
        """True when any ActionRun has failed, or when all ActionRuns are done.
        """
        return all(r.is_done for r in self.action_runs)

    def __iter__(self):
        """Return all actions that are not cleanup actions."""
        return iter(self.action_runs)

    def __getattr__(self, name):
        # The order here is important.  We don't want to raise too many
        # exceptions, so proxies should be ordered by those most likely
        # to be used.
        for proxy in [
            self.proxy_action_runs_with_cleanup,
            self.proxy_action_runs
        ]:
            try:
                return proxy.perform(name)
            except AttributeError:
                pass

        # We want to re-raise this exception because the proxy code
        # will not be relevant in the stack trace
        raise AttributeError(name)