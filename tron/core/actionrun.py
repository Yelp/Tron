"""
 tron.core.actionrun
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import traceback

import six
from six.moves import filter

from tron import command_context
from tron import node
from tron.actioncommand import ActionCommand
from tron.actioncommand import NoActionRunnerFactory
from tron.core import action
from tron.serialize import filehandler
from tron.utils import iteration
from tron.utils import proxy
from tron.utils import state
from tron.utils import timeutils
from tron.utils.observer import Observer

log = logging.getLogger(__name__)


class ActionRunFactory(object):
    """Construct ActionRuns and ActionRunCollections for a JobRun and
    ActionGraph.
    """

    @classmethod
    def build_action_run_collection(cls, job_run, action_runner):
        """Create an ActionRunGraph from an ActionGraph and JobRun."""
        action_map = six.iteritems(job_run.action_graph.get_action_map())
        action_run_map = {
            name: cls.build_run_for_action(job_run, action_inst, action_runner)
            for name, action_inst in action_map
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def action_run_collection_from_state(
        cls, job_run, runs_state_data,
        cleanup_action_state_data,
    ):
        action_runs = [
            cls.action_run_from_state(job_run, state_data)
            for state_data in runs_state_data
        ]
        if cleanup_action_state_data:
            action_runs.append(cls.action_run_from_state(
                job_run, cleanup_action_state_data, cleanup=True,
            ))

        action_run_map = {
            action_run.action_name: action_run for action_run in action_runs
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def build_run_for_action(cls, job_run, action, action_runner):
        """Create an ActionRun for a JobRun and Action."""
        run_node = action.node_pool.next() if action.node_pool else job_run.node

        return ActionRun(
            job_run.id,
            action.name,
            run_node,
            action.command,
            parent_context=job_run.context,
            output_path=job_run.output_path.clone(),
            cleanup=action.is_cleanup,
            action_runner=action_runner,
        )

    @classmethod
    def action_run_from_state(cls, job_run, state_data, cleanup=False):
        """Restore an ActionRun for this JobRun from the state data."""
        return ActionRun.from_state(
            state_data,
            job_run.context,
            job_run.output_path.clone(),
            job_run.node,
            cleanup=cleanup,
        )


class ActionRun(Observer):
    """Tracks the state of a single run of an Action.

    ActionRuns observers ActionCommands they create and are observed by a
    parent JobRun.
    """
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
    STATE_SCHEDULED['start'] = STATE_STARTING

    STATE_QUEUED['cancel'] = STATE_CANCELLED
    STATE_QUEUED['start'] = STATE_STARTING
    STATE_QUEUED['schedule'] = STATE_SCHEDULED

    STATE_STARTING['started'] = STATE_RUNNING
    STATE_STARTING['fail'] = STATE_FAILED

    STATE_RUNNING['fail'] = STATE_FAILED
    STATE_RUNNING['fail_unknown'] = STATE_UNKNOWN
    STATE_RUNNING['success'] = STATE_SUCCEEDED

    STATE_FAILED['skip'] = STATE_SKIPPED
    STATE_CANCELLED['skip'] = STATE_SKIPPED

    # We can force many states to be success or failure
    for event_state in (STATE_UNKNOWN, STATE_QUEUED, STATE_SCHEDULED):
        event_state['success'] = STATE_SUCCEEDED
        event_state['fail'] = STATE_FAILED

    # The set of states that are considered end states. Technically some of
    # these states can be manually transitioned to other states.
    END_STATES = {
        STATE_FAILED,
        STATE_SUCCEEDED,
        STATE_CANCELLED,
        STATE_SKIPPED,
        STATE_UNKNOWN,
    }

    # Failed render command is false to ensure that it will fail when run
    FAILED_RENDER = 'false'

    context_class = command_context.ActionRunContext

    # TODO: create a class for ActionRunId, JobRunId, Etc
    def __init__(
        self, job_run_id, name, node, bare_command=None,
        parent_context=None, output_path=None, cleanup=False,
        start_time=None, end_time=None, run_state=STATE_SCHEDULED,
        rendered_command=None, exit_status=None, action_runner=None,
    ):
        self.job_run_id = job_run_id
        self.action_name = name
        self.node = node
        self.start_time = start_time
        self.end_time = end_time
        self.exit_status = exit_status
        self.bare_command = bare_command
        self.rendered_command = rendered_command
        self.action_runner = action_runner or NoActionRunnerFactory
        self.machine = state.StateMachine(
            self.STATE_SCHEDULED, delegate=self, force_state=run_state,
        )
        self.is_cleanup = cleanup
        self.output_path = output_path or filehandler.OutputPath()
        self.output_path.append(self.id)
        self.context = command_context.build_context(self, parent_context)

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
    def from_state(
        cls, state_data, parent_context, output_path,
        job_run_node, cleanup=False,
    ):
        """Restore the state of this ActionRun from a serialized state."""
        pool_repo = node.NodePoolRepository.get_instance()

        # Support state from older version
        if 'id' in state_data:
            job_run_id, action_name = state_data['id'].rsplit('.', 1)
        else:
            job_run_id = state_data['job_run_id']
            action_name = state_data['action_name']

        job_run_node = pool_repo.get_node(
            state_data.get('node_name'), job_run_node,
        )

        rendered_command = state_data.get('rendered_command')
        run = cls(
            job_run_id,
            action_name,
            job_run_node,
            parent_context=parent_context,
            output_path=output_path,
            rendered_command=rendered_command,
            bare_command=state_data['command'],
            cleanup=cleanup,
            start_time=state_data['start_time'],
            end_time=state_data['end_time'],
            run_state=state.named_event_by_name(
                cls.STATE_SCHEDULED, state_data['state'],
            ),
            exit_status=state_data.get('exit_status'),
        )

        # Transition running to fail unknown because exit status was missed
        if run.is_running:
            run._done('fail_unknown')
        if run.is_starting:
            run.fail(None)
        return run

    def start(self):
        """Start this ActionRun."""
        if not self.machine.check('start'):
            return False

        log.info("Starting action run %s", self.id)
        self.start_time = timeutils.current_time()
        self.machine.transition('start')

        if not self.is_valid_command:
            log.error(
                "Command for action run %s is invalid: %r",
                self.id, self.bare_command,
            )
            self.fail(-1)
            return

        action_command = self.build_action_command()
        try:
            self.node.submit_command(action_command)
        except node.Error as e:
            log.warning("Failed to start %s: %r", self.id, e)
            self.fail(-2)
            return

        return True

    def stop(self):
        stop_command = self.action_runner.build_stop_action_command(
            self.id, 'terminate',
        )
        self.node.submit_command(stop_command)

    def kill(self):
        kill_command = self.action_runner.build_stop_action_command(
            self.id, 'kill',
        )
        self.node.submit_command(kill_command)

    def build_action_command(self):
        """Create a new ActionCommand instance to send to the node."""
        serializer = filehandler.OutputStreamSerializer(self.output_path)
        self.action_command = self.action_runner.create(
            id=self.id,
            command=self.command,
            serializer=serializer,
        )
        self.watch(self.action_command)
        return self.action_command

    def handle_action_command_state_change(self, action_command, event):
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
    handler = handle_action_command_state_change

    def _done(self, target, exit_status=0):
        log.info(
            "Action run %s completed with %s and exit status %r",
            self.id, target, exit_status,
        )
        if self.machine.check(target):
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            return self.machine.transition(target)

    def fail(self, exit_status=0):
        return self._done('fail', exit_status)

    def success(self):
        return self._done('success')

    def fail_unknown(self):
        """Failed with unknown reason."""
        log.warn("Lost communication with action run %s", self.id)
        return self.machine.transition('fail_unknown')

    @property
    def state_data(self):
        """This data is used to serialize the state of this action run."""
        rendered_command = self.rendered_command
        # Freeze command after it's run
        command = rendered_command if rendered_command else self.bare_command
        return {
            'job_run_id':       self.job_run_id,
            'action_name':      self.action_name,
            'state':            str(self.state),
            'start_time':       self.start_time,
            'end_time':         self.end_time,
            'command':          command,
            'rendered_command': self.rendered_command,
            'node_name':        self.node.get_name() if self.node else None,
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
        except Exception:
            log.error("Failed generating rendering command\n%s" %
                      traceback.format_exc())

            # Return a command string that will always fail
            self.rendered_command = self.FAILED_RENDER
        return self.rendered_command

    @property
    def is_valid_command(self):
        """Returns True if the bare_command was rendered without any errors.
        This has the side effect of actually rendering the bare_command.
        """
        return self.command != self.FAILED_RENDER

    @property
    def is_done(self):
        return self.state in self.END_STATES

    @property
    def is_complete(self):
        return self.is_succeeded or self.is_skipped

    @property
    def is_broken(self):
        return self.is_failed or self.is_cancelled or self.is_unknown

    @property
    def is_active(self):
        return self.is_starting or self.is_running

    def cleanup(self):
        self.machine.clear_observers()
        self.cancel()

    def __getattr__(self, name):
        """Support convenience properties for checking if this ActionRun is in
        a specific state (Ex: self.is_running would check if self.state is
        STATE_RUNNING) or for transitioning to a new state (ex: ready).
        """
        if name in self.machine.transitions:
            return lambda: self.machine.transition(name)

        state_name = name.replace('is_', 'state_').upper()
        try:
            return self.state == self.__getattribute__(state_name)
        except AttributeError:
            raise AttributeError(name)

    def __str__(self):
        return "ActionRun: %s" % self.id


class ActionRunCollection(object):
    """A collection of ActionRuns used by a JobRun."""

    # An ActionRunCollection is blocked when it has runs running which
    # are required for other blocked runs to start.
    STATE_BLOCKED = state.NamedEventState('blocked')

    def __init__(self, action_graph, run_map):
        self.action_graph = action_graph
        self.run_map = run_map
        # Setup proxies
        self.proxy_action_runs_with_cleanup = proxy.CollectionProxy(
            self.get_action_runs_with_cleanup, [
                proxy.attr_proxy('is_running',      any),
                proxy.attr_proxy('is_starting',     any),
                proxy.attr_proxy('is_scheduled',    any),
                proxy.attr_proxy('is_cancelled',    any),
                proxy.attr_proxy('is_active',       any),
                proxy.attr_proxy('is_queued',       all),
                proxy.attr_proxy('is_complete',     all),
                proxy.func_proxy('queue',           iteration.list_all),
                proxy.func_proxy('cancel',          iteration.list_all),
                proxy.func_proxy('success',         iteration.list_all),
                proxy.func_proxy('fail',            iteration.list_all),
                proxy.func_proxy('ready',           iteration.list_all),
                proxy.func_proxy('cleanup',         iteration.list_all),
                proxy.func_proxy('stop',            iteration.list_all),
                proxy.attr_proxy('start_time',      iteration.min_filter),
            ],
        )

    def action_runs_for_actions(self, actions):
        return (self.run_map[a.name] for a in actions if a.name in self.run_map)

    def get_action_runs_with_cleanup(self):
        return six.itervalues(self.run_map)
    action_runs_with_cleanup = property(get_action_runs_with_cleanup)

    def get_action_runs(self):
        return (run for run in six.itervalues(self.run_map) if not run.is_cleanup)
    action_runs = property(get_action_runs)

    @property
    def cleanup_action_run(self):
        return self.run_map.get(action.CLEANUP_ACTION_NAME)

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
        return filter(func, action_runs)

    def get_startable_action_runs(self):
        """Returns any actions that are scheduled or queued that can be run."""
        def startable(action_run):
            return (action_run.check_state('start') and
                    not self._is_run_blocked(action_run))
        return self._get_runs_using(startable)

    @property
    def has_startable_action_runs(self):
        return any(self.get_startable_action_runs())

    def _is_run_blocked(self, action_run):
        """Returns True if the ActionRun is waiting on a required run to
        finish before it can run.
        """
        if action_run.is_done or action_run.is_active:
            return False

        required_actions = self.action_graph.get_required_actions(
            action_run.action_name,
        )
        if not required_actions:
            return False

        required_runs = self.action_runs_for_actions(required_actions)

        def is_required_run_blocking(required_run):
            if required_run.is_complete:
                return False
            return True

        return any(is_required_run_blocking(run) for run in required_runs)

    @property
    def is_done(self):
        """Returns True when there are no running ActionRuns and all
        non-blocked ActionRuns are done.
        """
        if self.is_running:
            return False

        def done_or_blocked(action_run):
            return action_run.is_done or self._is_run_blocked(action_run)
        return all(done_or_blocked(run) for run in self.action_runs)

    @property
    def is_failed(self):
        """Return True if there are failed actions and all ActionRuns are
        done or blocked.
        """
        return self.is_done and any(run.is_failed for run in self.action_runs)

    @property
    def is_complete_without_cleanup(self):
        return all(run.is_complete for run in self.action_runs)

    @property
    def names(self):
        return self.run_map.keys()

    @property
    def end_time(self):
        if not self.is_done:
            return None
        end_times = (
            run.end_time for run in self.get_action_runs_with_cleanup()
        )
        return iteration.max_filter(end_times)

    def __str__(self):
        def blocked_state(action_run):
            return ":blocked" if self._is_run_blocked(action_run) else ""

        run_states = ', '.join(
            "%s(%s%s)" % (a.action_name, a.state, blocked_state(a))
            for a in six.itervalues(self.run_map)
        )
        return "%s[%s]" % (self.__class__.__name__, run_states)

    def __getattr__(self, name):
        return self.proxy_action_runs_with_cleanup.perform(name)

    def __getitem__(self, name):
        return self.run_map[name]

    def __contains__(self, name):
        return name in self.run_map

    def __iter__(self):
        return six.itervalues(self.run_map)

    def get(self, name):
        return self.run_map.get(name)
