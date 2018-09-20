"""
 tron.core.actionrun
"""
import logging

import six
from twisted.internet import reactor

from tron import command_context
from tron import node
from tron.actioncommand import ActionCommand
from tron.actioncommand import NoActionRunnerFactory
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.config.config_utils import StringFormatter
from tron.config.schema import ExecutorTypes
from tron.core import action
from tron.mesos import MesosClusterRepository
from tron.serialize import filehandler
from tron.utils import iteration
from tron.utils import maybe_decode
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
    def build_action_run_collection(cls, job_run, action_runner, eventbus_publish):
        """Create an ActionRunGraph from an ActionGraph and JobRun."""
        action_map = six.iteritems(job_run.action_graph.get_action_map())
        action_run_map = {
            maybe_decode(name): cls.build_run_for_action(
                job_run,
                action_inst,
                action_runner,
                eventbus_publish,
            )
            for name, action_inst in action_map
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def action_run_collection_from_state(
        cls,
        job_run,
        runs_state_data,
        cleanup_action_state_data,
        eventbus_publish,
    ):
        action_runs = [
            cls.action_run_from_state(job_run, state_data, eventbus_publish)
            for state_data in runs_state_data
        ]
        if cleanup_action_state_data:
            action_runs.append(
                cls.action_run_from_state(
                    job_run,
                    cleanup_action_state_data,
                    eventbus_publish,
                    cleanup=True,
                ),
            )

        action_run_map = {
            maybe_decode(action_run.action_name): action_run
            for action_run in action_runs
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def build_run_for_action(cls, job_run, action, action_runner, eventbus_publish):
        """Create an ActionRun for a JobRun and Action."""
        run_node = action.node_pool.next(
        ) if action.node_pool else job_run.node

        args = {
            'job_run_id': job_run.id,
            'name': action.name,
            'node': run_node,
            'eventbus_publish': eventbus_publish,
            'bare_command': action.command,
            'parent_context': job_run.context,
            'output_path': job_run.output_path.clone(),
            'cleanup': action.is_cleanup,
            'action_runner': action_runner,
            'retries_remaining': action.retries,
            'retries_delay': action.retries_delay,
            'executor': action.executor,
            'cpus': action.cpus,
            'mem': action.mem,
            'constraints': action.constraints,
            'docker_image': action.docker_image,
            'docker_parameters': action.docker_parameters,
            'env': action.env,
            'extra_volumes': action.extra_volumes,
            'trigger_downstreams': action.trigger_downstreams,
            'triggered_by': action.triggered_by,
            'on_upstream_rerun': action.on_upstream_rerun,
        }
        if action.executor == ExecutorTypes.mesos:
            return MesosActionRun(**args)
        return SSHActionRun(**args)

    @classmethod
    def action_run_from_state(cls, job_run, state_data, eventbus_publish, cleanup=False):
        """Restore an ActionRun for this JobRun from the state data."""
        args = {
            'state_data': state_data,
            'parent_context': job_run.context,
            'output_path': job_run.output_path.clone(),
            'job_run_node': job_run.node,
            'cleanup': cleanup,
            'eventbus_publish': eventbus_publish,
        }

        if state_data.get('executor') == ExecutorTypes.mesos:
            return MesosActionRun.from_state(**args)
        return SSHActionRun.from_state(**args)


class ActionRun(object):
    """Base class for tracking the state of a single run of an Action.

    ActionRuns are observed by a parent JobRun.
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

    STATE_UNKNOWN['running'] = STATE_RUNNING

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
    FAILED_RENDER = 'false # Command failed to render correctly. See the Tron error log.'

    context_class = command_context.ActionRunContext

    # TODO: create a class for ActionRunId, JobRunId, Etc
    def __init__(
        self,
        job_run_id,
        name,
        node,
        eventbus_publish,
        bare_command=None,
        parent_context=None,
        output_path=None,
        cleanup=False,
        start_time=None,
        end_time=None,
        run_state=STATE_SCHEDULED,
        rendered_command=None,
        exit_status=None,
        action_runner=None,
        retries_remaining=None,
        retries_delay=None,
        exit_statuses=None,
        machine=None,
        executor=None,
        cpus=None,
        mem=None,
        constraints=None,
        docker_image=None,
        docker_parameters=None,
        env=None,
        extra_volumes=None,
        mesos_task_id=None,
        trigger_downstreams=None,
        triggered_by=None,
        on_upstream_rerun=None,
    ):
        self.job_run_id = maybe_decode(job_run_id)
        self.action_name = maybe_decode(name)
        self.node = node
        self.eventbus_publish = eventbus_publish
        self.start_time = start_time
        self.end_time = end_time
        self.exit_status = exit_status
        self.bare_command = maybe_decode(bare_command)
        self.rendered_command = rendered_command
        self.action_runner = action_runner or NoActionRunnerFactory()
        self.machine = machine or state.StateMachine(
            self.STATE_SCHEDULED,
            delegate=self,
            force_state=run_state,
        )
        self.is_cleanup = cleanup
        self.executor = executor
        self.cpus = cpus
        self.mem = mem
        self.constraints = constraints
        self.docker_image = docker_image
        self.docker_parameters = docker_parameters
        self.env = env
        self.extra_volumes = extra_volumes
        self.mesos_task_id = mesos_task_id
        self.output_path = output_path or filehandler.OutputPath()
        self.output_path.append(self.id)
        self.context = command_context.build_context(self, parent_context)
        self.retries_remaining = retries_remaining
        self.retries_delay = retries_delay
        self.exit_statuses = exit_statuses
        self.trigger_downstreams = trigger_downstreams
        self.triggered_by = triggered_by
        self.on_upstream_rerun = on_upstream_rerun

        if self.exit_statuses is None:
            self.exit_statuses = []

        self.action_command = None
        self.in_delay = None

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
        cls,
        state_data,
        parent_context,
        output_path,
        job_run_node,
        eventbus_publish,
        cleanup=False,
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
            state_data.get('node_name'),
            job_run_node,
        )

        action_runner_data = state_data.get('action_runner')
        if action_runner_data:
            action_runner = SubprocessActionRunnerFactory(**action_runner_data)
        else:
            action_runner = NoActionRunnerFactory()

        rendered_command = state_data.get('rendered_command')
        run = cls(
            job_run_id=job_run_id,
            name=action_name,
            node=job_run_node,
            eventbus_publish=eventbus_publish,
            parent_context=parent_context,
            output_path=output_path,
            rendered_command=rendered_command,
            bare_command=state_data['command'],
            cleanup=cleanup,
            start_time=state_data['start_time'],
            end_time=state_data['end_time'],
            run_state=state.named_event_by_name(
                ActionRun.STATE_SCHEDULED,
                state_data['state'],
            ),
            exit_status=state_data.get('exit_status'),
            retries_remaining=state_data.get('retries_remaining'),
            retries_delay=state_data.get('retries_delay'),
            exit_statuses=state_data.get('exit_statuses'),
            action_runner=action_runner,
            executor=state_data.get('executor', ExecutorTypes.ssh),
            cpus=state_data.get('cpus'),
            mem=state_data.get('mem'),
            constraints=state_data.get('constraints'),
            docker_image=state_data.get('docker_image'),
            docker_parameters=state_data.get('docker_parameters'),
            env=state_data.get('env'),
            extra_volumes=state_data.get('extra_volumes'),
            mesos_task_id=state_data.get('mesos_task_id'),
            trigger_downstreams=state_data.get('trigger_downstreams'),
            triggered_by=state_data.get('triggered_by'),
            on_upstream_rerun=state_data.get('on_upstream_rerun'),
        )

        # Transition running to fail unknown because exit status was missed
        if run.is_running:
            run._done('fail_unknown')
        if run.is_starting:
            run._exit_unsuccessful(None)
        return run

    def start(self):
        """Start this ActionRun."""
        if self.in_delay is not None:
            log.warning(
                f"Start of suspended action run {self.id}, cancelling suspend timer"
            )
            self.in_delay.cancel()
            self.in_delay = None

        if not self.machine.check('start'):
            return False

        if len(self.exit_statuses) == 0:
            log.info("Starting action run %s", self.id)
        else:
            log.info(
                "Restarting action run {}, retry {}".format(
                    self.id,
                    len(self.exit_statuses),
                )
            )

        self.start_time = timeutils.current_time()
        self.machine.transition('start')

        if not self.is_valid_command:
            log.error(
                "Command for action run %s is invalid: %r",
                self.id,
                self.bare_command,
            )
            self.fail(-1)
            return

        return self.submit_command()

    def submit_command(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def kill(self, final=True):
        raise NotImplementedError()

    def _done(self, target, exit_status=0):
        log.info(
            "Action run %s completed with %s and exit status %r",
            self.id,
            target,
            exit_status,
        )
        # TODO: - state machine already does .check()
        #       - only set exit_status / end_time if transition succeeds?
        if self.machine.check(target):
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            return self.machine.transition(target)
        else:
            log.debug(f"{self} failed transition {self.state} -> {target}")

    def retry(self):
        """Invoked externally (via API) when action needs to be re-tried
        manually.
        """
        if self.retries_remaining is None or self.retries_remaining <= 0:
            self.retries_remaining = 1

        if self.is_done:
            return self._exit_unsuccessful(self.exit_status)
        else:
            log.info(f"Killing action run {self.id} for a retry")
            return self.kill(final=False)

    def start_after_delay(self):
        log.info(f"Resuming action run {self.id} after retry delay")
        self.machine.reset()
        self.in_delay = None
        self.start()

    def restart(self):
        """Used by `fail` when action run has to be re-tried."""
        if self.retries_delay is not None:
            self.in_delay = reactor.callLater(
                self.retries_delay.seconds, self.start_after_delay
            )
            log.info(
                f"Delaying action run {self.id} for a retry in {self.retries_delay}s"
            )
        else:
            self.machine.reset()
            return self.start()

    def fail(self, exit_status=None):
        if self.retries_remaining:
            self.retries_remaining = -1

        return self._done('fail', exit_status)

    def _exit_unsuccessful(self, exit_status=None):
        if self.retries_remaining is not None:
            if self.retries_remaining > 0:
                self.retries_remaining -= 1
                self.exit_statuses.append(exit_status)
                return self.restart()
            else:
                log.info(
                    "Reached maximum number of retries: {}".format(
                        len(self.exit_statuses),
                    )
                )
        return self.fail(exit_status)

    def emit_triggers(self):
        if isinstance(self.trigger_downstreams, bool):
            shortdate = self.render_template("{shortdate}")
            triggers = [f"shortdate.{shortdate}"]
        elif isinstance(self.trigger_downstreams, dict):
            rendered = [
                (k, self.render_template(v))
                for k, v in self.trigger_downstreams.items()
            ]
            triggers = [f"{key}.{value}" for key, value in rendered]
        else:
            log.error(f"{self} trigger_downstreams must be true or dict")
            return
        log.info(f"{self} publishing triggers: [{', '.join(triggers)}]")
        for trigger in triggers:
            # self.id in here to make the log message above more concise
            self.eventbus_publish(f"{self.id}.{trigger}")

    def success(self):
        if self.trigger_downstreams:
            self.emit_triggers()
        return self._done('success')

    def fail_unknown(self):
        """Failed with unknown reason."""
        log.warning("Lost communication with action run %s", self.id)
        return self.machine.transition('fail_unknown')

    def cancel_delay(self):
        if self.in_delay is not None:
            self.in_delay.cancel()
            self.in_delay = None
            self.fail(-3)
            return True

    @property
    def state_data(self):
        """This data is used to serialize the state of this action run."""
        rendered_command = self.rendered_command

        action_runner = None if type(
            self.action_runner
        ) == NoActionRunnerFactory else {
            'status_path': self.action_runner.status_path,
            'exec_path': self.action_runner.exec_path,
        }
        # Freeze command after it's run
        command = rendered_command if rendered_command else self.bare_command
        return {
            'job_run_id': self.job_run_id,
            'action_name': self.action_name,
            'state': self.state.name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'command': command,
            'rendered_command': self.rendered_command,
            'node_name': self.node.get_name() if self.node else None,
            'exit_status': self.exit_status,
            'retries_remaining': self.retries_remaining,
            'retries_delay': self.retries_delay,
            'exit_statuses': self.exit_statuses,
            'action_runner': action_runner,
            'executor': self.executor,
            'cpus': self.cpus,
            'mem': self.mem,
            'constraints': self.constraints,
            'docker_image': self.docker_image,
            'docker_parameters': self.docker_parameters,
            'env': self.env,
            'extra_volumes': self.extra_volumes,
            'mesos_task_id': self.mesos_task_id,
            'trigger_downstreams': self.trigger_downstreams,
            'triggered_by': self.triggered_by,
            'on_upstream_rerun': self.on_upstream_rerun,
        }

    def render_template(self, template):
        """Render our configured command using the command context."""
        return StringFormatter(self.context).format(template)

    def render_command(self):
        """Render our configured command using the command context."""
        return self.render_template(self.bare_command)

    @property
    def command(self):
        if self.rendered_command:
            return self.rendered_command

        try:
            self.rendered_command = self.render_command()
        except Exception as e:
            log.error(
                "Failed generating rendering command: %s: %s" %
                (e.__class__.__name__, e)
            )

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


class SSHActionRun(ActionRun, Observer):
    """An ActionRun that executes the command on a node through SSH.
    """

    def __init__(self, *args, **kwargs):
        super(SSHActionRun, self).__init__(*args, **kwargs)

    def submit_command(self):
        action_command = self.build_action_command()
        try:
            self.node.submit_command(action_command)
        except node.Error as e:
            log.warning("Failed to start %s: %r", self.id, e)
            self._exit_unsuccessful(-2)
            return
        return True

    def stop(self):
        if self.retries_remaining is not None:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        stop_command = self.action_runner.build_stop_action_command(
            self.id,
            'terminate',
        )
        self.node.submit_command(stop_command)

    def kill(self, final=True):
        if self.retries_remaining is not None and final:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        kill_command = self.action_runner.build_stop_action_command(
            self.id,
            'kill',
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
            return self._exit_unsuccessful(None)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                return self.fail_unknown()

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)

    handler = handle_action_command_state_change


class MesosActionRun(ActionRun, Observer):
    """An ActionRun that executes the command on a Mesos cluster.
    """

    def submit_command(self):
        serializer = filehandler.OutputStreamSerializer(self.output_path)
        mesos_cluster = MesosClusterRepository.get_cluster()
        task = mesos_cluster.create_task(
            action_run_id=self.id,
            command=self.command,
            cpus=self.cpus,
            mem=self.mem,
            constraints=self.constraints,
            docker_image=self.docker_image,
            docker_parameters=self.docker_parameters,
            env=self.env,
            extra_volumes=self.extra_volumes,
            serializer=serializer,
        )
        if not task:  # Mesos is disabled
            self.fail(None)
            return

        self.mesos_task_id = task.get_mesos_id()

        # Watch before submitting, in case submit causes a transition
        self.watch(task)
        mesos_cluster.submit(task)
        return task

    def recover(self):
        if self.mesos_task_id is None:
            log.error(f'No task ID, cannot recover {self}')
            return

        if not self.machine.check('running'):
            log.error(
                f'Unable to transition {self} from {self.machine.state}'
                'to running for recovery'
            )
            return

        log.info(f'Recovering Mesos run {self}')

        serializer = filehandler.OutputStreamSerializer(self.output_path)
        mesos_cluster = MesosClusterRepository.get_cluster()
        task = mesos_cluster.create_task(
            action_run_id=self.id,
            command=self.command,
            cpus=self.cpus,
            mem=self.mem,
            constraints=self.constraints,
            docker_image=self.docker_image,
            docker_parameters=self.docker_parameters,
            env=self.env,
            extra_volumes=self.extra_volumes,
            serializer=serializer,
            task_id=self.mesos_task_id,
        )
        if not task:
            log.warning(
                f'Cannot recover {self}, Mesos is disabled or invalid task ID'
            )
            self.fail_unknown()
            return

        self.watch(task)
        mesos_cluster.recover(task)

        # Reset status
        self.exit_status = None
        self.end_time = None
        self.machine.transition('running')

        return task

    def stop(self):
        if self.retries_remaining is not None:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        return self._kill_mesos_task()

    def kill(self, final=True):
        if self.retries_remaining is not None and final:
            self.retries_remaining = -1

        if self.cancel_delay():
            return

        return self._kill_mesos_task()

    def _kill_mesos_task(self):
        msgs = []
        if not self.is_active:
            msgs.append(f'Action is {self.state}, not running. Continuing anyway.')

        mesos_cluster = MesosClusterRepository.get_cluster()
        if self.mesos_task_id is None:
            msgs.append("Error: Can't find task id for the action.")
        else:
            msgs.append(f"Sending kill for {self.mesos_task_id}...")
            succeeded = mesos_cluster.kill(self.mesos_task_id)
            if succeeded:
                msgs.append("Sent! It can take up to docker_stop_timeout (current setting is 2 mins) to stop.")
            else:
                msgs.append("Error while sending kill request. Please try again.")

        return '\n'.join(msgs)

    def handle_action_command_state_change(self, action_command, event):
        """Observe ActionCommand state changes."""
        # TODO: consolidate? Same as SSHActionRun for now
        log.debug("Action command state change: %s", action_command.state)

        if event == ActionCommand.RUNNING:
            return self.machine.transition('started')

        if event == ActionCommand.FAILSTART:
            return self._exit_unsuccessful(None)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                return self.fail_unknown()

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)

    handler = handle_action_command_state_change


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
            self.get_action_runs_with_cleanup,
            [
                proxy.attr_proxy('is_running', any),
                proxy.attr_proxy('is_starting', any),
                proxy.attr_proxy('is_scheduled', any),
                proxy.attr_proxy('is_cancelled', any),
                proxy.attr_proxy('is_active', any),
                proxy.attr_proxy('is_queued', all),
                proxy.attr_proxy('is_complete', all),
                proxy.func_proxy('queue', iteration.list_all),
                proxy.func_proxy('cancel', iteration.list_all),
                proxy.func_proxy('success', iteration.list_all),
                proxy.func_proxy('fail', iteration.list_all),
                proxy.func_proxy('ready', iteration.list_all),
                proxy.func_proxy('cleanup', iteration.list_all),
                proxy.func_proxy('stop', iteration.list_all),
                proxy.attr_proxy('start_time', iteration.min_filter),
                proxy.attr_proxy('state_data', iteration.list_all),
            ],
        )

    def action_runs_for_actions(self, actions):
        return (
            self.run_map[a.name] for a in actions if a.name in self.run_map
        )

    def get_action_runs_with_cleanup(self):
        return six.itervalues(self.run_map)

    action_runs_with_cleanup = property(get_action_runs_with_cleanup)

    def get_action_runs(self):
        return (
            run for run in six.itervalues(self.run_map) if not run.is_cleanup
        )

    action_runs = property(get_action_runs)

    @property
    def cleanup_action_run(self) -> ActionRun:
        return self.run_map.get(action.CLEANUP_ACTION_NAME)

    @property
    def state_data(self):
        return [run.state_data for run in self.action_runs]

    @property
    def cleanup_action_state_data(self):
        if self.cleanup_action_run:
            return self.cleanup_action_run.state_data

    def get_startable_action_runs(self):
        """Returns any actions that are scheduled or queued that can be run."""

        return [
            r for r in self.action_runs
            if r.check_state('start') and not self._is_run_blocked(r)
        ]

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
            "%s(%s%s)" % (
                a.action_name,
                a.state,
                blocked_state(a),
            ) for a in six.itervalues(self.run_map)
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
