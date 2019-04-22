"""
 tron.core.actionrun
"""
import datetime
import logging
from typing import List

from twisted.internet import reactor

from tron import command_context
from tron import node
from tron.actioncommand import ActionCommand
from tron.actioncommand import NoActionRunnerFactory
from tron.actioncommand import SubprocessActionRunnerFactory
from tron.config.config_utils import StringFormatter
from tron.config.schema import ExecutorTypes
from tron.core import action
from tron.eventbus import EventBus
from tron.mesos import MesosClusterRepository
from tron.serialize import filehandler
from tron.utils import maybe_decode
from tron.utils import proxy
from tron.utils import timeutils
from tron.utils.observer import Observable
from tron.utils.observer import Observer
from tron.utils.state import Machine

log = logging.getLogger(__name__)


class ActionRunFactory(object):
    """Construct ActionRuns and ActionRunCollections for a JobRun and
    ActionGraph.
    """

    @classmethod
    def build_action_run_collection(cls, job_run, action_runner):
        """Create an ActionRunCollection from an ActionGraph and JobRun."""
        action_run_map = {
            maybe_decode(name): cls.build_run_for_action(
                job_run,
                action_inst,
                action_runner,
            )
            for name, action_inst in job_run.action_graph.action_map.items()
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def action_run_collection_from_state(
        cls,
        job_run,
        runs_state_data,
        cleanup_action_state_data,
    ):
        action_runs = [
            cls.action_run_from_state(job_run, state_data)
            for state_data in runs_state_data
        ]
        if cleanup_action_state_data:
            action_runs.append(
                cls.action_run_from_state(
                    job_run,
                    cleanup_action_state_data,
                    cleanup=True,
                ),
            )

        action_run_map = {
            maybe_decode(action_run.action_name): action_run
            for action_run in action_runs
        }
        return ActionRunCollection(job_run.action_graph, action_run_map)

    @classmethod
    def build_run_for_action(cls, job_run, action, action_runner):
        """Create an ActionRun for a JobRun and Action."""
        run_node = action.node_pool.next(
        ) if action.node_pool else job_run.node

        if action.trigger_timeout:
            trigger_timeout = job_run.run_time + action.trigger_timeout
        else:
            trigger_timeout = job_run.run_time + datetime.timedelta(days=1)

        args = {
            'job_run_id': job_run.id,
            'name': action.name,
            'node': run_node,
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
            'disk': action.disk,
            'constraints': action.constraints,
            'docker_image': action.docker_image,
            'docker_parameters': action.docker_parameters,
            'env': action.env,
            'extra_volumes': action.extra_volumes,
            'trigger_downstreams': action.trigger_downstreams,
            'triggered_by': action.triggered_by,
            'on_upstream_rerun': action.on_upstream_rerun,
            'trigger_timeout_timestamp': trigger_timeout.timestamp(),
        }
        if action.executor == ExecutorTypes.mesos.value:
            return MesosActionRun(**args)
        return SSHActionRun(**args)

    @classmethod
    def action_run_from_state(cls, job_run, state_data, cleanup=False):
        """Restore an ActionRun for this JobRun from the state data."""
        args = {
            'state_data': state_data,
            'parent_context': job_run.context,
            'output_path': job_run.output_path.clone(),
            'job_run_node': job_run.node,
            'cleanup': cleanup,
        }

        if state_data.get('executor') == ExecutorTypes.mesos.value:
            return MesosActionRun.from_state(**args)
        return SSHActionRun.from_state(**args)


class ActionRun(Observable):
    """Base class for tracking the state of a single run of an Action.

    ActionRun's state machine is observed by a parent JobRun.
    """

    CANCELLED = 'cancelled'
    FAILED = 'failed'
    QUEUED = 'queued'
    RUNNING = 'running'
    SCHEDULED = 'scheduled'
    SKIPPED = 'skipped'
    STARTING = 'starting'
    SUCCEEDED = 'succeeded'
    WAITING = 'waiting'
    UNKNOWN = 'unknown'

    default_transitions = dict(fail=FAILED, success=SUCCEEDED)
    STATE_MACHINE = Machine(
        SCHEDULED,
        **{
            CANCELLED:
                dict(skip=SKIPPED),
            FAILED:
                dict(skip=SKIPPED),
            RUNNING:
                dict(fail_unknown=UNKNOWN, **default_transitions),
            STARTING:
                dict(started=RUNNING, fail=FAILED, fail_unknown=UNKNOWN),
            UNKNOWN:
                dict(running=RUNNING, fail_unknown=UNKNOWN, **default_transitions),
            WAITING:
                dict(
                    cancel=CANCELLED,
                    start=STARTING,
                    **default_transitions,
                ),
            QUEUED:
                dict(
                    ready=WAITING,
                    cancel=CANCELLED,
                    start=STARTING,
                    schedule=SCHEDULED,
                    **default_transitions,
                ),
            SCHEDULED:
                dict(
                    ready=WAITING,
                    queue=QUEUED,
                    cancel=CANCELLED,
                    start=STARTING,
                    **default_transitions,
                ),
        }
    )

    # The set of states that are considered end states. Technically some of
    # these states can be manually transitioned to other states.
    END_STATES = {FAILED, SUCCEEDED, CANCELLED, SKIPPED, UNKNOWN}

    # Failed render command is false to ensure that it will fail when run
    FAILED_RENDER = 'false # Command failed to render correctly. See the Tron error log.'
    NOTIFY_TRIGGER_READY = 'trigger_ready'

    EXIT_INVALID_COMMAND = -1
    EXIT_NODE_ERROR = -2
    EXIT_STOP_KILL = -3
    EXIT_TRIGGER_TIMEOUT = -4
    EXIT_MESOS_DISABLED = -5

    EXIT_REASONS = {
        EXIT_INVALID_COMMAND: 'Invalid command',
        EXIT_NODE_ERROR: 'Node error',
        EXIT_STOP_KILL: 'Stopped or killed',
        EXIT_TRIGGER_TIMEOUT: 'Timed out waiting for trigger',
        EXIT_MESOS_DISABLED: 'Mesos disabled',
    }

    context_class = command_context.ActionRunContext

    # TODO: create a class for ActionRunId, JobRunId, Etc
    def __init__(
        self,
        job_run_id,
        name,
        node,
        bare_command=None,
        parent_context=None,
        output_path=None,
        cleanup=False,
        start_time=None,
        end_time=None,
        run_state=SCHEDULED,
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
        disk=None,
        constraints=None,
        docker_image=None,
        docker_parameters=None,
        env=None,
        extra_volumes=None,
        mesos_task_id=None,
        trigger_downstreams=None,
        triggered_by=None,
        on_upstream_rerun=None,
        trigger_timeout_timestamp=None,
    ):
        super().__init__()
        self.job_run_id = maybe_decode(job_run_id)
        self.action_name = maybe_decode(name)
        self.node = node
        self.start_time = start_time
        self.end_time = end_time
        self.exit_status = exit_status
        self.bare_command = maybe_decode(bare_command)
        self.rendered_command = rendered_command
        self.action_runner = action_runner or NoActionRunnerFactory()
        self.machine = machine or Machine.from_machine(
            ActionRun.STATE_MACHINE, None, run_state
        )
        self.is_cleanup = cleanup
        self.executor = executor
        self.cpus = cpus
        self.mem = mem
        self.disk = disk
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
        self.trigger_timeout_timestamp = trigger_timeout_timestamp
        self.trigger_timeout_call = None

        if self.exit_statuses is None:
            self.exit_statuses = []

        self.action_command = None
        self.in_delay = None

    @property
    def state(self):
        return self.machine.state

    @property
    def id(self):
        return f"{self.job_run_id}.{self.action_name}"

    @property
    def name(self):
        return self.action_name

    @classmethod
    def from_state(
        cls,
        state_data,
        parent_context,
        output_path,
        job_run_node,
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

        run = cls(
            job_run_id=job_run_id,
            name=action_name,
            node=job_run_node,
            parent_context=parent_context,
            output_path=output_path,
            rendered_command=maybe_decode(state_data.get('rendered_command')),
            bare_command=maybe_decode(state_data['command']),
            cleanup=cleanup,
            start_time=state_data['start_time'],
            end_time=state_data['end_time'],
            run_state=state_data['state'],
            exit_status=state_data.get('exit_status'),
            retries_remaining=state_data.get('retries_remaining'),
            retries_delay=state_data.get('retries_delay'),
            exit_statuses=state_data.get('exit_statuses'),
            action_runner=action_runner,
            executor=state_data.get('executor', ExecutorTypes.ssh.value),
            cpus=state_data.get('cpus'),
            mem=state_data.get('mem'),
            disk=state_data.get('disk'),
            constraints=state_data.get('constraints'),
            docker_image=state_data.get('docker_image'),
            docker_parameters=state_data.get('docker_parameters'),
            env=state_data.get('env'),
            extra_volumes=state_data.get('extra_volumes'),
            mesos_task_id=state_data.get('mesos_task_id'),
            trigger_downstreams=state_data.get('trigger_downstreams'),
            triggered_by=state_data.get('triggered_by'),
            on_upstream_rerun=state_data.get('on_upstream_rerun'),
            trigger_timeout_timestamp=state_data.get('trigger_timeout_timestamp'),
        )

        # Transition running to fail unknown because exit status was missed
        # Recovery will look for unknown runs
        if run.is_active:
            run.transition_and_notify('fail_unknown')
        return run

    def start(self):
        """Start this ActionRun."""
        if self.in_delay is not None:
            log.warning(f"{self} cancelling suspend timer")
            self.in_delay.cancel()
            self.in_delay = None

        if not self.machine.check('start'):
            return False

        if len(self.exit_statuses) == 0:
            log.info(f"{self} starting")
        else:
            log.info(f"{self} restarting, retry {len(self.exit_statuses)}")

        self.start_time = timeutils.current_time()
        self.transition_and_notify('start')

        if not self.is_valid_command:
            log.error(f"{self} invalid command: {self.bare_command}")
            self.fail(self.EXIT_INVALID_COMMAND)
            return

        return self.submit_command()

    def submit_command(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def kill(self, final=True):
        raise NotImplementedError()

    def _done(self, target, exit_status=0):
        if self.machine.check(target):
            if self.triggered_by:
                EventBus.clear_subscriptions(self.__hash__())
            self.clear_trigger_timeout()
            self.exit_status = exit_status
            self.end_time = timeutils.current_time()
            log.info(
                f"{self} completed with {target}, transitioned to "
                f"{self.state}, exit status: {exit_status}"
            )
            return self.transition_and_notify(target)
        else:
            log.debug(
                f"{self} cannot transition from {self.state} via {target}"
            )

    def retry(self):
        """Invoked externally (via API) when action needs to be re-tried
        manually.
        """
        if self.retries_remaining is None or self.retries_remaining <= 0:
            self.retries_remaining = 1

        if self.is_done:
            return self._exit_unsuccessful(self.exit_status)
        else:
            log.info(f"{self} getting killed for a retry")
            return self.kill(final=False)

    def start_after_delay(self):
        log.info(f"{self} resuming after retry delay")
        self.machine.reset()
        self.in_delay = None
        self.start()

    def restart(self):
        """Used by `fail` when action run has to be re-tried."""
        if self.retries_delay is not None:
            self.in_delay = reactor.callLater(
                self.retries_delay.total_seconds(), self.start_after_delay
            )
            log.info(f"{self} delaying for a retry in {self.retries_delay}s")
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
        if exit_status is None:
            return self._done('fail_unknown', exit_status)
        else:
            return self._done('fail', exit_status)

    def triggers_to_emit(self) -> List[str]:
        if not self.trigger_downstreams:
            return []

        if isinstance(self.trigger_downstreams, bool):
            templates = ["shortdate.{shortdate}"]
        elif isinstance(self.trigger_downstreams, dict):
            templates = [
                f"{k}.{v}" for k, v in self.trigger_downstreams.items()
            ]
        else:
            log.error(f"{self} trigger_downstreams must be true or dict")

        return [self.render_template(trig) for trig in templates]

    def emit_triggers(self):
        triggers = self.triggers_to_emit()
        if not triggers:
            return

        log.info(f"{self} publishing triggers: [{', '.join(triggers)}]")
        job_id = '.'.join(self.job_run_id.split('.')[:-1])
        for trigger in triggers:
            EventBus.publish(f"{job_id}.{self.action_name}.{trigger}")

    # TODO: cache if safe
    @property
    def rendered_triggers(self) -> List[str]:
        return [self.render_template(trig) for trig in self.triggered_by or []]

    # TODO: subscribe for events and maintain a list of remaining triggers
    @property
    def remaining_triggers(self):
        return [
            trig for trig in self.rendered_triggers
            if not EventBus.has_event(trig)
        ]

    def success(self):
        if self.trigger_downstreams:
            self.emit_triggers()
        return self._done('success')

    def fail_unknown(self):
        """Failed with unknown reason."""
        log.warning(f"{self} failed with no exit code")
        return self._done('fail_unknown', None)

    def cancel_delay(self):
        if self.in_delay is not None:
            self.in_delay.cancel()
            self.in_delay = None
            self.fail(self.EXIT_STOP_KILL)
            return True

    @property
    def state_data(self):
        """This data is used to serialize the state of this action run."""
        rendered_command = self.rendered_command

        if isinstance(self.action_runner, NoActionRunnerFactory):
            action_runner = None
        else:
            action_runner = dict(
                status_path=self.action_runner.status_path,
                exec_path=self.action_runner.exec_path,
            )
        # Freeze command after it's run
        command = rendered_command if rendered_command else self.bare_command
        return {
            'job_run_id': self.job_run_id,
            'action_name': self.action_name,
            'state': self.state,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'command': maybe_decode(command),
            'rendered_command': maybe_decode(self.rendered_command),
            'node_name': self.node.get_name() if self.node else None,
            'exit_status': self.exit_status,
            'retries_remaining': self.retries_remaining,
            'retries_delay': self.retries_delay,
            'exit_statuses': self.exit_statuses,
            'action_runner': action_runner,
            'executor': self.executor,
            'cpus': self.cpus,
            'mem': self.mem,
            'disk': self.disk,
            'constraints': self.constraints,
            'docker_image': self.docker_image,
            'docker_parameters': self.docker_parameters,
            'env': self.env,
            'extra_volumes': self.extra_volumes,
            'mesos_task_id': self.mesos_task_id,
            'trigger_downstreams': self.trigger_downstreams,
            'triggered_by': self.triggered_by,
            'on_upstream_rerun': self.on_upstream_rerun,
            'trigger_timeout_timestamp': self.trigger_timeout_timestamp,
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
            log.error(f"{self} failed rendering command: {e}")
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
        self.clear_observers()
        if self.triggered_by:
            EventBus.clear_subscriptions(self.__hash__())
        self.clear_trigger_timeout()
        self.cancel()

    def clear_trigger_timeout(self):
        if self.trigger_timeout_call:
            self.trigger_timeout_call.cancel()
            self.trigger_timeout_call = None

    def setup_subscriptions(self):
        remaining_triggers = self.remaining_triggers
        if not remaining_triggers:
            return

        if self.trigger_timeout_timestamp:
            now = timeutils.current_time().timestamp()
            delay = max(self.trigger_timeout_timestamp - now, 1)
            self.trigger_timeout_call = reactor.callLater(
                delay, self.trigger_timeout_reached
            )
        else:
            log.error(f"{self} has no trigger_timeout_timestamp")

        for trigger in remaining_triggers:
            EventBus.subscribe(trigger, self.__hash__(), self.trigger_notify)

    def trigger_timeout_reached(self):
        if self.remaining_triggers:
            self.trigger_timeout_call = None
            log.warning(
                f"{self} reached timeout waiting for: {self.remaining_triggers}"
            )
            self.fail(self.EXIT_TRIGGER_TIMEOUT)
        else:
            self.notify(ActionRun.NOTIFY_TRIGGER_READY)

    def trigger_notify(self, *_):
        if not self.remaining_triggers:
            self.clear_trigger_timeout()
            self.notify(ActionRun.NOTIFY_TRIGGER_READY)

    @property
    def is_blocked_on_trigger(self):
        return not self.is_done and bool(self.remaining_triggers)

    def __getattr__(self, name: str):
        """Support convenience properties for checking if this ActionRun is in
        a specific state (Ex: self.is_running would check if self.state is
        STATE_RUNNING) or for transitioning to a new state (ex: ready).
        """
        if name in self.machine.transition_names:
            return lambda: self.transition_and_notify(name)

        if name.startswith('is_'):
            state_name = name.replace('is_', '')
            if state_name not in self.machine.states:
                raise AttributeError(f"{name} is not a state")
            return self.state == state_name
        else:
            raise AttributeError(name)

    def __str__(self):
        return f"ActionRun: {self.id}"

    def transition_and_notify(self, target):
        if self.machine.transition(target):
            self.notify(self.state)
            return True


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
            self._exit_unsuccessful(self.EXIT_NODE_ERROR)
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
        log.debug(
            f"{self} action_command state change: {action_command.state}"
        )

        if event == ActionCommand.RUNNING:
            return self.transition_and_notify('started')

        if event == ActionCommand.FAILSTART:
            return self._exit_unsuccessful(self.EXIT_NODE_ERROR)

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
    def _create_mesos_task(self, mesos_cluster, serializer, task_id=None):
        return mesos_cluster.create_task(
            action_run_id=self.id,
            command=self.command,
            cpus=self.cpus,
            mem=self.mem,
            disk=1024.0 if self.disk is None else self.disk,
            constraints=[[c.attribute, c.operator, c.value]
                         for c in self.constraints],
            docker_image=self.docker_image,
            docker_parameters=[e._asdict() for e in self.docker_parameters],
            env=self.env,
            extra_volumes=[e._asdict() for e in self.extra_volumes],
            serializer=serializer,
            task_id=task_id,
        )

    def submit_command(self):
        serializer = filehandler.OutputStreamSerializer(self.output_path)
        mesos_cluster = MesosClusterRepository.get_cluster()
        task = self._create_mesos_task(mesos_cluster, serializer)
        if not task:  # Mesos is disabled
            self.fail(self.EXIT_MESOS_DISABLED)
            return

        self.mesos_task_id = task.get_mesos_id()

        # Watch before submitting, in case submit causes a transition
        self.watch(task)
        mesos_cluster.submit(task)
        return task

    def recover(self):
        if not self.machine.check('running'):
            log.error(
                f'{self} unable to transition from {self.machine.state}'
                'to running for recovery'
            )
            return

        if self.mesos_task_id is None:
            log.error(f'{self} no task ID, cannot recover')
            self.fail_unknown()
            return

        log.info(f'{self} recovering Mesos run')

        serializer = filehandler.OutputStreamSerializer(self.output_path)
        mesos_cluster = MesosClusterRepository.get_cluster()
        task = self._create_mesos_task(
            mesos_cluster,
            serializer,
            self.mesos_task_id,
        )
        if not task:
            log.warning(
                f'{self} cannot recover, Mesos is disabled or '
                f'invalid task ID {self.mesos_task_id!r}'
            )
            self.fail_unknown()
            return

        self.watch(task)
        mesos_cluster.recover(task)

        # Reset status
        self.exit_status = None
        self.end_time = None
        self.transition_and_notify('running')

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
            msgs.append(
                f'Action is {self.state}, not running. Continuing anyway.'
            )

        mesos_cluster = MesosClusterRepository.get_cluster()
        if self.mesos_task_id is None:
            msgs.append("Error: Can't find task id for the action.")
        else:
            msgs.append(f"Sending kill for {self.mesos_task_id}...")
            succeeded = mesos_cluster.kill(self.mesos_task_id)
            if succeeded:
                msgs.append(
                    "Sent! It can take up to docker_stop_timeout (current setting is 2 mins) to stop."
                )
            else:
                msgs.append(
                    "Error while sending kill request. Please try again."
                )

        return '\n'.join(msgs)

    def handle_action_command_state_change(self, action_command, event):
        """Observe ActionCommand state changes."""
        log.debug(
            f"{self} action_command state change: {action_command.state}"
        )

        if event == ActionCommand.RUNNING:
            return self.transition_and_notify('started')

        if event == ActionCommand.FAILSTART:
            return self._exit_unsuccessful(action_command.exit_status)

        if event == ActionCommand.EXITING:
            if action_command.exit_status is None:
                # This is different from SSHActionRun
                # Allows retries to happen, if configured
                return self._exit_unsuccessful(None)

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)

    handler = handle_action_command_state_change


def min_filter(seq):
    seq = list(filter(None, seq))
    return min(seq) if any(seq) else None


def eager_all(seq):
    return all(list(seq))


class ActionRunCollection(object):
    """A collection of ActionRuns used by a JobRun."""

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
                proxy.attr_proxy('is_waiting', any),
                proxy.attr_proxy('is_queued', all),
                proxy.attr_proxy('is_complete', all),
                proxy.func_proxy('queue', eager_all),
                proxy.func_proxy('cancel', eager_all),
                proxy.func_proxy('success', eager_all),
                proxy.func_proxy('fail', eager_all),
                proxy.func_proxy('ready', eager_all),
                proxy.func_proxy('cleanup', eager_all),
                proxy.func_proxy('stop', eager_all),
                proxy.attr_proxy('start_time', min_filter),
                proxy.attr_proxy('state_data', eager_all),
            ],
        )

    def action_runs_for_actions(self, actions):
        return (
            self.run_map[a.name] for a in actions if a.name in self.run_map
        )

    def get_action_runs_with_cleanup(self):
        return self.run_map.values()

    action_runs_with_cleanup = property(get_action_runs_with_cleanup)

    def get_action_runs(self):
        return (run for run in self.run_map.values() if not run.is_cleanup)

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
            if r.machine.check('start') and not self._is_run_blocked(r)
        ]

    @property
    def has_startable_action_runs(self):
        return any(self.get_startable_action_runs())

    def _is_run_blocked(self, action_run, in_job_only=False):
        """Returns True if the ActionRun is waiting on a required run to
        finish before it can run.

        If in_job_only is True, only considers required actions in this job,
        not triggers.
        """
        if action_run.is_done or action_run.is_active:
            return False

        required_actions = self.action_graph.get_dependencies(
            action_run.action_name,
        )

        if required_actions:
            required_runs = self.action_runs_for_actions(required_actions)
            if any(not run.is_complete for run in required_runs):
                return True

        if action_run.is_blocked_on_trigger and not in_job_only:
            return True

        return False

    @property
    def is_blocked_on_trigger(self):
        return any(r.is_blocked_on_trigger for r in self.action_runs)

    @property
    def is_done(self):
        """Returns True when there are no running ActionRuns and all
        non-blocked ActionRuns are done.
        """
        if self.is_running:
            return False

        def done_or_blocked(action_run):
            # Can't make progress if blocked by actions in the job, and other actions are done.
            # On the other hand, not necessarily done if still waiting for cross-job dependencies.
            return action_run.is_done or self._is_run_blocked(action_run, in_job_only=True)

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
        end_times = list(
            run.end_time
            for run in self.get_action_runs_with_cleanup()
            if run.end_time
        )
        return max(end_times) if any(end_times) else None

    def __str__(self):
        def blocked_state(action_run):
            return ":blocked" if self._is_run_blocked(action_run) else ""

        run_states = ', '.join(
            f"{a.action_name}({a.state}{blocked_state(a)})"
            for a in self.run_map.values()
        )
        return f"{self.__class__.__name__}[{run_states}]"

    def __getattr__(self, name):
        return self.proxy_action_runs_with_cleanup.perform(name)

    def __getitem__(self, name):
        return self.run_map[name]

    def __contains__(self, name):
        return name in self.run_map

    def __iter__(self):
        return iter(self.run_map.values())

    def get(self, name):
        return self.run_map.get(name)
