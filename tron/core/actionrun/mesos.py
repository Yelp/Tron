"""
 tron.core.actionrun.mesos
"""
import logging

from tron.actioncommand import ActionCommand
from tron.bin.action_runner import build_environment
from tron.core.actionrun.base import ActionRun
from tron.mesos import MesosClusterRepository
from tron.serialize import filehandler
from tron.utils.observer import Observer

log = logging.getLogger(__name__)


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
            env=build_environment(original_env=self.env, run_id=self.id),
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
