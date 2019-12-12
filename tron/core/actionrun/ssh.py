"""
 tron.core.actionrun.ssh
"""
import logging

from twisted.internet import reactor

from tron import node
from tron.actioncommand import ActionCommand
from tron.actioncommand import NoActionRunnerFactory
from tron.core.actionrun.base import ActionRun
from tron.serialize import filehandler
from tron.utils.observer import Observer

log = logging.getLogger(__name__)
MAX_RECOVER_TRIES = 5
INITIAL_RECOVER_DELAY = 3


class SSHActionRun(ActionRun, Observer):
    """An ActionRun that executes the command on a node through SSH.
    """

    def __init__(self, *args, **kwargs):
        super(SSHActionRun, self).__init__(*args, **kwargs)
        self.recover_tries = 0

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

    def handle_unknown(self):
        if isinstance(self.action_runner, NoActionRunnerFactory):
            log.info(
                f"Unable to recover action_run {self.id}: "
                "action_run has no action_runner"
            )
            return self.fail_unknown()

        if self.recover_tries >= MAX_RECOVER_TRIES:
            log.info(f'Reached maximum tries {MAX_RECOVER_TRIES} for recovering {self.id}')
            return self.fail_unknown()

        desired_delay = INITIAL_RECOVER_DELAY * (3 ** self.recover_tries)
        self.recover_tries += 1
        log.info(f'Starting try #{self.recover_tries} to recover {self.id}, waiting {desired_delay}')
        return self.do_recover(delay=desired_delay)

    def recover(self):
        log.info(f"Creating recovery run for actionrun {self.id}")
        if isinstance(self.action_runner, NoActionRunnerFactory):
            log.info(
                f"Unable to recover action_run {self.id}: "
                "action_run has no action_runner"
            )
            return None

        if not self.machine.check('running'):
            log.error(
                f'Unable to transition action run {self.id} '
                f'from {self.machine.state} to running. '
                f'Only UNKNOWN actions can be recovered. '
            )
            return None

        return self.do_recover(delay=0)

    def do_recover(self, delay):
        recovery_command = f"{self.action_runner.exec_path}/recover_batch.py {self.action_runner.status_path}/{self.id}/status"

        # Might not need a separate action run
        # Using for the separate name
        recovery_run = SSHActionRun(
            job_run_id=self.job_run_id,
            name=f"recovery-{self.id}",
            node=self.node,
            bare_command=recovery_command,
            output_path=self.output_path,
        )
        recovery_action_command = recovery_run.build_action_command()
        recovery_action_command.write_stdout(
            f"Recovering action run {self.id}",
        )
        # Put action command in "running" state so if it fails to connect
        # and exits with no exit code, the real action run will not retry.
        recovery_action_command.started()

        # this line is where the magic happens.
        # the action run watches another actioncommand,
        # and updates its internal state according to its result.
        self.watch(recovery_action_command)

        self.exit_status = None
        self.end_time = None
        self.machine.transition('running')

        # Still want the action to appear running while we're waiting to submit the recovery
        # So we do the delay at the end, after the transition to 'running' above
        if not delay:
            return self.submit_recovery_command(recovery_run, recovery_action_command)
        else:
            return reactor.callLater(delay, self.submit_recovery_command, recovery_run, recovery_action_command)

    def submit_recovery_command(self, recovery_run, recovery_action_command):
        log.info(
            f"Submitting recovery job with command {recovery_action_command.command} "
            f"to node {recovery_run.node}"
        )
        try:
            deferred = recovery_run.node.submit_command(recovery_action_command)
            deferred.addCallback(
                lambda x: log.info(f"Completed recovery run {recovery_run.id}")
            )
            return True
        except node.Error as e:
            log.warning(f"Failed to submit recovery for {self.id}: {e!r}")

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
                return self.handle_unknown()

            if not action_command.exit_status:
                return self.success()

            return self._exit_unsuccessful(action_command.exit_status)

    handler = handle_action_command_state_change
