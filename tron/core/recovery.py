from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron.actioncommand import NoActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun

log = logging.getLogger(__name__)


def filter_action_runs_needing_recovery(action_runs):
    ssh_runs = []
    mesos_runs = []
    for action_run in action_runs:
        if isinstance(action_run, SSHActionRun):
            if action_run.state == ActionRun.UNKNOWN:
                ssh_runs.append(action_run)
        elif isinstance(action_run, MesosActionRun):
            if action_run.state == ActionRun.UNKNOWN and action_run.end_time is None:
                mesos_runs.append(action_run)
    return ssh_runs, mesos_runs


def build_recovery_command(recovery_binary, path):
    return f"{recovery_binary} {path}"


def recover_action_run(action_run, action_runner):
    log.info(f"Creating recovery run for actionrun {action_run.id}")
    if type(action_runner) == NoActionRunnerFactory:
        log.info(
            f"Unable to recover action_run {action_run.id}: "
            "action_run has no action_runner"
        )
        return None

    recovery_run = SSHActionRun(
        job_run_id=action_run.job_run_id,
        name=f"recovery-{action_run.id}",
        node=action_run.node,
        bare_command=build_recovery_command(
            recovery_binary=f"{action_runner.exec_path}/recover_batch.py",
            path=f"{action_runner.status_path}/{action_run.id}/status",
        ),
        output_path=action_run.output_path,
    )
    recovery_action_command = recovery_run.build_action_command()
    recovery_action_command.write_stdout(
        f"Recovering action run {action_run.id}",
    )
    # Put action command in "running" state so if it fails to connect
    # and exits with no exit code, the real action run will not retry.
    recovery_action_command.started()

    # this line is where the magic happens.
    # the action run watches another actioncommand,
    # and updates its internal state according to its result.
    action_run.watch(recovery_action_command)

    if not action_run.machine.check('running'):
        log.error(
            f'Unable to transition action run {action_run.id} '
            f'from {action_run.machine.state} to start'
        )
    else:
        action_run.exit_status = None
        action_run.end_time = None
        action_run.machine.transition('running')

    log.info(
        f"Submitting recovery job with command {recovery_action_command.command} "
        f"to node {recovery_run.node}"
    )
    deferred = recovery_run.node.submit_command(recovery_action_command)
    deferred.addCallback(
        lambda x: log.info(f"Completed recovery run {recovery_run.id}")
    )
    return deferred


def launch_recovery_actionruns_for_job_runs(job_runs, master_action_runner):
    for run in job_runs:
        if not run._action_runs:
            log.info(f'Skipping recovery of {run} with no action runs (may have been cleaned up)')
            continue

        ssh_runs, mesos_runs = filter_action_runs_needing_recovery(run._action_runs)
        for action_run in ssh_runs:
            if type(action_run.action_runner) == NoActionRunnerFactory and \
               type(master_action_runner) != NoActionRunnerFactory:
                action_runner = master_action_runner
            else:
                action_runner = action_run.action_runner
            deferred = recover_action_run(action_run, action_runner)
            if not deferred:
                log.debug("unable to recover action run %s" % action_run.id)

        for action_run in mesos_runs:
            action_run.recover()
