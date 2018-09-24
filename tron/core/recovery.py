from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron.actioncommand import NoActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun

log = logging.getLogger(__name__)


def filter_action_runs_needing_recovery(action_runs):
    return [
        action_run for action_run in action_runs
        if action_run.state == ActionRun.UNKNOWN
    ]


def group_by_actionrun_type(action_runs):
    """
    Given a list of action_runs, group them by type.
    """
    ssh_runs = []
    mesos_runs = []
    for action_run in action_runs:
        if isinstance(action_run, SSHActionRun):
            ssh_runs.append(action_run)
        elif isinstance(action_run, MesosActionRun):
            mesos_runs.append(action_run)
    return ssh_runs, mesos_runs


def build_recovery_command(recovery_binary, path):
    return "%s %s" % (recovery_binary, path)


def recover_action_run(action_run, action_runner):
    log.info("creating recovery run for actionrun %s" % action_run.id)
    if type(action_runner) == NoActionRunnerFactory:
        log.info(
            "unable to recover action_run %s: action_run has no action_runner"
            % action_run.id,
        )
        return None

    recovery_run = SSHActionRun(
        job_run_id=action_run.job_run_id,
        name="recovery-%s" % action_run.id,
        node=action_run.node,
        bare_command=build_recovery_command(
            recovery_binary="%s/recover_batch.py" % (action_runner.exec_path),
            path="%s/%s/status" % (
                action_runner.status_path,
                action_run.id,
            ),
        ),
        output_path=action_run.output_path,
    )
    recovery_action_command = recovery_run.build_action_command()
    recovery_action_command.write_stdout(
        "recovering action run %s" % action_run.id,
    )

    # this line is where the magic happens.
    # the action run watches another actioncommand,
    # and updates its internal state according to its result.
    action_run.watch(recovery_action_command)

    if not action_run.machine.check('running'):
        log.error(
            'unable to transition action run %s from %s to start' %
            (action_run.id, action_run.machine.state)
        )
    else:
        action_run.exit_status = None
        action_run.end_time = None
        action_run.machine.transition('running')

    log.info(
        "submitting recovery job with command %s to node %s" % (
            recovery_action_command.command,
            recovery_run.node,
        )
    )
    deferred = recovery_run.node.submit_command(recovery_action_command)
    deferred.addCallback(
        lambda x: log.info("completed recovery run %s" % recovery_run.id)
    )
    return deferred


def launch_recovery_actionruns_for_job_runs(job_runs, master_action_runner):
    for run in job_runs:
        to_recover = filter_action_runs_needing_recovery(run._action_runs)
        ssh_runs, mesos_runs = group_by_actionrun_type(to_recover)
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
