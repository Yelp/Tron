from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron.actioncommand import NoActionRunnerFactory
from tron.core.actionrun import ActionRun
from tron.core.actionrun import SSHActionRun

RECOVERY_BINARY = "/work/bin/recover_batch.py"

log = logging.getLogger(__name__)


def filter_action_runs_needing_recovery(action_runs):
    return [
        action_run for action_run in action_runs
        if action_run.state == ActionRun.STATE_UNKNOWN
    ]


def build_recovery_command(recovery_binary, path):
    return "%s %s" % (recovery_binary, path)


def recover_action_run(action_run):
    log.info("creating recovery run")
    if action_run.action_runner == NoActionRunnerFactory:
        log.info(
            "unable to recover action_run %s: action_run has no action_runner" % action_run.id,
        )
        return None

    recovery_run = SSHActionRun(
        job_run_id=action_run.job_run_id,
        name="recovery-%s" % action_run.id,
        node=action_run.node,
        bare_command=build_recovery_command(
            recovery_binary=RECOVERY_BINARY,
            path="%s/%s/status" % (
                action_run.action_runner.status_path,
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
    action_run.watch(recovery_action_command)

    log.info(
        "submitting recovery job with command %s to node %s" % (
            recovery_action_command.command,
            recovery_run.node,
        )
    )
    deferred = recovery_run.node.submit_command(recovery_action_command, )
    deferred.addCallback(
        lambda x: log.info("completed recovery run %s" % recovery_run.id, )
    )
    return deferred
