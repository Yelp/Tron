import logging

from tron.core.actionrun import ActionRun
from tron.core.actionrun import KubernetesActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun


log = logging.getLogger(__name__)


def filter_action_runs_needing_recovery(action_runs):
    ssh_runs = []
    mesos_runs = []
    kubernetes_runs = []
    for action_run in action_runs:
        if isinstance(action_run, SSHActionRun):
            if action_run.state == ActionRun.UNKNOWN:
                ssh_runs.append(action_run)
        elif isinstance(action_run, MesosActionRun):
            if action_run.state == ActionRun.UNKNOWN and action_run.end_time is None:
                mesos_runs.append(action_run)
        elif isinstance(action_run, KubernetesActionRun):
            if action_run.state == ActionRun.UNKNOWN and action_run.end_time is None:
                kubernetes_runs.append(action_run)
    return ssh_runs, mesos_runs, kubernetes_runs


def launch_recovery_actionruns_for_job_runs(job_runs, master_action_runner):
    for run in job_runs:
        if not run._action_runs:
            log.info(f"Skipping recovery of {run} with no action runs (may have been cleaned up)")
            continue

        # TODO: Why do we do this separately if we just need to call recover()
        ssh_runs, mesos_runs, kubernetes_runs = filter_action_runs_needing_recovery(run._action_runs)
        for action_run in ssh_runs:
            action_run.recover()

        for action_run in mesos_runs:
            action_run.recover()

        for action_run in kubernetes_runs:
            action_run.recover()
