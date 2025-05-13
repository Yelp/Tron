import logging
from typing import Iterable
from typing import List
from typing import Tuple

from tron.core.actionrun import ActionRun
from tron.core.actionrun import KubernetesActionRun
from tron.core.actionrun import MesosActionRun
from tron.core.actionrun import SSHActionRun
from tron.core.jobrun import JobRun

log = logging.getLogger(__name__)


def filter_action_runs_needing_recovery(
    action_runs: Iterable[ActionRun],
) -> Tuple[List[SSHActionRun], List[MesosActionRun], List[KubernetesActionRun]]:
    ssh_runs: List[SSHActionRun] = []
    mesos_runs: List[MesosActionRun] = []
    kubernetes_runs: List[KubernetesActionRun] = []
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


def launch_recovery_actionruns_for_job_runs(job_runs: Iterable[JobRun], master_action_runner: object) -> None:
    for run in job_runs:
        if not run._action_runs:
            log.info(f"Skipping recovery of {run} with no action runs (may have been cleaned up)")
            continue

        ssh_runs, mesos_runs, kubernetes_runs = filter_action_runs_needing_recovery(run._action_runs)

        all_runs_to_recover: List[ActionRun] = []
        all_runs_to_recover.extend(ssh_runs)
        all_runs_to_recover.extend(mesos_runs)
        all_runs_to_recover.extend(kubernetes_runs)

        for action_run in all_runs_to_recover:
            action_run.recover()
