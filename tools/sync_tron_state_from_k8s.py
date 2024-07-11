"""
Update tron state from k8s api if tron has not yet updated correctly

 Usage:
    python tools/sync_tron_state_from_k8s.py -c <kubeconfig_path> (--do-work|--num-runs N|--tronctl-wrapper tronctl-pnw-devc)

This will search for completed pods in the cluster specified in the kubeconfig in the `tron` namespace and use tronctl to transition any whose states do not match.
"""
import argparse
import base64
import hashlib
import logging
import subprocess
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from kubernetes.client import V1Pod
from task_processing.plugins.kubernetes.kube_client import KubeClient

from tron.commands.client import Client
from tron.commands.cmd_utils import get_client_config

POD_STATUS_TO_TRON_STATE = {
    "Succeeded": "success",
    "Failed": "fail",
    "Unknown": "Unknown",  # This should never really happen
}

TRON_MODIFIABLE_STATES = [
    "starting",  # stuck jobs
    "running",  # stuck jobs
    "unknown",
    "lost",
]

log = logging.getLogger("sync_tron_from_k8s")


# NOTE: Copied from paasta_tools.kubernetes_tools, if it changes there it must be updated here
def limit_size_with_hash(name: str, limit: int = 63, suffix: int = 4) -> str:
    """Returns `name` unchanged if it's length does not exceed the `limit`.
    Otherwise, returns truncated `name` with it's hash of size `suffix`
    appended.

    base32 encoding is chosen as it satisfies the common requirement in
    various k8s names to be alphanumeric.
    """
    if len(name) > limit:
        digest = hashlib.md5(name.encode()).digest()
        hash = base64.b32encode(digest).decode().replace("=", "").lower()
        return f"{name[:(limit-suffix-1)]}-{hash[:suffix]}"
    else:
        return name


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig-path", dest="kubeconfig_path", help="KUBECONFIG path")
    parser.add_argument(
        "--do-work",
        dest="do_work",
        action="store_true",
        default=False,
        help="Actually modify tron actions that need updating; without this flag we will only print those that would be updated",
    )
    parser.add_argument("--tron-url", default=None, help="Tron url (default will read from paasta tron config)")
    parser.add_argument(
        "--tronctl-wrapper",
        default="tronctl",
        dest="tronctl_wrapper",
        help="Tronctl wrapper to use (will not use wrapper by default)",
    )
    parser.add_argument("-n", "--num-runs", dest="num_runs", default=100, help="Maximum number of job runs to retrieve")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Verbose logging")
    args = parser.parse_args()

    # tron's base level is critical, not info, adjust accoringly
    if args.verbose:
        level = logging.DEBUG
        tron_level = logging.WARN
    else:
        level = logging.INFO
        tron_level = logging.CRITICAL

    logging.basicConfig(level=level, stream=sys.stdout)

    tron_client_logger = logging.getLogger("tron.commands.client")
    tron_client_logger.setLevel(tron_level)

    # We also don't want kube_client debug logs
    kube_logger = logging.getLogger("kubernetes.client.rest")
    kube_logger.setLevel(logging.INFO)

    return args


def fetch_pods(kubeconfig_path: str) -> Dict[str, V1Pod]:
    kube_client = KubeClient(kubeconfig_path=kubeconfig_path, user_agent="sync_tron_state_from_k8s")

    # Bit of a hack, no helper to fetch pods so reach into core api
    completed_pod_list = kube_client.core.list_namespaced_pod(
        namespace="tron",
    )

    return {pod.metadata.name: pod for pod in completed_pod_list.items}


def get_tron_state_from_api(tron_server: str, num_runs: int = 100) -> List[Dict[str, Dict[Any, Any]]]:
    if not tron_server:
        client_config = get_client_config()
        tron_server = client_config.get("server", "http://localhost:8089")
    client = Client(tron_server)
    # /jobs returns only the latest 5 runs, we'll need to request all runs instead ourselves
    jobs = client.jobs(
        include_job_runs=False,
        include_action_runs=False,
        include_action_graph=False,
        include_node_pool=False,
    )

    for job in jobs:
        # Update job URL to be used with API instead of web
        url = f'/api{job["url"]}'
        log.debug(f'Fetching job {job["name"]} at {url}')
        job_runs = client.job(
            url,
            include_action_runs=True,
            count=num_runs,  # TODO: fetch job run_limit and use that for count ?
        )
        job["runs"] = job_runs["runs"]
    return jobs


def get_matching_pod(action_run: Dict[str, Any], pods: Dict[str, V1Pod]) -> Optional[V1Pod]:
    """Given a tron action_run, try to find the right pod that matches."""
    action_name = action_run["action_name"]
    job_name = action_run["job_name"]
    run_num = action_run["run_num"]
    service, job = job_name.split(".")
    instance_name = f"{job}.{action_name}"
    sanitized_instance_name = limit_size_with_hash(instance_name)
    matching_pods = sorted(
        [
            pod
            for pod in pods.values()
            if pod.metadata.labels["paasta.yelp.com/service"] == service
            and pod.metadata.labels["paasta.yelp.com/instance"] == sanitized_instance_name
            and pod.metadata.labels["tron.yelp.com/run_num"] == run_num
        ],
        # If action has retries, there will be multiple pods w/ same job_run; we only want the latest
        key=lambda pod: pod.metadata.creation_timestamp,
        reverse=True,
    )
    return (
        matching_pods[0] if matching_pods and matching_pods[0].status.phase in POD_STATUS_TO_TRON_STATE.keys() else None
    )


def get_desired_state_from_pod(pod: V1Pod) -> str:
    k8s_state = pod.status.phase
    return POD_STATUS_TO_TRON_STATE.get(k8s_state, "NoMatch")


def update_tron_from_pods(
    jobs: List[Dict[str, Any]], pods: Dict[str, V1Pod], tronctl_wrapper: str = "tronctl", do_work: bool = False
):
    updated = []
    error = []
    for job in jobs:
        if job["runs"]:
            # job_runs
            for job_run in job["runs"]:
                # actions for this job_run
                for action in job_run.get("runs", []):
                    action_run_id = action["id"]
                    if action["state"] in TRON_MODIFIABLE_STATES:
                        pod = get_matching_pod(action, pods)
                        if pod:
                            desired_state = get_desired_state_from_pod(pod)
                            if action["state"] != desired_state:
                                log.debug(f'{action_run_id} state {action["state"]} needs updating to {desired_state}')
                                cmd = [tronctl_wrapper, desired_state, action_run_id]
                                if do_work:
                                    # tronctl-$cluster success/fail svc.job.run.action
                                    try:
                                        log.info(f"Running {cmd}")
                                        proc = subprocess.run(cmd, capture_output=True, text=True)
                                        if proc.returncode != 0:
                                            log.error(f"Got non-zero exit code: {proc.returncode}")
                                            log.error(f"\t{proc.stderr}")
                                            error.append(action_run_id)
                                        updated.append(action_run_id)
                                    except Exception:
                                        log.exception("ERROR: Hit exception:")
                                        error.append(action_run_id)
                                else:
                                    log.info(f"Dry-Run: Would run {cmd}")
                                    updated.append(action_run_id)
                        else:
                            log.debug(f"action run {action_run_id} not found in list of finished pods, no action taken")
                    else:
                        log.debug(f'Action state {action["state"]} for {action_run_id} not modifiable, no action taken')
    log.info(f"Updated {len(updated)} actions: {','.join(updated)}")
    log.info(f"Hit {len(error)} errors on actions: {','.join(error)}")
    return {"updated": updated, "error": error}


if __name__ == "__main__":
    args = parse_args()

    jobs = get_tron_state_from_api(args.tron_url, args.num_runs)
    log.debug(f"Found {len(jobs)} jobs.")

    pods = fetch_pods(args.kubeconfig_path)
    log.debug(f"Found {len(pods.keys())} pods.")

    update_tron_from_pods(jobs, pods, args.tronctl_wrapper, args.do_work)
