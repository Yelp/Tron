"""
Update tron state from k8s api if tron has not yet updated correctly

 Usage:
    python tools/sync_tron_state_from_k8s.py -c <kubeconfig_path> (--do-work|--num-runs N|--tronctl-wrapper tronctl-pnw-devc)

This will search for completed pods in the cluster specified in the kubeconfig in the `tron` namespace and use tronctl to transition any whose states do not match.
"""
import argparse
import subprocess
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
    args = parser.parse_args()

    return args


def fetch_completed_pods(kubeconfig_path: str) -> Dict[str, V1Pod]:
    kube_client = KubeClient(kubeconfig_path=kubeconfig_path, user_agent="sync_tron_state_from_k8s")

    # Bit of a hack, no helper to fetch pods so reach into core api
    completed_pod_list = kube_client.core.list_namespaced_pod(
        namespace="tron", field_selector="status.phase!=Running,status.phase!=Pending"
    )

    return {pod.metadata.name: pod for pod in completed_pod_list.items}


def get_tron_state_from_api(tron_server: str, num_runs: int = 100) -> Dict[str, Dict[any, any]]:
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
        # What am I doing wrong here, why do I have to append /api
        url = f'/api{job["url"]}'
        print(f'Fetching job {job["name"]} at {url}')
        try:
            job_runs = client.job(
                url,
                include_action_runs=True,  # action runs
                count=num_runs,  # TODO: fetch job run_limit and use that for count ?
            )
            job["runs"] = job_runs["runs"]
        except Exception as e:
            print(f"Hit exception: {e}")
    return jobs


def get_matching_pod(action_run: Dict[str, any], pods: Dict[str, V1Pod]) -> Optional[V1Pod]:
    """Given a tron action_run, try to find the right pod that matches."""
    action_name = action_run["action_name"]
    job_name = action_run["job_name"]
    run_num = action_run["run_num"]

    service, job = job_name.split(".")
    # TODO:  how to fetch k8s shortened instance name to match labels?
    instance_name = f"{job}.{action_name}"
    # If action has retries, there will be multiple pods w/ same job_run; we only want the latest
    matching_pods = sorted(
        [
            pod
            for pod in pods.values()
            if pod.metadata.labels["paasta.yelp.com/service"] == service
            and pod.metadata.labels["paasta.yelp.com/instance"] == instance_name
            and pod.metadata.labels["tron.yelp.com/run_num"] == run_num
        ],
        key=lambda pod: pod.metadata.creation_timestamp,
        reverse=True,
    )
    return matching_pods[0] if matching_pods else None


def get_desired_state_from_pod(pod: V1Pod) -> str:
    k8s_state = pod.status.phase
    return POD_STATUS_TO_TRON_STATE.get(k8s_state, "NoMatch")


def update_tron_from_pods(
    jobs: List[Dict[str, Any]], pods: Dict[str, V1Pod], tronctl_wrapper: str = "tronctl", do_work: bool = True
):
    updated = []
    error = []
    # todo: calculate whether there are more jobs in pnw-prod than completed pods
    for job in jobs:
        if job["runs"]:
            # job_runs
            for job_run in job["runs"]:
                for action in job_run.get("runs", []):
                    action_run_id = action["id"]
                    if action["state"] in TRON_MODIFIABLE_STATES:
                        pod = get_matching_pod(action, pods)
                        if pod:
                            desired_state = get_desired_state_from_pod(pod)
                            if action["state"] != desired_state:
                                print(f'{action_run_id} state {action["state"]} needs updating to {desired_state}')
                                cmd = [tronctl_wrapper, desired_state, action_run_id]
                                if do_work:
                                    # tronctl-$cluster success/fail svc.job.run.action
                                    try:
                                        print(f"Running {cmd}")
                                        proc = subprocess.run(cmd, capture_output=True, text=True)
                                        if proc.returncode != 0:
                                            print(f"Got non-zero exit code: {proc.returncode}")
                                            print(f"\t{proc.stderr}")
                                            error.append(action_run_id)
                                        updated.append(action_run_id)
                                    except Exception as e:
                                        print(f"ERROR: Hit exception: {repr(e)}")
                                        error.append(action_run_id)
                                else:
                                    print(f"Dry-Run: Would run {cmd}")
                                    updated.append(action_run_id)
                        else:
                            print(f"action run {action_run_id} not found in list of finished pods, no action taken")
                    else:
                        print(f'Action state {action["state"]} for {action_run_id} not modifiable, no action taken')
    print(f"Updated {len(updated)} actions: {','.join(updated)}")
    print(f"Hit {len(error)} errors on actions: {','.join(error)}")
    return {"updated": updated, "error": error}


if __name__ == "__main__":
    args = parse_args()

    jobs = get_tron_state_from_api(args.tron_url, args.num_runs)
    print(f"Found {len(jobs)} jobs.")

    pods = fetch_completed_pods(args.kubeconfig_path)

    update_tron_from_pods(jobs, pods, args.tronctl_wrapper, args.do_work)
