from typing import Dict
from unittest import mock

import pytest
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodStatus

from tools.sync_tron_state_from_k8s import get_matching_pod
from tools.sync_tron_state_from_k8s import get_tron_state_from_api
from tools.sync_tron_state_from_k8s import update_tron_from_pods


def create_mock_pod(name: str, phase: str, labels: Dict[str, str], creation_timestamp: str):
    metadata = V1ObjectMeta(name=name, creation_timestamp=creation_timestamp, labels=labels)
    status = V1PodStatus(phase=phase)
    return V1Pod(metadata=metadata, status=status)


class TestSyncTronStateFromK8s:
    @pytest.fixture(autouse=True)
    def setup_test_data(self):
        # oops why did I make this a dict
        self.pods = {
            p.metadata.name: p
            for p in [
                create_mock_pod(
                    "service.job.2.action",
                    "Succeeded",
                    {
                        "paasta.yelp.com/service": "service",
                        "paasta.yelp.com/instance": "job.action",
                        "tron.yelp.com/run_num": "2",
                    },
                    "2024-01-01T00:00:00",
                ),
                create_mock_pod(
                    "service.job.3.action-nomatch",
                    "Failed",
                    {
                        "paasta.yelp.com/service": "service",
                        "paasta.yelp.com/instance": "job.action",
                        "tron.yelp.com/run_num": "3",
                    },
                    "2024-01-01T00:00:00",
                ),
                create_mock_pod(
                    "service.job.4.action-nomatch",
                    "Failed",
                    {
                        "paasta.yelp.com/service": "service",
                        "paasta.yelp.com/instance": "job.action",
                        "tron.yelp.com/run_num": "4",
                    },
                    "2024-01-01T00:00:00",
                ),
                create_mock_pod(
                    "service.job.4.action-nomatch-retry2",
                    "Succeeded",
                    {
                        "paasta.yelp.com/service": "service",
                        "paasta.yelp.com/instance": "job.action",
                        "tron.yelp.com/run_num": "4",
                    },
                    "2024-01-01T01:00:00",
                ),
                create_mock_pod(
                    "service.job2.10.action",
                    "Failed",
                    {
                        "paasta.yelp.com/service": "service",
                        "paasta.yelp.com/instance": "job2.action",
                        "tron.yelp.com/run_num": "10",
                    },
                    "2024-01-01T01:00:00",
                ),
                create_mock_pod(
                    "service.job2.10.action",
                    "Running",
                    {
                        "paasta.yelp.com/service": "service",
                        "paasta.yelp.com/instance": "job2.action",
                        "tron.yelp.com/run_num": "10",
                    },
                    "2024-01-01T01:05:00",
                ),
            ]
        }

    # 1 matching pod by labels
    # 2 matching pod by labels
    # no matching pod
    @pytest.mark.parametrize(
        "job_name,run_num,expected_pod_name",
        [
            ("service.job", "3", "service.job.3.action-nomatch"),
            ("service.job", "4", "service.job.4.action-nomatch-retry2"),
            ("service.job2", "10", None),
            ("service2.job", "1", None),
        ],
    )
    def test_get_matching_pod(self, job_name, run_num, expected_pod_name):
        test_action_run = {"action_name": "action", "job_name": f"{job_name}", "run_num": run_num}
        matching_pod = get_matching_pod(test_action_run, self.pods)
        assert matching_pod == self.pods.get(expected_pod_name)

    # verify we send correct num_runs
    # verify we are sending request for jobs + one for each job
    @mock.patch("tools.sync_tron_state_from_k8s.get_client_config", autospec=True)
    @mock.patch("tools.sync_tron_state_from_k8s.Client", autospec=True)
    def test_get_tron_state_from_api(self, mock_client, mock_get_client_config):
        mock_client.return_value = mock.Mock()
        mock_client.return_value.jobs.return_value = [{"url": "/uri", "name": "some job"}]
        mock_client.return_value.job.return_value = {"runs": []}
        mock_get_client_config.return_value = {"server": "https://localhost:8888"}
        get_tron_state_from_api(None, num_runs=10)

        mock_client.assert_called_with("https://localhost:8888")
        mock_client.return_value.jobs.assert_called_with(
            include_job_runs=False, include_action_runs=False, include_action_graph=False, include_node_pool=False
        )

        mock_client.return_value.job.assert_called_with("/api/uri", include_action_runs=True, count=10)

    @mock.patch("tools.sync_tron_state_from_k8s.subprocess.run", autospec=True)
    def test_update_tron(self, mock_subprocess_run):
        # sorry for the blob of test data
        tron_state = [
            {
                "name": "service.job",
                "runs": [
                    {
                        "runs": [
                            {
                                "id": "service.job.2.action",
                                "action_name": "action",
                                "run_num": "2",
                                "job_name": "service.job",
                                "state": "unknown",
                            }
                        ]
                    },
                    {
                        "runs": [
                            {
                                "id": "service.job.3.action",
                                "action_name": "action",
                                "run_num": "3",
                                "job_name": "service.job",
                                "state": "running",
                            },
                            {
                                "id": "service.job.3.action2",
                                "action_name": "action2",
                                "run_num": "3",
                                "job_name": "service.job",
                                "state": "running",
                            },
                        ]
                    },
                    {
                        "runs": [
                            {
                                "id": "service.job.4.action",
                                "action_name": "action",
                                "run_num": "4",
                                "job_name": "service.job",
                                "state": "starting",
                            }
                        ]
                    },
                    {
                        "runs": [
                            {
                                "id": "service.job.5.action",
                                "action_name": "action",
                                "run_num": "5",
                                "job_name": "service.job",
                                "state": "starting",
                            }
                        ]
                    },
                ],
            },
            {
                "name": "service.job2",
                "runs": [
                    {
                        "runs": [
                            {
                                "id": "service.job2.10.action",
                                "action_name": "action",
                                "run_num": "10",
                                "job_name": "service.job2",
                                "state": "succeeded",
                            },
                        ]
                    },
                ],
            },
        ]

        good_subprocess_run = mock.Mock(returncode=0)
        bad_subprocess_run = mock.Mock(returncode=1)

        expected_calls = [
            mock.call(["tronctl", "success", "service.job.2.action"], capture_output=True, text=True),
            mock.call(["tronctl", "fail", "service.job.3.action"], capture_output=True, text=True),
            mock.call(["tronctl", "success", "service.job.4.action"], capture_output=True, text=True),
        ]
        mock_subprocess_run.return_value = good_subprocess_run

        result = update_tron_from_pods(tron_state, self.pods, tronctl_wrapper="tronctl", do_work=True)

        assert result["updated"] == ["service.job.2.action", "service.job.3.action", "service.job.4.action"]
        assert result["error"] == []
        mock_subprocess_run.assert_has_calls(expected_calls, any_order=True)

        mock_subprocess_run.return_value = bad_subprocess_run
        result = update_tron_from_pods(tron_state, self.pods, tronctl_wrapper="tronctl", do_work=True)
        assert result["error"] == ["service.job.2.action", "service.job.3.action", "service.job.4.action"]
