import pytest

from tron.config.schema import ConfigAction
from tron.config.schema import ConfigConstraint
from tron.config.schema import ConfigFieldSelectorSource
from tron.config.schema import ConfigNodeAffinity
from tron.config.schema import ConfigParameter
from tron.config.schema import ConfigProjectedSAVolume
from tron.config.schema import ConfigSecretSource
from tron.config.schema import ConfigSecretVolume
from tron.config.schema import ConfigSecretVolumeItem
from tron.config.schema import ConfigTopologySpreadConstraints
from tron.config.schema import ConfigVolume
from tron.core.action import Action
from tron.core.action import ActionCommandConfig


class TestAction:
    @pytest.mark.parametrize("disk", [600.0, None])
    def test_from_config_full(self, disk):
        config = ConfigAction(
            name="ted",
            command="do something",
            node="first",
            executor="ssh",
            cpus=1,
            mem=100,
            disk=disk,  # default: 1024.0
            constraints=[
                ConfigConstraint(
                    attribute="pool",
                    operator="LIKE",
                    value="default",
                ),
            ],
            docker_image="fake-docker.com:400/image",
            docker_parameters=[
                ConfigParameter(
                    key="test",
                    value=123,
                ),
            ],
            env={"TESTING": "true"},
            secret_env={"TEST_SECRET": ConfigSecretSource(secret_name="tron-secret-svc-sec--A", key="sec_A")},
            secret_volumes=[
                ConfigSecretVolume(
                    secret_volume_name="secretvolumename",
                    secret_name="secret",
                    container_path="/b",
                    default_mode="0644",
                    items=[ConfigSecretVolumeItem(key="key", path="path", mode="0755")],
                ),
            ],
            extra_volumes=[
                ConfigVolume(
                    host_path="/tmp",
                    container_path="/nail/tmp",
                    mode="RO",
                ),
            ],
            trigger_downstreams=True,
            triggered_by=["foo.bar"],
        )
        new_action = Action.from_config(config)
        assert new_action.name == config.name
        assert new_action.node_pool is None
        assert new_action.executor == config.executor
        assert new_action.trigger_downstreams is True
        assert new_action.triggered_by == ["foo.bar"]

        command_config = new_action.command_config
        assert command_config.command == config.command
        assert command_config.cpus == config.cpus
        assert command_config.mem == config.mem
        assert command_config.disk == (600.0 if disk else 1024.0)
        assert command_config.constraints == {("pool", "LIKE", "default")}
        assert command_config.docker_image == config.docker_image
        assert command_config.docker_parameters == {("test", 123)}
        assert command_config.env == config.env
        assert command_config.secret_env == config.secret_env
        # cant do direct tuple equality, since this is not hashable
        assert command_config.secret_volumes == config.secret_volumes
        assert command_config.extra_volumes == {("/nail/tmp", "/tmp", "RO")}

    def test_from_config_none_values(self):
        config = ConfigAction(
            name="ted",
            command="do something",
            node="first",
            executor="ssh",
        )
        new_action = Action.from_config(config)
        assert new_action.name == config.name
        assert new_action.executor == config.executor
        command_config = new_action.command_config
        assert command_config.command == config.command
        assert command_config.constraints == set()
        assert command_config.docker_image is None
        assert command_config.docker_parameters == set()
        assert command_config.env == {}
        assert command_config.secret_env == {}
        assert command_config.secret_volumes == []
        assert command_config.extra_volumes == set()

    @pytest.fixture
    def action_command_config_json(self):
        raw_json = """
        {
            "command": "echo 'Hello, World!'",
            "cpus": 1.0,
            "mem": 512.0,
            "disk": 1024.0,
            "cap_add": ["NET_ADMIN"],
            "cap_drop": ["MKNOD"],
            "constraints": [
                {
                    "attribute": "pool",
                    "operator": "LIKE",
                    "value": "default"
                }
            ],
            "docker_image": "fake-docker.com:400/image",
            "docker_parameters": [
                {
                    "key": "test",
                    "value": 123
                }
            ],
            "env": {"TESTING": "true"},
            "secret_env": {
                "TEST_SECRET": {
                    "secret_name": "tron-secret-svc-sec--A",
                    "key": "sec_A"
                }
            },
            "secret_volumes": [
                {
                    "secret_volume_name": "secretvolumename",
                    "secret_name": "secret",
                    "container_path": "/b",
                    "default_mode": "0644",
                    "items": [
                        {
                            "key": "key",
                            "path": "path",
                            "mode": "0755"
                        }
                    ]
                }
            ],
            "projected_sa_volumes": [
                {
                    "container_path": "/var/run/secrets/whatever",
                    "audience": "for.bar.com",
                    "expiration_seconds": 3600
                }
            ],
            "extra_volumes": [
                {
                    "container_path": "/tmp",
                    "host_path": "/home/tmp",
                    "mode": "RO"
                }
            ],
            "node_affinities": [
                {
                    "key": "topology.kubernetes.io/zone",
                    "operator": "In",
                    "value": ["us-west-1a", "us-west-1c"]
                }
            ],
            "topology_spread_constraints": [
                {
                    "topology_key": "zone",
                    "max_skew": 1,
                    "when_unsatisfiable": "DoNotSchedule",
                    "label_selector": {
                        "match_labels": {
                            "app": "myapp"
                        }
                    }
                }
            ],
            "labels": {"app": "myapp"},
            "annotations": {"annotation_key": "annotation_value"},
            "service_account_name": "default",
            "ports": [8080, 9090],
            "field_selector_env": {
                "key": {
                    "field_path": "value"
                }
            },
            "node_selectors": {"key": "node-A"}
        }
        """
        return raw_json

    def test_action_command_config_from_json(self, action_command_config_json):
        result = ActionCommandConfig.from_json(action_command_config_json)

        expected = {
            "command": "echo 'Hello, World!'",
            "cpus": 1.0,
            "mem": 512.0,
            "disk": 1024.0,
            "cap_add": ["NET_ADMIN"],
            "cap_drop": ["MKNOD"],
            "constraints": [ConfigConstraint(attribute="pool", operator="LIKE", value="default")],
            "docker_image": "fake-docker.com:400/image",
            "docker_parameters": [ConfigParameter(key="test", value=123)],
            "env": {"TESTING": "true"},
            "secret_env": {"TEST_SECRET": ConfigSecretSource(secret_name="tron-secret-svc-sec--A", key="sec_A")},
            "secret_volumes": [
                ConfigSecretVolume(
                    secret_volume_name="secretvolumename",
                    secret_name="secret",
                    container_path="/b",
                    default_mode="0644",
                    items=[{"key": "key", "path": "path", "mode": "0755"}],
                )
            ],
            "projected_sa_volumes": [
                ConfigProjectedSAVolume(
                    container_path="/var/run/secrets/whatever",
                    audience="for.bar.com",
                    expiration_seconds=3600,
                )
            ],
            "extra_volumes": [ConfigVolume(container_path="/tmp", host_path="/home/tmp", mode="RO")],
            "node_affinities": [
                ConfigNodeAffinity(key="topology.kubernetes.io/zone", operator="In", value=["us-west-1a", "us-west-1c"])
            ],
            "topology_spread_constraints": [
                ConfigTopologySpreadConstraints(
                    topology_key="zone",
                    max_skew=1,
                    when_unsatisfiable="DoNotSchedule",
                    label_selector={"match_labels": {"app": "myapp"}},
                )
            ],
            "labels": {"app": "myapp"},
            "annotations": {"annotation_key": "annotation_value"},
            "service_account_name": "default",
            "ports": [8080, 9090],
            "node_selectors": {"key": "node-A"},
            "field_selector_env": {"key": ConfigFieldSelectorSource(field_path="value")},
        }

        assert result == expected
