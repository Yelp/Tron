import json

import pytest

from tron.config.schema import ConfigAction
from tron.config.schema import ConfigConstraint
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

    def action_command_config_json(self):
        def serialize_namedtuple(obj):
            if isinstance(obj, tuple) and hasattr(obj, "_fields"):
                # checks if obj is a tuple and convert it to a dict
                return obj._asdict()
            return obj

        constraints = [ConfigConstraint(attribute="pool", operator="LIKE", value="default")]
        docker_parameters = [ConfigParameter(key="test", value=123)]
        secret_env = {"TEST_SECRET": ConfigSecretSource(secret_name="tron-secret-svc-sec--A", key="sec_A")}
        extra_volumes = [ConfigVolume(container_path="/tmp", host_path="/home/tmp", mode="RO")]
        node_affinities = [
            ConfigNodeAffinity(key="topology.kubernetes.io/zone", operator="In", value=["us-west-1a", "us-west-1c"])
        ]
        toplogy_spread_contraints = [
            ConfigTopologySpreadConstraints(
                topology_key="zone",
                max_skew=1,
                when_unsatisfiable="DoNotSchedule",
                label_selector={"match_labels": {"app": "myapp"}},
            )
        ]
        return json.dumps(
            {
                "command": "echo 'Hello, World!'",
                "cpus": 1.0,
                "mem": 512.0,
                "disk": 1024.0,
                "cap_add": ["NET_ADMIN"],
                "cap_drop": ["MKNOD"],
                "constraints": [constraint._asdict() for constraint in constraints],
                "docker_image": "fake-docker.com:400/image",
                "docker_parameters": [parameter._asdict() for parameter in docker_parameters],
                "env": {"TESTING": "true"},
                "secret_env": {key: val._asdict() for key, val in secret_env.items()},
                "secret_volumes": [
                    {
                        "secret_volume_name": "secretvolumename",
                        "secret_name": "secret",
                        "container_path": "/b",
                        "default_mode": "0644",
                        "items": [{"key": "key", "path": "path", "mode": "0755"}],
                    }
                ],
                "projected_sa_volumes": [
                    {
                        "container_path": "/var/run/secrets/whatever",
                        "audience": "for.bar.com",
                        "expiration_seconds": 3600,
                    }
                ],
                "extra_volumes": [serialize_namedtuple(volume) for volume in extra_volumes],
                "node_affinities": [serialize_namedtuple(affinity) for affinity in node_affinities],
                "topology_spread_constraints": [
                    serialize_namedtuple(constraint) for constraint in toplogy_spread_contraints
                ],
                "labels": {"app": "myapp"},
                "annotations": {"annotation_key": "annotation_value"},
                "service_account_name": "default",
                "ports": [8080, 9090],
            }
        )

    def test_action_command_config_from_json(self):
        data = self.action_command_config_json()
        result = ActionCommandConfig.from_json(data)

        assert result["command"] == "echo 'Hello, World!'"
        assert result["cpus"] == 1.0
        assert result["mem"] == 512.0
        assert result["disk"] == 1024.0
        assert result["cap_add"] == ["NET_ADMIN"]
        assert result["cap_drop"] == ["MKNOD"]
        assert result["constraints"] == [ConfigConstraint(attribute="pool", operator="LIKE", value="default")]
        assert result["docker_image"] == "fake-docker.com:400/image"
        assert result["docker_parameters"] == [ConfigParameter(key="test", value=123)]
        assert result["env"] == {"TESTING": "true"}
        assert result["secret_env"] == {
            "TEST_SECRET": ConfigSecretSource(secret_name="tron-secret-svc-sec--A", key="sec_A")
        }
        assert result["secret_volumes"] == [
            ConfigSecretVolume(
                secret_volume_name="secretvolumename",
                secret_name="secret",
                container_path="/b",
                default_mode="0644",
                items=[{"key": "key", "path": "path", "mode": "0755"}],
            )
        ]
        assert result["projected_sa_volumes"] == [
            ConfigProjectedSAVolume(
                container_path="/var/run/secrets/whatever",
                audience="for.bar.com",
                expiration_seconds=3600,
            )
        ]
        assert result["extra_volumes"] == [ConfigVolume(container_path="/tmp", host_path="/home/tmp", mode="RO")]
        assert result["node_affinities"] == [
            ConfigNodeAffinity(key="topology.kubernetes.io/zone", operator="In", value=["us-west-1a", "us-west-1c"])
        ]
        assert result["topology_spread_constraints"] == [
            ConfigTopologySpreadConstraints(
                topology_key="zone",
                max_skew=1,
                when_unsatisfiable="DoNotSchedule",
                label_selector={"match_labels": {"app": "myapp"}},
            )
        ]
        assert result["labels"] == {"app": "myapp"}
        assert result["annotations"] == {"annotation_key": "annotation_value"}
        assert result["service_account_name"] == "default"
        assert result["ports"] == [8080, 9090]
