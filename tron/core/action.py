import datetime
import json
import logging
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from typing import List
from typing import Optional
from typing import Union

from tron import node
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.config.schema import ConfigAction
from tron.config.schema import ConfigConstraint
from tron.config.schema import ConfigFieldSelectorSource
from tron.config.schema import ConfigNodeAffinity
from tron.config.schema import ConfigParameter
from tron.config.schema import ConfigProjectedSAVolume
from tron.config.schema import ConfigSecretSource
from tron.config.schema import ConfigSecretVolume
from tron.config.schema import ConfigTopologySpreadConstraints
from tron.config.schema import ConfigVolume
from tron.utils.persistable import Persistable

log = logging.getLogger(__name__)


@dataclass
class ActionCommandConfig(Persistable):
    """A configurable data object for one try of an Action."""

    command: str
    cpus: Optional[float] = None
    mem: Optional[float] = None
    disk: Optional[float] = None
    cap_add: List[str] = field(default_factory=list)
    cap_drop: List[str] = field(default_factory=list)
    docker_image: Optional[str] = None
    env: dict = field(default_factory=dict)
    secret_env: dict = field(default_factory=dict)
    secret_volumes: List[ConfigSecretVolume] = field(default_factory=list)
    projected_sa_volumes: List[ConfigProjectedSAVolume] = field(default_factory=list)
    field_selector_env: dict = field(default_factory=dict)
    extra_volumes: set = field(default_factory=set)
    node_selectors: dict = field(default_factory=dict)
    node_affinities: List[ConfigNodeAffinity] = field(default_factory=list)
    topology_spread_constraints: List[ConfigTopologySpreadConstraints] = field(default_factory=list)
    labels: dict = field(default_factory=dict)
    idempotent: bool = False
    annotations: dict = field(default_factory=dict)
    service_account_name: Optional[str] = None
    ports: List[int] = field(default_factory=list)

    @property
    def state_data(self):
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def copy(self):
        return ActionCommandConfig(**self.state_data)

    @staticmethod
    def from_json(state_data: str):
        """Deserialize a JSON string to an ActionCommandConfig instance."""
        try:
            json_data = json.loads(state_data)
            deserialized_data = {
                # remove?
                "constraints": [
                    ConfigConstraint.from_dict(val) for val in json_data["constraints"]
                ],  # convert back the list of dictionaries to a list of ConfigConstraint
                # remove?
                "docker_parameters": [ConfigParameter.from_dict(val) for val in json_data["docker_parameters"]],
                "extra_volumes": [ConfigVolume.from_dict(val) for val in json_data["extra_volumes"]],
                "node_affinities": [ConfigNodeAffinity.from_dict(val) for val in json_data["node_affinities"]],
                "topology_spread_constraints": [
                    ConfigTopologySpreadConstraints.from_dict(val) for val in json_data["topology_spread_constraints"]
                ],
                "secret_volumes": [ConfigSecretVolume.from_dict(val) for val in json_data["secret_volumes"]],
                "projected_sa_volumes": [
                    ConfigProjectedSAVolume.from_dict(val) for val in json_data["projected_sa_volumes"]
                ],
                "secret_env": {key: ConfigSecretSource.from_dict(val) for key, val in json_data["secret_env"].items()},
                "field_selector_env": {
                    key: ConfigFieldSelectorSource.from_dict(val)
                    for key, val in json_data["field_selector_env"].items()
                },
                "command": json_data["command"],
                "cpus": json_data["cpus"],
                "mem": json_data["mem"],
                "disk": json_data["disk"],
                "cap_add": json_data["cap_add"],
                "cap_drop": json_data["cap_drop"],
                "docker_image": json_data["docker_image"],
                "env": json_data["env"],
                "node_selectors": json_data["node_selectors"],
                "labels": json_data["labels"],
                "idempotent": json_data.get("idempotent", False),
                "annotations": json_data["annotations"],
                "service_account_name": json_data["service_account_name"],
                "ports": json_data["ports"],
            }
        except Exception:
            log.exception("Error deserializing ActionCommandConfig from JSON")
            raise
        return deserialized_data

    @staticmethod
    def to_json(state_data: dict) -> str:
        """Serialize the ActionCommandConfig instance to a JSON string."""

        def serialize_namedtuple(obj):
            if isinstance(obj, tuple) and hasattr(obj, "_fields"):
                # checks if obj is a namedtuple and convert it to a dict
                # TODO: future improvement here would be to use a custom json encoder to
                # have json.dumps() automatically convert namedtuples to dictionaries
                return obj._asdict()
            return obj

        try:
            # NOTE: you'll notice that there's a lot of get() accesses of state_data for
            # pretty common fields - this is because ActionCommandConfig is used by more
            # than one type of ActionRun (Kubernetes, Mesos, SSH) and these generally look
            # different. Alternatively, some of these fields are used by KubernetesActionRun
            # but are relatively new and older runs do not have data for them.
            # Once we get rid of the SSH and Mesos code as well as older runs in DynamoDB,
            # we'll likely be able to clean this up.
            return json.dumps(
                {
                    "command": state_data["command"],
                    "cpus": state_data["cpus"],
                    "mem": state_data["mem"],
                    "disk": state_data["disk"],
                    "cap_add": state_data["cap_add"],
                    "cap_drop": state_data["cap_drop"],
                    # remove?
                    "constraints": [
                        serialize_namedtuple(constraint) for constraint in state_data.get("constraints", [])
                    ],  # convert each ConfigConstraint to dictionary, so it would be a list of dicts
                    "docker_image": state_data["docker_image"],
                    # remove?
                    "docker_parameters": [
                        serialize_namedtuple(parameter) for parameter in state_data.get("docker_parameters", [])
                    ],
                    "env": state_data.get("env", {}),
                    "secret_env": {
                        key: serialize_namedtuple(val) for key, val in state_data.get("secret_env", {}).items()
                    },
                    "secret_volumes": [serialize_namedtuple(volume) for volume in state_data.get("secret_volumes", [])],
                    "projected_sa_volumes": [
                        serialize_namedtuple(volume) for volume in state_data.get("projected_sa_volumes", [])
                    ],
                    "field_selector_env": {
                        key: serialize_namedtuple(val) for key, val in state_data.get("field_selector_env", {}).items()
                    },
                    "extra_volumes": [serialize_namedtuple(volume) for volume in state_data.get("extra_volumes", [])],
                    "node_selectors": state_data.get("node_selectors", {}),
                    "node_affinities": [
                        serialize_namedtuple(affinity) for affinity in state_data.get("node_affinities", [])
                    ],
                    "labels": state_data.get("labels", {}),
                    "idempotent": state_data.get("idempotent", False),
                    "annotations": state_data.get("annotations", {}),
                    "service_account_name": state_data.get("service_account_name"),
                    "ports": state_data.get("ports", []),
                    "topology_spread_constraints": [
                        serialize_namedtuple(constraint)
                        for constraint in state_data.get("topology_spread_constraints", [])
                    ],
                }
            )
        except KeyError:
            log.exception("Missing key in state_data:")
            raise
        except Exception:
            log.exception("Error serializing ActionCommandConfig to JSON:")
            raise


@dataclass
class Action:
    """A configurable data object for an Action."""

    name: str
    command_config: ActionCommandConfig
    node_pool: str
    retries: Optional[int] = None
    retries_delay: Optional[datetime.timedelta] = None
    expected_runtime: Optional[datetime.timedelta] = None
    executor: Optional[str] = None
    trigger_downstreams: Optional[Union[bool, dict]] = None
    triggered_by: Optional[set] = None
    on_upstream_rerun: Optional[str] = None
    trigger_timeout: Optional[datetime.timedelta] = None
    idempotent: bool = False

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @property
    def command(self):
        return self.command_config.command

    @classmethod
    def from_config(cls, config: ConfigAction) -> "Action":
        """Factory method for creating a new Action."""
        node_repo = node.NodePoolRepository.get_instance()
        command_config = ActionCommandConfig(
            command=config.command,
            cpus=config.cpus,
            mem=config.mem,
            disk=(1024.0 if config.disk is None else config.disk),
            docker_image=config.docker_image,
            extra_volumes=set(config.extra_volumes or []),
            env=config.env or {},
            secret_env=config.secret_env or {},
            secret_volumes=config.secret_volumes or [],
            projected_sa_volumes=config.projected_sa_volumes or [],
            field_selector_env=config.field_selector_env or {},
            cap_add=config.cap_add or [],
            cap_drop=config.cap_drop or [],
            node_selectors=config.node_selectors or {},
            node_affinities=config.node_affinities or [],
            topology_spread_constraints=config.topology_spread_constraints or [],
            labels=config.labels or {},
            idempotent=config.idempotent,
            annotations=config.annotations or {},
            service_account_name=config.service_account_name or None,
            ports=config.ports or [],
        )
        kwargs = dict(
            name=config.name,
            command_config=command_config,
            node_pool=node_repo.get_by_name(config.node),
            retries=config.retries,
            retries_delay=config.retries_delay,
            expected_runtime=config.expected_runtime,
            executor=config.executor,
            trigger_downstreams=config.trigger_downstreams,
            triggered_by=config.triggered_by,
            on_upstream_rerun=config.on_upstream_rerun,
            trigger_timeout=config.trigger_timeout,
            idempotent=config.idempotent,
        )

        return cls(**kwargs)
