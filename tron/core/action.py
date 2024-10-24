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
from tron.config.schema import ConfigNodeAffinity
from tron.config.schema import ConfigProjectedSAVolume
from tron.config.schema import ConfigSecretVolume
from tron.config.schema import ConfigTopologySpreadConstraints
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
    constraints: set = field(default_factory=set)
    docker_image: Optional[str] = None
    docker_parameters: set = field(default_factory=set)
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
    annotations: dict = field(default_factory=dict)
    service_account_name: Optional[str] = None
    ports: List[int] = field(default_factory=list)

    @property
    def state_data(self):
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def copy(self):
        return ActionCommandConfig(**self.state_data)

    @staticmethod
    def to_json(state_data: dict) -> Optional[str]:
        """Serialize the ActionCommandConfig instance to a JSON string."""

        def serialize_namedtuple(obj):
            if isinstance(obj, tuple) and hasattr(obj, "_fields"):
                return obj._asdict()
            return obj

        try:
            return json.dumps(
                {
                    "command": state_data["command"],
                    "cpus": state_data["cpus"],
                    "mem": state_data["mem"],
                    "disk": state_data["disk"],
                    "cap_add": state_data["cap_add"],
                    "cap_drop": state_data["cap_drop"],
                    "constraints": list(state_data["constraints"]),
                    "docker_image": state_data["docker_image"],
                    "docker_parameters": list(state_data["docker_parameters"]),
                    "env": state_data["env"],
                    "secret_env": state_data["secret_env"],
                    "secret_volumes": [serialize_namedtuple(volume) for volume in state_data["secret_volumes"]],
                    "projected_sa_volumes": [
                        serialize_namedtuple(volume) for volume in state_data["projected_sa_volumes"]
                    ],
                    "field_selector_env": state_data["field_selector_env"],
                    "extra_volumes": list(state_data["extra_volumes"]),
                    "node_selectors": state_data["node_selectors"],
                    "node_affinities": [serialize_namedtuple(affinity) for affinity in state_data["node_affinities"]],
                    "labels": state_data["labels"],
                    "annotations": state_data["annotations"],
                    "service_account_name": state_data["service_account_name"],
                    "ports": state_data["ports"],
                }
            )
        except KeyError as e:
            log.error(f"Missing key in state_data: {e}")
            return None
        except Exception as e:
            log.error(f"Error serializing ActionCommandConfig to JSON: {e}")
            return None


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
            constraints=set(config.constraints or []),
            docker_parameters=set(config.docker_parameters or []),
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
        )

        return cls(**kwargs)
