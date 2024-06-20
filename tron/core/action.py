import datetime
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

log = logging.getLogger(__name__)


@dataclass
class ActionCommandConfig:
    """A configurable data object for one try of an Action."""

    command: str
    cpus: Optional[float] = None
    mem: Optional[float] = None
    disk: Optional[float] = None
    cap_add: List[str] = field(default_factory=list)
    cap_drop: List[str] = field(default_factory=list)
    constraints: set = field(default_factory=set)
    docker_image: Optional[str] = None
    # XXX: we can get rid of docker_parameters once we're off of Mesos
    docker_parameters: set = field(default_factory=set)
    env: dict = field(default_factory=dict)
    secret_env: dict = field(default_factory=dict)
    secret_volumes: List[ConfigSecretVolume] = field(default_factory=list)
    projected_sa_volumes: List[ConfigProjectedSAVolume] = field(default_factory=list)
    field_selector_env: dict = field(default_factory=dict)
    extra_volumes: set = field(default_factory=set)
    node_selectors: dict = field(default_factory=dict)
    node_affinities: List[ConfigNodeAffinity] = field(default_factory=list)
    labels: dict = field(default_factory=dict)
    annotations: dict = field(default_factory=dict)
    service_account_name: Optional[str] = None
    ports: List[int] = field(default_factory=list)

    @property
    def state_data(self):
        return {field.name: getattr(self, field.name) for field in fields(self)}

    def copy(self):
        return ActionCommandConfig(**self.state_data)


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
