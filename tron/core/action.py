import datetime
import logging
from typing import List

from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields

from tron import node
from tron.config.schema import CLEANUP_ACTION_NAME

log = logging.getLogger(__name__)


@dataclass
class ActionCommandConfig:
    """A configurable data object for one try of an Action."""

    command: str
    cpus: float = None
    mem: float = None
    disk: float = None
    cap_add: List[str] = field(default_factory=list)
    cap_drop: List[str] = field(default_factory=list)
    constraints: set = field(default_factory=set)
    docker_image: str = None
    # XXX: we can get rid of docker_parameters once we're off of Mesos
    docker_parameters: set = field(default_factory=set)
    env: dict = field(default_factory=dict)
    secret_env: dict = field(default_factory=dict)
    extra_volumes: set = field(default_factory=set)

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
    retries: int = None
    retries_delay: datetime.timedelta = None
    expected_runtime: datetime.timedelta = None
    executor: str = None
    trigger_downstreams: (bool, dict) = None
    triggered_by: set = None
    on_upstream_rerun: str = None
    trigger_timeout: datetime.timedelta = None

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @property
    def command(self):
        return self.command_config.command

    @classmethod
    def from_config(cls, config):
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
            cap_add=config.cap_add or [],
            cap_drop=config.cap_drop or [],
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
