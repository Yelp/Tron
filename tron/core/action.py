import datetime
import logging

from dataclasses import dataclass
from dataclasses import field

from tron import node
from tron.config.schema import CLEANUP_ACTION_NAME

log = logging.getLogger(__name__)


@dataclass
class Action:
    """A configurable data object for an Action."""
    name: str
    command: str
    node_pool: str
    retries: int = None
    retries_delay: datetime.timedelta = None
    expected_runtime: datetime.timedelta = None
    executor: str = None
    cpus: float = None
    mem: float = None
    constraints: set = field(default_factory=set)
    docker_image: str = None
    docker_parameters: set = field(default_factory=set)
    env: dict = field(default_factory=dict)
    extra_volumes: set = field(default_factory=set)
    trigger_downstreams: (bool, dict) = None
    triggered_by: set = None
    on_upstream_rerun: str = None
    trigger_timeout: datetime.timedelta = None
    required_actions: set = field(default_factory=set)
    dependent_actions: set = field(default_factory=set)

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @classmethod
    def from_config(cls, config):
        """Factory method for creating a new Action."""
        node_repo = node.NodePoolRepository.get_instance()

        kwargs = dict(
            name=config.name,
            command=config.command,
            node_pool=node_repo.get_by_name(config.node),
            retries=config.retries,
            retries_delay=config.retries_delay,
            expected_runtime=config.expected_runtime,
            executor=config.executor,
            cpus=config.cpus,
            mem=config.mem,
            docker_image=config.docker_image,
            trigger_downstreams=config.trigger_downstreams,
            triggered_by=config.triggered_by,
            on_upstream_rerun=config.on_upstream_rerun,
            trigger_timeout=config.trigger_timeout,
            constraints=set(config.constraints or []),
            docker_parameters=set(config.docker_parameters or []),
            extra_volumes=set(config.extra_volumes or []),
            env=config.env or {},
        )

        return cls(**kwargs)
