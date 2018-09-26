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
        )

        # Only convert config values if they are not None.
        constraints = config.constraints
        if constraints:
            constraints = set((c.attribute, c.operator, c.value) for c in constraints)
            kwargs['constraints'] = constraints

        docker_parameters = config.docker_parameters
        if docker_parameters:
            docker_parameters = set((c.key, c.value) for c in docker_parameters)
            kwargs['docker_parameters'] = docker_parameters

        extra_volumes = config.extra_volumes
        if extra_volumes:
            extra_volumes = set((c.container_path, c.host_path, c.mode) for c in extra_volumes)
            kwargs['extra_volumes'] = extra_volumes

        if config.env:
            kwargs['env'] = config.env

        return cls(**kwargs)
