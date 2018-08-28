from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron import node
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.utils import maybe_decode

log = logging.getLogger(__name__)


class Action(object):
    """A configurable data object for an Action."""

    equality_attributes = [
        'name',
        'command',
        'node_pool',
        'is_cleanup',
        'retries',
        'expected_runtime',
        'executor',
        'cpus',
        'mem',
        'constraints',
        'docker_image',
        'docker_parameters',
        'env',
        'extra_volumes',
        'retries_delay',
        'trigger_downstreamss',
        'triggered_by',
        'on_upstream_rerun',
    ]

    def __init__(
        self,
        name,
        command,
        node_pool,
        required_actions=None,
        dependent_actions=None,
        retries=None,
        retries_delay=None,
        expected_runtime=None,
        executor=None,
        cpus=None,
        mem=None,
        constraints=None,
        docker_image=None,
        docker_parameters=None,
        env=None,
        extra_volumes=None,
        trigger_downstreams=None,
        triggered_by=None,
        on_upstream_rerun=None,
    ):
        self.name = maybe_decode(name)
        self.command = command
        self.node_pool = node_pool
        self.retries = retries
        self.retries_delay = retries_delay
        self.required_actions = required_actions or []
        self.dependent_actions = dependent_actions or []
        self.expected_runtime = expected_runtime
        self.executor = executor
        self.cpus = cpus
        self.mem = mem
        self.constraints = constraints or []
        self.docker_image = docker_image
        self.docker_parameters = docker_parameters or []
        self.env = env or {}
        self.extra_volumes = extra_volumes or []
        self.trigger_downstreams = trigger_downstreams
        self.triggered_by = triggered_by
        self.on_upstream_rerun = on_upstream_rerun

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @classmethod
    def from_config(cls, config):
        """Factory method for creating a new Action."""
        node_repo = node.NodePoolRepository.get_instance()

        # Only convert config values if they are not None.
        constraints = config.constraints
        if constraints:
            constraints = [[c.attribute, c.operator, c.value]
                           for c in constraints]
        docker_parameters = config.docker_parameters
        if docker_parameters:
            docker_parameters = [c._asdict() for c in docker_parameters]
        extra_volumes = config.extra_volumes
        if extra_volumes:
            extra_volumes = [c._asdict() for c in extra_volumes]

        return cls(
            name=config.name,
            command=config.command,
            node_pool=node_repo.get_by_name(config.node),
            retries=config.retries,
            retries_delay=config.retries_delay,
            expected_runtime=config.expected_runtime,
            executor=config.executor,
            cpus=config.cpus,
            mem=config.mem,
            constraints=constraints,
            docker_image=config.docker_image,
            docker_parameters=docker_parameters,
            env=config.env,
            extra_volumes=extra_volumes,
            trigger_downstreams=config.trigger_downstreams,
            triggered_by=config.triggered_by,
            on_upstream_rerun=config.on_upstream_rerun,
        )

    def __eq__(self, other):
        attributes_match = all(
            getattr(self, attr, None) == getattr(other, attr, None)
            for attr in self.equality_attributes
        )
        return attributes_match and all(
            self_act == other_act for (
                self_act,
                other_act,
            ) in zip(self.required_actions, other.required_actions)
        )

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.name)
