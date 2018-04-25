from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron import node
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.config.schema import ExecutorTypes
from tron.utils import maybe_decode

log = logging.getLogger(__name__)


class Action(object):
    """A configurable data object for an Action."""

    equality_attributes = [
        'name',
        'command',
        'node_pool',
        'is_cleanup',
        'executor',
        'cluster',
        'pool',
        'cpus',
        'mem',
        'service',
        'deploy_group',
        'retries',
    ]

    def __init__(
        self, name, command, node_pool, required_actions=None,
        dependent_actions=None,
        retries=None,
        executor=None,
        cluster=None,
        pool=None,
        cpus=None,
        mem=None,
        service=None,
        deploy_group=None,
    ):
        self.name = maybe_decode(name)
        self.command = command
        self.node_pool = node_pool
        self.retries = retries
        self.required_actions = required_actions or []
        self.dependent_actions = dependent_actions or []
        self.executor = executor
        self.cluster = cluster
        self.pool = pool
        self.cpus = cpus
        self.mem = mem
        self.service = service
        self.deploy_group = deploy_group

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @classmethod
    def from_config(cls, config):
        """Factory method for creating a new Action."""
        node_repo = node.NodePoolRepository.get_instance()
        return cls(
            name=config.name,
            command=config.command,
            node_pool=node_repo.get_by_name(config.node),
            retries=config.retries,
            executor=ExecutorTypes(config.executor),
            cluster=config.cluster,
            pool=config.pool,
            cpus=config.cpus,
            mem=config.mem,
            service=config.service,
            deploy_group=config.deploy_group,
        )

    def __eq__(self, other):
        attributes_match = all(
            getattr(self, attr, None) == getattr(other, attr, None)
            for attr in self.equality_attributes
        )
        return attributes_match and all(
            self_act == other_act for (self_act, other_act)
            in zip(self.required_actions, other.required_actions)
        )

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.name)
