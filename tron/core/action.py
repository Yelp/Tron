from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron import node
from tron.config.schema import CLEANUP_ACTION_NAME

log = logging.getLogger(__name__)


class Action(object):
    """A configurable data object for an Action."""

    def __init__(
        self, name, command, node_pool, required_actions=None,
        dependent_actions=None,
    ):
        self.name = name
        self.command = command
        self.node_pool = node_pool
        self.required_actions = required_actions or []
        self.dependent_actions = dependent_actions or []

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
        )

    def __eq__(self, other):
        attributes_match = all(
            getattr(self, attr, None) == getattr(other, attr, None)
            for attr in ['name', 'command', 'node_pool', 'is_cleanup']
        )
        return attributes_match and all(
            self_act == other_act for (self_act, other_act)
            in zip(self.required_actions, other.required_actions)
        )

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.name)
