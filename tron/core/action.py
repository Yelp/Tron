import logging

log = logging.getLogger(__name__)

CLEANUP_ACTION_NAME = 'cleanup'


class Action(object):
    """A configurable data object for an Action."""

    def __init__(self, name, command, node_pool, required_actions=None,
                dependent_actions=None):
        self.name               = name
        self.command            = command
        self.node_pool          = node_pool
        self.required_actions   = required_actions or []
        self.dependent_actions  = dependent_actions or []

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @classmethod
    def from_config(cls, config, node_pools):
        """Factory method for creating a new Action."""
        return cls(
            name=       config.name,
            command=    config.command,
            node_pool=  node_pools[config.node] if config.node else None,
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
