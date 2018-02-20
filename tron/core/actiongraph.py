from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import six

from tron.core import action

log = logging.getLogger(__name__)


class ActionGraph(object):
    """A directed graph of actions and their requirements."""

    def __init__(self, graph, action_map):
        self.graph = graph
        self.action_map = action_map

    @classmethod
    def from_config(cls, actions_config, cleanup_action_config=None):
        """Create this graph from a job config."""
        actions = {
            name: action.Action.from_config(conf)
            for name, conf in six.iteritems(actions_config)
        }
        if cleanup_action_config:
            cleanup_action = action.Action.from_config(cleanup_action_config)
            actions[cleanup_action.name] = cleanup_action

        return cls(cls._build_dag(actions, actions_config), actions)

    @classmethod
    def _build_dag(cls, actions, actions_config):
        """Return a directed graph from a dict of actions keyed by name."""
        base = []
        for a in six.itervalues(actions):
            dependencies = cls._get_dependencies(actions_config, a.name)
            if not dependencies:
                base.append(a)
                continue

            for dependency in dependencies:
                dependency_action = actions[dependency]
                a.required_actions.append(dependency_action)
                dependency_action.dependent_actions.append(a)
        return base

    @classmethod
    def _get_dependencies(cls, actions_config, action_name):
        if action_name == action.CLEANUP_ACTION_NAME:
            return []
        return actions_config[action_name].requires

    def actions_for_names(self, names):
        return (self.action_map[name] for name in names)

    def get_required_actions(self, name):
        """Given an Action's name return the Actions required to run
        before that Action.
        """
        if name not in self.action_map:
            return []
        return self.action_map[name].required_actions

    def get_dependent_actions(self, name):
        return self.action_map[name].dependent_actions

    def get_actions(self):
        return six.itervalues(self.action_map)

    def get_action_map(self):
        return self.action_map

    @property
    def names(self):
        return self.action_map.keys()

    def __getitem__(self, name):
        return self.action_map[name]

    def __eq__(self, other):
        return self.graph == other.graph and self.action_map == other.action_map

    def __ne__(self, other):
        return not self == other
