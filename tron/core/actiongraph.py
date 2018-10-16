from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from tron.core import action
from tron.utils import maybe_decode
from tron.utils.timeutils import delta_total_seconds

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
            maybe_decode(name): action.Action.from_config(conf)
            for name, conf in actions_config.items()
        }
        if cleanup_action_config:
            cleanup_action = action.Action.from_config(cleanup_action_config)
            actions[maybe_decode(cleanup_action.name)] = cleanup_action

        return cls(cls._build_dag(actions, actions_config), actions)

    @classmethod
    def _build_dag(cls, actions, actions_config):
        """Return a directed graph from a dict of actions keyed by name."""
        base = []
        for _, a in actions.items():
            dependencies = cls._get_dependencies(actions_config, a.name)
            if not dependencies:
                base.append(a)
                continue

            for dependency in dependencies:
                dependency_action = actions[dependency]
                a.required_actions.add(dependency_action.name)
                dependency_action.dependent_actions.add(a.name)
        return base

    @classmethod
    def _get_dependencies(cls, actions_config, action_name):
        if action_name == action.CLEANUP_ACTION_NAME:
            return []
        return actions_config[maybe_decode(action_name)].requires

    def actions_for_names(self, names):
        return (self.action_map[name] for name in names)

    def get_required_actions(self, name):
        """Given an Action's name return the Actions required to run
        before that Action.
        """
        if name not in self.action_map:
            return []

        return (
            self.action_map[action]
            for action in self.action_map[name].required_actions
        )

    def get_dependent_actions(self, name):
        if name not in self.action_map:
            return []

        return (
            self.action_map[action]
            for action in self.action_map[name].dependent_actions
        )

    def get_actions(self):
        return iter(val for _, val in self.action_map.items())

    def get_action_map(self):
        return self.action_map

    def get_required_triggers(self, _action_name):
        return []

    @property
    def names(self):
        return self.action_map.keys()

    @property
    def expected_runtime(self):
        return {
            name: delta_total_seconds(self.action_map[name].expected_runtime)
            for name in self.action_map.keys()
        }

    def __getitem__(self, name):
        return self.action_map[name]

    def __eq__(self, other):
        return self.graph == other.graph and self.action_map == other.action_map

    def __ne__(self, other):
        return not self == other
