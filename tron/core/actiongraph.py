import logging
from tron.core import action

log = logging.getLogger(__name__)


class ActionGraph(object):
    """A directed graph of actions and their requirements."""

    def __init__(self, graph, action_map):
        self.graph              = graph
        self.action_map         = action_map

    @classmethod
    def from_config(cls, actions_config, nodes, cleanup_action_config=None):
        """Create this graph from a job config."""
        actions = dict(
            (name, action.Action.from_config(conf, nodes))
            for name, conf in actions_config.iteritems()
        )
        if cleanup_action_config:
            cleanup_action = action.Action.from_config(
                    cleanup_action_config, nodes)
            actions[cleanup_action.name] = cleanup_action

        graph = cls._build_dag(actions, actions_config)
        return cls(graph, actions)

    @classmethod
    def _build_dag(cls, actions, actions_config):
        """Return a directed graph from a dict of actions keyed by name."""
        base = []
        for action in actions.itervalues():
            dependencies = actions_config[action.name].requires
            if not dependencies:
                base.append(action)
                continue

            for dependency in dependencies:
                dependency_action = actions[dependency]
                action.required_actions.append(dependency_action)
                dependency_action.dependent_actions.append(action)
        return base

    def actions_for_names(self, names):
        return (self.action_map[name] for name in names)

    def get_required_actions(self, name):
        """Given an Action's name return the Actions required to run
        before that Action.
        """
        return self.action_map[name].required_actions

    def __getitem__(self, name):
        return self.action_map[name]

    def __eq__(self, other):
        return self.graph == other.graph and self.action_map == other.action_map

    def __ne__(self, other):
        return not self == other