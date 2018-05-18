from __future__ import absolute_import
from __future__ import unicode_literals

import logging

log = logging.getLogger(__name__)


class ActionGraph(object):
    """A directed graph of actions and their requirements."""

    def __init__(self, graph, action_map):
        self.graph = graph
        self.action_map = action_map

    @classmethod
    def from_config(cls, action_map, config_context, cleanup_action=None):
        """Create this graph from a job config."""
        if cleanup_action:
            action_map = action_map.set(cleanup_action.name, cleanup_action)

        return cls(*cls._build_dag(action_map))

    @classmethod
    def _build_dag(cls, action_map):
        """Return a directed graph from a dict of actions keyed by name."""
        base = []
        anames = action_map.keys()
        for aname in anames:
            action = action_map[aname]
            if not action.requires:
                base.append(aname)
                continue
            for dname in action.requires:
                daction = action_map[dname]
                action_map = action_map.update(
                    {
                        aname:
                            action.set(
                                required_actions=action.required_actions.
                                add(dname),
                            ),
                        dname:
                            daction.set(
                                dependent_actions=daction.dependent_actions.
                                add(aname),
                            ),
                    }
                )
        return base, action_map

    @classmethod
    def _get_dependencies(cls, actions_config, action_name):
        return actions_config[action_name].requires

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
