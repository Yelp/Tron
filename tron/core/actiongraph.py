from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

from tron.utils.timeutils import delta_total_seconds

log = logging.getLogger(__name__)
Trigger = namedtuple('Trigger', ['name', 'command'])


class ActionGraph(object):
    """A directed graph of actions and their requirements for a specific job."""

    def __init__(self, action_map, required_actions, required_triggers):
        self.action_map = action_map
        self.required_actions = required_actions
        self.required_triggers = required_triggers
        self.all_triggers = set(self.required_triggers)
        for action_triggers in self.required_triggers.values():
            self.all_triggers |= action_triggers
        self.all_triggers -= set(self.action_map)

    def get_dependencies(self, action_name, include_triggers=False):
        """Given an Action's name return the Actions (and optionally, Triggers)
        required to run before that Action.
        """
        if action_name not in set(self.action_map) | self.all_triggers:
            return []

        dependencies = [
            self.action_map[action]
            for action in self.required_actions[action_name]
        ]
        if include_triggers:
            dependencies += [
                self[trigger_name]
                for trigger_name in self.required_triggers[action_name]
            ]
        return dependencies

    def names(self, include_triggers=False):
        names = set(self.action_map)
        if include_triggers:
            names |= self.all_triggers
        return names

    @property
    def expected_runtime(self):
        return {
            name: delta_total_seconds(self.action_map[name].expected_runtime)
            for name in self.action_map.keys()
        }

    def __getitem__(self, name):
        if name in self.action_map:
            return self.action_map[name]
        elif name in self.all_triggers:
            # we don't have the Trigger config to know what the real command is,
            # so we just fill in the command with 'TRIGGER'
            return Trigger(name, 'TRIGGER')
        else:
            raise KeyError(f'{name} is not a valid action')

    def __eq__(self, other):
        return (
            self.action_map == other.action_map and
            self.required_actions == other.required_actions and
            self.required_triggers == other.required_triggers
        )

    def __ne__(self, other):
        return not self == other
