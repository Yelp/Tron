import logging
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Set
from typing import Union

from tron.core.action import Action
from tron.utils.timeutils import delta_total_seconds

log = logging.getLogger(__name__)


class Trigger(NamedTuple):
    name: str
    command: str


class ActionGraph:
    """A directed graph of actions and their requirements for a specific job."""

    def __init__(
        self,
        action_map: Dict[str, Action],
        required_actions: Dict[str, Set[str]],
        required_triggers: Dict[str, Set[str]],
    ) -> None:
        self.action_map: Dict[str, Action] = action_map
        self.required_actions: Dict[str, Set[str]] = required_actions
        self.required_triggers: Dict[str, Set[str]] = required_triggers

        self.all_triggers: Set[str] = set(self.required_triggers)
        for action_triggers in self.required_triggers.values():
            self.all_triggers |= action_triggers
        self.all_triggers -= set(self.action_map)

    def get_dependencies(self, action_name: str, include_triggers: bool = False) -> List[Union[Action, Trigger]]:
        """Given an Action's name return the Actions (and optionally, Triggers)
        required to run before that Action.
        """
        if action_name not in set(self.action_map) | self.all_triggers:
            return []

        dependencies: List[Union[Action, Trigger]] = [
            self.action_map[action] for action in self.required_actions[action_name]
        ]
        if include_triggers:
            dependencies += [self[trigger_name] for trigger_name in self.required_triggers[action_name]]
        return dependencies

    def names(self, include_triggers: bool = False) -> Set[str]:
        names: Set[str] = set(self.action_map.keys())
        if include_triggers:
            names |= self.all_triggers
        return names

    @property
    def expected_runtime(self) -> Dict[str, float]:
        return {name: delta_total_seconds(self.action_map[name].expected_runtime) for name in self.action_map.keys()}  # type: ignore[arg-type]  # TODO: there's probably a bug here, but we can fix it after tron is fully typed

    def __getitem__(self, name: str) -> Union[Action, Trigger]:
        if name in self.action_map:
            return self.action_map[name]
        elif name in self.all_triggers:
            # we don't have the Trigger config to know what the real command is,
            # so we just fill in the command with 'TRIGGER'
            return Trigger(name, "TRIGGER")
        else:
            raise KeyError(f"{name} is not a valid action")

    # TODO: correctly implement these - these are not quite correct as we're not checking if `other` is an ActionGraph
    def __eq__(self, other: object) -> bool:
        return bool(
            self.action_map == other.action_map  # type: ignore[attr-defined]  # this needs to be refactored
            and self.required_actions == other.required_actions  # type: ignore[attr-defined]  # this needs to be refactored
            and self.required_triggers == other.required_triggers  # type: ignore[attr-defined]  # this needs to be refactored
        )

    def __ne__(self, other: object) -> bool:
        return not self == other
