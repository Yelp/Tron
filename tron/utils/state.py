import logging
from collections import defaultdict
from typing import Any
from typing import Mapping
from typing import Optional
from typing import Set

log = logging.getLogger(__name__)


class Machine:
    @staticmethod
    def from_machine(machine: "Machine", initial: Optional[str] = None, state: Optional[str] = None) -> "Machine":
        if initial is None:
            initial = machine.initial
        if state is None:
            state = initial
        new_machine = Machine(initial, **machine.transitions)
        new_machine.state = state
        assert machine.transitions == new_machine.transitions
        assert machine.states == new_machine.states
        return new_machine

    def __init__(self, initial: str, **transitions: Any) -> None:  # TODO: use the correct type here
        super().__init__()
        self.transitions: Mapping[str, Mapping[str, str]] = defaultdict(dict, transitions)
        self.transition_names: Set[str] = {
            transition_name
            for (_, transitions) in self.transitions.items()
            for (transition_name, _) in (transitions or {}).items()
        }
        self.states: Set[str] = set(transitions.keys()).union(
            state for (_, dst) in transitions.items() for (_, state) in (dst or {}).items()
        )
        if initial not in self.states:
            raise RuntimeError(
                f"invalid machine: {initial} not in {self.states}",
            )
        self.state: str = initial
        self.initial: str = initial

    def set_state(self, state: str) -> None:
        if state not in self.states:
            raise RuntimeError(f"invalid state: {state} not in {self.states}")
        self.state = state

    def reset(self) -> None:
        self.state = self.initial

    def check(self, transition: str) -> Optional[str]:
        """Check if the state can be transitioned via `transition`. Returns the
        destination state.
        """
        next_state = self.transitions[self.state].get(transition, None)
        return next_state

    def transition(self, transition: str) -> bool:
        """Checks if machine can be transitioned from current state using
        provided transition name. Returns True if transition has taken place.
        Listeners for this change will also be notified before returning.
        """
        next_state = self.check(transition)
        if next_state is None:
            return False

        log.debug(f"transitioning from {self.state} to {next_state}")
        self.state = next_state
        return True

    def __repr__(self) -> str:
        return f"<Machine S={self.state} T=({self.transitions!r})>"
