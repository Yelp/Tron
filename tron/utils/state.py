import logging
from collections import defaultdict

log = logging.getLogger(__name__)


class Machine:
    @staticmethod
    def from_machine(machine, initial=None, state=None):
        if initial is None:
            initial = machine.initial
        if state is None:
            state = initial
        new_machine = Machine(initial, **machine.transitions)
        new_machine.state = state
        assert machine.transitions == new_machine.transitions
        assert machine.states == new_machine.states
        return new_machine

    def __init__(self, initial, **transitions):
        super().__init__()
        self.transitions = defaultdict(dict, transitions)
        self.transition_names = set(
            transition_name for (_, transitions) in self.transitions.items()
            for (transition_name, _) in (transitions or {}).items()
        )
        self.states = set(transitions.keys()).union(
            state for (_, dst) in transitions.items()
            for (_, state) in (dst or {}).items()
        )
        if initial not in self.states:
            raise RuntimeError(
                f"invalid machine: {initial} not in {self.states}"
            )
        self.state = initial
        self.initial = initial

    def set_state(self, state):
        if state not in self.states:
            raise RuntimeError(f"invalid state: {state} not in {self.states}")
        self.state = state

    def reset(self):
        self.state = self.initial

    def check(self, transition):
        """Check if the state can be transitioned via `transition`. Returns the
        destination state.
        """
        next_state = self.transitions[self.state].get(transition, None)
        return next_state

    def transition(self, transition):
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

    def __repr__(self):
        return f"<Machine S={self.state} T=({self.transitions!r})>"
