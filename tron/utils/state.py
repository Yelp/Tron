import logging
from tron.utils.observer import Observable


class Error(Exception):
    pass


class InvalidRuleError(Error):
    pass


class CircularTransitionError(Error):
    pass


log = logging.getLogger(__name__)


class NamedEventState(dict):
    """A dict like object with a name that acts as a state. The dict stores
    valid transition actions and the destination state.
    """

    def __init__(self, name, short_name=None, short_chars=4, **kwargs):
        self.name = name
        self._short_name = short_name
        self._short_chars = short_chars
        super(NamedEventState, self).__init__(**kwargs)

    def __eq__(self, other):
        try:
            return self.name == other.name
        except AttributeError:
            return False

    def __hash__(self):
        return hash(self.name)

    def __nonzero__(self):
        return bool(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%r %s>" % (self.__class__.__name__, self.name)

    @property
    def short_name(self):
        """If a short_name was given use that, otherwise return the first
        self._short_letters letters of the name in caps.
        """
        if self._short_name:
            return self._short_name
        return self.name[:self._short_chars].upper()


def named_event_by_name(starting_state, state_name):
    """Traverse the state graph and pull out the one with the provided name.
    Does a simple breadth-first-search, being careful to avoid cycles.
    """
    seen_states = set()
    state_list = [starting_state]
    while state_list:
        current_state = state_list.pop()
        seen_states.add(current_state.name)
        if state_name == current_state.name:
            return current_state

        for next_state in current_state.itervalues():
            if next_state.name not in seen_states:
                state_list.append(next_state)

    raise ValueError(state_name)


class StateMachine(Observable):
    """StateMachine is a class that can be used for managing state machines.

    A state machine is made up of a State() and a target. The target is the
    where all the input comes from for making decision about state changes,
    whatever that may be.

    A State is really just a fancy container for a set of rules for
    transitioning to other states based on the target.
    """

    def __init__(self, initial_state, delegate=None):
        super(StateMachine, self).__init__()
        self.initial_state = initial_state
        self.state = self.initial_state
        self._state_by_name = None
        self.delegate = delegate


    def check(self, target):
        """Check if the state can be transitioned to target. Returns the
        destination state if target is a valid state to transition to,
        None otherwise.
        """
        return self.state.get(target, None)

    def transition(self, target, stop_item=None):
        """Check our current state for a transition based on the input 'target'

        Returns True or False based on whether a transition has indeed taken
        place.  Listeners for this change will also be notified before
        returning.
        """
        log.debug("Checking for transition from %r (%r)", self.state, target)

        next_state = self.check(target)
        if next_state is None:
            return False

        prev_state = self.state
        log.debug("Transitioning from state %r to %r", self.state, next_state)

        # Check if we are doing some circular transition.
        if stop_item is not None and next_state is stop_item:
            raise CircularTransitionError()

        self.state = next_state
        self.notify(self.state)

        # We always call recursively after a state change incase there are
        # multiple steps to take.
        self.transition(target, stop_item=(stop_item or prev_state))
        return True

    def notify(self, event):
        """Notify observers."""
        watched = self.delegate if self.delegate else self
        for handler in self._get_handlers_for_event(event):
            handler.handler(watched, event)
