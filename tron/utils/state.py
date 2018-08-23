from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import six

from tron.utils import maybe_decode
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

    def __bool__(self):
        return bool(self.name)

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


def traverse(starting_state, match_func):
    visited = set()
    state_pairs = [(None, starting_state)]

    def pair_with_name(p):
        return (p[0], p[1].name)

    while state_pairs:
        transition_state_pair = state_pairs.pop()
        _, cur_state = transition_state_pair
        visited.add(pair_with_name(transition_state_pair))

        if match_func(*transition_state_pair):
            yield transition_state_pair

        for next_pair in six.iteritems(cur_state):
            if pair_with_name(next_pair) not in visited:
                state_pairs.append(next_pair)


def named_event_by_name(starting_state, name):
    name = maybe_decode(name)

    def name_match(t, s):
        return s.name == name

    try:
        _, state = next(traverse(starting_state, name_match))
        return state
    except StopIteration:
        raise ValueError("State %s not found." % name)


def get_transitions(starting_state):
    def transition_match(t, s):
        return bool(t)

    return [trans for trans, _ in traverse(starting_state, transition_match)]


class StateMachine(Observable):
    """StateMachine is a class that can be used for managing state machines.

    A state machine is made up of a State() and a target. The target is the
    where all the input comes from for making decision about state changes,
    whatever that may be.

    A State is really just a fancy container for a set of rules for
    transitioning to other states based on the target.
    """

    def __init__(self, initial_state, delegate=None, force_state=None):
        super(StateMachine, self).__init__()
        self.initial_state = initial_state
        self.state = force_state or self.initial_state
        self._state_by_name = None
        self.delegate = delegate
        self.transitions = get_transitions(self.initial_state)

    def reset(self):
        """Force state machine into initial state."""
        self.state = self.initial_state

    def check(self, target):
        """Check if the state can be transitioned to target. Returns the
        destination state if target is a valid state to transition to,
        None otherwise.
        """
        log.debug("Checking for transition from %s to %s", self.state, target)
        return self.state.get(target, None)

    def transition(self, target, stop_item=None):
        """Check our current state for a transition based on the input 'target'

        Returns True or False based on whether a transition has indeed taken
        place.  Listeners for this change will also be notified before
        returning.
        """

        next_state = self.check(target)
        if next_state is None:
            return False

        prev_state = self.state
        log.debug(f"Transitioning from {self.state} to {next_state}")

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
