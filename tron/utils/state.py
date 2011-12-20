import logging


class Error(Exception):
    pass


class InvalidRuleError(Error):
    pass


class CircularTransitionError(Error):
    pass


log = logging.getLogger(__name__)


class FunctionItemState(object):

    # Rules should be a list of tuples in the form (next_state,
    # validation_funtion) where if the function returns true, the next state is
    # valid.

    rules = list()

    def __getitem__(self, key):
        next_states = [state for state, func in self.rules if func(key)]
        if len(next_state) > 1:
            raise InvalidRuleError("Too many results")

        if next_states:
            return next_states[0]
        else:
            raise KeyError()


class NamedEventState(dict):
    """Simple state type that allows you to easily use a dictionary for a state
    implementation. A raw dictionary works fine as well, but this might be more
    clear, plus it gives you a name.
    """

    def __init__(self, name, **kwargs):
        self.name = name
        super(NamedEventState, self).__init__(**kwargs)

    def __eq__(self, other):
        try:
            return self.name == other.name
        except AttributeError:
            return False

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%r %s>" % (self.__class__.__name__, self.name)


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


class StateMachine(object):
    """StateMachine is a class that can be used for managing state machines.

    A state machine is made up of a State() and a target. The target is the
    where all the input comes from for making decision about state changes,
    whatever that may be.

    A State is really just a fancy container for a set of rules for
    transitioning to other states based on the target.
    """

    def __init__(self, initial_state):
        self.initial_state = initial_state
        self.state = self.initial_state
        self._listeners = list()
        self._state_by_name = None

    def transition(self, target, stop_item=None):
        """Check our current state for a transition based on the input 'target'

        Returns True or False based on whether a transition has indeed taken
        place.  Listeners for this change will also be notified before
        returning.
        """
        log.debug("Checking for transition from %r (%r)", self.state, target)

        try:
            next_state = self.state[target]
        except KeyError:
            return False

        prev_state = self.state
        log.debug("Transitioning from state %r to %r", self.state, next_state)

        # Check if we are doing some circular transition.
        # TODO: There might be some users who actually want this functionality,
        # as moving to other states could automatically cause a transition back
        # to the start. But let's add support for that later.

        if stop_item is not None and next_state is stop_item:
            raise CircularTransitionError()

        self.state = next_state
        self.notify()

        # We always call recursivly after a state change incase there are
        # multiple steps to take.
        # TODO: We may want to make this optional
        self.transition(target, stop_item=(stop_item or prev_state))

        return True

    def listen(self, listen_spec, callback):

        """Listen for the specific state of set of states and callback on the
        function provided.

        The callback is called AFTER the state transition has been made.

        Listener Spec matches on:
         True - Matches everything
         Specific state - Matches only that state
         List of states - Matches any of the states
        """
        self._listeners.append((listen_spec, callback))

    def clear_listeners(self, listen_spec=None):
        self._listeners = [(l, c) for l, c in self._listeners
                           if listen_spec is not None and listen_spec != l]

    def _listener_spec_match(self, listen_spec):
        """Does the specified listener specification match the current state.

        See listen() for more details.
        """
        if listen_spec is True:
            return True

        if self.state == listen_spec:
            return True

        try:
            if self.state in listen_spec:
                return True
        except TypeError:
            pass

        return False

    def notify(self):
        log.debug("Notifying listeners for new state %r", self.state)
        matched_listeners = [listener for spec, listener in self._listeners
                             if self._listener_spec_match(spec)]
        for listener in matched_listeners:
            listener()
