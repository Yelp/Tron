import logging

class Error(Exception): pass

class InvalidRuleError(Error): pass

class CircularTransitionError(Error): pass

log = logging.getLogger(__name__)

class State(object):
    rules = list()
    
    def __getitem__(self, key):
        # try:
        #     current_state, target = key
        # except TypeError:
        #     raise KeyError()
        
        next_states = [state for state, func in self.rules if func(key)]
        if len(next_state) > 1:
            raise InvalidRuleError("Too many results")
        
        if next_states:
            return next_states[0]
        else:
            raise KeyError()


class StateMachine(object):
    """StateMachine is a class that can be used for managing state machines
    
    A state machine is made up of a State() and a target. The target is the where
    all the input comes from for making decision about state changes, whatever that 
    may be.
    
    A State is really just a fancy container for a set of rules for transitioning to other
    states based on the target.
    """
    initial_state = None
    def __init__(self):
        if self.initial_state is None:
            raise NotImplementedError("initial_state must be set")

        self.state = self.initial_state
        self.listeners = list()
    
    def transition(self, target, stop_item=None):
        """"""
        log.debug("Checking for transition from %r", self.state)
        
        try:
            next_state = self.state[target]
        except KeyError:
            return None
            
        prev_state = self.state
        log.debug("Transitioning from state %r to %r", self.state, next_state)

        # Check if we are doing some circular transition.
        # TODO: There might be some users who actually want this functionality, as moving to
        # other states could automatically cause a transition back to the start. But let's
        # add support for that later.
        if stop_item is not None and next_state is stop_item:
            raise CircularTransitionError()

        self.state = next_state
        self._notify_listeners()

        # We always call recursivly after a state change incase there are multiple steps
        # to take.
        self.transition(target, stop_item=(stop_item or prev_state))
            
    def _notify_listeners(self):
        log.debug("Notifying listeners")
        for listener in self.listeners:
            listener(self)
        