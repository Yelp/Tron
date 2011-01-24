from testify import *

from tron.utils import state

class SimpleTestCase(TestCase):
    @setup
    def build_machine(self):
        self.state_green = {}
        self.state_red = {
            True: self.state_green, 
        }

        self.machine = state.StateMachine(self.state_red)
        
    def test(self):
        # Stay the same
        self.machine.transition(False)
        assert_equal(self.machine.state, self.state_red)
        
        # Traffic has arrived
        self.machine.transition(True)
        assert_equal(self.machine.state, self.state_green)
        
        # Still traffic
        self.machine.transition(True)
        assert_equal(self.machine.state, self.state_green)
        

class MultiOptionTestCase(TestCase):
    @setup
    def build_machine(self):
        # Generalized rules of a conversation
        # If they are talking, we should listen
        # If they are listening, we should talk
        # If they are ignoring us we should get angry
        
        self.state_ignoring = dict()
        self.state_talking = dict()
        self.state_angry = dict()
        
        self.state_listening = {
            'listening': self.state_talking,
        }
        
        self.state_talking.update({
            'ignoring': self.state_angry,
            'talking': self.state_listening,
        })

        
        self.machine = state.StateMachine(self.state_listening)
        
    def test(self):
        # Talking, we should listen
        self.machine.transition("talking")
        assert_equal(self.machine.state, self.state_listening)
        
        # Now be polite
        self.machine.transition("listening")
        assert_equal(self.machine.state, self.state_talking)

        self.machine.transition("listening")
        assert_equal(self.machine.state, self.state_talking)

        # But they are tired of us...
        self.machine.transition("ignoring")
        assert_equal(self.machine.state, self.state_angry)
        

class TestCircular(TestCase):
    @setup
    def build_machine(self):
        # Going around and around in circles
        self.state_telling_false = dict()
        
        self.state_telling_truth = {
            True: self.state_telling_false,
        }
        self.state_telling_false.update({
            True: self.state_telling_truth,
        })
        
        self.machine = state.StateMachine(self.state_telling_truth)

    def test(self):
        assert_raises(state.CircularTransitionError, self.machine.transition, True)

class TestNamedSearch(TestCase):
    @setup
    def create_state_graph(self):
        self.start = STATE_A = state.NamedEventState("a")
        STATE_B = state.NamedEventState("b")
        self.end = STATE_C = state.NamedEventState("c", next=STATE_A)
        STATE_A['next'] = STATE_B
        STATE_B['next'] = STATE_C
    
    def test(self):
        assert_equal(state.named_event_by_name(self.start, "c"), self.end)