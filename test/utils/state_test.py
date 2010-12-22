from testify import *

from tron.utils import state

class SimpleTestCase(TestCase):
    @setup
    def build_machine(self):
        self.state_green = {}
        self.state_red = {
            True: self.state_green, 
        }

        class TestStateMachine(state.StateMachine):
            initial_state = self.state_red
    
        self.machine = TestStateMachine()
        
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

        
        class TestStateMachine(state.StateMachine):
            initial_state = self.state_listening
    
        self.machine = TestStateMachine()
        
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
        
        class TestStateMachine(state.StateMachine):
            initial_state = self.state_telling_truth
    
        self.machine = TestStateMachine()

    def test(self):
        assert_raises(state.CircularTransitionError, self.machine.transition, True)