from testify import TestCase, setup, assert_equal, assert_raises
from testify.utils import turtle

from tron.utils import state
from tron.utils.state import NamedEventState

class StateMachineSimpleTestCase(TestCase):

    @setup
    def build_machine(self):
        self.state_green = NamedEventState('green')
        self.state_red = NamedEventState('red', true=self.state_green)

        self.machine = state.StateMachine(self.state_red)

    def test_transition_many(self):
        # Stay the same
        assert not self.machine.transition(False)
        assert_equal(self.machine.state, self.state_red)

        # Traffic has arrived
        self.machine.transition('true')
        assert_equal(self.machine.state, self.state_green)

        # Still traffic
        self.machine.transition('true')
        assert_equal(self.machine.state, self.state_green)

    def test_check(self):
        assert not self.machine.check(False)
        assert_equal(self.machine.check('true'), self.state_green)
        assert_equal(self.machine.state, self.state_red)

    def test_transition(self):
        handler = turtle.Turtle()
        self.machine.attach(True, handler)
        self.machine.transition('true')
        assert_equal(handler.handler.calls,
            [((self.machine, self.state_green),{})])

    def test_notify_delegate(self):
        delegate = turtle.Turtle()
        handler = turtle.Turtle()
        self.machine = state.StateMachine(self.state_red, delegate=delegate)
        self.machine.attach(True, handler)
        self.machine.transition('true')
        assert_equal(handler.handler.calls,
            [((delegate, self.state_green),{})])


class StateMachineMultiOptionTestCase(TestCase):
    @setup
    def build_machine(self):
        # Generalized rules of a conversation
        # If they are talking, we should listen
        # If they are listening, we should talk
        # If they are ignoring us we should get angry

        self.state_ignoring = NamedEventState('ignoring')
        self.state_talking = NamedEventState('talking')
        self.state_angry = NamedEventState('angry')

        self.state_listening = NamedEventState('listening',
            listening=self.state_talking
        )

        self.state_talking.update({
            'ignoring': self.state_angry,
            'talking': self.state_listening,
        })

        self.machine = state.StateMachine(self.state_listening)

    def test_transition_many(self):
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

    def test_transition_set(self):
        expected = set(['listening', 'talking', 'ignoring'])
        assert_equal(set(self.machine.transitions), expected)


class TraverseCircularTestCase(TestCase):
    @setup
    def build_machine(self):
        # Going around and around in circles
        self.state_telling_false = NamedEventState('telling_false')

        self.state_telling_truth = NamedEventState('telling_truth',
            true=self.state_telling_false)
        self.state_telling_false.update({'true': self.state_telling_truth})

        self.machine = state.StateMachine(self.state_telling_truth)

    def test_transition(self):
        assert_raises(state.CircularTransitionError, self.machine.transition, 'true')


class NamedEventByNameTestCase(TestCase):

    @setup
    def create_state_graph(self):
        self.start = STATE_A = state.NamedEventState("a")
        STATE_B = state.NamedEventState("b")
        self.end = STATE_C = state.NamedEventState("c", next=STATE_A)
        STATE_A['next'] = STATE_B
        STATE_B['next'] = STATE_C

    def test_match(self):
        assert_equal(state.named_event_by_name(self.start, "c"), self.end)

    def test_miss(self):
        assert_raises(ValueError, state.named_event_by_name, self.start, 'x')
