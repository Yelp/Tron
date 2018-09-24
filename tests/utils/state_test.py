from __future__ import absolute_import
from __future__ import unicode_literals

from testifycompat import assert_equal
from testifycompat import setup
from testifycompat import TestCase
from tron.utils import state


class TestStateMachineSimple(TestCase):
    @setup
    def build_machine(self):
        self.state_green = 'green'
        self.state_red = 'red'

        self.machine = state.Machine(self.state_red, red=dict(true='green'))

    def test_transition_many(self):
        # Stay the same
        assert not self.machine.transition('missing')
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


class TestStateMachineMultiOption(TestCase):
    @setup
    def build_machine(self):
        # Generalized rules of a conversation
        # If they are talking, we should listen
        # If they are listening, we should talk
        # If they are ignoring us we should get angry
        self.machine = state.Machine(
            'listening',
            listening=dict(listening='talking'),
            talking=dict(ignoring='angry', talking='listening'),
        )

    def test_transition_many(self):
        # Talking, we should listen
        self.machine.transition("talking")
        assert_equal(self.machine.state, 'listening')

        # Now be polite
        self.machine.transition("listening")
        assert_equal(self.machine.state, 'talking')

        self.machine.transition("listening")
        assert_equal(self.machine.state, 'talking')

        # But they are tired of us...
        self.machine.transition("ignoring")
        assert_equal(self.machine.state, 'angry')

    def test_transition_set(self):
        expected = {'listening', 'talking', 'ignoring'}
        assert_equal(set(self.machine.transition_names), expected)
