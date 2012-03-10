"""Some basic utilization of our testing utilities"""

from testify import *
from tron.utils import testingutils
from twisted.internet import reactor, defer

class SimpleTestCase(TestCase):
    """Make sure our setup methods get called"""
    ran_class_setup = 0
    ran_setup = False
    ran_methods = 0

    @class_setup
    @testingutils.run_reactor()
    def simple_test_setup(self):
        self.ran_class_setup += 1
        self.ran_setup = False

    @setup
    @testingutils.run_reactor()
    def per_method_setup(self):
        self.ran_setup = True

    @testingutils.run_reactor()
    def test_method_one(self):
        assert_equal(self.ran_class_setup, 1)
        assert self.ran_setup
        self.ran_methods += 1

    @testingutils.run_reactor()
    def test_method_two(self):
        assert_equal(self.ran_class_setup, 1)
        assert self.ran_setup
        self.ran_methods += 1

    @class_teardown
    def check_we_ran(self):
        assert self.ran_setup
        assert_equal(self.ran_methods, 2)

class SimpleDeferredTestCase(testingutils.ReactorTestCase):

    @testingutils.run_reactor()
    def test_do_something_deferred(self):
        df = defer.Deferred()
        reactor.callLater(1, df.callback, 42)

        df.addCallback(self._cb_record_value)
        return df

    def _cb_record_value(self, value):
        self.value = value

    @teardown
    def verify_deferred_call(self):
        # This is a little weird since the teardown is checking that the test worked, but we are testing a test here.
        assert_equal(self.value, 42)

class DeferredTimeoutTestCase(testingutils.ReactorTestCase):
    @testingutils.run_reactor(timeout=1, assert_raises=defer.TimeoutError)
    def test_deferred_never_finishes(self):
        df = defer.Deferred()
        reactor.callLater(30, df.callback, None)
        return df

if __name__ == '__main__':
    run()
