from testify import *
from tron.utils import testingutils
from testify.utils import turtle
from twisted.internet import reactor, defer

from tron import monitor

class TestError(Exception):
    pass

class SimpleDeferredTestCase(TestCase):
    
    @setup
    def add_crash_observer(self):
        self.test_emailer = turtle.Turtle()
        self.mon = monitor.CrashReporter(self.test_emailer, turtle.Turtle())
        self.mon.start()

    @testingutils.run_reactor()
    def test_do_something_deferred(self):
        self.df = defer.Deferred()
        reactor.callLater(0, self.cause_failure)
        return self.df

    def cause_failure(self):
        self.df.callback(None)
        #raise TestError()

    @teardown
    def verify_deferred_call(self):
        self.mon.stop()
        #args, kwargs = self.test_emailer.send.calls.pop()
        #assert "TestError" in args[0], args
if __name__ == '__main__':
    run()
