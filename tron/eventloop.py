"""Minimal abstraction oer an event loop."""

from twisted.internet import reactor
from twisted.internet.base import DelayedCall


class Callback(DelayedCall):
    """
        active() - not (cancelled or called)
    """
    pass

class NullCallback(object):

    @staticmethod
    def cancel():
        pass

    @staticmethod
    def active():
        return False


def call_later(interval, *args, **kwargs):
    return reactor.callLater(interval, *args, **kwargs)


class UniqueCallback(object):
    """Wrap a DelayedCall so there can be only one instance of this call
    queued at a time. A Falsy delay causes this object to do nothing.
    """

    def __init__(self, delay, func, *args, **kwargs):
        self.delay          = delay
        self.func           = func
        self.args           = args
        self.kwargs         = kwargs
        self.delayed_call   = NullCallback

    def start(self):
        if not self.delay or self.delayed_call.active():
            return

        self.delayed_call = call_later(
            self.delay, self.func, *self.args, **self.kwargs)

    def cancel(self):
        if self.delayed_call.active():
            self.delayed_call.cancel()