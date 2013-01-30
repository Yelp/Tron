"""Minimal abstraction oer an event loop."""

from twisted.internet import reactor
from twisted.internet.base import DelayedCall


class Callback(DelayedCall):
    """
        action() - not (cancelled or called)
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
    return reactor.callLater(interval *args, **kwargs)
