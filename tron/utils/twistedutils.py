from __future__ import absolute_import
from __future__ import unicode_literals

from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import failure


class Error(Exception):
    pass


def _cancel(deferred):
    """Re-implementing what's available in newer twisted in a crappy, but
    workable way."""

    if not deferred.called:
        deferred.errback(failure.Failure(Error()))
    elif isinstance(deferred.result, defer.Deferred):
        _cancel(deferred.result)


def defer_timeout(deferred, timeout):
    try:
        reactor.callLater(timeout, deferred.cancel)
    except AttributeError:
        reactor.callLater(timeout, lambda: _cancel(deferred))
