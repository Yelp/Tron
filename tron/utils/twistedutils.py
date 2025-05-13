from typing import Any
from typing import cast
from typing import TYPE_CHECKING

from twisted.internet import defer
from twisted.internet import reactor  # This is the global reactor instance
from twisted.python import failure

if TYPE_CHECKING:
    from twisted.internet.epollreactor import EPollReactor


class Error(Exception):
    pass


def _cancel(deferred: defer.Deferred[object]) -> None:
    """Re-implementing what's available in newer twisted in a crappy, but
    workable way."""

    if not deferred.called:
        deferred.errback(failure.Failure(Error()))
    elif isinstance(deferred.result, defer.Deferred):
        _cancel(deferred.result)


def defer_timeout(deferred: defer.Deferred[Any], timeout: float) -> None:
    try:
        cast("EPollReactor", reactor).callLater(timeout, deferred.cancel)
    except AttributeError:
        cast("EPollReactor", reactor).callLater(timeout, lambda: _cancel(deferred))
