from twisted.internet import defer, reactor

class Error(Exception): pass

def defer_timeout(deferred, timeout):
    try:
        reactor.callLater(timeout, deferred.cancel)
    except AttributeError:
        # Re-implementing what's available in newer twisted in a crappy, but workable way.
        if not deferred.called:
            deferred.errback(failure.Failure(Error()))
        elif isinstance(deferred.result, defer.Deferred):
            defer_timeout(deferred.result, 0)
                
