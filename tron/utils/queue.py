import queue

from twisted.internet import defer


class PyDeferredQueue(defer.DeferredQueue):
    """
    Implements the stdlib queue.Queue get/put interface with a DeferredQueue.
    """

    def __init__(self, *args, **kwargs):
        super(PyDeferredQueue, self).__init__(*args, **kwargs)

    def put(self, item, block=None, timeout=None):
        # Call from reactor thread so callbacks from get() will be executed
        # on the reactor thread, even if this is called from another thread.
        from twisted.internet import reactor
        try:
            reactor.callFromThread(super(PyDeferredQueue, self).put, item)
        except defer.QueueOverflow:
            raise queue.Full

    def get(self, block=None, timeout=None):
        try:
            return super(PyDeferredQueue, self).get()
        except defer.QueueUnderflow:
            raise queue.Empty
