import queue
from typing import Optional
from typing import TypeVar

from twisted.internet import defer

T = TypeVar("T")


class PyDeferredQueue(defer.DeferredQueue[T]):
    """
    Implements the stdlib queue.Queue get/put interface with a DeferredQueue.
    """

    def __init__(self, size: int = 0, backlog: Optional[int] = None) -> None:
        super().__init__(size=size, backlog=backlog)

    def put(self, item: T, block: Optional[bool] = None, timeout: Optional[float] = None) -> None:
        # Call from reactor thread so callbacks from get() will be executed
        # on the reactor thread, even if this is called from another thread.
        from twisted.internet import reactor

        try:
            reactor.callFromThread(super().put, item)  # type: ignore[attr-defined]  # usual twisted shenanigans
        except defer.QueueOverflow:
            raise queue.Full

    def get(self, block: Optional[bool] = None, timeout: Optional[float] = None) -> defer.Deferred[T]:
        try:
            return super().get()
        except defer.QueueUnderflow:
            raise queue.Empty
