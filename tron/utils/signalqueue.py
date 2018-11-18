import asyncio
import logging
import signal

log = logging.getLogger(__name__)


# We cannot use the queue module's Queue because the latter locks on queue
# operations, which do not play well with signal handlers since handlers can
# interrupt at any time. Consider the following main thread traceback:
#
# --- Normal Main Thread Operation ---
# 1. q.wait()
# 2. q.get_nowait()
# 3. q._lock.acquire() (internal)
# --- Signal Handler Interrupts (always on main thread)---
# 4. q.handler()
# 5. q.put_nowait()
# 6. q._lock.acquire() (internal) <------- DEADLOCK (lock hasn't been released)
#
# Given that signal handlers work by interruption, it is impossible to enforce
# locking on queue operations without encountering deadlock issues.
#
# Instead, we use asyncio's Queue, because its operations are not locked.
# Given this module's simplicity, however, this is not an issue, even in a
# multi-threaded program.

class SignalQueue(object):
    """ A queue for signals, which provides functionality for producing signals
    from signal handlers, and consuming signals if there are anuy pending
    """

    def __init__(self):
        self._q = asyncio.Queue()  # technically not thread-safe

    def empty(self):
        """ Returns whether or not the queue is empty """
        return self._q.empty()

    def wait(self):
        """ Dequeues and returns a signal if one is available """
        try:
            signum = self._q.get_nowait()
            log.info(f'Dequeued signal: {str(signal.Signals(signum))}')
            return signum
        except asyncio.QueueEmpty:
            return None

    def handler(self, signum, frame):
        """ Signal handler that enqueues the received signal into the signal
        queue.
        """
        self._q.put_nowait(signum)
        log.info(f'Enqueued signal: {str(signal.Signals(signum))}')
