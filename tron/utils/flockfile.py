from __future__ import absolute_import
from __future__ import unicode_literals

import fcntl
import logging

log = logging.getLogger(__name__)


class FlockFile(object):
    """ Provides a simple interface to locking and unlocking files, including
    through context management.
    """

    def __init__(self, path):
        self.path = path
        self.file = open(self.path, 'a')  # can raise OSError
        self._has_lock = False

    def __str__(self):
        return self.path

    @property
    def is_locked(self):
        if self._has_lock:
            return True
        try:
            fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            return False
        except BlockingIOError:
            log.debug(f"Locked by another process: {self.path}")
            return True

    def acquire(self):
        log.debug(f"Attempting to lock: {self.path}")
        try:
            fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._has_lock = True
        except BlockingIOError as e:  # locked by someone else
            log.debug(f"Locked by another process: {self.path}")
            raise e
        log.debug(f"Locked {self.path}")

    def release(self):
        log.debug(f"Attempting to unlock: {self.path}")
        if self._has_lock:  # locked by us
            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            self._has_lock = False
        elif self.is_locked:  # locked by someone else
            raise BlockingIOError(f"Locked by another process: {self.path}")
        else:
            log.warning(f"Attempted to release an unlocked file: {self.path}")
        log.debug(f"Unlocked {self.path}")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()
