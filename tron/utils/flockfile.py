from __future__ import absolute_import
from __future__ import unicode_literals

import fcntl
import logging
import os

import lockfile

log = logging.getLogger(__name__)


class FlockFile(object):
    """
    A lockfile (matching the specification of the builtin lockfile class)
    based off of flock. Single lockfile per process (no thread support)..
    """

    def __init__(self, path):
        self.path = path
        self.lock_file = None
        try:
            self.lock_file = open(self.path, 'a')
        except (IOError, OSError) as e:
            raise lockfile.LockFailed(e, self.path)
        self._has_lock = False

    @property
    def file(self):
        """Get a handle to the underlying lock file (to write out data to)"""
        return self.lock_file

    def acquire(self):
        log.debug("Locking %s", self.path)
        try:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._has_lock = True
        except IOError as e:
            raise lockfile.AlreadyLocked(e, self.path)
        log.debug("Locked %s", self.path)

    def break_lock(self):
        """Can't break posix locks, sorry man"""
        raise lockfile.LockError()

    @property
    def i_am_locking(self):
        return self._has_lock

    def is_locked(self):
        if self._has_lock:
            return True
        try:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            return False
        except IOError:
            return True

    def release(self):
        log.debug("Releasing lock on %s", self.path)
        if self.i_am_locking:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self._has_lock = False
        else:
            raise lockfile.NotLocked(self.path)
        log.debug("Unlocked %s", self.path)

    def destroy(self):
        try:
            if self.i_am_locking:
                self.release()
            self.lock_file.close()
        finally:
            if os.path.exists(self.path):
                os.unlink(self.path)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()
