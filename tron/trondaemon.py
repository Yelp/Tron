"""
 Daemonize trond.
"""
import logging
import os

import lockfile


log = logging.getLogger(__name__)

class PIDFile(object):
    """Create and check for a PID file for the daemon."""

    def __init__(self, filename):
        self.filename = filename
        self.check_if_pidfile_exists()


    def check_if_pidfile_exists(self):
        self.lock = lockfile.FileLock(self.filename)
        self.lock.acquire(0)

        try:
            with open(self.filename) as fh:
                pid = int(fh.read().strip())
        except IOError:
            pid = None

        if self.is_process_running(pid):
            self._try_unlock()
            raise SystemExit("Daemon running as %s" % pid)

        if pid:
            self._try_unlock()
            raise SystemExit("Daemon was running as %s. Remove PID file." % pid)

    def is_process_running(self, pid):
        """Return True the process is still running."""
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def __enter__(self):
        with open(self.filename, 'w') as fh:
            fh.write('%s\n' % os.getpid())

    def _try_unlock(self):
        try:
            self.lock.release()
        except lockfile.NotLocked:
            log.warn("Lockfile was already unlocked.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._try_unlock()
        try:
            os.unlink(self.filename)
        except OSError:
            log.warn("Failed to remove pidfile: %s" % self.filename)