"""
 A mock daemon for testing service handling.
"""
import os

import daemon
import time


class PIDFile(object):

    def __init__(self, filename):
        self.filename = filename
        self.check_if_pidfile_exists()

    def check_if_pidfile_exists(self):
        try:
            with open(self.filename) as fh:
                pid = int(fh.read().strip())
        except IOError:
            pid = None

        if self.is_process_running(pid):
            raise SystemExit("Daemon running as %s" % pid)

        if pid:
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.unlink(self.filename)


def do_main_program():
    while True:
        print "ok"
        time.sleep(2)


if __name__ == "__main__":
    with daemon.DaemonContext(pidfile=PIDFile('/tmp/mock_daemon.pid')):
        do_main_program()