import os
import tempfile
import lockfile
from testify import TestCase, assert_equal, run, setup, teardown
import threading
from testify.assertions import assert_in
from tests.assertions import assert_raises
from tron.trondaemon import PIDFile

class PIDFileTestCase(TestCase):

    @setup
    def setup_pidfile(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'test.pid')
        self.pidfile = PIDFile(self.filename)

    @teardown
    def teardown_pidfile(self):
        self.pidfile.__exit__(None, None, None)

    def test__init__(self):
        # Called from setup already
        assert self.pidfile.lock.is_locked()
        assert_equal(self.filename, self.pidfile.filename)

    def _test_in_thread(self, func):
        """lockfile acquisitions will only fail if they come from another thread
        so create the thread and run the tests in that thread.  Then test
        the tread completed properly.
        """
        def runnable():
            func()
            self.completed = True

        self.completed = False
        pid_thread = threading.Thread(target=runnable)
        pid_thread.start()
        pid_thread.join()
        assert self.completed

    def test_check_if_pidfile_exists_file_locked(self):
        def test_func():
            assert_raises(lockfile.AlreadyLocked, PIDFile, self.filename)
        self._test_in_thread(test_func)

    def test_check_if_pidfile_exists_file_exists(self):
        self.pidfile.__exit__(None, None, None)
        with open(self.filename, 'w') as fh:
            fh.write('123\n')

        def test_func():
            exception = assert_raises(SystemExit, PIDFile, self.filename)
            assert_in('Daemon was running as 123', str(exception))
        self._test_in_thread(test_func)

    def test_is_process_running(self):
        assert self.pidfile.is_process_running(os.getpid())

    def test_is_process_running_not_running(self):
        assert not self.pidfile.is_process_running(None)
        assert not self.pidfile.is_process_running(12345)

    def test__enter__(self):
        self.pidfile.__enter__()
        with open(self.filename, 'r') as fh:
            assert_equal(fh.read(), '%s\n' % os.getpid())

    def test__exit__(self):
        self.pidfile.__exit__(None, None, None)
        assert not self.pidfile.lock.is_locked()
        assert not os.path.exists(self.filename)


if __name__ == "__main__":
    run()