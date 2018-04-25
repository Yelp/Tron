from __future__ import absolute_import
from __future__ import unicode_literals

import os
import tempfile

import lockfile
import mock
from testify import assert_equal
from testify import run
from testify import setup
from testify import teardown
from testify import TestCase
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

    def test_check_if_pidfile_exists_file_locked(self):
        assert_raises(lockfile.AlreadyLocked, PIDFile, self.filename)

    def test_check_if_pidfile_exists_file_exists(self):
        self.pidfile.__exit__(None, None, None)
        with open(self.filename, 'w') as fh:
            fh.write('123\n')

        with mock.patch.object(PIDFile, 'is_process_running') as mock_method:
            mock_method.return_value = True
            exception = assert_raises(SystemExit, PIDFile, self.filename)
            assert_in('Daemon running as 123', str(exception))

    def test_is_process_running(self):
        assert self.pidfile.is_process_running(os.getpid())

    def test_is_process_running_not_running(self):
        assert not self.pidfile.is_process_running(None)
        # Hope this isn't in use
        assert not self.pidfile.is_process_running(99999)

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
