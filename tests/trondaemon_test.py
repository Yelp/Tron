from __future__ import absolute_import
from __future__ import unicode_literals

import os
import tempfile

import lockfile
import mock

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tests.assertions import assert_raises
from tron.trondaemon import PIDFile
from tron.trondaemon import TronDaemon


class TestPIDFile(TestCase):
    @setup
    @mock.patch('tron.trondaemon.log', autospec=None)
    def setup_pidfile(self, _):
        self.filename = os.path.join(tempfile.gettempdir(), 'test.pid')
        self.pidfile = PIDFile(self.filename)

    @teardown
    @mock.patch('tron.trondaemon.log', autospec=None)
    def teardown_pidfile(self, _):
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


class TronDaemonTestCase(TestCase):
    @setup
    def setup(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        trond_opts = mock.Mock()
        trond_opts.working_dir = self.tmpdir.name
        trond_opts.pid_file = os.path.join(self.tmpdir.name, "pidfile")
        self.trond = TronDaemon(trond_opts)

    @teardown
    def teardown(self):
        self.tmpdir.cleanup()
