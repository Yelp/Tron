from __future__ import absolute_import
from __future__ import unicode_literals

import tempfile

import mock
import pytest

from testifycompat import assert_equal
from testifycompat import setup
from testifycompat import setup_teardown
from testifycompat import TestCase
from tron.bin import action_runner


class TestStatusFile(TestCase):
    @setup
    def setup_status_file(self):
        self.filename = tempfile.NamedTemporaryFile().name
        self.status_file = action_runner.StatusFile(self.filename)

    def test_get_content(self):
        command, proc, run_id = 'do this', mock.Mock(), 'Job.test.1'
        with mock.patch('tron.bin.action_runner.time.time', autospec=True) as faketime, \
                mock.patch('tron.bin.action_runner.os.getpid', autospec=True) as fakepid:
            faketime.return_value = 0
            fakepid.return_value = 2
            content = self.status_file.get_content(
                command=command,
                proc=proc,
                run_id=run_id,
            )
            expected = dict(
                run_id=run_id,
                command=command,
                pid=proc.pid,
                return_code=proc.returncode,
                runner_pid=2,
                timestamp=0,
            )
        assert_equal(content, expected)


class TestRegister(TestCase):
    mock_isdir = mock_status_file = None
    mock_makedirs = None

    @setup_teardown
    def patch_sys(self):
        with mock.patch('tron.bin.action_runner.os.path.isdir', autospec=True) as self.mock_isdir, \
                mock.patch('tron.bin.action_runner.os.makedirs', autospec=True) as self.mock_makedirs, \
                mock.patch('tron.bin.action_runner.os.access', autospec=True) as self.mock_access, \
                mock.patch('tron.bin.action_runner.StatusFile', autospec=True) as self.mock_status_file:
            self.output_path = '/bogus/path/does/not/exist'
            self.command = 'command'
            self.run_id = 'Job.test.1'
            self.proc = mock.Mock()
            self.proc.wait.return_value = 0
            yield

    def test_validate_output_dir_does_not_exist(self):
        self.mock_isdir.return_value = False
        self.mock_access.return_value = True
        action_runner.validate_output_dir(self.output_path)
        self.mock_makedirs.assert_called_with(self.output_path)

    def test_validate_output_dir_does_not_exist_create_fails(self):
        self.mock_isdir.return_value = False
        self.mock_access.return_value = True
        self.mock_makedirs.side_effect = OSError
        with pytest.raises(OSError):
            action_runner.validate_output_dir(self.output_path)

    def test_validate_output_dir_exists_not_writable(self):
        self.mock_isdir.return_value = True
        self.mock_access.return_value = False
        with pytest.raises(OSError):
            action_runner.validate_output_dir(self.output_path)

    def test_run_proc(self):
        self.mock_isdir.return_value = True
        self.mock_access.return_value = True
        action_runner.run_proc(
            self.output_path,
            self.command,
            self.run_id,
            self.proc,
        )
        self.mock_status_file.assert_called_with(
            self.output_path + '/' + action_runner.STATUS_FILE,
        )
        self.mock_status_file.return_value.wrap.assert_called_with(
            command=self.command,
            run_id=self.run_id,
            proc=self.proc,
        )
        self.proc.wait.assert_called_with()


class TestBuildEnvironment:
    def test_build_environment(self):
        with mock.patch('tron.bin.action_runner.os.environ', dict(PATH='/usr/bin/nowhere'), autospec=None):
            env = action_runner.build_environment('MASTER.foo.10.bar')

        assert env == dict(
            PATH='/usr/bin/nowhere',
            TRON_JOB_NAMESPACE='MASTER',
            TRON_JOB_NAME='foo',
            TRON_RUN_NUM='10',
            TRON_ACTION='bar',
        )

    def test_build_environment_invalid_run_id(self):
        with mock.patch('tron.bin.action_runner.os.environ', dict(PATH='/usr/bin/nowhere'), autospec=None):
            env = action_runner.build_environment('asdf')

        assert env == dict(
            PATH='/usr/bin/nowhere',
            TRON_JOB_NAMESPACE='UNKNOWN',
            TRON_JOB_NAME='UNKNOWN',
            TRON_RUN_NUM='UNKNOWN',
            TRON_ACTION='UNKNOWN',
        )

    def test_build_environment_too_long_run_id(self):
        with mock.patch('tron.bin.action_runner.os.environ', dict(PATH='/usr/bin/nowhere'), autospec=None):
            env = action_runner.build_environment('MASTER.foo.10.bar.baz')

        assert env == dict(
            PATH='/usr/bin/nowhere',
            TRON_JOB_NAMESPACE='MASTER',
            TRON_JOB_NAME='foo',
            TRON_RUN_NUM='10',
            TRON_ACTION='bar.baz',
        )
