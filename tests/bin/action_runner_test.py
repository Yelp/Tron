from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import tempfile

import action_runner
import mock
from testify import assert_equal
from testify import setup
from testify import setup_teardown
from testify import TestCase

from tests.testingutils import autospec_method


class StatusFileTestCase(TestCase):

    @setup
    def setup_status_file(self):
        self.filename = tempfile.NamedTemporaryFile().name
        self.status_file = action_runner.StatusFile(self.filename)

    @mock.patch('action_runner.opener', autospec=True)
    @mock.patch('action_runner.yaml', autospec=True)
    def test_write(self, mock_yaml, mock_open):
        command, proc = 'do this', mock.Mock()
        autospec_method(self.status_file.get_content)
        self.status_file.write(command, proc)
        self.status_file.get_content.assert_called_with(command, proc)
        mock_yaml.safe_dump.assert_called_with(
            self.status_file.get_content.return_value,
            mock_open.return_value.__enter__.return_value,
        )

    def test_get_content(self):
        command, proc = 'do this', mock.Mock()
        content = self.status_file.get_content(command, proc)
        expected = dict(
            command=command, pid=proc.pid,
            return_code=proc.returncode,
        )
        assert_equal(content, expected)


class RegisterTestCase(TestCase):
    mock_isdir = mock_status_file = None
    mock_makedirs = None

    @setup_teardown
    def patch_sys(self):
        with contextlib.nested(
            mock.patch('action_runner.os.path.isdir', autospec=True),
            mock.patch('action_runner.os.makedirs', autospec=True),
            mock.patch('action_runner.StatusFile', autospec=True),
        ) as (
            self.mock_isdir,
            self.mock_makedirs,
            self.mock_status_file,
        ):
            self.output_path = '/bogus/path/does/not/exist'
            self.command = 'command'
            self.proc = mock.Mock()
            yield

    def test_get_status_file_dir_does_not_exist_created(self):
        self.mock_isdir.return_value = False
        status_file = action_runner.get_status_file(self.output_path)
        assert_equal(status_file, self.mock_status_file.return_value)
        self.mock_status_file.assert_called_with(
            self.output_path + '/' + action_runner.STATUS_FILE,
        )

    def test_get_status_file_dir_does_not_exist_create_failed(self):
        self.mock_isdir.return_value = False
        self.mock_makedirs.side_effect = OSError
        status_file = action_runner.get_status_file(self.output_path)
        assert_equal(status_file, action_runner.NoFile)

    @mock.patch('action_runner.sys.exit', autospec=True)
    def test_register(self, mock_sys_exit):
        action_runner.register(self.output_path, self.command, self.proc)
        self.mock_status_file.assert_called_with(
            self.output_path + '/' + action_runner.STATUS_FILE,
        )
        self.mock_status_file.return_value.wrap.assert_called_with(
            self.command, self.proc,
        )
        self.proc.wait.assert_called_with()
        mock_sys_exit.assert_called_with(self.proc.returncode)
