import contextlib
import tempfile

import mock
from testify import assert_equal, setup, TestCase, setup_teardown


import action_runner
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
        mock_yaml.dump.assert_called_with(
            self.status_file.get_content.return_value,
            mock_open.return_value.__enter__.return_value)

    def test_get_content(self):
        command, proc = 'do this', mock.Mock()
        content = self.status_file.get_content(command, proc)
        expected = dict(command=command, pid=proc.pid, return_code=proc.returncode)
        assert_equal(content, expected)


class RegisterTestCase(TestCase):

    @setup_teardown
    def patch_sys(self):
        with contextlib.nested(
            mock.patch('action_runner.sys.exit', autospec=True),
            mock.patch('action_runner.os.path.isdir', autospec=True),
            mock.patch('action_runner.StatusFile', autospec=True),
            mock.patch('action_runner.NoFile', autospec=True)
            ) as (self.mock_exit,
                  self.mock_isdir,
                  self.mock_status_file,
                  self.mock_no_file):
            self.output_path = '/bogus/path/does/not/exist'
            self.command = 'command'
            self.proc = mock.Mock()
            yield

    def test_register_nodir(self):
        self.mock_isdir.return_value = False
        action_runner.register(self.output_path, self.command, self.proc)
        self.mock_no_file.assert_called_with()
        self.mock_no_file.return_value.wrap.assert_called_with(
            self.command, self.proc)
        self.mock_exit.assert_called_with(self.proc.returncode)

    def test_register_dir_exists(self):
        action_runner.register(self.output_path, self.command, self.proc)
        self.mock_status_file.assert_called_with(
            self.output_path + '/' + action_runner.STATUS_FILE)
        self.mock_status_file.return_value.wrap.assert_called_with(
            self.command, self.proc)
        self.mock_exit.assert_called_with(self.proc.returncode)