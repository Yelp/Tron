from __future__ import absolute_import
from __future__ import unicode_literals

import signal
import tempfile

import action_status
import mock
import yaml
from testify import setup_teardown
from testify import TestCase


class ActionStatusTestCase(TestCase):

    @setup_teardown
    def setup_status_file(self):
        self.status_file = tempfile.NamedTemporaryFile()
        self.status_content = {
            'pid': 1234,
            'return_code': None,
            'run_id': 'MASTER.foo.bar.1234',
        }
        self.status_file.write(yaml.safe_dump(self.status_content))
        self.status_file.flush()
        self.status_file.seek(0)
        yield
        self.status_file.close()

    @mock.patch('action_status.os.kill', autospec=True)
    def test_send_signal(self, mock_kill):
        action_status.send_signal(signal.SIGKILL, self.status_file)
        mock_kill.assert_called_with(
            self.status_content['pid'], signal.SIGKILL,
        )

    def test_get_field_retrieves_last_entry(self):
        self.status_file.seek(0, 2)
        additional_status_content = {
            'pid': 1234,
            'return_code': 0,
            'run_id': 'MASTER.foo.bar.1234',
        }
        self.status_file.write(yaml.safe_dump(additional_status_content))
        self.status_file.flush()
        self.status_file.seek(0)
        assert action_status.get_field('return_code', self.status_file) == 0

    def test_get_field_none(self):
        assert action_status.get_field('return_code', self.status_file) is None
