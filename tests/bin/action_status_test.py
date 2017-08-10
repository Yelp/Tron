from __future__ import absolute_import
from __future__ import unicode_literals

import action_status
import mock
from testify import TestCase


class ActionStatusTestCase(TestCase):

    @mock.patch('action_status.os.kill', autospec=True)
    def test_send_signal(self, mock_kill):
        status_file = {'pid': 123}
        signal_num = 7
        action_status.send_signal(signal_num, status_file)
        mock_kill.assert_called_with(status_file['pid'], signal_num)

    @mock.patch.dict(action_status.commands)
    def test_run_command(self):
        command, func, status_file = 'print', mock.Mock(), 'status_file'
        action_status.commands['print'] = func
        action_status.run_command(command, status_file)
        func.assert_called_with(status_file)
