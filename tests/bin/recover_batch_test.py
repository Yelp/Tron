from queue import Queue

import mock
import pytest
import yaml

from tron.bin import recover_batch


@mock.patch('tron.bin.recover_batch.read_last_yaml_entries', autospec=True)
@mock.patch.object(recover_batch, 'reactor', autospec=True)
@pytest.mark.parametrize('line,exit_code,error_msg', [
    (  # action runner finishes successfully
        '\n'.join(['return_code: 0', 'runner_pid: 12345']),
        0,
        None
    ), (  # action runner is killed
        '\n'.join(['return_code: -9', 'runner_pid: 12345']),
        9,
        'Action run killed by signal SIGKILL',
    ), (  # action runner is somehow no longer running
        'runner_pid: 12345',
        1,
        'Action runner pid 12345 no longer running. Assuming an exit of 1.'
    )
])
def test_notify(mock_reactor, mock_read_last_yaml_entries, line, exit_code, error_msg):
    fake_path = mock.MagicMock()
    mock_read_last_yaml_entries.return_value = yaml.safe_load(line)
    q = Queue()

    recover_batch.notify(q, 'some_ignored', fake_path, 'a_mask')

    actual_exit_code, actual_error_msg = q.get_nowait()
    assert actual_exit_code == exit_code
    assert actual_error_msg == error_msg
    assert mock_reactor.stop.call_count == 1
