from queue import Queue

import mock
import pytest

from tron.bin import recover_batch


@mock.patch('builtins.open', autospec=True)
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
        'Action runner pid 12345 no longer running; unable to recover it'
    )
])
def test_notify(mock_reactor, mock_open, line, exit_code, error_msg):
    fake_path = mock.MagicMock()
    mock_open.return_value.__enter__.return_value.readlines.return_value = [line]
    q = Queue()

    recover_batch.notify(q, 'some_ignored', fake_path, 'a_mask')

    actual_exit_code, actual_error_msg = q.get_nowait()
    assert actual_exit_code == exit_code
    assert actual_error_msg == error_msg
    assert mock_reactor.stop.call_count == 1


@mock.patch.object(recover_batch, 'get_key_from_last_line', mock.Mock(return_value=234))
def test_run_already_returned():
    with pytest.raises(SystemExit) as exc_info:
        recover_batch.run('a_path')

    assert exc_info.value.code == 234


@mock.patch.object(recover_batch, 'get_key_from_last_line', mock.Mock(side_effect=[
    None,  # return_code
    'a_pid',
]))
@mock.patch('psutil.pid_exists', mock.Mock(return_value=False), autospec=None)
def test_run_not_running():
    with pytest.raises(SystemExit) as exc_info:
        recover_batch.run('a_path')

    assert exc_info.value.code == 1


@mock.patch.object(recover_batch, 'get_key_from_last_line', mock.Mock(side_effect=[None, None]))
@mock.patch('psutil.pid_exists', mock.Mock(return_value=True), autospec=None)
@mock.patch.object(recover_batch, 'Queue', autospec=True)
@mock.patch.object(recover_batch, 'StatusFileWatcher', mock.Mock())
@mock.patch.object(recover_batch, 'reactor', mock.Mock())
def test_run_end_after_notify(mock_queue):
    mock_queue.return_value.get.return_value = (42, 'a_message')

    with pytest.raises(SystemExit) as exc_info:
        recover_batch.run('a_path')

    assert exc_info.value.code == 42
