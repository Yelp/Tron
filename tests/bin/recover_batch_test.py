import tempfile
from queue import Queue

import mock
import pytest

from tron.bin import recover_batch
from tron.bin.action_runner import StatusFile


@pytest.fixture
def mock_file():
    f = tempfile.NamedTemporaryFile()
    yield f.name
    f.close()


@mock.patch.object(recover_batch, 'reactor')
@mock.patch('tron.bin.recover_batch.get_exit_code', autospec=True)
@pytest.mark.parametrize('exit_code,error_msg,should_stop', [
    (1, 'failed', True),
    (None, None, False),
])
def test_notify(mock_get_exit_code, mock_reactor, exit_code, error_msg, should_stop):
    mock_get_exit_code.return_value = exit_code, error_msg
    queue = Queue()
    path = mock.Mock()
    recover_batch.notify(queue, 'some_ignored', path, 'mask')
    if should_stop:
        assert mock_reactor.stop.call_count == 1
        assert queue.get_nowait() == (exit_code, error_msg)
    else:
        assert mock_reactor.stop.call_count == 0
        assert queue.empty()


@mock.patch('tron.bin.recover_batch.psutil.pid_exists', autospec=True)
@mock.patch('tron.bin.recover_batch.read_last_yaml_entries', autospec=True)
@pytest.mark.parametrize('line,exit_code,is_running,error_msg', [
    (  # action runner finishes successfully
        {'return_code': 0, 'runner_pid': 12345},
        0,
        False,
        None
    ), (  # action runner is killed
        {'return_code': -9, 'runner_pid': 12345},
        9,
        False,
        'Action run killed by signal SIGKILL',
    ), (  # No return code but action_runner pid is not running
        {'runner_pid': 12345},
        1,
        False,
        'Action runner pid 12345 no longer running. Assuming an exit of 1.'
    ), (  # No return code but action_runner pid is running
        {'runner_pid': 12345},
        None,
        True,
        None,
    ), (  # No return code or PID from the file
        {},
        None,
        Exception,
        None,
    )
])
def test_get_exit_code(mock_read_last_yaml_entries, mock_pid_running, line, exit_code, is_running, error_msg):
    fake_path = '/file/path'
    mock_read_last_yaml_entries.return_value = line
    mock_pid_running.side_effect = [is_running]

    actual_exit_code, actual_error_msg = recover_batch.get_exit_code(fake_path)
    assert actual_exit_code == exit_code
    assert actual_error_msg == error_msg


def test_read_last_yaml_roundtrip(mock_file):
    """Check that read_last_yaml_entries returns the same thing that the action
    runner wrote."""
    status = StatusFile(mock_file)
    expected_content = [
        {'return_code': None, 'pid': 10345, 'command': 'foo'},
        {'return_code': 1, 'pid': 10345, 'command': 'foo'},
    ]
    with mock.patch.object(status, 'get_content', side_effect=expected_content):
        with status.wrap(command='echo hello', run_id='job.1.action', proc=mock.Mock()):
            # In the context manager, we've written the first value of get_content.
            first = recover_batch.read_last_yaml_entries(mock_file)
            assert first == expected_content[0]

    # After, we write another status entry. We should return the latest.
    second = recover_batch.read_last_yaml_entries(mock_file)
    assert second == expected_content[1]


@mock.patch.object(recover_batch, 'reactor')
@mock.patch('tron.bin.recover_batch.Queue', autospec=True)
@mock.patch('tron.bin.recover_batch.get_exit_code', autospec=True, return_value=(None, None))
@mock.patch('tron.bin.recover_batch.StatusFileWatcher', autospec=True)
@pytest.mark.parametrize('existing_code,watcher_code', [(None, 1), (123, None)])
def test_run(mock_watcher, mock_get_exit_code, mock_queue, mock_reactor, existing_code, watcher_code):
    mock_get_exit_code.return_value = (existing_code, '')
    mock_queue.return_value.get.return_value = (watcher_code, '')
    mock_path = mock.Mock()
    if existing_code is not None:
        expected = existing_code
    else:
        expected = watcher_code

    with pytest.raises(SystemExit) as e:
        recover_batch.run(mock_path)
        assert e.code == expected

    assert mock_get_exit_code.call_args_list == [mock.call(mock_path)]
    if existing_code is not None:
        assert mock_watcher.call_count == 0
    else:
        assert mock_watcher.call_count == 1
