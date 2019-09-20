import tempfile
from queue import Queue

import mock
import pytest
import yaml

from tron.bin import recover_batch
from tron.bin.action_runner import StatusFile


@pytest.fixture
def mock_file():
    f = tempfile.NamedTemporaryFile()
    yield f.name
    f.close()


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


@pytest.mark.parametrize('content,expected', [
    ('', None),
    ('{"bar": 1}', None),
    ('{"foo": 1}', 1),
])
def test_get_key_from_last_line(mock_file, content, expected):
    with open(mock_file, 'w') as f:
        f.write(content)
        f.flush()
    assert recover_batch.get_key_from_last_line(mock_file, 'foo') == expected
