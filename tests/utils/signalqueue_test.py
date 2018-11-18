import signal

import mock
import pytest

from tron.utils.signalqueue import SignalQueue


@pytest.fixture
def mock_lock():
    with mock.patch('threading.Lock', autospec=True) as mock_Lock:
        yield mock_Lock


@pytest.fixture
def mock_sigqueue(mock_lock):
    return SignalQueue()


def test_init(mock_sigqueue, mock_lock):
    # we don't want any locking in this module, since it'll cause deadlock
    assert mock_lock.call_count == 0


def test_wait_empty(mock_sigqueue):
    signum = mock_sigqueue.wait()

    assert signum is None


def test_wait_pending(mock_sigqueue):
    mock_sigqueue._q.put_nowait(signal.SIGINT)

    signum = mock_sigqueue.wait()

    assert signum == signal.SIGINT


def test_handler(mock_sigqueue):
    mock_sigqueue.handler(signal.SIGINT, None)

    assert mock_sigqueue._q.get_nowait() == signal.SIGINT
