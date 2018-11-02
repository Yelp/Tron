from __future__ import absolute_import
from __future__ import unicode_literals

import os
import tempfile

import mock
import pytest

from tron.utils.flockfile import FlockFile


@pytest.fixture
def tmp_lockfile():
    tmpdir = tempfile.TemporaryDirectory()
    yield os.path.join(tmpdir.name, "test.lock")
    tmpdir.cleanup()


@pytest.fixture
def mock_flockfile(tmp_lockfile):
    return FlockFile(tmp_lockfile)


@pytest.mark.parametrize('is_locked', [True, False])
def test_is_locked(mock_flockfile, is_locked):
    mock_flockfile._has_lock = is_locked

    assert mock_flockfile.is_locked == is_locked


@mock.patch('fcntl.flock', mock.Mock(side_effect=OSError), autospec=None)
def test_is_locked_by_other(mock_flockfile):
    mock_flockfile._has_lock = False

    assert mock_flockfile.is_locked


def test_acquire(mock_flockfile):
    mock_flockfile.acquire()

    assert mock_flockfile._has_lock


@mock.patch('fcntl.flock', mock.Mock(side_effect=OSError), autospec=None)
def test_acquire_other_process_locked(mock_flockfile):
    with pytest.raises(OSError):
        mock_flockfile.acquire()

    assert not mock_flockfile._has_lock


def test_release(mock_flockfile):
    mock_flockfile.acquire()  # already tested

    mock_flockfile.release()

    assert not mock_flockfile._has_lock


@mock.patch('fcntl.flock', mock.Mock(side_effect=BlockingIOError), autospec=None)
def test_release_other_process_locked(mock_flockfile):
    mock_flockfile._has_lock = True  # not possible, but allows us to test below

    with pytest.raises(BlockingIOError):
        mock_flockfile.release()

    assert mock_flockfile._has_lock
