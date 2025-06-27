import contextlib
import fcntl
import logging
import os
import signal
from typing import Union

log = logging.getLogger(__name__)


# TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
def maybe_decode(maybe_string: Union[str, bytes]) -> str:
    if isinstance(maybe_string, bytes):
        return maybe_string.decode()
    return maybe_string


# TODO: TRON-2293 maybe_encode is a relic of Python2->Python3 migration. Remove it.
def maybe_encode(maybe_bytes: Union[str, bytes]) -> bytes:
    if not isinstance(maybe_bytes, bytes):
        return maybe_bytes.encode()
    return maybe_bytes


def next_or_none(iterable):
    try:
        return next(iterable)
    except StopIteration:
        pass


@contextlib.contextmanager
def flock(fd):
    close = False
    if isinstance(fd, str):
        fd = open(fd, "a")
        close = True

    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as e:  # locked by someone else
        log.debug(f"Locked by another process: {fd}")
        raise e

    try:
        yield
    finally:
        fcntl.lockf(fd, fcntl.LOCK_UN)
        if close:
            fd.close()


@contextlib.contextmanager
def chdir(path):
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@contextlib.contextmanager
def signals(signal_map):
    orig_map = {}
    for signum, handler in signal_map.items():
        orig_map[signum] = signal.signal(signum, handler)

    try:
        yield
    finally:
        for signum, handler in orig_map.items():
            signal.signal(signum, handler)
