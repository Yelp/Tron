import contextlib
import fcntl
import logging
import os
import signal

log = logging.getLogger(__name__)


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
