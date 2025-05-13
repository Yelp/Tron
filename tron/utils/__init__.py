import contextlib
import fcntl
import logging
import os
import signal
from pathlib import Path
from types import FrameType
from typing import BinaryIO
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Iterator
from typing import Optional
from typing import TextIO
from typing import TypeVar
from typing import Union


log = logging.getLogger(__name__)

T = TypeVar("T")

# Type aliases for signal handling
SignalHandlerFunc = Callable[[int, Optional[FrameType]], None]
SignalHandler = Union[SignalHandlerFunc, int, None]


# TODO: TRON-2293 maybe_decode is a relic of Python2->Python3 migration. Remove it.
def maybe_decode(maybe_string: Union[str, bytes]) -> str:
    if isinstance(maybe_string, bytes):
        return maybe_string.decode()
    return maybe_string


# TODO: TRON-2293 maybe_encode is a relic of Python2->Python3 migration. Remove it.
def maybe_encode(maybe_bytes: Union[str, bytes]) -> bytes:
    if isinstance(maybe_bytes, str):
        return maybe_bytes.encode()
    return maybe_bytes


def next_or_none(iterable: Iterator[T]) -> Optional[T]:
    try:
        return next(iterable)
    except StopIteration:
        return None


@contextlib.contextmanager
def flock(fd: Union[str, BinaryIO, TextIO]) -> Generator[None, None, None]:
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
def chdir(path: Union[str, bytes, Path]) -> Generator[None, None, None]:
    cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


@contextlib.contextmanager
def signals(signal_map: Dict[int, SignalHandler]) -> Generator[None, None, None]:
    orig_map = {}
    for signum, handler in signal_map.items():
        orig_map[signum] = signal.signal(signum, handler)

    try:
        yield
    finally:
        for signum, handler in orig_map.items():
            signal.signal(signum, handler)
