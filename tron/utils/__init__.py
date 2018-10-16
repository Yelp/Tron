from __future__ import absolute_import
from __future__ import unicode_literals


def maybe_decode(maybe_string):
    if type(maybe_string) is bytes:
        return maybe_string.decode()
    return maybe_string


def maybe_encode(maybe_bytes):
    if type(maybe_bytes) is not bytes:
        return maybe_bytes.encode()
    return maybe_bytes


def next_or_none(iterable):
    try:
        return next(iterable)
    except StopIteration:
        pass
