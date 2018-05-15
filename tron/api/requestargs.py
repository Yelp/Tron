"""Functions for returning validated values from a twisted.web.Request object.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_integer(request, key):
    """Returns the first value in the request args for the given key, if that
    value is an integer. Otherwise returns None.
    """
    value = get_string(request, key)
    if value is None or not value.isdigit():
        return None

    return int(value)


def get_string(request, key):
    """Returns the first value in the request args for a given key."""
    if not request.args:
        return None

    if type(key) is not bytes:
        key = key.encode()

    if key not in request.args:
        return None

    val = request.args[key][0]
    if val is not None and type(val) is bytes:
        val = val.decode()

    return val


def get_bool(request, key, default=None):
    """Returns True if the key exists and is truthy in the request args."""
    int_value = get_integer(request, key)
    if int_value is None:
        return default

    return bool(int_value)


def get_datetime(request, key):
    """Returns the first value in the request args for a given key. Casts to
    a datetime. Returns None if the value cannot be converted to datetime.
    """
    val = get_string(request, key)
    if not val:
        return None

    try:
        return datetime.datetime.strptime(val, DATE_FORMAT)
    except ValueError:
        return None
