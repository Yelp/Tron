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
    if not request.args or key not in request.args:
        return None

    value = request.args[key][0]
    if not value.isdigit():
        return None
    return int(value)


def get_string(request, key):
    """Returns the first value in the request args for a given key."""
    if not request.args or key not in request.args:
        return None
    return request.args[key][0]


def get_bool(request, key):
    """Returns True if the key exists and is truthy in the request args."""
    return bool(get_integer(request, key))


def get_datetime(request, key):
    """Returns the first value in the request args for a given key. Casts to
    a datetime. Returns None if the value cannot be converted to datetime.
    """
    if not request.args or key not in request.args:
        return False
    try:
        return datetime.datetime.strptime(request.args[key][0], DATE_FORMAT)
    except ValueError:
        return None
