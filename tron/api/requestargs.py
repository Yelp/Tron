"""Functions for returning validated values from a twisted.web.Request object.
"""
import datetime
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from twisted.web.server import Request

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_integer(request: Request, key: Union[str, bytes]) -> Optional[int]:
    """Returns the first value in the request args for the given key, if that
    value is an integer. Otherwise returns None.
    """
    value = get_string(request, key)
    if value is None or not value.isdigit():
        return None

    return int(value)


def get_string(request: Request, key: Union[str, bytes]) -> Optional[str]:
    """Returns the first value in the request args for a given key."""
    # this is a bit of a hack, but mypy seems to get lost here to to the way that Request is typed
    request_args: Optional[Dict[bytes, List[bytes]]] = request.args
    if request_args is None:
        return None

    if not isinstance(key, bytes):
        key = key.encode()

    if key not in request_args:
        return None

    raw_val = request_args[key][0]
    return raw_val.decode() if isinstance(raw_val, bytes) else raw_val


def get_bool(request: Request, key: Union[str, bytes], default: Optional[bool] = None) -> Optional[bool]:
    """Returns True if the key exists and is truthy in the request args."""
    int_value = get_integer(request, key)
    if int_value is None:
        return default

    return bool(int_value)


def get_datetime(request: Request, key: Union[str, bytes]) -> Optional[datetime.datetime]:
    """Returns the first value in the request args for a given key. Casts to
    a datetime. Returns None if the value cannot be converted to datetime.
    """
    val = get_string(request, key)
    if not val:  # val is Optional[str], so this checks for None or empty string
        return None

    try:
        return datetime.datetime.strptime(val, DATE_FORMAT)
    except ValueError:
        return None
