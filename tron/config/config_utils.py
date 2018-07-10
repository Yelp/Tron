# pylint: disable=E1102
import datetime
import re

import pytz

from tron.config import ConfigError
from tron.utils import dicts

MAX_IDENTIFIER_LENGTH = 255
IDENTIFIER_RE = re.compile(r'^[A-Za-z_][\w\-]{0,254}$')


def valid_time(value, *_):
    for format in ['%H:%M', '%H:%M:%S']:
        try:
            return datetime.datetime.strptime(value, format)
        except Exception:
            pass
    raise ValueError(f'{value} is not a valid time')


def valid_time_zone(tz):
    if tz is None:
        return None

    if isinstance(tz, datetime.tzinfo):
        return tz

    try:
        return pytz.timezone(tz)
    except pytz.exceptions.UnknownTimeZoneError:
        raise ValueError(f'{tz} is not a valid time zone')


# Translations from possible configuration units to the argument to
# datetime.timedelta
TIME_INTERVAL_UNITS = dicts.invert_dict_list({
    'days': ['d', 'day', 'days'],
    'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
    'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
    'seconds': ['s', 'sec', 'secs', 'second', 'seconds'],
})

TIME_INTERVAL_RE = re.compile(r"^\s*(?P<value>\d+)\s*(?P<units>[a-zA-Z]+)\s*$")


def valid_time_delta(value, *_args):
    if value is None:
        return None

    if isinstance(value, datetime.timedelta):
        return value

    error_msg = "Value at %s is not a valid time delta: %s"
    matches = TIME_INTERVAL_RE.match(value)
    if not matches:
        raise ConfigError(error_msg.format(value))

    units = matches.group('units')
    if units not in TIME_INTERVAL_UNITS:
        raise ConfigError(error_msg.format(value))

    time_spec = {TIME_INTERVAL_UNITS[units]: int(matches.group('value'))}
    return datetime.timedelta(**time_spec)


class ConfigContext(object):
    """An object to encapsulate the context in a configuration file. Supplied
    to Validators to perform validation which requires knowledge of
    configuration outside of the immediate configuration dictionary.
    """
    partial = False

    def __init__(self, path, nodes, command_context, namespace):
        self.path = path
        self.nodes = set(nodes or [])
        self.command_context = command_context or {}
        self.namespace = namespace

    def build_child_context(self, path):
        """Construct a new ConfigContext based on this one."""
        path = '%s.%s' % (self.path, path)
        args = path, self.nodes, self.command_context, self.namespace
        return ConfigContext(*args)
