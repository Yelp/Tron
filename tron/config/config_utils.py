# pylint: disable=E1102
import datetime
import functools
import re

import pytz
from six import string_types

from tron.config import ConfigError
from tron.config.schema import MASTER_NAMESPACE
from tron.utils import dicts

MAX_IDENTIFIER_LENGTH = 255
IDENTIFIER_RE = re.compile(r'^[A-Za-z_][\w\-]{0,254}$')


def build_type_validator(validator, error_fmt):
    """Create a validator function using `validator` to validate the value.
        validator - a function which takes a single argument `value`
        error_fmt - a string which accepts two format variables (path, value)

        Returns a function func(value, config_context) where
            value - the value to validate
            config_context - a ConfigContext object
            Returns True if the value is valid
    """

    def f(value, config_context):
        if not validator(value):
            raise ConfigError(error_fmt % (config_context.path, value))
        return value

    return f


def valid_number(type_func, value, config_context):
    path = config_context.path
    try:
        value = type_func(value)
    except TypeError:
        name = type_func.__name__
        raise ConfigError('Value at %s is not an %s: %s' % (path, name, value))

    if value < 0:
        raise ConfigError('%s must be a positive int.' % path)

    return value


valid_int = functools.partial(valid_number, int)
valid_float = functools.partial(valid_number, float)

valid_identifier = build_type_validator(
    lambda s: isinstance(s, string_types) and IDENTIFIER_RE.match(s),
    'Identifier at %s is not a valid identifier: %s',
)

valid_list = build_type_validator(
    lambda s: isinstance(s, list),
    'Value at %s is not a list: %s',
)

valid_string = build_type_validator(
    lambda s: isinstance(s, string_types),
    'Value at %s is not a string: %s',
)


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


def build_list_of_type_validator(item_validator, allow_empty=False):
    """Build a validator which validates a list contains items which pass
    item_validator.
    """

    def validator(value, config_context):
        if allow_empty and not value:
            return ()
        seq = valid_list(value, config_context)
        if not seq:
            msg = "Required non-empty list at %s"
            raise ConfigError(msg % config_context.path)
        return tuple(item_validator(item, config_context) for item in seq)

    return validator


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


class PartialConfigContext(object):
    """A context object which has only a partial context. It is missing
    command_context and nodes.  This is likely because it is being used in
    a named configuration fragment that does not have access to those pieces
    of the configuration.
    """
    partial = True

    def __init__(self, path, namespace):
        self.path = path
        self.namespace = namespace

    def build_child_context(self, path):
        path = '%s.%s' % (self.path, path)
        return PartialConfigContext(path, self.namespace)


class NullConfigContext(object):
    path = ''
    nodes = set()
    command_context = {}
    namespace = MASTER_NAMESPACE
    partial = False

    @staticmethod
    def build_child_context(_):
        return NullConfigContext
