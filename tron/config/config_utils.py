"""Utilities used for configuration parsing and validation."""
import functools
import itertools
import re
import datetime
from tron.config import ConfigError
from tron.config.schema import MASTER_NAMESPACE
from tron.utils.dicts import FrozenDict


MAX_IDENTIFIER_LENGTH       = 255
IDENTIFIER_RE               = re.compile(r'^[A-Za-z_][\w\-]{0,254}$')


class UniqueNameDict(dict):
    """A dict like object that throws a ConfigError if a key exists and
    __setitem__ is called to change the value of that key.

     fmt_string - format string used to create an error message, expects a
                  single format argument of 'key'
    """
    def __init__(self, fmt_string):
        super(dict, self).__init__()
        self.fmt_string = fmt_string

    def __setitem__(self, key, value):
        if key in self:
            raise ConfigError(self.fmt_string % key)
        super(UniqueNameDict, self).__setitem__(key, value)


def unique_names(fmt_string, *seqs):
    """Validate that each object in all sequences has a unique name."""
    name_dict = UniqueNameDict(fmt_string)
    for item in itertools.chain.from_iterable(seqs):
        name_dict[item] = True
    return name_dict


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

valid_int   = functools.partial(valid_number, int)
valid_float = functools.partial(valid_number, float)

valid_identifier = build_type_validator(
    lambda s: isinstance(s, basestring) and IDENTIFIER_RE.match(s),
    'Identifier at %s is not a valid identifier: %s')

valid_list = build_type_validator(
    lambda s: isinstance(s, list), 'Value at %s is not a list: %s')

valid_string  = build_type_validator(
    lambda s: isinstance(s, basestring), 'Value at %s is not a string: %s')

valid_dict = build_type_validator(
    lambda s: isinstance(s, dict), 'Value at %s is not a dictionary: %s')

valid_bool = build_type_validator(
    lambda s: isinstance(s, bool), 'Value at %s is not a boolean: %s')


def valid_time(value, config_context):
    valid_string(value, config_context)
    for format in ['%H:%M', '%H:%M:%S']:
        try:
            return datetime.datetime.strptime(value, format)
        except ValueError, exc:
            pass

    msg = 'Value at %s is not a valid time: %s'
    raise ConfigError(msg % (config_context.path, exc))


def valid_name_identifier(value, config_context):
    valid_identifier(value, config_context)
    if config_context.partial:
        return value
    return '%s.%s' % (config_context.namespace, value)


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


def build_dict_name_validator(item_validator, allow_empty=False):
    """Build a validator which validates a list, and returns a dict."""
    valid = build_list_of_type_validator(item_validator, allow_empty)
    def validator(value, config_context):
        msg = "Duplicate name %%s at %s" % config_context.path
        name_dict = UniqueNameDict(msg)
        for item in valid(value, config_context):
            name_dict[item.name] = item
        return FrozenDict(**name_dict)
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