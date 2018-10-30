"""Utilities used for configuration parsing and validation."""
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import functools
import itertools
import re
from string import Formatter

import six
from six import string_types

from tron.config import ConfigError
from tron.config.schema import MASTER_NAMESPACE
from tron.utils import dicts
from tron.utils.dicts import FrozenDict

MAX_IDENTIFIER_LENGTH = 255
IDENTIFIER_RE = re.compile(r'^[A-Za-z_][\w\-]{0,254}$')


class StringFormatter(Formatter):
    def __init__(self, context=None):
        Formatter.__init__(self)
        self.context = context

    def get_value(self, key, args, kwds):
        if isinstance(key, str):
            try:
                return kwds[key]
            except KeyError:
                return self.context[key]
        else:
            return Formatter.get_value(key, args, kwds)


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

valid_dict = build_type_validator(
    lambda s: isinstance(s, dict),
    'Value at %s is not a dictionary: %s',
)

valid_bool = build_type_validator(
    lambda s: isinstance(s, bool),
    'Value at %s is not a boolean: %s',
)


def build_enum_validator(enum):
    enum = set(enum)
    msg = 'Value at %%s is not in %s: %%s.' % str(enum)
    return build_type_validator(enum.__contains__, msg)


def valid_time(value, config_context):
    valid_string(value, config_context)
    for format in ['%H:%M', '%H:%M:%S']:
        try:
            return datetime.datetime.strptime(value, format)
        except ValueError:
            pass
    msg = 'Value at %s is not a valid time'
    raise ConfigError(msg % config_context.path)


# Translations from possible configuration units to the argument to
# datetime.timedelta
TIME_INTERVAL_UNITS = dicts.invert_dict_list({
    'days': ['d', 'day', 'days'],
    'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
    'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
    'seconds': ['s', 'sec', 'secs', 'second', 'seconds'],
})

TIME_INTERVAL_RE = re.compile(r"^\s*(?P<value>\d+)\s*(?P<units>[a-zA-Z]+)\s*$")


def valid_time_delta(value, config_context):
    error_msg = "Value at %s is not a valid time delta: %s"
    matches = TIME_INTERVAL_RE.match(value)
    if not matches:
        raise ConfigError(error_msg % (config_context.path, value))

    units = matches.group('units')
    if units not in TIME_INTERVAL_UNITS:
        raise ConfigError(error_msg % (config_context.path, value))

    time_spec = {TIME_INTERVAL_UNITS[units]: int(matches.group('value'))}
    return datetime.timedelta(**time_spec)


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
    """Build a validator which validates a list or dict, and returns a dict."""
    valid = build_list_of_type_validator(item_validator, allow_empty)

    def validator(value, config_context):
        if isinstance(value, dict):
            value = [{
                'name': name,
                **config
            } for name, config in value.items()]

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


# TODO: extract code
class Validator(object):
    """Base class for validating a collection and creating a mutable
    collection from the source.
    """
    config_class = None
    defaults = {}
    validators = {}
    optional = False

    def validate(self, in_dict, config_context):
        if self.optional and in_dict is None:
            return None

        if in_dict is None:
            raise ConfigError("A %s is required." % self.type_name)

        shortcut_value = self.do_shortcut(in_dict)
        if shortcut_value:
            return shortcut_value

        config_context = self.build_context(in_dict, config_context)
        in_dict = self.cast(in_dict, config_context)
        self.validate_required_keys(in_dict)
        self.validate_extra_keys(in_dict)
        return self.build_config(in_dict, config_context)

    def __call__(self, in_dict, config_context=NullConfigContext):
        return self.validate(in_dict, config_context)

    @property
    def type_name(self):
        """Return a string that represents the config_class being validated.
        This name is used for error messages, so we strip off the word
        Config so the name better matches what the user sees in the config.
        """
        return self.config_class.__name__.replace("Config", "")

    @property
    def all_keys(self):
        return self.config_class.required_keys + self.config_class.optional_keys

    def do_shortcut(self, in_dict):
        """Override if your validator can skip most of the validation by
        checking this condition.  If this returns a truthy value, the
        validation will end immediately and return that value.
        """
        pass

    def cast(self, in_dict, _):
        """If your validator accepts input in different formations, override
        this method to cast your input into a common format.
        """
        return in_dict

    def build_context(self, in_dict, config_context):
        path = self.path_name(in_dict.get('name'))
        return config_context.build_child_context(path)

    def validate_required_keys(self, in_dict):
        """Check that all required keys are present."""
        missing_keys = set(self.config_class.required_keys) - set(in_dict)
        if not missing_keys:
            return

        missing_key_str = ', '.join(missing_keys)
        if 'name' in self.all_keys and 'name' in in_dict:
            msg = "%s %s is missing options: %s"
            name = in_dict['name']
            raise ConfigError(msg % (self.type_name, name, missing_key_str))

        msg = "Nameless %s is missing options: %s"
        raise ConfigError(msg % (self.type_name, missing_key_str))

    def validate_extra_keys(self, in_dict):
        """Check that no unexpected keys are present."""
        extra_keys = set(in_dict) - set(self.all_keys)
        if not extra_keys:
            return

        msg = "Unknown keys in %s %s: %s"
        name = in_dict.get('name', '')
        raise ConfigError(msg % (self.type_name, name, ', '.join(extra_keys)))

    def set_defaults(self, output_dict, _config_context):
        """Set any default values for any optional values that were not
        specified.
        """
        for key, value in six.iteritems(self.defaults):
            output_dict.setdefault(key, value)

    def path_name(self, name=None):
        return '%s.%s' % (self.type_name, name) if name else self.type_name

    def post_validation(self, valid_input, config_context):
        """Hook to perform additional validation steps after key validation
        completes.
        """
        pass

    def build_config(self, in_dict, config_context):
        """Construct the configuration by validating the contents, setting
        defaults, and returning an instance of the config_class.
        """
        output_dict = self.validate_contents(in_dict, config_context)
        self.post_validation(output_dict, config_context)
        self.set_defaults(output_dict, config_context)
        return self.config_class(**output_dict)

    def validate_contents(self, input, config_context):
        """Override this to validate each value in the input."""
        valid_input = {}
        for key, value in six.iteritems(input):
            if key in self.validators:
                child_context = config_context.build_child_context(key)
                valid_input[key] = self.validators[key](value, child_context)
            else:
                valid_input[key] = value
        return valid_input
