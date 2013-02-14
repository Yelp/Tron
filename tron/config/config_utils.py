"""Utilities used for configuration parsing and validation."""
import itertools
from tron.config import ConfigError
from tron.config.schema import MASTER_NAMESPACE


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