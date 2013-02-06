"""
Parse a dictionary structure and return an immutable structure that
contain a validated configuration.
"""
from functools import partial
import itertools
import logging
import os
import re

import pytz

from tron.config import ConfigError, config_utils
from tron.config.config_utils import UniqueNameDict, build_type_validator
from tron.config.config_utils import NullConfigContext, ConfigContext
from tron.config.config_utils import PartialConfigContext
from tron.config.schedule_parse import valid_schedule
from tron.config.schema import TronConfig, NamedTronConfig, NotificationOptions
from tron.config.schema import ConfigSSHOptions, CommandFormatKeys
from tron.config.schema import ConfigNode, ConfigNodePool, ConfigState
from tron.config.schema import ConfigJob, ConfigAction, ConfigCleanupAction
from tron.config.schema import ConfigService
from tron.config.schema import MASTER_NAMESPACE
from tron.utils.dicts import FrozenDict
from tron.core.action import CLEANUP_ACTION_NAME


log = logging.getLogger(__name__)


MAX_IDENTIFIER_LENGTH       = 255
IDENTIFIER_RE               = re.compile(r'^[A-Za-z_][\w\-]{0,254}$')


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

valid_int   = partial(valid_number, int)
valid_float = partial(valid_number, float)

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


def build_list_of_type_validator(item_validator, allow_empty=False):
    """Build a validator which validates a list contains items which pass
    item_validator.
    """
    def validator(value, config_context):
        seq = valid_list(value, config_context)
        if not allow_empty and not seq:
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


def build_format_string_validator(valid_keys):
    """Validate that a string does not contain any unexpected formatting keys.
        valid_keys - a sequence of strings
    """
    def validator(value, config_context):
        if config_context.partial:
            return valid_string(value, config_context)

        keys = set(valid_keys) | set(config_context.command_context.keys())
        context = dict.fromkeys(keys, ' ')
        try:
            value % context
            return value
        except KeyError:
            error_msg = "Invalid template string at %s: %s"
            raise ConfigError(error_msg % (config_context.path, value))

    return validator


# TODO: extract code
class Validator(object):
    """Base class for validating a collection and creating a mutable
    collection from the source.
    """
    config_class            = None
    defaults                = {}
    validators              = {}
    optional                = False

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

        keys = self.config_class.required_keys + self.config_class.optional_keys
        missing_key_str = ', '.join(missing_keys)
        if 'name' in keys and 'name' in in_dict:
            msg  = "%s %s is missing options: %s"
            name = in_dict['name']
            raise ConfigError(msg % (self.type_name, name, missing_key_str))

        msg = "Nameless %s is missing options: %s"
        raise ConfigError(msg % (self.type_name, missing_key_str))

    def validate_extra_keys(self, in_dict):
        """Check that no unexpected keys are present."""
        conf_class      = self.config_class
        all_keys        = conf_class.required_keys + conf_class.optional_keys
        extra_keys      = set(in_dict) - set(all_keys)
        if not extra_keys:
            return

        msg             = "Unknown keys in %s %s: %s"
        name            = in_dict.get('name', '')
        raise ConfigError(msg % (self.type_name, name, ', '.join(extra_keys)))

    def set_defaults(self, output_dict, _config_context):
        """Set any default values for any optional values that were not
        specified.
        """
        for key, value in self.defaults.iteritems():
            if key not in output_dict:
                output_dict[key] = value

    def path_name(self, name=None):
        return '%s.%s' % (self.type_name, name) if name else self.type_name

    def post_validation(self, valid_input, config_context):
        """Perform additional validation."""
        pass

    def build_config(self, in_dict, config_context):
        output_dict = self.validate_contents(in_dict, config_context)
        self.post_validation(output_dict, config_context)
        self.set_defaults(output_dict, config_context)
        return self.config_class(**output_dict)

    def validate_contents(self, input, config_context):
        """Override this to validate each value in the input."""
        valid_input = {}
        for key, value in input.iteritems():
            if key in self.validators:
                child_context = config_context.build_child_context(key)
                valid_input[key] = self.validators[key](value, child_context)
            else:
                valid_input[key] = value
        return valid_input


def valid_output_stream_dir(output_dir, config_context):
    """Returns a valid string for the output directory, or raises ConfigError
    if the output_dir is not valid.
    """
    if not output_dir:
        return

    valid_string(output_dir, config_context)
    if not os.path.isdir(output_dir):
        msg = "output_stream_dir '%s' is not a directory"
        raise ConfigError(msg % output_dir)

    if not os.access(output_dir, os.W_OK):
        raise ConfigError("output_stream_dir '%s' is not writable" % output_dir)

    return output_dir


def valid_command_context(context, config_context):
    # context can be any dict.
    return FrozenDict(**valid_dict(context or {}, config_context))


def valid_time_zone(tz, config_context):
    if tz is None:
        return None
    valid_string(tz, config_context)
    try:
        return pytz.timezone(tz)
    except pytz.exceptions.UnknownTimeZoneError:
        raise ConfigError('%s is not a valid time zone' % tz)


def valid_node_name(value, config_context):
    valid_identifier(value, config_context)
    if not config_context.partial and value not in config_context.nodes:
        msg = "Unknown node name %s at %s"
        raise ConfigError(msg % (value, config_context.path))
    return value


class ValidateSSHOptions(Validator):
    """Validate SSH options."""
    config_class =              ConfigSSHOptions
    optional =                  True
    defaults = {
        'agent':                False,
        'identities':           ()
    }
    validators = {
        'agent':                valid_bool,
        'identities':           valid_list,
    }

valid_ssh_options = ValidateSSHOptions()


class ValidateNotificationOptions(Validator):
    """Validate notification options."""
    config_class =              NotificationOptions
    optional =                  True

valid_notification_options = ValidateNotificationOptions()


class ValidateNode(Validator):
    config_class =              ConfigNode
    validators = {
        'name':                 valid_identifier,
        'username':             valid_string,
        'hostname':             valid_string,
    }

    DEFAULT_USER =              os.environ['USER']

    def do_shortcut(self, node):
        """Nodes can be specified with just a hostname string."""
        if isinstance(node, basestring):
            return ConfigNode(
                        hostname=node, name=node, username=self.DEFAULT_USER)

    def set_defaults(self, output_dict, _):
        output_dict.setdefault('name', output_dict['hostname'])
        output_dict.setdefault('username', self.DEFAULT_USER)

valid_node = ValidateNode()


class ValidateNodePool(Validator):
    config_class =              ConfigNodePool
    validators = {
        'name':                 valid_identifier,
        'nodes':                build_list_of_type_validator(valid_identifier),
    }

    def cast(self, node_pool, _context):
        if isinstance(node_pool, list):
            node_pool = dict(nodes=node_pool)
        return node_pool

    def set_defaults(self, node_pool, _):
        node_pool.setdefault('name', '_'.join(node_pool['nodes']))


valid_node_pool = ValidateNodePool()


def valid_action_name(value, config_context):
    valid_identifier(value, config_context)
    if value == CLEANUP_ACTION_NAME:
        error_msg = "Invalid action name %s at %s"
        raise ConfigError(error_msg % (value, config_context.path))
    return value


class ValidateAction(Validator):
    """Validate an action."""
    config_class =              ConfigAction
    context_keys =              CommandFormatKeys.job_keys
    defaults = {
        'node':                 None,
        'requires':             (),
    }
    requires = build_list_of_type_validator(valid_action_name, allow_empty=True)
    validators = {
        'name':                 valid_action_name,
        'command':              build_format_string_validator(context_keys),
        'node':                 valid_node_name,
        'requires':             requires,
    }

valid_action = ValidateAction()


def valid_cleanup_action_name(value, config_context):
    if value != CLEANUP_ACTION_NAME:
        msg = "Cleanup actions cannot have custom names %s.%s"
        raise ConfigError(msg % (config_context.path, value))


class ValidateCleanupAction(Validator):
    config_class =              ConfigCleanupAction
    context_keys =              CommandFormatKeys.job_keys
    defaults = {
        'node':                 None,
        'name':                 CLEANUP_ACTION_NAME,
    }
    validators = {
        'name':                 valid_cleanup_action_name,
        'command':              build_format_string_validator(context_keys),
        'node':                 valid_node_name,
    }

valid_cleanup_action = ValidateCleanupAction()


class ValidateJob(Validator):
    """Validate jobs."""
    config_class =              ConfigJob
    defaults = {
        'run_limit':            50,
        'all_nodes':            False,
        'cleanup_action':       None,
        'enabled':              True,
        'queueing':             True,
        'allow_overlap':        False
    }

    validators = {
        'name':                 valid_identifier,
        'schedule':             valid_schedule,
        'run_limit':            valid_int,
        'all_nodes':            valid_bool,
        'actions':              build_dict_name_validator(valid_action),
        'cleanup_action':       valid_cleanup_action,
        'node':                 valid_node_name,
        'queueing':             valid_bool,
        'enabled':              valid_bool,
        'allow_overlap':        valid_bool,
    }

    def cast(self, in_dict, config_context):
        in_dict['namespace'] = config_context.namespace
        return in_dict

    # TODO: extract common code to a util function
    def _validate_dependencies(self, job, actions,
        base_action, current_action=None, stack=None):
        """Check for circular or misspelled dependencies."""
        stack = stack or []
        current_action = current_action or base_action

        stack.append(current_action.name)
        for dep in current_action.requires:
            if dep == base_action.name and len(stack) > 0:
                msg = 'Circular dependency in job.%s: %s'
                raise ConfigError(msg % (job['name'], ' -> '.join(stack)))
            if dep not in actions:
                raise ConfigError(
                    'Action jobs.%s.%s has a dependency "%s"'
                    ' that is not in the same job!' %
                    (job['name'], current_action.name, dep))
            self._validate_dependencies(
                job, actions, base_action, actions[dep], stack)

        stack.pop()

    def post_validation(self, job, config_context):
        """Validate actions for the job."""
        for action in job['actions'].itervalues():
            self._validate_dependencies(job, job['actions'], action)

valid_job = ValidateJob()


class ValidateService(Validator):
    """Validate a services configuration."""
    config_class =              ConfigService
    context_keys =              CommandFormatKeys.service_keys
    defaults = {
        'count':                1,
        'restart_interval':     None
    }

    validators = {
        'name':                 valid_identifier,
        'pid_file':             build_format_string_validator(context_keys),
        'command':              build_format_string_validator(context_keys),
        'monitor_interval':     valid_float,
        'count':                valid_int,
        'node':                 valid_node_name,
        'restart_interval':     valid_float,
    }

    def cast(self, in_dict, config_context):
        in_dict['namespace'] = config_context.namespace
        return in_dict

valid_service = ValidateService()


class ValidateStatePersistence(Validator):
    config_class                = ConfigState
    defaults = {
        'buffer_size':          1,
        'connection_details':   None,
    }

    validators = {
        'name':                 valid_string,
        'store_type':           valid_string,
        'connection_details':   valid_string,
        'buffer_size':          valid_int,
    }

    def post_validation(self, config, config_context):
        buffer_size = config.get('buffer_size')

        if buffer_size and buffer_size < 1:
            path = config_context.path
            raise ConfigError("%s buffer_size must be >= 1." % path)

valid_state_persistence = ValidateStatePersistence()


def validate_jobs_and_services(config, config_context):
    """Validate jobs and services."""
    valid_jobs      = build_dict_name_validator(valid_job, allow_empty=True)
    valid_services  = build_dict_name_validator(valid_service, allow_empty=True)
    validation      = [('jobs', valid_jobs), ('services', valid_services)]

    for config_name, valid in validation:
        config[config_name] = valid(config.get(config_name, []), config_context)

    fmt_string = 'Job and Service names must be unique %s'
    config_utils.unique_names(fmt_string, config['jobs'], config['services'])


DEFAULT_STATE_PERSISTENCE = ConfigState('tron_state', 'shelve', None, 1)
DEFAULT_NODE = ConfigNode('localhost', 'localhost', 'tronuser')


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """
    config_class =              TronConfig
    defaults = {
        'output_stream_dir':    None,
        'command_context':      {},
        'ssh_options':          ValidateSSHOptions.defaults,
        'notification_options': None,
        'time_zone':            None,
        'state_persistence':    DEFAULT_STATE_PERSISTENCE,
        'nodes':                {'localhost': DEFAULT_NODE},
        'node_pools':           (),
        'jobs':                 (),
        'services':             (),
    }
    node_pools = build_dict_name_validator(valid_node_pool, allow_empty=True)
    validators = {
        'output_stream_dir':    valid_output_stream_dir,
        'command_context':      valid_command_context,
        'ssh_options':          valid_ssh_options,
        'notification_options': valid_notification_options,
        'time_zone':            valid_time_zone,
        'state_persistence':    valid_state_persistence,
        'nodes':                build_dict_name_validator(valid_node),
        'node_pools':           node_pools,
    }
    optional = False

    def validate_node_pool_nodes(self, config):
        """Validate that each node in a node_pool is in fact a node, and not
        another pool.
        """
        all_node_names = set(config['nodes'])
        for node_pool in config['node_pools'].itervalues():
            invalid_names = set(node_pool.nodes) - all_node_names
            if invalid_names:
                msg = "NodePool %s contains other NodePools: " % node_pool.name
                raise ConfigError(msg + ",".join(invalid_names))

    def post_validation(self, config, _):
        """Validate a non-named config."""
        node_names = config_utils.unique_names(
            'Node and NodePool names must be unique %s',
            config['nodes'], config.get('node_pools', []))

        if config.get('node_pools'):
            self.validate_node_pool_nodes(config)

        config_context = ConfigContext('config', node_names,
            config.get('command_context'), MASTER_NAMESPACE)
        validate_jobs_and_services(config, config_context)


class ValidateNamedConfig(Validator):
    """A shorter validator for named configurations, which allow for
    jobs and services to be defined as configuration fragments that
    are, in turn, reconciled by Tron.
    """
    config_class =              NamedTronConfig
    type_name =                 "NamedConfigFragment"
    defaults = {
        'jobs':                 (),
        'services':             ()
    }

    optional = False

    def post_validation(self, config, config_context):
        validate_jobs_and_services(config, config_context)


valid_config = ValidateConfig()
valid_named_config = ValidateNamedConfig()


def validate_fragment(name, fragment):
    if name == MASTER_NAMESPACE:
        return valid_config(fragment)
    config_context = PartialConfigContext(name, name)
    return valid_named_config(fragment, config_context=config_context)


def get_nodes_from_master_namespace(master):
    return set(itertools.chain(master.nodes, master.node_pools))


def validate_config_mapping(config_mapping):
    if MASTER_NAMESPACE not in config_mapping:
        msg = "A config mapping requires a %s namespace"
        raise ConfigError(msg % MASTER_NAMESPACE)

    master = valid_config(config_mapping.pop(MASTER_NAMESPACE))
    nodes = get_nodes_from_master_namespace(master)
    yield MASTER_NAMESPACE, master

    for name, content in config_mapping.iteritems():
        context = ConfigContext(name, nodes, master.command_context, name)
        yield name, valid_named_config(content, config_context=context)


class ConfigContainer(object):
    """A container around configuration fragments (and master)."""

    def __init__(self, config_mapping):
        self.configs = config_mapping

    def iteritems(self):
        return self.configs.iteritems()

    @classmethod
    def create(cls, config_mapping):
        container = cls(dict(validate_config_mapping(config_mapping)))
        container.validate()
        return container

    def validate(self):
        """Validate the integrity of all the configuration fragments as a whole.
        """
        collate_jobs_and_services(self)

    def add(self, name, config_content):
        master = self.get_master()
        nodes = self.get_node_names()
        context = ConfigContext(name, nodes, master.command_context, name)
        self.configs[name] = valid_named_config(config_content, context)

    def get_master(self):
        return self.configs[MASTER_NAMESPACE]

    def get_node_names(self):
        return get_nodes_from_master_namespace(self.get_master())

    def __getitem__(self, name):
        return self.configs[name]

    def __contains__(self, name):
        return name in self.configs


# TODO(0.6): Remove once name is constructed as part of validators
def collate_jobs_and_services(configs):
    """Collate jobs and services from an iterable of Config objects."""
    jobs = {}
    services = {}

    def build_identifier(name, namespace):
        return '%s_%s' % (namespace, name)

    def _iter_items(config, namespace, attr):
        for item in getattr(config, attr):
            identifier = build_identifier(item, namespace)
            if identifier in jobs or identifier in services:
                raise ConfigError("Collision found for identifier '%s'" % job_identifier)
            content = getattr(config, attr)[item]
            yield identifier, content

    for namespace, config in configs.iteritems():
        for job_identifier, content in _iter_items(config, namespace, "jobs"):
            jobs[job_identifier] = content

        for service_identifier, content in _iter_items(config, namespace, "services"):
            services[service_identifier] = content

    return jobs, services