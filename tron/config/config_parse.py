"""
Parse a dictionary structure and return an immutable structure that
contain a validated configuration.

This module contains two sets of classes.

Config objects: These are immutable structures (namedtuples, FrozenDict) that
contain the configuration data. The top level structure (ConfigTron) is returned
from valid_config().

Validator objects: These are responsible for validating a dictionary structure
and returning a valid immutable config object.
"""

from functools import partial
import itertools
import logging
import os
import re

import pytz
import yaml

from tron.config import ConfigError
from tron.config.schedule_parse import valid_schedule
from tron.config.schema import TronConfig, NamedTronConfig, NotificationOptions, ConfigSSHOptions
from tron.config.schema import ConfigNode, ConfigNodePool, ConfigState
from tron.config.schema import ConfigJob, ConfigAction, ConfigCleanupAction
from tron.config.schema import ConfigService
from tron.config.schema import MASTER_NAMESPACE
from tron.utils.dicts import FrozenDict
from tron.core.action import CLEANUP_ACTION_NAME


log = logging.getLogger(__name__)

def load_config(config):
    """Given a string or file object, load it with PyYAML and return an
    immutable, validated representation of the configuration it specifies.
    """
    # TODO: load with YAML. safe_load() disables python classes
    # TODO: this logic provides automatic forward-porting of legacy
    # configuration files. These files should be deprecated and this
    # logic should be considered for removal in later versions.

    parsed_yaml = yaml.safe_load(config)

    if MASTER_NAMESPACE not in parsed_yaml:
        namespace = parsed_yaml.get("config_name") or MASTER_NAMESPACE
        parsed_yaml = {namespace: parsed_yaml}

    parsed_config = ConfigContainer()
    for fragment in parsed_yaml:
        if fragment == MASTER_NAMESPACE:
            parsed_config[fragment] = valid_config(parsed_yaml[fragment])
        else:
            parsed_config[fragment] = valid_named_config(parsed_yaml[fragment])
    collate_jobs_and_services(parsed_config)
    return parsed_config

def update_config(filepath, content):
    """ Given a configuration, perform input validation, parse the
    YAML into what we hope to be a valid configuration object, then
    reconcile the altered configuration with the remainder of the
    container.
    """
    original = _initialize_original_config(filepath)
    namespace, update = _initialize_namespaced_update(content)
    original[namespace] = update
    ret = yaml.dump(original)
    load_config(ret)
    return ret

def _initialize_original_config(filepath):
    """Initialize the dictionary for our original configuration file."""
    if os.path.exists(filepath):
        with open(filepath, 'r') as config:
            original = yaml.safe_load(config)

        # Forward-convert legacy configurations
        # TODO: Make legacy detection non-reliant on side
        # effects
        if MASTER_NAMESPACE not in original:
            return {MASTER_NAMESPACE: original}
        return original
    else:
        return {}

def _initialize_namespaced_update(content):
    """Initialize the update configuration object."""
    update = yaml.safe_load(content)
    namespace = update.get("config_name")
    if not namespace:
        namespace = MASTER_NAMESPACE
        # TODO: Remove the duplicate entry for config_name, by
        # relaxing the __new__ needs of our class builder.
        update['config_name'] = MASTER_NAMESPACE

    if namespace == MASTER_NAMESPACE:
        assert valid_config(update)
    else:
        assert valid_named_config(update)

    return namespace, update

def collate_jobs_and_services(configs):
    """Collate jobs and services from an iterable of Config objects."""
    jobs = {}
    services = {}

    def _iter_items(config, namespace, attr):
        for item in getattr(config, attr):
            identifier = '_'.join((namespace, item))
            if identifier in jobs or identifier in services:
                raise ConfigError("Collision found for identifier '%s'" % job_identifier)
            content = getattr(config, attr)[item]
            yield identifier, content

    for namespace, config in configs.items():
        for job_identifier, content in _iter_items(config, namespace, "jobs"):
            jobs[job_identifier] = (content, namespace)

        for service_identifier, content in _iter_items(config, namespace, "services"):
            services[service_identifier] = (content, namespace)

    return jobs, services


class UniqueNameDict(dict):
    """A dict like object that throws a ConfigError if a key exists and a set
    is called to change the value of that key.
     *fmt_string* will be interpolated with (key,)
    """
    def __init__(self, fmt_string, **kwargs):
        super(dict, self).__init__(**kwargs)
        self.fmt_string = fmt_string

    def __setitem__(self, key, value):
        if key in self:
            raise ConfigError(self.fmt_string % key)
        super(UniqueNameDict, self).__setitem__(key, value)


def type_validator(validator, error_fmt):
    def f(path, value, optional=False):
        """If *validator* does not return True for *value*, raise ConfigError
        If optional is True, None values will be returned without validation.
        """
        if value is None and optional:
            return None
        if not validator(value):
            raise ConfigError(error_fmt % (path, value))
        return value
    return f


def valid_number(type_func, path, value, optional=False):
    if value is None and optional:
        return None
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

MAX_IDENTIFIER_LENGTH       = 255
IDENTIFIER_RE               = re.compile(r'^[A-Za-z_][\w\-]{0,254}$')

valid_identifier = type_validator(
    lambda s: isinstance(s, basestring) and IDENTIFIER_RE.match(s),
    'Identifier at %s is not a valid identifier: %s')

valid_populated_list = type_validator(
    bool, 'Value at %s is not a list with items: %s')

valid_list = type_validator(
    lambda s: isinstance(s, list), 'Value at %s is not a list: %s')

valid_str  = type_validator(
    lambda s: isinstance(s, basestring), 'Value at %s is not a string: %s')

valid_dict = type_validator(
    lambda s: isinstance(s, dict), 'Value at %s is not a dictionary: %s')

valid_bool = type_validator(
    lambda s: isinstance(s, bool), 'Value at %s is not a boolean: %s')


class Validator(object):
    """Base class for validating a collection and creating a mutable
    collection from the source.
    """
    config_class            = None
    defaults                = {}
    validators              = {}
    optional                = False

    def validate(self, in_dict):
        if self.optional and in_dict is None:
            return None

        if in_dict is None:
            raise ConfigError("A %s is required." % self.type_name)

        shortcut_value = self.do_shortcut(in_dict)
        if shortcut_value:
            return shortcut_value

        in_dict = self.cast(in_dict)
        self.validate_required_keys(in_dict)
        self.validate_extra_keys(in_dict)

        output_dict = self.build_dict(in_dict)
        self.set_defaults(output_dict)
        return self.config_class(**output_dict)

    __call__ = validate

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

    def cast(self, in_dict):
        """If your validator accepts input in different formations, override
        this method to cast your input into a common format.
        """
        return in_dict

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

        msg             = "Unknown options in %s %s: %s"
        name            = in_dict.get('name', '')
        raise ConfigError(msg % (self.type_name, name, ', '.join(extra_keys)))

    def set_defaults(self, output_dict):
        """Set any default values for any optional values that were not
        specified.
        """
        for key, value in self.defaults.iteritems():
            if key not in output_dict:
                output_dict[key] = value

    def post_validation(self, valid_input):
        """Perform additional validation."""
        pass

    def build_dict(self, input):
        """Override this to validate each value in the input."""
        valid_input = {}
        for key, value in input.iteritems():
            if key in self.validators:
                valid_input[key] = self.validators[key](value)
            else:
                valid_input[key] = value
        self.post_validation(valid_input)
        return valid_input


class ValidatorWithNamedPath(Validator):
    """A validator that expects a name to use for validation failure messages
    and calls a post_validation() method after building the dict."""

    def post_validation(self, valid_input, path_name):
        pass

    def build_dict(self, input):
        path_name = '%s.%s' % (self.type_name, input.get('name'))
        valid_input = {}
        for key, value in input.iteritems():
            if key in self.validators:
                valid_input[key] = self.validators[key](path_name, value)
            else:
                valid_input[key] = value
        self.post_validation(valid_input, path_name)
        return valid_input


def valid_output_stream_dir(output_dir):
    """Returns a valid string for the output directory, or raises ConfigError
    if the output_dir is not valid.
    """
    output_dir = valid_str('output_stream_dir', output_dir, optional=True)
    if not output_dir:
        return

    if not os.path.isdir(output_dir):
        msg = "output_stream_dir '%s' is not a directory"
        raise ConfigError(msg % output_dir)

    if not os.access(output_dir, os.W_OK):
        raise ConfigError("output_stream_dir '%s' is not writable" % output_dir)

    return output_dir


def valid_command_context(context):
    # context can be any dict.
    return FrozenDict(**valid_dict('command_context', context or {}))


def valid_time_zone(tz):
    if tz is None:
        return None
    try:
        return pytz.timezone(valid_str('time_zone', tz))
    except pytz.exceptions.UnknownTimeZoneError:
        raise ConfigError('%s is not a valid time zone' % tz)


class ValidateSSHOptions(Validator):
    """Validate SSH options."""
    config_class =              ConfigSSHOptions
    optional =                  True
    defaults = {
        'agent':                False,
        'identities':           ()
    }
    validators = {
        'agent':                partial(valid_bool, 'ssh_options.agent'),
        'identities':           partial(valid_list, 'ssh_options.identities')
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
        'name':                 partial(valid_identifier, 'nodes'),
        'username':             partial(valid_str, 'nodes'),
        'hostname':             partial(valid_str, 'nodes')
    }

    def do_shortcut(self, node):
        """Nodes can be specified with just a hostname string."""
        if isinstance(node, basestring):
            return ConfigNode(hostname=node, name=node)

    def set_defaults(self, output_dict):
        output_dict.setdefault('name', output_dict['hostname'])
        output_dict.setdefault('username', os.environ['USER'])

valid_node = ValidateNode()


class ValidateNodePool(Validator):
    config_class =              ConfigNodePool
    validators = {
        'name':                 partial(valid_identifier, 'node_pools'),
        'nodes':                partial(valid_populated_list, 'node_pools')
    }

    def cast(self, node_pool):
        if isinstance(node_pool, list):
            node_pool = dict(nodes=node_pool)
        return node_pool

    def set_defaults(self, node_pool):
        node_pool.setdefault('name', '_'.join(node_pool['nodes']))

    def post_validation(self, node_pool):
        node_pool['nodes'] = [
            valid_identifier('node_pools', node) for node in node_pool['nodes']]

valid_node_pool = ValidateNodePool()


class ValidateAction(ValidatorWithNamedPath):
    """Validate an action."""
    config_class =              ConfigAction
    defaults = {
        'node':                 None
    }
    validators = {
        'name':                 valid_identifier,
        'command':              valid_str,
        'node':                 valid_identifier,
    }

    def post_validation(self, action, path_name):
        # check name
        if action['name'] == CLEANUP_ACTION_NAME:
            raise ConfigError("Bad action name at %s: %s" %
                              (path_name, action['name']))

        requires = []

        # accept a string, pointer, or list
        old_requires = action.get('requires', [])

        # string identifier
        if isinstance(old_requires, basestring):
            log.warn("Require without a list is deprecated. "
                "You should update requires for %s %s" %
                (path_name, action['name']))
            old_requires = [old_requires]

        # pointer
        if isinstance(old_requires, dict):
            old_requires = [old_requires['name']]

        old_requires = valid_list(path_name, old_requires)

        for r in old_requires:
            if not isinstance(r, basestring):
                # old style, alias
                r = r['name']

            requires.append(r)
            if r == CLEANUP_ACTION_NAME:
                raise ConfigError('Actions cannot depend on the cleanup action.'
                                  ' (%s)' % path_name)

        action['requires'] = tuple(requires)

valid_action = ValidateAction()


class ValidateCleanupAction(ValidatorWithNamedPath):
    config_class =              ConfigCleanupAction
    defaults = {
        'node':                 None,
        'name':                 CLEANUP_ACTION_NAME,
    }
    validators = {
        'name':                 valid_identifier,
        'command':              valid_str,
        'node':                 valid_identifier
    }

    def post_validation(self, action, path_name):
        expected_names = (None, CLEANUP_ACTION_NAME)
        if 'name' in action and action['name'] not in expected_names:
            msg = "Cleanup actions cannot have custom names (%s.%s)"
            raise ConfigError(msg % (path_name, action['name']))

        if 'requires' in action:
            msg = "Cleanup action %s can not have requires."
            raise ConfigError(msg % path_name)

        action['requires'] = tuple()

valid_cleanup_action = ValidateCleanupAction()


class ValidateJob(ValidatorWithNamedPath):
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
        'actions':              valid_populated_list,
        'cleanup_action':       lambda _, v: valid_cleanup_action(v),
        'node':                 valid_identifier,
        'queueing':             valid_bool,
        'enabled':              valid_bool,
        'allow_overlap':        valid_bool,
    }

    def _validate_dependencies(self, job, actions,
        base_action, current_action=None, stack=None):
        """Check for circular or misspelled dependencies."""
        stack = stack or []
        current_action = current_action or base_action

        stack.append(current_action.name)
        for dep in current_action.requires:
            if dep == base_action.name and len(stack) > 0:
                raise ConfigError(
                    'Circular dependency in job.%s: %s' % (
                    job['name'], ' -> '.join(stack)))
            if dep not in actions:
                raise ConfigError(
                    'Action jobs.%s.%s has a dependency "%s"'
                    ' that is not in the same job!' %
                    (job['name'], current_action.name, dep))
            self._validate_dependencies(
                job, actions, base_action, actions[dep], stack)

        stack.pop()

    def post_validation(self, job, path):
        """Validate actions for the job."""
        actions = UniqueNameDict(
                'Action name %%s on job %s used twice' % job['name'])
        for action in job['actions'] or []:
            try:
                final_action = valid_action(action)
            except ConfigError, e:
                raise ConfigError("Invalid action config for %s: %s" % (path, e))

            if not (final_action.node or job['node']):
                msg = '%s has no node configured for %s'
                raise ConfigError(msg % (path, final_action.name))
            actions[final_action.name] = final_action

        for action in actions.values():
            self._validate_dependencies(job, actions, action)

        job['actions'] = FrozenDict(**actions)

valid_job = ValidateJob()


class ValidateService(ValidatorWithNamedPath):
    """Validate a services configuration."""
    config_class =              ConfigService
    defaults = {
        'count':                1,
        'restart_interval':     None
    }

    validators = {
        'name':                 valid_identifier,
        'pid_file':             valid_str,
        'command':              valid_str,
        'monitor_interval':     valid_float,
        'count':                valid_int,
        'node':                 valid_identifier,
        'restart_interval':     valid_float,
    }

valid_service = ValidateService()


class ValidateStatePersistence(ValidatorWithNamedPath):
    config_class                = ConfigState
    defaults = {
        'buffer_size':          1,
        'connection_details':   None,
    }

    validators = {
        'name':                 valid_str,
        'store_type':           valid_str,
        'connection_details':   valid_str,
        'buffer_size':          valid_int,
    }

    def post_validation(self, config, path_name):
        buffer_size = config.get('buffer_size')

        if buffer_size and buffer_size < 1:
            raise ConfigError("%s buffer_size must be >= 1." % path_name)

valid_state_persistence = ValidateStatePersistence()

DEFAULT_STATE_PERSISTENCE = ConfigState('tron_state', 'shelve', None, 1)

def parse_sub_config(config, cname, valid, name_dict):
    target_dict = UniqueNameDict(
        "%s name %%s used twice" % cname.replace('_', ' '))
    for item in config.get(cname) or []:
        final = valid(item)
        target_dict[final.name] = final
        name_dict[final.name] = True
    config[cname] = FrozenDict(**target_dict)

def validate_jobs_and_services(config):
    """Validate jobs and services."""

    job_service_names = UniqueNameDict(
            'Job and Service names must be unique %s')

    parse_sub_config(config, 'jobs',        valid_job ,         job_service_names)
    parse_sub_config(config, 'services',    valid_service,      job_service_names)


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """
    config_class =              TronConfig
    defaults = {
        'config_name':          MASTER_NAMESPACE,
        'output_stream_dir':    None,
        'command_context':      None,
        'ssh_options':          valid_ssh_options({}),
        'notification_options': None,
        'time_zone':            None,
        'state_persistence':    DEFAULT_STATE_PERSISTENCE,
        'nodes':                (dict(name='localhost', username='tronuser', hostname='localhost'),),
        'node_pools':           (),
        'jobs':                 (),
        'services':             (),
    }
    validators = {
        'output_stream_dir':    valid_output_stream_dir,
        'command_context':      valid_command_context,
        'ssh_options':          valid_ssh_options,
        'notification_options': valid_notification_options,
        'time_zone':            valid_time_zone,
        'state_persistence':    valid_state_persistence
    }
    optional = False

    def validate_node_names(self, config, node_names):
        """Validate that any node/node_pool name that were used are configured
        as nodes/node_pools.
        """
        actions = itertools.chain.from_iterable(
            job.actions.values()
            for job in config['jobs'].values())

        task_list = itertools.chain(
            config['jobs'].values(),
            config['services'].values(),
            actions)
        for task in task_list:
            if task.node and task.node not in node_names:
                raise ConfigError("Unknown node %s configured for %s %s" % (
                    task.node, task.__class__.__name__, task.name))

    def validate_node_pool_nodes(self, config):
        """Validate that each node in a node_pool is in fact a node, and not
        another pool.
        """
        for node_pool in config['node_pools'].itervalues():
            for node_name in node_pool.nodes:
                node = config['nodes'].get(node_name)
                if node:
                    continue
                msg = "NodePool %s contains another NodePool %s. "
                raise ConfigError(msg % (node_pool.name, node_name))

    def post_validation(self, config):
        """Validate a non-named config."""
        validate_jobs_and_services(config)
        node_names = UniqueNameDict('Node and NodePool names must be unique %s')
        parse_sub_config(config, 'nodes',       valid_node,         node_names)
        parse_sub_config(config, 'node_pools',  valid_node_pool,    node_names)
        self.validate_node_names(config, node_names)
        self.validate_node_pool_nodes(config)


class ValidateNamedConfig(Validator):
    """A shorter validator for named configurations, which allow for
    jobs and services to be defined as configuration fragments that
    are, in turn, reconciled by Tron.
    """
    config_class =              NamedTronConfig
    defaults = {
        'config_name':          None,
        'jobs':                 (),
        'services':             ()
    }
    optional = False

    def post_validation(self, config):
        """Validate a named config."""
        validate_jobs_and_services(config)

valid_config = ValidateConfig()
valid_named_config = ValidateNamedConfig()
ConfigContainer = dict
