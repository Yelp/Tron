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

from collections import namedtuple
from functools import partial
import itertools
import logging
import os
import re

import pytz
import yaml

from tron.config import ConfigError
from tron.config.schedule_parse import valid_schedule
from tron.config.schedule_parse import ConfigConstantScheduler
from tron.config.schedule_parse import ConfigIntervalScheduler
from tron.utils.dicts import FrozenDict


log = logging.getLogger("tron.config")

CLEANUP_ACTION_NAME = "cleanup"


YAML_TAG_RE = re.compile(r'!\w+\b')

def load_config(string_or_file):
    """Given a string or file object, load it with PyYAML and return an
    immutable, validated representation of the configuration it specifies.
    """
    # TODO: 0.5 remove this
    # Hackishly strip !Tags from the file so PyYAML doesn't try to make them
    # into Python objects
    if not isinstance(string_or_file, basestring):
        s_tags = string_or_file.read()
    else:
        s_tags = string_or_file
    s_notags = YAML_TAG_RE.sub('', s_tags)

    if len(s_tags) > len(s_notags):
        log.warn('Tron no longer uses !Tags to parse config files. Please'
                 ' remove them from yours.')

    # Load with YAML. safe_load() disables python classes
    config = yaml.safe_load(s_notags)

    return valid_config(config)


### SCHEMA DEFINITION ###

def config_object_factory(name, required=None, optional=None):
    """
    Creates a namedtuple which has two additional attributes:
        required_keys:
            all keys required to be set on this configuration object
        optional keys:
            optional keys for this configuration object

    The tuple is created from required + optional
    """
    required = required or []
    optional = optional or []
    config_class = namedtuple(name, required + optional)
    config_class.required_keys = required
    config_class.optional_keys = optional
    return config_class


TronConfig = config_object_factory(
    'TronConfig',
    optional=[
         'working_dir',         # str
         'output_stream_dir',   # str
         'command_context',     # FrozenDict of str
         'ssh_options',         # ConchOptions
         'notification_options',# NotificationOptions or None
         'time_zone',           # pytz time zone
         'nodes',               # FrozenDict of ConfigNode
         'node_pools',          # FrozenDict of ConfigNodePool
         'jobs',                # FrozenDict of ConfigJob
         'services'             # FrozenDict of ConfigService
    ])


NotificationOptions = config_object_factory(
    'NotificationOptions',
    [
        'smtp_host',            # str
        'notification_addr',    # str
    ])


ConfigSSHOptions = config_object_factory(
    'ConfigSSHOptions',
    optional=[
        'agent',                # bool
        'identities',           # list of str
    ])


ConfigNode = config_object_factory('ConfigNode', ['hostname'], ['name'])


ConfigNodePool = config_object_factory('ConfigNodePool', ['nodes'], ['name'])


ConfigJob = config_object_factory(
    'ConfigJob',
    [
        'name',                 # str
        'node',                 # str
        'schedule',             # Config*Scheduler
        'actions',              # FrozenDict of ConfigAction
    ],[
        'queueing',             # bool
        'run_limit',            # int
        'all_nodes',            # bool
        'cleanup_action',       # ConfigAction
        'enabled',              # bool
    ])


ConfigAction = config_object_factory(
    'ConfigAction',
    [
        'name',                 # str
        'command',              # str
    ],[
        'requires',             # tuple of str
        'node',                 # str
    ])

ConfigCleanupAction = config_object_factory(
    'ConfigCleanupAction',
    [
        'command',              # str
    ],[
        'requires',             # tuple of str
        'name',                 # str
        'node',                 # str
    ])


ConfigService = config_object_factory(
    'ConfigService',
    [
        'name',                 # str
        'node',                 # str
        'pid_file',             # str
        'command',              # str
        'monitor_interval',     # float
    ],[
        'restart_interval',     # float
        'count',                # int
    ])


### VALIDATION ###

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


def type_converter(convert, error_fmt):
    def f(path, value, optional=False):
        """Convert *value* at config path *path* into something else via the
        *convert* function, raising ConfigError if *convert* raises a
        TypeError. *error_fmt* will be interpolated with (path, value).
        If optional is True, None values will be returned without converting.
        """
        if value is None and optional:
            return None
        try:
            return convert(value)
        except TypeError:
            raise ConfigError(error_fmt % (path, value))
    return f

valid_float = type_converter(float, 'Value at %s is not a number: %s')

valid_int = type_converter(int, 'Value at %s is not an integer: %s')


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

valid_str = type_validator(
    lambda s: isinstance(s, basestring),
    'Value at %s is not a string: %s')

IDENTIFIER_RE = re.compile(r'^[A-Za-z_]\w*$')
valid_identifier = type_validator(
    lambda s: IDENTIFIER_RE.match(s),
    'Identifier at %s is not a valid identifier')

valid_list = type_validator(
    lambda s: isinstance(s, list), 'Value at %s is not a list: %s')

valid_populated_list = type_validator(
    bool, 'Value at %s is not a list with items: %s')

valid_dict = type_validator(
    lambda s: isinstance(s, dict), 'Value at %s is not a dictionary: %s')

valid_bool = type_validator(
    lambda s: isinstance(s, bool), 'Value at %s is not a boolean: %s')


class Validator(object):
    """Base class for validating a collection and creating a mutable
    collection from the source.
    """
    config_class =              None
    defaults =                  {}
    validators =                {}
    optional =                  False

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
            raise ConfigError("%s %s is missing options: %s" % (
                    self.type_name, in_dict['name'], missing_key_str)
            )

        raise ConfigError("Nameless %s is missing options: %s" % (
                self.type_name, missing_key_str)
        )

    def validate_extra_keys(self, in_dict):
        """Check that no unexpected keys are present."""
        all_keys = self.config_class.required_keys + self.config_class.optional_keys
        extra_keys = set(in_dict) - set(all_keys)
        if not extra_keys:
            return

        raise ConfigError("Unknown options in %s %s: %s" % (
                self.type_name,
                in_dict.get('name', ''),
                ', '.join(extra_keys)
        ))

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


# TODO: remove in 0.5
def valid_working_dir(wd):
    """Given a working directory or None, return a valid working directory.
    If wd=None the mcp will attempt to use a default.
    """
    log.warning("working_dir is no longer supported. "
        "Use output_stream_dir to set the directory for stdout/stderr files.")


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


# TODO: remove in 0.5
def normalize_node(node):
    """Given a node value from a config, determine if it's the node's name, the
    node's value, or actually a node pool. The former case is the "new style"
    of identifiers and the latter case is the "old style" of anchors and
    aliases (*/&). Return the name.
    """
    if node is None:
        return None
    if isinstance(node, basestring):
        return node
    # probably a reference back to an already validated node
    try:
        return valid_node(node).name
    except ConfigError:
        return valid_node_pool(node).name

class ValidateNode(Validator):
    config_class =              ConfigNode

    def do_shortcut(self, node):
        """Nodes can be specified with just a hostname string."""
        if isinstance(node, basestring):
            return ConfigNode(hostname=node, name=node)

    def set_defaults(self, output_dict):
        output_dict.setdefault('name', output_dict['hostname'])

valid_node = ValidateNode()


class ValidateNodePool(Validator):
    config_class =              ConfigNodePool

    def cast(self, node_pool):
        if isinstance(node_pool, list):
            node_pool = dict(nodes=node_pool)
        if 'nodes' in node_pool:
            node_pool['nodes'] = [
                normalize_node(node) for node in node_pool['nodes']
            ]
        return node_pool

    def set_defaults(self, node_pool):
        node_pool.setdefault('name', '_'.join(node_pool['nodes']))

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
        'node':                 lambda _, v: normalize_node(v)
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
        'node':                 lambda _, v: normalize_node(v)
    }

    def post_validation(self, action, path_name):
        if (
            'name' in action
            and action['name'] not in (None, CLEANUP_ACTION_NAME)
        ):
            raise ConfigError("Cleanup actions cannot have custom names (you"
                              " wanted %s.%s)" % (path_name, action['name']))

        if 'requires' in action:
            raise ConfigError("Cleanup action %s can not have requires." % path_name)

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
    }

    validators = {
        'name':                 valid_identifier,
        'schedule':             valid_schedule,
        'run_limit':            valid_int,
        'all_nodes':            valid_bool,
        'actions':              valid_populated_list,
        'cleanup_action':       lambda _, v: valid_cleanup_action(v),
        'node':                 lambda _, v: normalize_node(v),
        'queueing':             valid_bool,
        'enabled':              valid_bool,
    }

    def set_defaults(self, job):
        super(ValidateJob, self).set_defaults(job)
        if 'queueing' not in job:
            # Set queueing default based on scheduler. Usually False, But daily
            # jobs probably deal with daily data and should not be skipped
            job['queueing'] = not isinstance(job['schedule'],
                    (ConfigConstantScheduler, ConfigIntervalScheduler)
            )

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
        actions = UniqueNameDict('Action name %%s on job %s used twice' % job['name'])
        for action in job['actions'] or []:
            try:
                final_action = valid_action(action)
            except ConfigError, e:
                raise ConfigError("Invalid action config for %s: %s" % (path, e))

            if not (final_action.node or job['node']):
                raise ConfigError(
                    '%s has no node configured for %s' % (path, final_action.name))
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
        'monitor_interval':     valid_int,
        'count':                valid_int,
        'node':                 lambda _, v: normalize_node(v),
        'restart_interval':     valid_int,
    }

valid_service = ValidateService()


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """
    config_class =              TronConfig
    defaults = {
        'working_dir':          None,
        'output_stream_dir':    None,
        'command_context':      None,
        'ssh_options':          valid_ssh_options({}),
        'notification_options': None,
        'time_zone':            None,
        'nodes':                (dict(name='localhost', hostname='localhost'),),
        'node_pools':           (),
        'jobs':                 (),
        'services':             (),
    }
    validators = {
        'output_stream_dir':    valid_output_stream_dir,
        'working_dir':          valid_working_dir,
        'command_context':      valid_command_context,
        'ssh_options':          valid_ssh_options,
        'notification_options': valid_notification_options,
        'time_zone':            valid_time_zone,
    }
    optional = False

    def post_validation(self, config):
        """Validate jobs, nodes, and services."""

        node_names = UniqueNameDict('Node and NodePool names must be unique %s')
        job_service_names = UniqueNameDict('Job and Service names must be unique %s')

        # We need to set this default here until 0.5, see comment below
        config.setdefault('node_pools', [])
        # 'nodes' may contain nodes or node pools (for now). Internally split them
        # into 'nodes' and 'node_pools'. Process the nodes.
        nodes = config.pop('nodes', None) or []
        config['nodes'] = []
        for node in nodes:
            # TODO: 0.5 remove
            # A node pool is characterized by being a list, or a dictionary that
            # contains a list of nodes
            if isinstance(node, (list, dict)) and 'nodes' in node:
                log.warn('Node pools should be moved from "nodes" to "node_pools"'
                         ' before upgrading to Tron 0.5.')
                config['node_pools'].append(node)
            else:
                config['nodes'].append(node)

        def parse_sub_config(cname, valid, name_dict):
            target_dict = UniqueNameDict(
                "%s name %%s used twice" % cname.replace('_', ' '))
            for item in config.get(cname) or []:
                final = valid(item)
                target_dict[final.name] = final
                name_dict[final.name] = True
            config[cname] = FrozenDict(**target_dict)

        parse_sub_config('nodes',       valid_node,         node_names)
        parse_sub_config('node_pools',  valid_node_pool,    node_names)
        parse_sub_config('jobs',        valid_job ,         job_service_names)
        parse_sub_config('services',    valid_service,      job_service_names)

        self.validate_node_names(config, node_names)
        self.validate_node_pool_nodes(config)

    def validate_node_names(self, config, node_names):
        """Validate that any node/node_pool name that were used are configured
        as nodes/node_pools.
        """
        actions = itertools.chain.from_iterable(
            job.actions.values()
            for job in config['jobs'].values()
        )
        task_list = itertools.chain(
            config['jobs'].values(),
            config['services'].values(),
            actions
        )
        for task in task_list:
            if task.node and task.node not in node_names:
                raise ConfigError("Unknown node %s configured for %s %s" % (
                    task.node, task.__class__.__name__, task.name))

    def validate_node_pool_nodes(self, config):
        """Validate that each node in a node_pool is in fact a node, and not
        another pool.
        """
        # TODO: this can be cleaned up after 0.5
        for node_pool in config['node_pools'].itervalues():
            for node_name in node_pool.nodes:
                node = config['nodes'].get(node_name)
                if node:
                    continue
                raise ConfigError(
                    "NodePool %s contains another NodePool %s. " % (
                        node_pool.name,
                        node_name
                    )
                )


valid_config = ValidateConfig()
