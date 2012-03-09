"""Rewritten config system. Functionality is limited to parsing and schema
validation.
"""

from collections import namedtuple
import datetime
from functools import partial
import logging
import re

import pytz
import yaml

from tron.schedule_parse import CONVERT_DAYS_INT    # map day index to name
from tron.schedule_parse import parse_daily_expression
from tron.utils.dicts import FrozenDict


log = logging.getLogger("tron.config")

CLEANUP_ACTION_NAME = "cleanup"


YAML_TAG_RE = re.compile(r'!\w+\b')

def load_config(string_or_file):
    """Given a string or file object, load it with PyYAML and return an
    immutable, validated representation of the configuration it specifies.
    """
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


class ConfigError(Exception):
    """Generic exception class for errors with config validation"""
    pass


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
         'working_dir',          # str
         'syslog_address',       # str
         'command_context',      # FrozenDict of str
         'ssh_options',          # ConchOptions
         'notification_options', # NotificationOptions or None
         'time_zone',            # pytz time zone
         'nodes',                # FrozenDict of ConfigNode
         'node_pools',           # FrozenDict of ConfigNodePool
         'jobs',                 # FrozenDict of ConfigJob
         'services'              # FrozenDict of ConfigService
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
        'agent',        # bool
        'identities',   # list of str
    ])


ConfigNode = config_object_factory('ConfigNode', ['hostname'], ['name'])


ConfigNodePool = config_object_factory('ConfigNodePool', ['nodes'], ['name'])


ConfigJob = config_object_factory(
    'ConfigJob',
    [
        'name',             # str
        'node',             # str
        'schedule',         # Config*Scheduler
        'actions',          # FrozenDict of ConfigAction
    ],
    [
        'queueing',         # bool
        'run_limit',        # int
        'all_nodes',        # bool
        'cleanup_action',   # ConfigAction
    ])


ConfigAction = config_object_factory(
    'ConfigAction',
    [
        'name',     # str
        'command',  # str
    ],[
        'requires', # tuple of str
        'node',     # str
    ])

ConfigCleanupAction = config_object_factory(
    'ConfigCleanupAction',
    [
        'command',  # str
    ],[
        'requires', # tuple of str
        'name',     # str
        'node',     # str
    ])


ConfigService = config_object_factory(
    'ConfigService',
    [
        'name',             # str
        'node',             # str
        'pid_file',         # str
        'command',          # str
        'monitor_interval', # float
    ],[
        'restart_interval', # float
        'count',            # int
    ])


ConfigConstantScheduler = namedtuple(
    'ConfigConstantScheduler', [])


ConfigIntervalScheduler = namedtuple(
    'ConfigIntervalScheduler', [
        'timedelta',    # datetime.timedelta
    ])


# ConfigDailyScheduler lives in tron.schedule_parse, which contains its support
# functions.


# Final note about schedulers. All schedulers have a timezone attribute, but it
# is assigned *after* the whole config has been parsed.


### VALIDATION ###

class DictNoUpdate(dict):
    """A dict like object that throws a ConfigError if a key exists and a set
    is called to change the value of that key.
     *fmt_string* will be interpolated with (key,)
    """
    def __init__(self, fmt_string, **kwargs):
        super(dict, self).__init__(**kwargs)
        self.fmt_string = fmt_string

    def __setitem__(self, key, value):
        if key in self:
            raise ConfigError(self.fmt_string, key)
        super(DictNoUpdate, self).__setitem__(key, value)


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
        """
        if value is None and optional:
            return None
        if not validator(value):
            raise ConfigError(error_fmt, (path, value))
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
    lambda s: isinstance(s, list),
    'Value at %s is not a list: %s')

valid_dict = type_validator(
    lambda s: isinstance(s, dict),
    'Value at %s is not a dictionary: %s')

valid_bool = type_validator(
    lambda s: isinstance(s, bool),
    'Value at %s is not a boolean: %s')


### LOADING THE SCHEMA ###




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


class Validator(object):
    """Base class for validating a collection and creating a mutable
    collection from the source.
    """
    config_class = None
    defaults = {}
    validators = {}
    optional = False

    def validate(self, input):
        if self.optional and input is None:
            return None

        shortcut_value = self.do_shortcut(input)
        if shortcut_value:
            return shortcut_value

        input = self.cast(input)
        self.validate_required_keys(input)
        self.validate_extra_keys(input)
        output_dict = self.build_dict(input)
        self.set_defaults(output_dict)
        return self.config_class(**output_dict)

    __call__ = validate

    @property
    def type_name(self):
        return self.config_class.__name__

    def do_shortcut(self, input):
        """Override if your validator can skip most of the validation by
        checking this condition.  If this returns a truthy value, the
        validation will end immediately and return that value.
        """
        pass

    def cast(self, input):
        """If your validator accepts input in different formations, override
        this method to cast your input into a common format.
        """
        return input

    def validate_required_keys(self, input):
        """Check that all required keys are present."""
        missing_keys = set(self.config_class.required_keys) - set(input.keys())
        if missing_keys:
            if 'name' in self.config_class.required_keys and 'name' in input:
                raise ConfigError("%s %s is missing options: %s" % (
                        self.type_name.capitalize(),
                        input['name'],
                        ', '.join(missing_keys)
                ))
            else:
                raise ConfigError("Nameless %s is missing options: %s" % (
                        self.type_name,
                        ', '.join(list(missing_keys))
                ))

    def validate_extra_keys(self, input):
        """Check that no unexpected keys are present."""
        extra_keys = set(input.keys()) - set(
            self.config_class.required_keys + self.config_class.optional_keys)
        if extra_keys:
            raise ConfigError("Unknown options in %s %s: %s" % (
                    self.type_name,
                    input.get('name', ''),
                    ', '.join(list(extra_keys))
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


def valid_working_dir(wd):
    """Given a working directory or None, return a valid working directory.
    If wd=None, try os.environ['TMPDIR']. If that doesn't exist, use /tmp.
    """
    # TODO: should this do the lookups the docstring claims it does?
    return valid_str('working_dir', wd, optional=True)


def valid_syslog(syslog):
    return valid_str('syslog', syslog, optional=True)


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
    """ Validate SSH options."""
    config_class = ConfigSSHOptions
    defaults = {'agent': False, 'identities': ()}
    validators = {
        'agent': partial(valid_bool, 'ssh_options.agent'),
        'identities': partial(valid_list, 'ssh_options.identities')
    }
    optional = True

valid_ssh_options = ValidateSSHOptions()


class ValidateNotificationOptions(Validator):
    """Validate notification options."""
    config_class = NotificationOptions
    optional = True

valid_notification_options = ValidateNotificationOptions()


class ValidateNode(Validator):
    config_class = ConfigNode

    def do_shortcut(self, node):
        # Sure, let's accept plain strings, why not.
        if isinstance(node, basestring):
            return ConfigNode(hostname=node, name=node)

    def set_defaults(self, output_dict):
        output_dict.setdefault('name', output_dict['hostname'])

valid_node = ValidateNode()


class ValidateNodePool(Validator):
    config_class = ConfigNodePool

    def cast(self, node_pool):
        if isinstance(node_pool, list):
            node_pool = dict(nodes=node_pool)
        return dict(nodes=[normalize_node(node) for node in node_pool['nodes']])

    def set_defaults(self, node_pool):
        node_pool.setdefault('name', '_'.join(node_pool['nodes']))

valid_node_pool = ValidateNodePool()


def valid_schedule(_, schedule):
    if isinstance(schedule, basestring):
        schedule = schedule.strip()
        scheduler_args = schedule.split()
        scheduler_name = scheduler_args.pop(0).lower()

        if schedule == 'constant':
            return ConfigConstantScheduler()
        elif scheduler_name == 'daily':
            return valid_daily_scheduler(*scheduler_args)
        elif scheduler_name == 'interval':
            return valid_interval_scheduler(' '.join(scheduler_args))
        else:
            return parse_daily_expression(schedule)
        
    if 'interval' in schedule:
        return valid_interval_scheduler(**schedule)
    elif 'start_time' in schedule or 'days' in schedule:
        return valid_daily_scheduler(**schedule)
    else:
        raise ConfigError("Unknown scheduler: %s" % schedule)


def valid_daily_scheduler(start_time=None, days=None):
    """Old style, will be converted to DailyScheduler with a compatibility
    function

    schedule:
        start_time: "07:00:00"
        days: "MWF"
    """

    err_msg = ("Start time must be in string format HH:MM[:SS]. Seconds"
               " are ignored but parsed so as to be backward-compatible."
               " You said: %s")

    if start_time is None:
        hms = ['00', '00']
    else:
        if not isinstance(start_time, basestring):
            raise ConfigError(err_msg % start_time)

        # make sure at least hours and minutes are specified
        hms = start_time.strip().split(':')

        if len(hms) < 2:
            raise ConfigError(err_msg % start_time)

    weekdays = set(CONVERT_DAYS_INT[d] for d in days or 'MTWRFSU')
    if weekdays == set([0, 1, 2, 3, 4, 5, 6]):
        days_str = 'day'
    else:
        # incoming string is MTWRF, we want M,T,W,R,F for the parser
        days_str = ','.join(days)

    return parse_daily_expression(
        'every %s of month at %s:%s' % (days_str, hms[0], hms[1])
    )


def valid_interval_scheduler(interval):
    # remove spaces
    interval = ''.join(interval.split())

    # Shortcut values for intervals
    TIME_INTERVAL_SHORTCUTS = {
        'hourly': dict(hours=1),
    }

    # Translations from possible configuration units to the argument to
    # datetime.timedelta
    TIME_INTERVAL_UNITS = {
        'months': ['mo', 'month', 'months'],
        'days': ['d', 'day', 'days'],
        'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
        'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
        'seconds': ['s', 'sec', 'secs', 'second', 'seconds']
    }

    if interval in TIME_INTERVAL_SHORTCUTS:
        kwargs = TIME_INTERVAL_SHORTCUTS[interval]
    else:
        # Split digits and characters into tokens
        interval_re = re.compile(r"\d+|[a-zA-Z]+")
        interval_tokens = interval_re.findall(interval)
        if len(interval_tokens) != 2:
            raise ConfigError("Invalid interval specification: %s", interval)

        value, units = interval_tokens

        kwargs = {}
        for key, unit_set in TIME_INTERVAL_UNITS.iteritems():
            if units in unit_set:
                kwargs[key] = int(value)
                break
        else:
            raise ConfigError("Invalid interval specification: %s", interval)

    return ConfigIntervalScheduler(timedelta=datetime.timedelta(**kwargs))


class ValidateAction(ValidatorWithNamedPath):
    """Validate an action."""
    config_class = ConfigAction
    defaults = {'node': None}
    validators = {
        'name': valid_identifier,
        'command': valid_str,
        # TODO: have all of these been updated to use a lambda?
        'node': lambda _, v: normalize_node(v)
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
            old_requires = [old_requires]

        # pointer
        if isinstance(old_requires, dict):
            old_requires = [old_requires['name']]

        old_requires = valid_list(path_name, old_requires)

        for r in old_requires:
            if isinstance(r, basestring):
                # new style, identifier
                requires.append(r)
            else:
                # old style, alias
                requires.append(r['name'])

            if requires[-1] == CLEANUP_ACTION_NAME:
                raise ConfigError('Actions cannot depend on the cleanup action.'
                                  ' (%s)' % path_name)

        action['requires'] = tuple(requires)

valid_action = ValidateAction()


class ValidateCleanupAction(ValidatorWithNamedPath):
    config_class = ConfigCleanupAction
    defaults = {
        'node': None,
        'name': CLEANUP_ACTION_NAME,
    }
    validators = {
        'name': valid_identifier,
        'command': valid_str,
        # TODO: have all of these been updated to use a lambda?
        'node': lambda _, v: normalize_node(v)
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
    config_class = ConfigJob
    defaults = {
        'run_limit': 50,
        'all_nodes': False,
        'cleanup_action': None
    }

    validators = {
        'name': valid_identifier,
        'schedule': valid_schedule,
        'run_limit': valid_int,
        'all_nodes': valid_bool,
        'cleanup_action': lambda _, v: valid_cleanup_action(v),
        'node': lambda _, v: normalize_node(v),
        'queueing': valid_bool,
    }

    def set_defaults(self, job):
        super(ValidateJob, self).set_defaults(job)
        if 'queueing' not in job:
            # TODO: this should be based on a field in the associated scheduler
            # Set queueing default based on scheduler. Usually False, But daily
            # jobs probably deal with daily data and should not be skipped
            job['queueing'] = not (
                isinstance(job['schedule'], ConfigIntervalScheduler) or
                isinstance(job['schedule'], ConfigConstantScheduler)
            )

    def post_validation(self, job, path):
        """Validate actions for the job."""
        actions = DictNoUpdate('Action name %%s on job %s used twice' % job['name'])
        for action in job['actions'] or []:
            try:
                final_action = valid_action(action)
            except ConfigError, e:
                raise ConfigError("Invalid action config for %s: %s" % (path, e))

            if not (final_action.node or job['node']):
                raise ConfigError('%s has no actions configured for %s' %
                                  (path, final_action.name))
            actions[final_action.name] = final_action

        if len(actions) < 1:
            raise ConfigError("Job %s must have at least one action" %
                              job['name'])

        # TODO: revisit this
        # make sure there are no circular or misspelled dependencies
        def dfs_action(base_action, current_action, stack):
            stack.append(current_action.name)
            for dep in current_action.requires:
                try:
                    if dep == base_action.name and len(stack) > 0:
                        raise ConfigError('Circular dependency in %s: %s' %
                                          (path, ' -> '.join(stack)))
                    dfs_action(base_action, actions[dep], stack)
                except KeyError:
                    raise ConfigError('Action jobs.%s.%s has a dependency "%s"'
                                      ' that is not in the same job!' %
                                      (job['name'],
                                       current_action.name,
                                       dep))
            stack.pop()

        for action in actions.values():
            dfs_action(action, action, [])

        job['actions'] = FrozenDict(**actions)

valid_job = ValidateJob()


class ValidateService(ValidatorWithNamedPath):
    """Validate a services configuration."""
    config_class = ConfigService
    defaults = {
        'count': 1,
        'restart_interval': None
    }

    validators = {
        'name': valid_identifier,
        'pid_file': valid_str,
        'command': valid_str,
        'monitor_interval': valid_int,
        'count': valid_int,
        'node': lambda _, v: normalize_node(v),
        'restart_interval': valid_int,
    }

valid_service = ValidateService()


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """
    config_class = TronConfig
    defaults = {
        'working_dir': None,
        'syslog_address': None,
        'command_context': None,
        'ssh_options': None,
        'notification_options': None,
        'time_zone': None,
        'nodes': (dict(name='localhost', hostname='localhost'),),
        'node_pools': (),
        'jobs': (),
        'services': (),
    }
    validators = {
        'working_dir': valid_working_dir,
        'syslog_address': valid_syslog,
        'command_context': valid_command_context,
        'ssh_options': valid_ssh_options,
        'notification_options': valid_notification_options,
        'time_zone': valid_time_zone,
    }
    optional = False

    def post_validation(self, config):
        # We need to set this default here until 0.5, see comment below
        config.setdefault('node_pools', [])

        # 'nodes' may contain nodes or node pools (for now). Internally split them
        # into 'nodes' and 'node_pools'. Process the nodes.
        nodes = DictNoUpdate('Node name %s is used twice')
        for node in config.get('nodes', []):
            # A node pool is characterized by being a list, or a dictionary that
            # contains a list of nodes
            if (isinstance(node, list) or
                (isinstance(node, dict) and 'nodes' in node)):
                log.warn('Node pools should be moved from "nodes" to "node_pools"'
                         ' before upgrading to Tron 0.5.')
                config['node_pools'].append(node)
                continue

            final_node = valid_node(node)
            nodes[final_node.name] = final_node
        config['nodes'] = FrozenDict(**nodes)

        # process node pools
        node_pools = DictNoUpdate('Node pool name %s is used twice')
        for node_pool in config.get('node_pools', []):
            final_pool = valid_node_pool(node_pool)
            node_pools[final_pool.name] = final_pool
        config['node_pools'] = FrozenDict(**node_pools)

        # process jobs. output is a dict mapping name to values.
        jobs = DictNoUpdate('Job name %s is used twice')
        for job in config.get('jobs', []):
            final_job = valid_job(job)
            jobs[final_job.name] = final_job
        config['jobs'] = FrozenDict(**jobs)

        # process services
        services = DictNoUpdate('Service name %s is used twice')
        for service in config.get('services', []):
            final_service = valid_service(service)
            services[final_service.name] = final_service
        config['services'] = FrozenDict(**services)

valid_config = ValidateConfig()

# TODO: move to a file
DEFAULT_CONFIG = """
ssh_options:
    ## Tron needs SSH keys to allow the effective user to login to each of the
    ## nodes specified in the "nodes" section. You can choose to use either an
    ## SSH agent or list
    # identities:
    #     - /home/tron/.ssh/id_dsa
    agent: true

## Uncomment if you want logging to syslog. Typical values for different
## platforms:
##    Linux: "/dev/log"
##    OS X: "/var/run/syslog"
##    Windows: ["localhost", 514]
# syslog_address: /dev/log

# notification_options:
      ## In case of trond failures, where should we send notifications to ?
      # smtp_host: localhost
      # notification_addr: nobody@localhost

nodes:
    ## You'll need to list out all the available nodes for doing work.
    # - name: "node"
    #   hostname: 'localhost'

## Optionally you can list 'pools' of nodes where selection of a node will
## be randomly determined or jobs can be configured to be run on all nodes
## in the pool
# node_pools:
    # - name: NodePool
    #   nodes: [node]

command_context:
    # Variable subsitution
    # There are some built-in values such as 'node', 'runid', 'actionname' and
    # run-time based variables such as 'shortdate'. (See tronfig.1 for
    # reference.) You can specify whatever else you want similiar to
    # environment variables:
    # PYTHON: "/usr/bin/python"

jobs:
    ## Configure your jobs here by specifing a name, node, schedule and the
    ## work flow that should executed.
    # - name: "sample_job"
    #   node: node
    #   schedule: "daily"
    #   actions:
    #     - name: "uname"
    #       command: "uname -a"
    #   cleanup_action:
    #     command: "rm -rf /tmp/sample_job_scratch"

services:
    ## Configure services here. Services differ from jobs in that they are
    ## expected to have an enable/disable and monitoring phase.
    # - name: "sample_service"
    #   node: node
    #   count: 2
    #   pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
    #   command: "run_service --pid-file=%(pid_file)s start"
    #   monitor_interval: 20
    #   restart_interval: 60s
"""
