"""Rewritten config system. Functionality is limited to parsing and schema
validation.
"""

from collections import Mapping, namedtuple
import datetime
import logging
import os
import re

import pytz
from twisted.conch.client import options
import yaml

log = logging.getLogger("tron.config")

CLEANUP_ACTION_NAME = "cleanup"


TAG_RE = re.compile(r'!\w+\b')

def load_config(string_or_file):
    # Hackishly strip !Tags from the file so PyYAML doesn't try to make them
    # into Python objects
    if not isinstance(string_or_file, basestring):
        s_tags = string_or_file.read()
    else:
        s_tags = string_or_file
    s_notags = TAG_RE.sub('', s_tags)

    if len(s_tags) > len(s_notags):
        log.warn('Tron no longer uses !Tags to parse config files. Please'
                 ' remove them from yours.')

    # safe_load disables python classes
    config = yaml.safe_load(s_notags)

    return valid_config(config)


class ConfigError(Exception):
    pass


### VALIDATION ###


def type_converter(convert, error_fmt):
    def f(path, value):
        try:
            return convert(value)
        except TypeError:
            raise ConfigError(error_fmt % (path, value))
        return value
    return f

valid_float = type_converter(
    float, 'Value at %s is not a number: %s')

valid_int = type_converter(
    int, 'Value at %s is not an integer: %s')


def type_validator(validator, error_fmt):
    def f(path, value):
        if not validator(value):
            raise ConfigError(error_fmt, (path, value))
        return value
    return f

valid_str = type_validator(
    lambda s: isinstance(s, basestring),
    'Value at %s is not a string: %s')

valid_list = type_validator(
    lambda s: isinstance(s, list),
    'Value at %s is not a list: %s')

valid_dict = type_validator(
    lambda s: isinstance(s, dict),
    'Value at %s is not a dictionary: %s')

valid_bool = type_validator(
    lambda s: isinstance(s, bool),
    'Value at %s is not a boolean: %s')


class FrozenDict(Mapping):
    """Simple implementation of an immutable dictionary

    from http://stackoverflow.com/questions/2703599/what-would-be-a-frozen-dict
    """

    def __init__(self, *args, **kwargs):
        if hasattr(self, '_d'):
            raise Exception("Can't call __init__ twice")
        self._d = dict(*args, **kwargs)
        self._hash = None

    def __repr__(self):
        return 'FrozenDict(%r)' % self._d

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]

    def __hash__(self):
        # It would have been simpler and maybe more obvious to 
        # use hash(tuple(sorted(self._d.iteritems()))) from this discussion
        # so far, but this solution is O(n). I don't know what kind of 
        # n we are going to run into, but sometimes it's hard to resist the 
        # urge to optimize when it will gain improved algorithmic performance.
        if self._hash is None:
            self._hash = 0
            for key, value in self.iteritems():
                self._hash ^= hash(key)
                self._hash ^= hash(value)
        return self._hash


### LOADING THE SCHEMA ###


def insert_nodup(d, key, item, error_fmt):
    if key in d:
        raise ConfigError(error_fmt, key)
    else:
        d[key] = item


TronConfig = namedtuple(
    'TronConfig',
     [
         'working_dir',          # str
         'syslog_address',       # str
         'command_context',      # FrozenDict of str
         'ssh_options',          # ConchOptions
         'notification_options', # NotificationOptions
         'time_zone',            # str
         'nodes',                # FrozenDict of ConfigNode
         'node_pools',           # FrozenDict of ConfigNodePool
         'jobs',                 # FrozenDict of ConfigJob
         'services'              # FrozenDict of ConfigService
     ])


NotificationOptions = namedtuple(
    'NotificationOptions',
    [
        'smtp_host',            # str
        'notification_addr',    # str
    ])


ConfigNode = namedtuple(
    'ConfigNode',
    [
        'name',     # str
        'hostname', # str
    ])


ConfigNodePool = namedtuple(
    'ConfigNodePool',
    [
        'nodes',    # str
        'name',     # str
    ])


ConfigJob = namedtuple(
    'ConfigJob',
    [
        'name',             # str
        'node',             # str
        'schedule',         # Config*Scheduler
        'actions',          # FrozenDict of ConfigAction
        'queueing',         # bool
        'run_limit',        # int
        'all_nodes',        # bool
        'cleanup_action',   # ConfigAction
    ])


ConfigAction = namedtuple(
    'ConfigAction',
    [
        'name',     # str
        'command',  # str
        'requires', # tuple of str
        'node',     # str
    ])


ConfigService = namedtuple(
    'ConfigService',
    [
        'name',             # str
        'node',             # str
        'pid_file',         # str
        'command',          # str
        'monitor_interval', # float
        'restart_interval', # float
        'count',            # int
    ])


ConfigConstantScheduler = namedtuple(
    'ConfigConstantScheduler', []
)


ConfigIntervalScheduler = namedtuple(
    'ConfigIntervalScheduler', [
        'timedelta',    # datetime.timedelta
    ])

ConfigDailyScheduler = namedtuple(
    'ConfigDailyScheduler', [
        'start_time',   # str HH:MM[:SS]
        'days',         # str MTWRF
    ])

ConfigGrocScheduler = namedtuple(
    'ConfigGrocScheduler', [
        'scheduler_string',    # str
    ]
)


def normalize_node(node):
    """Given a node value from a config, determine if it's the node's name or
    the node's value. The former case is the "new style" of identifiers and the
    latter case is the "old style" of anchors and aliases (*/&).
    """
    if isinstance(node, basestring):
        return node
    else:
        # probably a reference back to an already validated node
        try:
            return valid_node(node).name
        except ConfigError:
            return valid_node_pool(node).name


def valid_config(config):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.  """

    final_config = {}

    def store(key, validate_function):
        """If key is in config, pass it through validate_function(), store the
        result in final_config, and delete the key from config. Otherwise, pass
        None through validate_function() and store that result in final_config.
        This way we can keep the defaults logic where it makes sense.
        """
        if key in config:
            final_config[key] = validate_function(config[key])
            del config[key]
        else:
            final_config[key] = validate_function(None)

    store('working_dir', valid_working_dir)
    store('syslog_address', valid_syslog)
    store('command_context', valid_command_context)
    store('ssh_options', valid_ssh_options)
    store('notification_options', valid_notification_options)
    store('time_zone', valid_time_zone)

    config.setdefault('nodes', [dict(name='localhost', hostname='localhost')])
    config.setdefault('node_pools', [])
    config.setdefault('jobs', [])
    config.setdefault('services', [])

    # 'nodes' may contain nodes or node pools (for now). Internally split them
    # into 'nodes' and 'node_pools'.
    nodes = {}
    for node in config['nodes']:
        if (isinstance(node, list) or
            (isinstance(node, dict) and 'nodes' in node)):
            log.warn('Node pools should be moved from "nodes" to "node_pools"'
                     ' before upgrading to Tron 0.5.')
            # this is actually a node pool, process it later
            config['node_pools'].append(node)
            continue
        else:
            final_node = valid_node(node)
            insert_nodup(nodes, final_node.name, final_node,
                         'Node name %r is used twice')
    del config['nodes']
    final_config['nodes'] = FrozenDict(**nodes)

    # process node pools
    node_pools = {}
    for node_pool in config['node_pools']:
        final_pool = valid_node_pool(node_pool)
        insert_nodup(node_pools, final_pool.name, final_pool,
                     'Node pool name %r is used twice')
    del config['node_pools']
    final_config['node_pools'] = FrozenDict(**node_pools)

    # process jobs. output is a dict mapping name to values.
    jobs = {}
    for job in config['jobs']:
        final_job = valid_job(job)
        insert_nodup(jobs, final_job.name, final_job,
                     'Job name %r is used twice')
    del config['jobs']
    final_config['jobs'] = FrozenDict(**jobs)

    # process services
    services = {}
    for service in config['services']:
        final_service = valid_service(service)
        insert_nodup(services, final_service.name, final_service,
                     'Service name %r is used twice')
    del config['services']
    final_config['services'] = FrozenDict(**services)

    # Make sure we used everything
    if len(config) > 0:
        raise ConfigError("Unknown options: %s" % ', '.join(config.keys()))

    return TronConfig(**final_config)


def valid_working_dir(wd):
    """Given a working directory or None, return a valid working directory.
    If wd=None, try os.environ['TMPDIR']. If that doesn't exist, use /tmp.
    """
    if not wd:
        if 'TMPDIR' in os.environ:
            wd = os.environ['TMPDIR']
        else:
            wd = '/tmp'

    if not os.path.isdir(wd):
        raise ConfigError("Specified working directory \'%s\' is not a"
                          " directory" % wd)

    if not os.access(wd, os.W_OK):
        raise ConfigError("Specified working directory \'%s\' is not"
                          " writable" % wd)

    return wd


def valid_syslog(syslog):
    if syslog:
        syslog = valid_str('syslog', syslog)

    return syslog


def valid_command_context(context):
    # context can be any dict.
    return FrozenDict(**valid_dict('command_context', context or {}))


def valid_ssh_options(opts):
    ssh_options = options.ConchOptions()
    if opts.get('agent', False):
        if 'SSH_AUTH_SOCK' in os.environ:
            ssh_options['agent'] = True
        else:
            raise ConfigError("No SSH Agent available ($SSH_AUTH_SOCK)")
    else:
        ssh_options['noagent'] = True

    if 'identities' in opts:
        for file_name in opts['identities']:
            file_path = os.path.expanduser(file_name)
            if not os.path.exists(file_path):
                raise ConfigError("Private key file '%s' doesn't exist" %
                                  file_name)
            if not os.path.exists(file_path + ".pub"):
                raise ConfigError("Public key '%s' doesn't exist" %
                                  (file_name + ".pub"))

            ssh_options.opt_identity(file_name)

    extra_keys = set(opts.keys()) - set(['agent', 'identities'])
    if extra_keys:
        raise ConfigError("Unknown SSH options: %s" %
                          ', '.join(list(extra_keys)))

    return ssh_options


def valid_notification_options(options):
    if options is None:
        return None
    else:
        if sorted(options.keys()) != ['notification_addr', 'smtp_host']:
            raise ConfigError('notification_options must contain smtp_host,'
                              ' notification_addr, and nothing else.')
    return NotificationOptions(**options)


def valid_time_zone(tz):
    if tz is None:
        return None
    else:
        try:
            return pytz.timezone(valid_str('time_zone', tz))
        except pytz.exceptions.UnknownTimeZoneError:
            raise ConfigError('%s is not a valid time zone' % tz)


def valid_node(node):
    # Sure, let's accept plain strings, why not.
    if isinstance(node, basestring):
        return ConfigNode(hostname=node, name=node)

    final_node = valid_dict('nodes', node)
    sorted_keys = sorted(final_node.keys())
    if sorted_keys != ['hostname'] and sorted_keys != ['hostname', 'name']:
        raise ConfigError('Nodes must be either a string representing the'
                          ' hostname or a dictionary with the key "hostname"'
                          ' and optionally "name", which defaults to the'
                          ' hostname. You said: %s' % node)

    final_node.setdefault('name', final_node['hostname'])

    return ConfigNode(**final_node)


def valid_node_pool(node_pool):
    if isinstance(node_pool, list):
        node_pool = dict(nodes=node_pool)
    final_node_pool = dict(
        nodes=[normalize_node(node) for node in node_pool['nodes']],
    )
    if 'name' in node_pool:
        final_node_pool['name'] = node_pool['name']
    else:
        final_node_pool['name'] = '_'.join(final_node_pool['nodes'])

    return ConfigNodePool(**final_node_pool)


def valid_job(job):
    required_keys = ['name', 'node', 'schedule', 'actions']
    optional_keys = ['queueing', 'run_limit', 'all_nodes', 'cleanup_action']

    missing_keys = set(required_keys) - set(job.keys())
    if missing_keys:
        if 'name' in job:
            raise ConfigError("Job %s is missing options: %s" %
                              (job['name'], ', '.join(list(missing_keys))))
        else:
            raise ConfigError("Nameless job is missing options: %s" %
                              (', '.join(list(extra_keys))))

    extra_keys = set(job.keys()) - set(required_keys + optional_keys)
    if extra_keys:
        raise ConfigError("Unknown options in %s: %s" %
                          (job['name'], ', '.join(list(extra_keys))))

    path = 'jobs.%s' % job['name']
    final_job = dict(
        name=valid_str('jobs', job['name']),
        schedule=valid_schedule(path, job['schedule']),
        queueing=valid_bool(path, job.get('queueing', True)),
        run_limit=valid_int(path, job.get('run_limit', 50)),
        all_nodes=valid_bool(path, job.get('all_nodes', False)),
        cleanup_action=valid_cleanup_action(path,
                                            job.get('cleanup_action', None)),
        node=normalize_node(job['node']),
    )

    actions = {}
    for action in job['actions'] or []:
        final_action = valid_action(path, action)
        insert_nodup(actions, final_action.name, final_action,
                     'Action name %%r on job %r used twice' %
                     final_job['name'])
    if len(actions) < 1:
        raise ConfigError("Job %s must have at least one action" %
                          final_job['name'])
    final_job['actions'] = FrozenDict(**actions)

    return ConfigJob(**final_job)


def valid_schedule(path, schedule):
    if isinstance(schedule, basestring):
        schedule = schedule.strip()
        scheduler_args = schedule.split()
        scheduler_name = scheduler_args.pop(0).lower()

        if schedule == 'constant':
            return ConfigConstantScheduler()
        elif scheduler_name == 'daily':
            return valid_daily_scheduler(*scheduler_args)
        elif scheduler_name == 'interval':
            return valid_interval_scheduler(*scheduler_args)
        else:
            return valid_groc_scheduler(schedule)
    else:
        if 'interval' in schedule:
            return valid_interval_scheduler(**schedule)
        elif 'start_time' in schedule or 'days' in schedule:
            return valid_daily_scheduler(**schedule)
        else:
            raise ConfigError("Unknown scheduler: %r" % schedule)


def valid_daily_scheduler(start_time=None, days=None):
    """Old style, will be converted to GrocScheduler with a compatibility
    function

    schedule: !DailyScheduler
        start_time: "07:00:00"
        days: "MWF"
    """

    err_msg = ("Start time must be in string format HH:MM[:SS]. Seconds"
               " are ignored but parsed so as to be backward-compatible."
               " You said: %r")

    if start_time is not None:
        if not isinstance(start_time, basestring):
            raise ConfigError(err_msg % start_time)

        # make sure at least hours and minutes are specified
        hms = start_time.strip().split(':')

        if len(hms) < 2:
            raise ConfigError(err_msg % start_time)

    return ConfigDailyScheduler(
        start_time=start_time,
        days=days,
    )


def valid_groc_scheduler(scheduler_string):
    return ConfigGrocScheduler(
        scheduler_string=scheduler_string,
    )


def valid_interval_scheduler(interval):
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
            raise ConfigError("Invalid interval specification: %r",
                              interval)

        value, units = interval_tokens

        kwargs = {}
        for key, unit_set in TIME_INTERVAL_UNITS.iteritems():
            if units in unit_set:
                kwargs[key] = int(value)
                break
        else:
            raise ConfigError("Invalid interval specification: %r",
                              interval)

    return ConfigIntervalScheduler(
        timedelta=datetime.timedelta(**kwargs)
    )


def valid_action(path, action, is_cleanup=False):
    # check set of keys
    required_keys = ['name', 'command']
    optional_keys = ['requires', 'node']

    missing_keys = set(required_keys) - set(action.keys())
    if missing_keys:
        if 'name' in job:
            raise ConfigError("Action %s.%s is missing options: %s" %
                              (path, action['name'],
                               ', '.join(list(missing_keys))))
        else:
            raise ConfigError("Nameless action in %s is missing options: %s" %
                              (path, ', '.join(list(extra_keys))))

    extra_keys = set(action.keys()) - set(required_keys + optional_keys)
    if extra_keys:
        raise ConfigError("Unknown options in %s.%s: %s" %
                          (path, action['name'],
                           ', '.join(list(extra_keys))))

    # basic values
    my_path = '%s.%s' % (path, action['name'])
    final_action = dict(
        name=valid_str(path, action['name']),
        command=valid_str(path, action['command']),
    )

    # check name
    if ((is_cleanup and final_action['name'] != CLEANUP_ACTION_NAME) or
        (not is_cleanup and final_action['name'] == CLEANUP_ACTION_NAME)):
        raise ConfigError("Bad action name at %s: %s" %
                          (path, final_action['name']))

    if 'node' in action and action['node'] is not None:
        final_action['node'] = normalize_node(action['node'])
    else:
        final_action['node'] = None

    requires = []

    # accept a string or a list
    old_requires = action.get('requires', [])
    if isinstance(old_requires, basestring):
        old_requires = [old_requires]
    old_requires = valid_list(my_path, old_requires)

    for r in old_requires:
        if isinstance(r, basestring):
            # new style, identifier
            requires.append(r)
        else:
            # old style, alias
            requires.append(r['name'])
    final_action['requires'] = tuple(requires)

    return ConfigAction(**final_action)


def valid_cleanup_action(path, action):
    if action is None:
        return None

    if ('name' in action and
        action['name'] not in (None, CLEANUP_ACTION_NAME)):
        raise ConfigError("Cleanup actions cannot have custom names (you"
                          " wanted %s.%s)" % (path, action['name']))
    if action.get('requires', []):
        raise ConfigError("Cleanup actions cannot have dependencies (%r)" %
                          path)

    action['name'] = CLEANUP_ACTION_NAME
    action['requires'] = []

    return valid_action(path, action, is_cleanup=True)


def valid_service(service):
    required_keys = ['name', 'node', 'pid_file', 'command', 'monitor_interval']
    optional_keys = ['restart_interval', 'count']

    missing_keys = set(required_keys) - set(service.keys())
    if missing_keys:
        if 'name' in service:
            raise ConfigError("Service %s is missing options: %s" %
                              (service['name'], ', '.join(list(missing_keys))))
        else:
            raise ConfigError("Nameless service is missing options: %s" %
                              (', '.join(list(extra_keys))))

    extra_keys = set(service.keys()) - set(required_keys + optional_keys)
    if extra_keys:
        raise ConfigError("Unknown options in %s: %s" %
                          (service['name'], ', '.join(list(extra_keys))))

    path = 'services.%s' % service['name']
    final_service = dict(
        name=valid_str(path, service['name']),
        pid_file=valid_str(path, service['pid_file']),
        command=valid_str(path, service['command']),
        monitor_interval=valid_int(path, service['monitor_interval']),
        count=valid_int(path, service.get('count', 1)),
        node=normalize_node(service['node']),
    )

    if 'restart_interval' in service:
        final_service['restart_interval'] = valid_int(
                                                path,
                                                service['restart_interval'])
    else:
        final_service['restart_interval'] = None

    return ConfigService(**final_service)
