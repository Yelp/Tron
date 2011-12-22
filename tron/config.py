import datetime
import logging
import logging.handlers
import os
import re
import socket
import sys
import weakref

import pytz
import yaml
from twisted.conch.client import options

from tron import action
from tron import job
from tron import node
from tron import scheduler
from tron import monitor
from tron import emailer
from tron import command_context
from tron import service

log = logging.getLogger("tron.config")

CLEANUP_ACTION_NAME = "cleanup"


class Error(Exception):
    pass


class ConfigError(Exception):
    pass


# If a configuration is not provided when trond starts, here is what we begin
# with.  The user can then use tronfig command to customize their installation.
DEFAULT_CONFIG = """--- !TronConfiguration

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
    # - &node
    #     hostname: 'localhost'

    ## Optionally you can list 'pools' of nodes where selection of a node will
    ## be randomly determined or jobs can be configured to be run on all nodes
    ## in the pool
    # - &all_nodes !NodePool
    #     nodes: [*node]

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
    # - &sample_job
    #     name: "sample_job"
    #     node: *node
    #     schedule: "daily"
    #     actions:
    #         -
    #             name: "uname"
    #             command: "uname -a"
    #     cleanup_action:
    #         command: "rm -rf /tmp/sample_job_scratch"

services:
    ## Configure services here. Services differ from jobs in that they are
    ## expected to have an enable/disable and monitoring phase.
    # - &sample_service
    #     name: "sample_service"
    #     node: *node
    #     count: 2
    #     pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
    #     command: "run_service --pid-file=%(pid_file)s start"
    #     monitor_interval: 20
    #     restart_interval: 60s
"""


class FromDictBuilderMixin(object):
    """Mixin class for building YAMLObjects from dictionaries"""

    @classmethod
    def from_dict(cls, obj_dict):
        # We just assume we want to make all the dictionary values into
        # attributes
        new_obj = cls()
        new_obj.__dict__.update(obj_dict)
        return new_obj


def default_or_from_tag(value, cls):
    """Construct a YAMLObject unless it already is one

    We use this for providing default config types so it isn't required to
    "tag" everything with !MyConfigClass Since YAML may present us the same
    dictionary a few times thanks to references any actual operations we do in
    this function will be *persisted* by adding a key to the base dictionary
    with the instance we create
    """
    if not isinstance(value, yaml.YAMLObject):
        # First we check if we've already defaulted this instance before
        if '__obj__' in value:
            classified = value['__obj__']
            if classified:
                return classified

        classified = cls.from_dict(value)
        value['__obj__'] = classified
        return classified

    return value


class _ConfiguredObject(yaml.YAMLObject, FromDictBuilderMixin):
    """Base class for common configured objects where the configuration
    generates one actualized object in the app that may be referenced by
    other objects.
    """

    # Redefined to indicate the type of object this configuration will build
    actual_class = None

    line_number = None

    @classmethod
    def from_yaml(cls, loader, node):
        line_number = node.start_mark.line

        # These loader calls return generators for some reason I don't
        # understand. For our purposes, we can just assume a single object is
        # being generated.
        results = list(loader.construct_yaml_object(node, cls))
        if len(results) > 1:
            raise ConfigError("More than one object")

        result = results[0]
        result.line_number = line_number
        result._validate()

        return result

    @classmethod
    def from_dict(cls, obj_dict):
        result = super(_ConfiguredObject, cls).from_dict(obj_dict)
        result._validate()
        return result

    def __init__(self, *args, **kwargs):
        # No arguments
        super(_ConfiguredObject, self).__init__()

    def _apply(self):
        raise NotImplementedError

    def update(self, obj):
        self._ref = weakref.ref(obj)
        self._apply()

    def _build(self):
        return self.actual_class()

    def _validate(self):
        """Optional method for providing validation steps to the configuration
        settings. This will be called before we start constructing real
        actualized objects.
        """
        pass

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return -1

        our_dict = [(key, value) for key, value in self.__dict__.iteritems()
                    if not key.startswith('_')]
        other_dict = [(key, value) for key, value in other.__dict__.iteritems()
                      if not key.startswith('_')]

        c = cmp(our_dict, other_dict)
        return c

    def __hash__(self):
        raise Exception('hashing')

    @property
    def actualized(self):
        if not hasattr(self, '_ref'):
            actualized_obj = self._build()
            self.update(actualized_obj)
        else:
            actualized_obj = self._ref()

        return actualized_obj

    def is_actualized(self):
        return hasattr(self, '_ref') and self._ref()


class TronConfiguration(yaml.YAMLObject):
    yaml_tag = u'!TronConfiguration'

    def _apply_nodes(self, mcp):
        """Handle our node/node pool configuration and make sure MCP knows
        about them all
        """

        existing_nodes = mcp.nodes
        mcp.nodes = []

        if self.nodes is None:
            return

        try:
            for node_conf in self.nodes:
                node = default_or_from_tag(node_conf, Node)
                # We only deal with the actual nodes, as NodePools are just
                # collections of the same underlying Node instances.
                if isinstance(node, Node):
                    mcp.nodes.append(node.actualized)

        except Exception:
            # Restore on failure
            mcp.nodes = existing_nodes
            raise

    def _apply_loggers(self, mcp):
        root = logging.getLogger('')
        handlers_to_be_removed = set(h for h in root.handlers
                                     if h not in mcp.base_logging_handlers)

        # Only change handlers if they will actually be different from the old
        # handlers
        new_handlers = []
        if hasattr(self, 'syslog_address'):
            if not isinstance(self.syslog_address, basestring):
                self.syslog_address = tuple(self.syslog_address)

            try:
                h = logging.handlers.SysLogHandler(self.syslog_address)
                fmt_str = "tron[%(process)d]: %(message)s"
                h.setFormatter(logging.Formatter(fmt_str))
                new_handlers.append(h)
            except socket.error:
                raise ConfigError('%s is not a valid syslog address' %
                                  self.syslog_address)

        for h in handlers_to_be_removed:
            log.info('Removing logging handler %s', h)
            root.removeHandler(h)

        for h in new_handlers:
            log.info('Adding logging handler %s', h)
            root.addHandler(h)

    def _apply_time_zone_name(self, mcp):
        if hasattr(self, 'time_zone'):
            mcp.time_zone = pytz.timezone(self.time_zone)

    def _apply_jobs(self, mcp):
        """Configure jobs"""
        found_jobs = []

        # Check for duplicates before we start editing jobs
        def check_dup(dic, nex):
            if nex.name in dic:
                raise yaml.YAMLError("%s is previously defined" % nex.name)
            dic[nex.name] = 1
            return dic

        jobs = []
        if getattr(self, 'jobs', None):
            jobs = [default_or_from_tag(job_val, Job) for job_val in self.jobs]

        found_jobs = reduce(check_dup, jobs, {})
        for job_config in jobs:
            log.debug("Building new job %s", job_config.name)
            new_job = job_config.actualized
            mcp.add_job(new_job)

        for job_name in mcp.jobs.keys():
            if job_name not in found_jobs:
                log.debug("Removing job %s", job_name)
                mcp.remove_job(job_name)

    def _apply_services(self, mcp):
        """Configure services"""
        service_configs = []
        built_services = []
        if getattr(self, 'services', None):
            service_configs.extend([default_or_from_tag(srv_val, Service)
                                    for srv_val in self.services])

        found_srv_names = set()
        for srv_config in service_configs:
            if not isinstance(srv_config, Service):
                raise ConfigError("There is a !Job in the services list: %s" %
                                  srv_config)
            if srv_config.name in found_srv_names:
                raise yaml.YAMLError("Duplicate service name %s" %
                                     srv_config.name)
            found_srv_names.add(srv_config.name)

            log.debug("Building new services %s", srv_config.name)
            new_service = srv_config.actualized
            built_services.append(new_service)

        for srv_name in mcp.services.keys():
            if srv_name not in found_srv_names:
                log.debug("Removing service %s", srv_name)
                mcp.remove_service(srv_name)

        # Go through our constructed services and add them. We'll catch all the
        # failures and throw an exception at the end if anything failed. This
        # is a mitigation against a bug easily cause us to be in an
        # inconsistent state

        failure = False
        for service in built_services:
            try:
                mcp.add_service(service)
            except Exception, e:
                log.exception("Failed adding new service")
                failure = True

        if failure:
            raise ConfigError("Failed adding services")

    def _get_working_dir(self, mcp):
        if mcp.state_handler.working_dir:
            return mcp.state_handler.working_dir
        if hasattr(self, 'working_dir'):
            return self.working_dir
        if 'TMPDIR' in os.environ:
            return os.environ['TMPDIR']
        return '/tmp'

    def apply(self, mcp):
        """Apply the configuration to the specified master control program"""
        working_dir = self._get_working_dir(mcp)
        if not os.path.isdir(working_dir):
            raise ConfigError("Specified working directory \'%s\' is not a"
                              " directory" % working_dir)
        if not os.access(working_dir, os.W_OK):
            raise ConfigError("Specified working directory \'%s\' is not"
                              " writable" % working_dir)

        mcp.state_handler.working_dir = working_dir

        self._apply_loggers(mcp)

        if hasattr(self, 'command_context'):
            if mcp.context:
                mcp.context.base = self.command_context
            else:
                mcp.context = command_context.CommandContext(
                    self.command_context)

        self._apply_nodes(mcp)

        if hasattr(self, 'ssh_options'):
            self.ssh_options = default_or_from_tag(self.ssh_options,
                                                   SSHOptions)
            self.ssh_options._apply(mcp)

        self._apply_time_zone_name(mcp)
        self._apply_jobs(mcp)
        self._apply_services(mcp)

        if hasattr(self, 'notification_options'):
            self.notification_options = default_or_from_tag(
                self.notification_options, NotificationOptions)
            self.notification_options._apply(mcp)


class SSHOptions(yaml.YAMLObject, FromDictBuilderMixin):
    yaml_tag = u'!SSHOptions'

    def _build_conch_options(self):
        """Verify and construct the ssh (conch) option object

        This is just a dictionary like object that options the twisted ssh
        implementation uses.
        """
        ssh_options = options.ConchOptions()
        if not hasattr(self, 'agent'):
            ssh_options['noagent'] = True
        else:
            if 'SSH_AUTH_SOCK' in os.environ:
                ssh_options['agent'] = True
            else:
                raise Error("No SSH Agent available ($SSH_AUTH_SOCK)")

        if hasattr(self, "identities"):
            for file_name in self.identities:
                file_path = os.path.expanduser(file_name)
                if not os.path.exists(file_path):
                    raise Error("Private key file %s doesn't exist" %
                                file_name)
                if not os.path.exists(file_path + ".pub"):
                    raise Error("Public key %s doesn't exist" %
                                (file_name + ".pub"))

                ssh_options.opt_identity(file_name)

        return ssh_options

    def _apply(self, mcp):
        options = self._build_conch_options()

        for node in mcp.nodes:
            node.conch_options = options


class NotificationOptions(yaml.YAMLObject, FromDictBuilderMixin):
    yaml_tag = u'!NotificationOptions'

    def _apply(self, mcp):
        if not hasattr(self, 'smtp_host'):
            raise Error("smtp_host required")
        if not hasattr(self, 'notification_addr'):
            raise Error("notification_addr required")

        if mcp.monitor:
            mcp.monitor.stop()

        em = emailer.Emailer(self.smtp_host, self.notification_addr)
        mcp.monitor = monitor.CrashReporter(em, mcp)
        mcp.monitor.start()


def _match_name(real, name):
    real.name = name

    if not re.match(r'[a-z_]\w*$', name, re.I):
        raise yaml.YAMLError("Invalid job name '%s' - not a valid identifier" %
                             name)


def _match_node(real, node_conf):
    node = default_or_from_tag(node_conf, Node)
    if not isinstance(node, NodePool):
        node_pool = NodePool()
        node_pool.nodes.append(node)
    else:
        node_pool = node

    real.node_pool = node_pool.actualized


def _match_schedule(real, schedule_conf):
    if isinstance(schedule_conf, basestring):
        # This is a short string
        real.scheduler = Scheduler.from_string(schedule_conf)
    else:
        # This is a scheduler instance, which has more info
        real.scheduler = schedule_conf.actualized

    real.scheduler.job_setup(real)


def _match_actions(real_job, action_conf_list):
    if not action_conf_list:
        raise ConfigError("Empty action list")

    name_set = set()
    for action_conf in action_conf_list:
        action = default_or_from_tag(action_conf, Action)

        if action.name in name_set:
            raise ConfigError("Duplicate action name '%s'" %
                              action.name, action.line_number)
        else:
            name_set.add(action.name)

        real_action = action.actualized

        # because of the ass-backwards way tron parses config, we need to
        # use getattr here
        if (not getattr(real_job, 'node_pool', None) and
            not getattr(real_action, 'node_pool', None)):
            raise ConfigError("Either job '%s' or its action '%s' must have a"
                              " node" % (real_job.name, real_action.name))

        # Do all the dependent actions belong to the same job?
        for dependent in real_action.required_actions:
            if dependent.job != real_job:
                raise ConfigError("Action dependency on invalid action: %s",
                                  dependent.name)

        real_job.add_action(real_action)

        # Configuration is dependent on the job being set here so we can verify
        # the dependencies make sense.
        assert real_action.job


class Job(_ConfiguredObject):
    yaml_tag = u'!Job'
    actual_class = job.Job

    def _validate(self):
        for key in ('name', 'node', 'schedule', 'actions'):
            if not hasattr(self, key):
                raise ConfigError("Missing config value for %s", key)

    def _apply(self):
        real_job = self._ref()

        _match_name(real_job, self.name)
        _match_node(real_job, self.node)
        _match_schedule(real_job, self.schedule)
        _match_actions(real_job, self.actions)

        if hasattr(self, "queueing"):
            real_job.queueing = self.queueing

        if hasattr(self, "run_limit"):
            real_job.run_limit = self.run_limit

        if hasattr(self, "all_nodes"):
            real_job.all_nodes = self.all_nodes

        if hasattr(self, "cleanup_action"):
            # condensed, specialized version of _match_actions()
            action = default_or_from_tag(self.cleanup_action, CleanupAction)
            real_action = action.actualized

            if not real_job.node_pool and not real_action.node_pool:
                raise ConfigError("Either job '%s' or its action '%s' must"
                                  " have a node" % (real_job.name,
                                                    action_action.name))
            real_job.cleanup_action = real_action
            real_action.job = real_job
            real_job._register_action(real_action)


class Service(_ConfiguredObject):
    yaml_tag = u'!Service'
    actual_class = service.Service

    def _validate(self):
        for key in ('name', 'command', 'monitor_interval', 'pid_file', 'node'):
            if not getattr(self, key, None):
                raise ConfigError("Missing value in service %s %r" %
                                  (key, self), self.line_number)

        if not re.match(r'[a-z_]\w*$', self.name, re.I):
            raise ConfigError("Invalid action name '%s' - not a valid"
                              " identifier" % self.name, self.line_number)

    def _apply(self):
        real_service = self._ref()

        _match_name(real_service, self.name)
        _match_node(real_service, self.node)
        real_service.monitor_interval = int(self.monitor_interval)
        if hasattr(self, 'restart_interval'):
            real_service.restart_interval = int(self.restart_interval)

        real_service.pid_file_template = self.pid_file

        real_service.command = self.command

        if hasattr(self, "count"):
            real_service.count = self.count

        if not real_service.node_pool:
            raise ConfigError("Missing node pool for %s" % self.name)


class Action(_ConfiguredObject):
    yaml_tag = u'!Action'
    actual_class = action.Action

    def __init__(self, *args, **kwargs):
        super(Action, self).__init__(*args, **kwargs)
        self.name = None
        self.command = None

    def _validate(self):
        for key in ('name', 'command'):
            if not getattr(self, key, None):
                raise ConfigError("Missing value in action %s %r" %
                                  (key, self), self.line_number)

        if (not re.match(r'[a-z_]\w*$', self.name, re.I) or
            self.name == CLEANUP_ACTION_NAME):
            raise ConfigError("Invalid action name '%s' - not a valid"
                              " identifier" % self.name, self.line_number)

    def _apply_requirements(self, real_action, requirements):
        if not isinstance(requirements, list):
            requirements = [requirements]

        # All the requirement actions should already have been setup
        for req in requirements:
            action_req = default_or_from_tag(req, Action)
            if not action_req.is_actualized():
                raise ConfigError("Action %s doesn't yet exist" %
                                  action_req.name, self.line_number)

            real_action.required_actions.append(action_req.actualized)

    def _apply(self):
        """Configured the specific action instance"""
        real_action = self._ref()

        real_action.name = self.name
        real_action.command = self.command
        if hasattr(self, "node"):
            node = default_or_from_tag(self.node, Node)
            if not isinstance(node, NodePool):
                node_pool = NodePool()
                node_pool.nodes.append(node)
            else:
                node_pool = node

            real_action.node_pool = node_pool.actualized

        if hasattr(self, "requires"):
            self._apply_requirements(real_action, self.requires)


class CleanupAction(Action):
    yaml_tag = u'!CleanupAction'
    actual_class = action.Action

    def _validate(self):
        if (hasattr(self, 'name') and
            self.name is not None and
            self.name != CLEANUP_ACTION_NAME):
            raise ConfigError("Cleanup actions cannot have custom names (you"
                              " wanted %s)" % self.name)

        self.name = CLEANUP_ACTION_NAME

        if not getattr(self, 'command', None):
            raise ConfigError("Missing value in action %s %r" % (key, self),
                              self.line_number)

        if hasattr(self, 'requires'):
            raise ConfigError("Cleanup actions cannot have dependencies")


class NodePool(_ConfiguredObject):
    yaml_tag = u'!NodePool'
    actual_class = node.NodePool

    def __init__(self, *args, **kwargs):
        super(NodePool, self).__init__(*args, **kwargs)
        self.nodes = []

    def _apply(self):
        real_node_pool = self._ref()
        for node in self.nodes:
            actual_node = default_or_from_tag(node, Node).actualized
            real_node_pool.nodes.append(actual_node)


class Node(_ConfiguredObject):
    yaml_tag = u'!Node'
    actual_class = node.Node

    def _apply(self):
        real_node = self._ref()
        real_node.hostname = self.hostname


class NodeResource(yaml.YAMLObject):
    yaml_tag = u'!NodeResource'


class ActionResource(yaml.YAMLObject):
    yaml_tag = u'!ActionResource'


class FileResource(yaml.YAMLObject):
    yaml_tag = u'!FileResource'


class Scheduler(object):
    @classmethod
    def from_string(self, scheduler_str):
        scheduler_args = scheduler_str.split()

        scheduler_name = scheduler_args.pop(0)

        if scheduler_name == "constant":
            return ConstantScheduler().actualized
        if scheduler_name == "daily":
            return DailyScheduler(*scheduler_args).actualized
        if scheduler_name == "interval":
            return IntervalScheduler(''.join(scheduler_args)).actualized

        m = scheduler.GROC_SCHEDULE_RE.match(scheduler_str.lower())
        if m:
            return GrocScheduler(scheduler_str).actualized

        raise ConfigError("Unknown scheduler %r" % scheduler_str)


class ConstantScheduler(_ConfiguredObject):
    yaml_tab = u'!ConstantScheduler'
    actual_class = scheduler.ConstantScheduler

    def _apply(self):
        sched = self._ref()


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': dict(hours=1),
}

# Translations from possible configuration units to the argument to
# datetime.timedelta
TIME_INTERVAL_UNITS = {
    'months': ['month', 'months'],
    'days': ['d', 'day', 'days'],
    'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
    'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
    'seconds': ['s', 'sec', 'secs', 'second', 'seconds']
}


class IntervalScheduler(_ConfiguredObject):
    yaml_tag = u'!IntervalScheduler'
    actual_class = scheduler.IntervalScheduler

    def __init__(self, *args, **kwargs):
        if len(args) > 0:
            self.interval = args[0]

        super(IntervalScheduler, self).__init__(*args, **kwargs)

    def _apply(self):
        sched = self._ref()

        # Now let's figure out the interval
        if self.interval in TIME_INTERVAL_SHORTCUTS:
            kwargs = TIME_INTERVAL_SHORTCUTS[self.interval]
        else:
            # We want to split out digits and characters into tokens
            interval_re = re.compile(r"\d+|[a-zA-Z]+")
            interval_tokens = interval_re.findall(self.interval)
            if len(interval_tokens) != 2:
                raise Error("Invalid interval specification: %r",
                            self.interval)

            value, units = interval_tokens

            kwargs = {}
            for key, unit_set in TIME_INTERVAL_UNITS.iteritems():
                if units in unit_set:
                    kwargs[key] = int(value)
                    break
            else:
                raise Error("Invalid interval specification: %r",
                            self.interval)

        sched.interval = datetime.timedelta(**kwargs)


class DailyScheduler(_ConfiguredObject):
    yaml_tag = u'!DailyScheduler'
    actual_class = scheduler.GrocScheduler

    def __init__(self, *args, **kwargs):

        if len(args) > 0:
            self.start_time = args[0]

        if len(args) > 1:
            self.days = args[1]
        if 'days' in kwargs:
            self.days = kwargs['days']

        super(DailyScheduler, self).__init__(*args, **kwargs)

    def _apply(self):
        sched = self._ref()

        err_msg = ("Start time must be in string format HH:MM[:SS]. Seconds"
                   " are ignored but parsed so as to be backward-compatible.")

        if hasattr(self, 'start_time'):
            if not isinstance(self.start_time, basestring):
                raise ConfigError(err_msg)

            # GrocScheduler will parse the values from the string.
            # However, it only accepts HH:MM, not HH:MM:SS, so we truncate
            # here if necessary.
            hms = self.start_time.strip().split(':')

            if len(hms) < 2:
                raise ConfigError(err_msg)

            sched.timestr = ':'.join(hms)

        if hasattr(self, 'days'):
            sched.parse_legacy_days(self.days)


class GrocScheduler(_ConfiguredObject):
    yaml_tag = u'!GrocScheduler'
    actual_class = scheduler.GrocScheduler

    def __init__(self, *args, **kwargs):
        self.expr = args[0]
        super(GrocScheduler, self).__init__(*args, **kwargs)

    def _apply(self):
        sched = self._ref()
        sched.parse(self.expr)


def load_config(config_file):
    """docstring for load_config"""
    config = yaml.load(config_file)
    if not isinstance(config, TronConfiguration):
        raise ConfigError("Failed to find a configuration document in"
                          " specified file")

    return config


def configure_daemon(path, daemon):
    config = load_config(path)
    config.apply(daemon)
