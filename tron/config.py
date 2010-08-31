import sys
import re
import logging
import weakref
import datetime
import os

import yaml
from twisted.conch.client import options

from tron import action, job, node, scheduler, monitor, emailer, command_context

log = logging.getLogger("tron.config")

class Error(Exception):
    pass
    
class ConfigError(Exception):
    pass

class FromDictBuilderMixin(object):
    """Mixin class for building YAMLObjects from dictionaries"""
    @classmethod
    def from_dict(cls, obj_dict):
        # We just assume we want to make all the dictionary values into attributes
        new_obj = cls()
        new_obj.__dict__.update(obj_dict)
        return new_obj


def default_or_from_tag(value, cls):
    """Construct a YAMLObject unless it already is one
    
    We use this for providing default config types so it isn't required to "tag" everything with !MyConfigClass
    """
    if not isinstance(value, yaml.YAMLObject):
        return cls.from_dict(value)
    return value

    
class _ConfiguredObject(yaml.YAMLObject, FromDictBuilderMixin):
    """Base class for common configured objects where the configuration generates one actualized 
    object in the app that may be referenced by other objects."""
    actual_class = None     # Redefined to indicate the type of object this configuration will build

    def _apply(self):
        raise NotImplementedError

    def update(self, obj):
        self._ref = weakref.ref(obj)
        self._apply()
        
    def _build(self):
        return self.actual_class()

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return -1

        our_dict = [(key, value) for key, value in self.__dict__.iteritems() if not key.startswith('_')]
        other_dict = [(key, value) for key, value in other.__dict__.iteritems() if not key.startswith('_')]
        
        c = cmp(our_dict, other_dict)
        print c, our_dict, other_dict
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

class TronConfiguration(yaml.YAMLObject):
    yaml_tag = u'!TronConfiguration'

    def _apply_jobs(self, mcp):
        """Configure actions"""
        found_jobs = []

        # Check for duplicates before we start editing jobs
        def check_dup(dic, nex):
            if nex.name in dic:
                raise yaml.YAMLError("%s is previously defined" % nex.name)
            dic[nex.name] = 1
            return dic
        
        jobs = [default_or_from_tag(job_val, Job) for job_val in getattr(self, 'jobs', [])]
        jobs.extend([default_or_from_tag(job_val, Service) for job_val in getattr(self, 'services', [])])

        found_jobs = reduce(check_dup, jobs, {})
        for job_config in jobs:
            new_job = job_config.actualized
            log.debug("Building new job %s", job_config.name)
            mcp.add_job(new_job)

        for job_name in mcp.jobs.keys():
            if job_name not in found_jobs:
                log.debug("Removing job %s", job_name)
                del mcp.jobs[job_name]

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
            raise ConfigError("Specified working directory \'%s\' is not a directory" % working_dir)
        if not os.access(working_dir, os.W_OK):
            raise ConfigError("Specified working directory \'%s\' is not writable" % working_dir)
        
        mcp.state_handler.working_dir = working_dir

        if hasattr(self, 'command_context'):
            mcp.context = command_context.CommandContext(self.command_context)
        
        self._apply_jobs(mcp)

        if hasattr(self, 'ssh_options'):
            self.ssh_options = default_or_from_tag(self.ssh_options, SSHOptions)
            self.ssh_options._apply(mcp)
        
        if hasattr(self, 'notification_options'):
            self.notification_options = default_or_from_tag(self.notification_options, NotificationOptions)
            self.notification_options._apply(mcp)
        

class SSHOptions(yaml.YAMLObject, FromDictBuilderMixin):
    yaml_tag = u'!SSHOptions'
    
    def _build_conch_options(self):
        """Verify and construct the ssh (conch) option object
        
        This is just a dictionary like object that options the twisted ssh implementation uses.
        """
        ssh_options = options.ConchOptions()
        if not self.agent:
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
                    raise Error("Private key file %s doesn't exist" % file_name)
                if not os.path.exists(file_path + ".pub"):
                    raise Error("Public key %s doesn't exist" % (file_name + ".pub"))
            
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
        
        em = emailer.Emailer(self.smtp_host, self.notification_addr)
        mcp.monitor = monitor.CrashReporter(em)
        mcp.monitor.start()

 
class Job(_ConfiguredObject):
    yaml_tag = u'!Job'
    actual_class = job.Job

    def _match_name(self, real, name):
        real.name = name
        
        if not re.match(r'[a-z_]\w*$', name, re.I):
            raise yaml.YAMLError("Invalid job name '%s' - not a valid identifier" % self.name)

    def _match_schedule(self, real, schedule):
        if isinstance(schedule, basestring):
            # This is a short string
            real.scheduler = Scheduler.from_string(schedule)
        else:
            # This is a scheduler instance, which has more info
            real.scheduler = schedule.actualized
        real.scheduler.job_setup(real)

    def _create_action(self, real, act_conf):
        action = act_conf.actualized
        action.job = real
        
        if not real.node_pool and not action.node_pool:
            raise yaml.YAMLError("Either job '%s' or its action '%s' must have a node" 
               % (real.name, action.name))

        return action

    def _match_actions(self, real, actions):
        for act_conf in actions:
            action = self._create_action(real, act_conf)
            real.topo_actions.append(action)
                    
    def _apply(self):
        real_job = self._ref()
        node = default_or_from_tag(self.node, Node)

        if not isinstance(node, NodePool):
            node_pool = NodePool()
            node_pool.nodes.append(node)
        else:
            node_pool = node
            
        real_job.node_pool = node_pool.actualized
        
        self._match_name(real_job, self.name)
        self._match_schedule(real_job, self.schedule)
        actions = [default_or_from_tag(action, Action) for action in self.actions]
        self._match_actions(real_job, actions)

        if hasattr(self, "queueing"):
            real_job.queueing = self.queueing

        if hasattr(self, "run_limit"):
            real_job.run_limit = self.run_limit

        if hasattr(self, "all_nodes"):
            real_job.all_nodes = self.all_nodes


class Service(Job):
    yaml_tag = u'!Service'
    actual_class = job.Job

    def _apply(self):
        real_service = self._ref()
        real_service.node_pool = self.node.actualized
        
        self._match_name(real_service, self.name)
        self._match_schedule(real_service, self.monitor['schedule'])
        self._match_actions(real_service, self.monitor['actions'])

        if hasattr(self, "enable"):
            real_service.enable_act = self._create_action(real_service, self.enable)
        
        if hasattr(self, "disable"):
            real_service.disable_act = self._create_action(real_service, self.disable)


class Action(_ConfiguredObject):
    yaml_tag = u'!Action'
    actual_class = action.Action
    
    def _apply_requirements(self, real_action, requirements):
        if not isinstance(requirements, list):
            requirements = [requirements]
            
        requirements = [default_or_from_tag(req, Action) for req in requirements]
        for req in requirements:
            real_action.required_actions.append(req.actualized)

    def _apply(self):
        """Configured the specific action instance"""
        real_action = self._ref()
        if not re.match(r'[a-z_]\w*$', self.name, re.I):
            raise yaml.YAMLError("Invalid action name '%s' - not a valid identifier" % self.name)

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


class NodePool(_ConfiguredObject):
    yaml_tag = u'!NodePool'
    actual_class = node.NodePool
    def __init__(self, *args, **kwargs):
        super(NodePool, self).__init__(*args, **kwargs)
        self.nodes = []
    def _apply(self):
        real_node_pool = self._ref()
        for node in self.nodes:
            real_node_pool.nodes.append(default_or_from_tag(node, Node).actualized)
        
        
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
    def from_string(self, scheduler_name):
        if scheduler_name == "constant":
            return ConstantScheduler().actualized
        if scheduler_name == "daily":
            return DailyScheduler().actualized
        if scheduler_name == "weekly":
            return scheduler.DailyScheduler(days=7)
        return scheduler.DailyScheduler(days=scheduler_name)


class ConstantScheduler(_ConfiguredObject):
    yaml_tab = u'!ConstantScheduler'
    actual_class = scheduler.ConstantScheduler
    
    def _apply(self):
        sched = self._ref()


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': dict(hours=1),
}

# Translations from possible configuration units to the argument to datetime.timedelta
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
    
    def _apply(self):
        sched = self._ref()
        
        # Now let's figure out the interval
        if self.interval in TIME_INTERVAL_SHORTCUTS:
            kwargs = TIME_INTERVAL_SHORTCUTS[self.interval]
        else:
            # We want to split out digits and characters into tokens
            interval_tokens = re.compile(r"\d+|[a-zA-Z]+").findall(self.interval)
            if len(interval_tokens) != 2:
                raise Error("Invalid interval specification: %r", self.interval)

            value, units = interval_tokens
        
            kwargs = {}
            for key, unit_set in TIME_INTERVAL_UNITS.iteritems():
                if units in unit_set:
                    kwargs[key] = int(value)
                    break
            else:
                raise Error("Invalid interval specification: %r", self.interval)
                
        sched.interval = datetime.timedelta(**kwargs)
        

class DailyScheduler(_ConfiguredObject):
    yaml_tag = u'!DailyScheduler'
    actual_class = scheduler.DailyScheduler

    def _apply(self):
        sched = self._ref()

        if hasattr(self, 'start_time'):
            if not isinstance(self.start_time, basestring):
                raise ConfigError("Start time must be in string format HH:MM:SS")

            hour, minute, second = [int(val) for val in self.start_time.strip().split(':')]
            sched.start_time = datetime.time(hour=hour, minute=minute, second=second)

        if hasattr(self, 'days'):
            sched.wait_days = sched.get_daily_waits(self.days)

def load_config(config_file):
    """docstring for load_config"""
    config = yaml.load(config_file)
    if not isinstance(config, TronConfiguration):
        raise ConfigError("Failed to find a configuration document in specified file")
    
    return config


def configure_daemon(path, daemon):
    config = load_config(path)
    config.apply(daemon)

