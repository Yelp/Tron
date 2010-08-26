import sys
import re
import logging
import weakref
import datetime
import os

import yaml
from twisted.conch.client import options

from tron import action, job, node, scheduler, monitor, emailer

log = logging.getLogger("tron.config")

class Error(Exception):
    pass
    
class ConfigError(Exception):
    pass
    
class _ConfiguredObject(yaml.YAMLObject):
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
                raise yaml.YAMLError("Job %s is previously defined" % nex.name)
            dic[nex.name] = 1
            return dic
         
        found_jobs = reduce(check_dup, self.jobs, {})

        for job_config in self.jobs:
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
        
        self._apply_jobs(mcp)
        if hasattr(self, 'ssh_options'):
            self.ssh_options._apply(mcp)
        
        if hasattr(self, 'notification_options'):
            self.notification_options._apply(mcp)

class SSHOptions(yaml.YAMLObject):
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


class NotificationOptions(yaml.YAMLObject):
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

    def _apply(self):
        real_job = self._ref()
        real_job.name = self.name
        
        if not re.match(r'[a-z_]\w*$', self.name, re.I):
            raise yaml.YAMLError("Invalid job name '%s' - not a valid identifier" % self.name)

        if hasattr(self, "node"):
            real_job.node_pool = self.node.actualized

        # Build scheduler
        if hasattr(self, "schedule"):
            if isinstance(self.schedule, basestring):
                # This is a short string
                real_job.scheduler = Scheduler.from_string(self.schedule)
            else:
                # This is a scheduler instance, which has more info
                real_job.scheduler = self.schedule.actualized
            
            real_job.scheduler.job_setup(real_job)

        if hasattr(self, "queueing"):
            real_job.queueing = self.queueing

        if hasattr(self, "run_limit"):
            real_job.run_limit = self.run_limit

        if hasattr(self, "all_nodes"):
            real_job.all_nodes = self.all_nodes

        for a_config in self.actions:
            action = a_config.actualized
            real_job.topo_actions.append(action)
            action.job = real_job
            
            if not real_job.node_pool and not action.node_pool:
                raise yaml.YAMLError("Either job '%s' or its action '%s' must have a node" 
                % (real_job.name, action.name))

class Action(_ConfiguredObject):
    yaml_tag = u'!Action'
    actual_class = action.Action
    
    def _apply_requirements(self, real_action, requirements):
        if hasattr(requirements, '__iter__'):
            for req in requirements:
                real_action.required_actions.append(req.actualized)
        else:
            real_action.required_actions.append(requirements.actualized)

    def _apply(self):
        """Configured the specific action instance"""
        real_action = self._ref()
        if not re.match(r'[a-z_]\w*$', self.name, re.I):
            raise yaml.YAMLError("Invalid action name '%s' - not a valid identifier" % self.name)

        real_action.name = self.name
        real_action.command = self.command
        real_action.node_pool = self.node.actualized if hasattr(self, "node") else None

        if hasattr(self, "requires"):
            self._apply_requirements(real_action, self.requires)

class NodePool(_ConfiguredObject):
    yaml_tag = u'!NodePool'
    actual_class = node.NodePool
    
    def _apply(self):
        real_node_pool = self._ref()
        for name in self.hostnames:
            real_node_pool.nodes.append(node.Node(name))
        
        
class Node(_ConfiguredObject):
    yaml_tag = u'!Node'
    actual_class = node.NodePool
    
    def _apply(self):
        real_node = self._ref()
        real_node.nodes.append(node.Node(self.hostname))
 

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

