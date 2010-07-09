import sys
import re
import logging
import weakref
import datetime
import os
import os.path

import yaml
from twisted.conch.client import options

from tron import job, node, scheduler, monitor, emailer

log = logging.getLogger("tron.config")

class Error(Exception):
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
        """Configure jobs"""
        found_jobs = []
        for job_config in self.jobs:
            found_jobs.append(job_config.name)
            existing_job = mcp.jobs.get(job_config.name)
            if existing_job is None:
                log.debug("Building new job %s", job_config.name)
                # Create a new one
                new_job = job_config.actualized
                mcp.add_job(new_job)
            else:
                log.debug("Updating existing job %s", existing_job.name)
                job_config.update(existing_job)

        for job_name in mcp.jobs.iterkeys():
            if job_name not in found_jobs:
                log.debug("Removing job %s", job_name)
                dead_job = mcp.jobs[job_name]
                mcp.jobs.remove(dead_job)
    
    def _get_state_dir(self, mcp):
        if mcp.state_handler.state_dir:
            return mcp.state_handler.state_dir
        if hasattr(self, 'state_dir'):
            return self.state_dir
        if 'TMPDIR' in os.environ:
            return os.environ['TMPDIR']
        return '/tmp'
    
    def apply(self, mcp):
        """Apply the configuration to the specified master control program"""
        self._apply_jobs(mcp)
        if hasattr(self, 'ssh_options'):
            self.ssh_options._apply(mcp)
        
        if hasattr(self, 'notification_options'):
            self.notification_options._apply(mcp)

        mcp.state_handler.state_dir = self._get_state_dir(mcp)

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
        """Configured the specific job instance"""
        real_job = self._ref()
        real_job.name = self.name
        real_job.command = self.command
        real_job.dependants = []
        real_job.output_dir = self.output_dir if hasattr(self, "output_dir") else None

        # Set the node
        real_job.node = self.node.actualized if hasattr(self, "node") else None

        if not hasattr(self, "schedule") and not hasattr(self, "follow_on_success"):
            raise Error("Job configuration needs a schedule or follow_on_success option")
        
        if hasattr(self, "follow_on_success"):
            real_job.depend = self.follow_on_success.actualized
            self.follow_on_success.actualized.dependants.append(real_job)
       
        # Build scheduler
        if hasattr(self, "schedule"):
            if isinstance(self.schedule, basestring):
                # This is a short string
                real_job.scheduler = Scheduler.from_string(self.schedule)
            else:
                # This is a scheduler instance, which has more info
                real_job.scheduler = self.schedule.actualized

            real_job.scheduler.set_job_queueing(real_job)

        if hasattr(self, "queueing_enabled"):
            real_job.queueing = self.queueing_enabled


class Node(_ConfiguredObject):
    yaml_tag = u'!Node'
    actual_class = node.Node
    
    def _apply(self):
        real_node = self._ref()
        real_node.hostname = self.hostname
        

class NodeResource(yaml.YAMLObject):
    yaml_tag = u'!NodeResource'


class JobResource(yaml.YAMLObject):
    yaml_tag = u'!JobResource'


class FileResource(yaml.YAMLObject):
    yaml_tag = u'!FileResource'


class Scheduler(object):
    @classmethod
    def from_string(self, scheduler_name):
        if scheduler_name == "daily":
            return DailyScheduler().actualized
        elif scheduler_name == "constant":
            return ConstantScheduler().actualized
        else:
            raise ValueError("Unknown scheduler %r", scheduler_name)


class ConstantScheduler(_ConfiguredObject):
    yaml_tab = u'!ConstantScheduler'
    actual_class = scheduler.ConstantScheduler
    
    def _apply(self):
        sched = self._ref()


# Shortcut values for intervals
TIME_INTERVAL_SHORTCUTS = {
    'hourly': dict(hours=1),
    'weekly': dict(days=7),
    'daily': dict(days=1),
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

class InvalidConfigError(Error): pass


def load_config(config_file):
    """docstring for load_config"""
    config = yaml.load(config_file)
    if not isinstance(config, TronConfiguration):
        raise InvalidConfigError("Failed to find a configuration document in specified file")
    
    return config


def configure_daemon(path, daemon):
    config = load_config(path)
    config.apply(daemon)

