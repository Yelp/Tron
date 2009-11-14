import sys
import re
import logging
import weakref
import datetime

import yaml

from tron import job, node, scheduler

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
                mcp.jobs[new_job.name] = new_job
            else:
                log.debug("Updating existing job %s", existing_job.name)
                job_config.update(existing_job)

        for job_name in mcp.jobs.iterkeys():
            if job_name not in found_jobs:
                log.debug("Removing job %s", job_name)
                dead_job = mcp.jobs[job_name]
                mcp.jobs.remove(dead_job)

    def apply(self, mcp):
        """Apply the configuration to the specified master control program"""
        self._apply_jobs(mcp)

class Job(_ConfiguredObject):
    yaml_tag = u'!Job'
    actual_class = job.Job
    
    def _apply(self):
        """Configured the specific job instance"""
        real_job = self._ref()
        real_job.name = self.name
        real_job.command = self.command
        
        # Set the node
        if self.node:
            real_job.node = self.node.actualized

        # Build scheduler
        if isinstance(self.schedule, basestring):
            # This is a short string
            real_job.scheduler = Scheduler.from_string(self.schedule)
        else:
            # This is a scheduler instance, which has more info
            real_job.scheduler = self.schedule.actualized

        # Setup dependencies

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
        else:
            raise ValueError("Unknown scheduler %r", scheduler_name)

class ConstantScheduler(_ConfiguredObject):
    yaml_tab = u'!ConstantScheduler'
    actual_class = scheduler.ConstantScheduler


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

class Error(Exception):
    pass

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

