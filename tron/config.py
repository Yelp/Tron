import sys
import logging
import weakref

import yaml

log = logging.getLogger("tron.config")

class TronConfiguration(yaml.YAMLObject):
    yaml_tag = u'!TronConfiguration'

    def _apply_jobs(self, daemon):
        """Configure jobs"""
        found_jobs = []
        for job_config in self.jobs:
            found_jobs.append(job_config.name)
            existing_job = daemon.jobs.get(job_config.name)
            if existing_job is None:
                # Create a new one
                job = job.Job()
                job_config.apply(job)

                daemon.jobs[job.name] = job
            else:
                job_config.apply(existing_job)
                existing_job.configure(job_config)

        for job_name daemon.jobs.iterkeys():
            if job_name not in found_jobs:
                dead_job = daemon.jobs[job_name]
                daemon.jobs.remove(dead_job)

    def apply(self, daemon):
        """Apply the configuration to the specified daemon"""
        self._apply_jobs(daemon)

class Job(yaml.YAMLObject):
    yaml_tag = u'!Job'
    
    def apply(self, job):
        """Configured the specific job instance"""
        # We're going keep track of this job because someone else might need to reference it.
        self.applied_job_ref = weakref.ref(job)

        job.name = self.name
        job.path = self.path
        
        # Set the node
        if self.node:
            job.node = self.node.applied_node_ref()

        # Build scheduler
        
        # Setup dependencies

class Node(yaml.YAMLObject):
    yaml_tag = u'!Node'

class NodeResource(yaml.YAMLObject):
    yaml_tag = u'!NodeResource'

class JobResource(yaml.YAMLObject):
    yaml_tag = u'!JobResource'

class FileResource(yaml.YAMLObject):
    yaml_tag = u'!FileResource'

class IntervalScheduler(yaml.YAMLObject):
    yaml_tag = u'!IntervalScheduler'

class DailyScheduler(yaml.YAMLObject):
    yaml_tag = u'!DailyScheduler'


class Error(Exception):
    pass

class InvalidConfigError(Error): pass

def load_config(path):
    """docstring for load_config"""
    config_file = open(path, 'r')
    config = yaml.load(config_file)
    if not isinstance(config, TronConfiguration):
        raise InvalidConfigError("Failed to find a configuration document in specified file")
    
    return config

def configure_daemon(path, daemon):
    config = load_config(path)
    config.apply(daemon)

def main():
    """docstring for main"""
    if len(sys.argv) < 2:
        print >>sys.stderr, "Usage: %s <config file>" % (sys.argv[0])
        sys.exit(1)
        
    conf = load_config(sys.argv[1])
    print "Loaded %d nodes" % (len(conf.nodes))
    print "Loaded %d resources" % (len(conf.resources))
    print "Loaded %d jobs" % (len(conf.jobs))
    
if __name__ == '__main__':
    main()