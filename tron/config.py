import sys

import yaml

class TronConfiguration(yaml.YAMLObject):
    yaml_tag = u'!TronConfiguration'

class BatchJob(yaml.YAMLObject):
    yaml_tag = u'!BatchJob'

    
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