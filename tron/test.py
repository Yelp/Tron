
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

config = yaml.load(open("trond.yaml", 'r'))
#print config
# print config.bill_to
# print config.ship_to
#print config.bill_to is config.ship_to
print config['jobs'][0].schedule.interval
print config['jobs'][0].dependencies

