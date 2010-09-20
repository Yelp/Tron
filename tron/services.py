from tron import job

class ServiceInstance(object):
    def __init__(self, service, inst_num):
        self.instance_num = inst_num
        self.service = service
        self.state_callback = service.state_callback
        self.id = "%s.%s" % (service.name, self.instance_num)

        self.enabled = False
        self.node = None
        self.context = command_context.CommandContext(self, service.context)



class Service(object):
    def __init__(self, name=None, action=None, context=None):
        self.name = name
        self.monitor = action
        self.scheduler = None
        self.fails = deque()

        self.count = None
        self.state_callback = lambda:None
        self.context = command_context.CommandContext(self)

    
