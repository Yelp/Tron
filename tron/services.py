from tron import job

class ServiceInstance(object):
    def __init__(self, service, inst_num):
        self.instance_num = inst_num
        self.service = service
        self.pid_url = None
        self.state_callback = service.state_callback
        self.id = "%s.%s" % (service.name, self.instance_num)

        self.enabled = False
        self.node = None
        self.context = command_context.CommandContext(self, service.context)
        
        self.start_action = None
        self.get_pid_action = Action("get_pid")
        self.get_pid_action.command = "cat %(pid_url)s"
        self.kill_action = Action("kill")
        self.kill_action.command = "kill -1 %(pid)s"

    def enable(self):
        if not self.is_running():
            self.start_action.next_run().start()

    def disable(self):
        if self.is_running():
            self.kill_action.next_run().start()

    def get_pid(self):
        pass

    def kill(self):
        pass


class Service(object):
    def __init__(self, name=None, action=None, context=None):
        self.name = name
        self.monitor = action
        self.scheduler = None
        self.fails = deque()

        self.count = None
        self.state_callback = lambda:None
        self.context = command_context.CommandContext(self)
        self.instances = []

    def enable(self):
        for i in self.instances:
            i.enable()

    def disable(self):
        pass
