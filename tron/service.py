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
        self.fails = deque()
        
        self.start_action = None
        self.check_action = Action("get_pid")
        self.check_action.command = "cat %(pid_url)s"
        self.kill_action = Action("kill")
        self.kill_action.command = "kill -1 %(pid)s"

    def enable(self):
        self.start_action.next_run().start()

    def disable(self):
        self.kill_action.next_run().start()

    def is_running(self):
        run = self.check_action.next_run()
        run.start()


class Service(object):
    def __init__(self, name=None, action=None, context=None):
        self.name = name
        self.monitor = action
        self.scheduler = None

        self.count = None
        self.state_callback = lambda:None
        self.context = command_context.CommandContext(self)
        self.instances = []

    def enable(self):
        for i in self.instances:
            i.enable()

    def disable(self):
        pass
