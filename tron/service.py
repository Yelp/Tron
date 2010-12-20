from tron import job

class ServiceInstance(object):
    def __init__(self, service, inst_num, node_pool):
        self.instance_num = inst_num
        self.service = service
        self.pid_url = None
        self.state_callback = service.state_callback
        self.id = "%s.%s" % (service.name, self.instance_num)

        self.enabled = False
        self.node = None
        self.context = command_context.CommandContext(self, service.context)
        self.restarts = deque()
    
        self.monitor_action = Action("check")
        self.monitor_aciton.node_pool
        self.monitor_action.command = "cat %(pid_url)s | xargs kill -0"
        self.kill_action = Action("kill")
        self.kill_action.command = "cat %(pid_url)s | xargs kill -1"

    def create_action_run(self, act):
        act_run = act.build_run(self.context)

        act_run.id = "%s.%s" % (self.id, act.name)
        act_run.state_callback = self.state_callback
        act_run.node = self.node


    def enable(self):
        Service.start_action.build_run()

    def disable(self):
        self.kill_action.build_run().start()

    def is_running(self):
        run = Service.monitor_action.build_run()
        run.start()


class Service(object):
    def __init__(self, name=None, context=None):
        self.name = name
        self.monitor = action
        self.scheduler = None
        self.start_action = None

        self.count = 1
        self.state_callback = lambda:None
        self.context = command_context.CommandContext(self)
        self.instances = []

    def enable(self):
        for i in self.instances:
            i.enable()

    def disable(self):
        for i in self.instances:
            i.disable()

