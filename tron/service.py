from tron import job
from tron import action

from twisted.internet import reactor

class ServiceInstance(object):
    def __init__(self, service, node, instance_number):
        self.service = service
        self.instance_number = instance_number
        self.id = "%s.%s" % (service.name, self.instance_number)
        
        self.pid_url = None

        self.node = None
        self.restarts = 0
        self.context = command_context.CommandContext(self, service.context)
        
        self.monitor_action = None
        self.start_action = None
        self.kill_action = None
        
    def create_action_run(self, act):
        act_run = act.build_run(self.context)

        act_run.id = "%s.%s" % (self.id, act.name)
        act_run.state_callback = self.state_callback
        act_run.node = self.node

    def _queue_monitor(self):
        self.monitor_action = None
        reactor.callLater(self._build_monitor, self.monitor_interval)

    def _build_monitor(self):
        if self.monitor_action:
            log.warning("Monitor action already exists, old callLater ?")
            return
            
        monitor_command = "cat %(pid_url)s | xargs kill -0" % self.context

        self.monitor_action = action.ActionComand("%s.monitor" % self.id, monitor_command)
        self.monitor_action.machine.listen(action.ActionCommand.COMPLETE, self._monitor_complete_callback)
        self.monitor_action.machine.listen(action.ActionCommand.FAILSTART, self._monitor_complete_failstart)

        self.node.run(self.monitor_action)

    def _monitor_complete_callback(self):
        """Callback when our monitor has completed"""
        assert self.monitor_action
        self.last_check = timeutils.current_time()
        
        if self.monitor_action.exit_status != 0:
            self._mark_down()
        else:
            self._build_monitor()

    def _monitor_complete_failstart(self):
        """Callback when our monitor failed to even start"""
        self._mark_down()

    def _mark_down(self):
        raise NotImplementedError()

    def start(self):
        pass

    def stop(self):
        pass

    @property
    def data(self):
        # We're going to need to keep track of stuff like pid_file
        raise NotImplementedError()

    def restore(self, data):
        raise NotImplementedError()


class Service(object):
    def __init__(self, name=None, node_pool=None, context=None):
        self.name = name

        self.scheduler = None
        self.node_pool = None
        self.enabled = False
        self.count = 0
        self.restarts = 0

        # Last instance number used
        self._last_instance = None

        self.context = context #command_context.CommandContext(self)
        self.instances = []

    def enable(self):
        self.enabled = True
        self.start()

    def disable(self):
        self.enabled = False

    def start(self):
        for i in self.instances:
            i.start()

    def stop(self):
        for i in self.instances:
            i.stop()

    def build_instance(self, instance_number):
        node = self.node_pool.next()

        if self._last_instance is None:
            self._last_instance = 0
        else:
            self._last_instance += 1

        instance_number = self._last_instance

        service_instance = ServiceInstance(self, node, instance_number, pid_file_template=self.pid_file, context=self.context)
        service_instance.restarts = self.restarts
        
        self.instances.append(service_instance)
        return service_instance
        
    @property
    def data(self):
        raise NotImplementedError()
    
    def restore(self, data):
        raise NotImplementedError()