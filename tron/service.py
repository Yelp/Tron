import logging

from twisted.internet import reactor

from tron import job
from tron import action
from tron import command_context
from tron.utils import state
from tron.utils import timeutils

log = logging.getLogger(__name__)

class Error(Exception): pass

class ServiceInstance(object):
    STATE_DOWN = state.NamedEventState("down")
    STATE_UP = state.NamedEventState("up")
    STATE_KILLING = state.NamedEventState("killing", mark_down=STATE_DOWN)
    STATE_MONITORING = state.NamedEventState("monitoring", mark_down=STATE_DOWN, stop=STATE_KILLING, mark_up=STATE_UP)
    STATE_STARTING = state.NamedEventState("starting", mark_down=STATE_DOWN, monitor=STATE_MONITORING, stop=STATE_KILLING)

    STATE_UNKNOWN = state.NamedEventState("unknown", mark_monitor=STATE_MONITORING)
    STATE_MONITORING['monitor_fail'] = STATE_UNKNOWN

    STATE_UP['stop'] = STATE_KILLING
    STATE_UP['monitor'] = STATE_MONITORING
    STATE_DOWN['start'] = STATE_STARTING
    
    def __init__(self, service, node, instance_number):
        self.service = service
        self.instance_number = instance_number
        self.node = node

        self.id = "%s.%s" % (service.name, self.instance_number)
        
        self.machine = state.StateMachine(ServiceInstance.STATE_DOWN)
        
        self.pid_path = None

        self.context = command_context.CommandContext(self, service.context)
        
        self.monitor_action = None
        self.start_action = None
        self.kill_action = None
 
    @property
    def state(self):
        return self.machine.state

    @property
    def listen(self):
        return self.machine.listen
    
    @property
    def pid_file(self):
        if self.service.pid_file_template:
            try:
                return self.service.pid_file_template % self.context
            except KeyError:
                log.error("Failed to render pid file template: %r" % self.service.pid_file_template)
        else:
            log.warning("No pid_file configured for service %s", self.service.name)

        return None
    
    @property
    def command(self):
        try:
            return self.service.command % self.context
        except KeyError:
            log.error("Failed to render service command for service %s: %s", self.service.name, self.service.command)

        return None
    
    def _queue_monitor(self):
        self.monitor_action = None
        if self.service.monitor_interval > 0:
            reactor.callLater(self.service.monitor_interval, self._run_monitor)

    def _run_monitor(self):
        if self.monitor_action:
            log.warning("Monitor action already exists, old callLater ?")
            return
        
        self.machine.transition("monitor")
        pid_file = self.pid_file
        
        if pid_file is None:
            # If our pid file doesn't exist or failed to be generated, we can't really monitor
            self._monitor_complete_failstart()
            return
        
        monitor_command = "cat %(pid_file)s | xargs kill -0" % self.context

        self.monitor_action = action.ActionCommand("%s.monitor" % self.id, monitor_command)
        self.monitor_action.machine.listen(action.ActionCommand.COMPLETE, self._monitor_complete_callback)
        self.monitor_action.machine.listen(action.ActionCommand.FAILSTART, self._monitor_complete_failstart)

        self.node.run(self.monitor_action)
        # TODO: Need a timer on this in case the monitor hangs

    def _monitor_complete_callback(self):
        """Callback when our monitor has completed"""
        assert self.monitor_action
        self.last_check = timeutils.current_time()
        log.debug("Monitor callback with exit %r", self.monitor_action.exit_status)
        if self.monitor_action.exit_status != 0:
            self.machine.transition("mark_down")
        else:
            self.machine.transition("mark_up")
            self._queue_monitor()

        self.monitor_action = None

    def _monitor_complete_failstart(self):
        """Callback when our monitor failed to even start"""
        self.machine.transition("monitor_fail")
        self._queue_monitor()

        self.monitor_action = None
        
    def start(self):
        if self.machine.state != self.STATE_DOWN:
            return

        self.machine.transition("start")

        command = self.command
        if command is None:
            self._start_failstart()
            return

        self.start_action = action.ActionCommand("%s.start" % self.id, command)
        self.start_action.machine.listen(action.ActionCommand.COMPLETE, self._start_complete_callback)
        self.start_action.machine.listen(action.ActionCommand.FAILSTART, self._start_complete_failstart)

        self.node.run(self.start_action)
    
    def _start_complete_callback(self):
        if self.start_action.exit_status != 0:
            self.machine.transition("mark_down")
        else:
            self._queue_monitor()

        self.start_action = None

    def _start_complete_failstart(self):
        log.warning("Failed to start service %s (%s)", self.id, self.node.hostname)
        self.machine.transition("mark_down")
        self.start_action = None

    def stop(self):
        if self.machine.state != self.STATE_UP:
            return
        
        self.machine.transition("stop")

        pid_file = self.pid_file
        if pid_file is None:
            # If our pid file doesn't exist or failed to be generated, we can't really monitor
            self._stop_complete_failstart()
            return
        
        kill_command = "cat %(pid_file)s | xargs kill" % self.context

        self.stop_action = action.ActionCommand("%s.stop" % self.id, kill_command)
        self.stop_action.machine.listen(action.ActionCommand.COMPLETE, self._stop_complete_callback)
        self.stop_action.machine.listen(action.ActionCommand.FAILSTART, self._stop_complete_failstart)

        self.node.run(self.stop_action)

    def _stop_complete_callback(self):
        if self.stop_action.exit_status != 0:
            log.error("Failed to stop service instance %s: Exit %r", self.id, self.stop_action.exit_status)

        self._queue_monitor()
        self.stop_action = None

    def _stop_complete_failstart(self):
        log.warning("Failed to start kill command for %s", self.id)
        self._queue_monitor()

    @property
    def data(self):
        # We're going to need to keep track of stuff like pid_file
        raise NotImplementedError()

    def restore(self, data):
        raise NotImplementedError()


class Service(object):
    STATE_DOWN = state.NamedEventState("down")
    STATE_UP = state.NamedEventState("up")
    
    STATE_STOPPING = state.NamedEventState("stopping", mark_all_down=STATE_DOWN)
    STATE_DEGRADED = state.NamedEventState("degraded", stop=STATE_STOPPING, mark_all_up=STATE_UP, mark_all_down=STATE_DOWN)
    STATE_STARTING = state.NamedEventState("starting", mark_all_up=STATE_UP, mark_down=STATE_DEGRADED)
    
    STATE_DOWN['start'] = STATE_STARTING
    STATE_UP['stop'] = STATE_STOPPING
    STATE_UP['mark_down'] = STATE_DEGRADED
    
    
    def __init__(self, name=None, command=None, node_pool=None, context=None):
        self.name = name
        self.command = command
        self.scheduler = None
        self.node_pool = node_pool
        self.count = 0
        self.monitor_interval = None
        self.machine = state.StateMachine(Service.STATE_DOWN)

        # Last instance number used
        self._last_instance_number = None

        self.pid_file_template = None

        self.context = None
        if context is not None:
            self.set_context(None)

        self.instances = []

    @property
    def state(self):
        return self.machine.state

    @property
    def listen(self):
        return self.machine.listen

    @property
    def is_started(self):
        """Indicate if the service has been started/initialized
        
        For now we're going to decide this if we have instances or not. It doesn't really
        coorespond well to a "state", but it might at some point need to be some sort of enable/disable thing.
        """
        return len(self.instances) > 0

    def set_context(self, context):
        self.context = command_context.CommandContext(self, context)

    def start(self):
        if self.instances:
            raise Error("Service %s already has instances: %r" % self.instances)
        
        self.machine.transition("start")
        for _ in range(self.count):
            instance = self.build_instance()
            instance.start()

    def stop(self):
        self.machine.transition("stop")

        while self.instances:
            instance = self.instances.pop()
            instance.stop()

    def build_instance(self):
        node = self.node_pool.next()

        if self._last_instance_number is None:
            self._last_instance_number = 0
        else:
            self._last_instance_number += 1

        instance_number = self._last_instance_number

        service_instance = ServiceInstance(self, node, instance_number)
        self.instances.append(service_instance)

        service_instance.listen(ServiceInstance.STATE_UP, self._instance_up)
        service_instance.listen(ServiceInstance.STATE_DOWN, self._instance_down)

        return service_instance
    
    def _instance_up(self):
        """Callback for service instance to inform us it is up"""
        if all([instance.state == ServiceInstance.STATE_UP for instance in self.instances]):
            self.machine.transition("mark_all_up")
        
    def _instance_down(self):
        """Callback for service instance to inform us it is down"""
        self.machine.transition("mark_down")

        if all([instance.state == ServiceInstance.STATE_DOWN for instance in self.instances]):
            self.machine.transition("mark_all_down")

    def absorb_previous(self, prev_service):
        # Some changes we need to worry about:
        # * Changing instance counts
        # * Changing the command
        # * Changing the node pool
        # * Changes to the context ?
        # * Restart counts for downed services ?

        # First just copy pieces of state that really matter
        self.machine = prev_service.machine
        self._last_instance_number = prev_service._last_instance_number
                
        rebuild_all_instances = any([
                                     self.node_pool != prev_service.node_pool, 
                                     self.command != prev_service.command,
                                     self.scheduler != prev_service.scheduler
                                    ])

        if rebuild_all_instances:
            self.start()
            prev_service.stop()
        else:
            # Copy over all the old instances
            self.instances += prev_service.instances

            # Now make adjustments to how many there are
            if self.count > prev_service.count:
                # We need to add some instances
                for _ in range(self.count - prev_service.count):
                    self.build_instance()
            elif self.count < prev_service.count:
                for _ in range(prev_service.count - self.count):
                    old_instance = self.instances.pop()
                    # This will fire off an action, we could do something with the result rather than just forget it ever existed.
                    old_instance.stop()
        
        
    @property
    def data(self):
        raise NotImplementedError()
    
    def restore(self, data):
        raise NotImplementedError()