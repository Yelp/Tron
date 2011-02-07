import logging

from twisted.internet import reactor

from tron import job
from tron import action
from tron import command_context
from tron.utils import state
from tron.utils import timeutils

log = logging.getLogger(__name__)

class Error(Exception): pass

class InvalidStateError(Error): pass

class ServiceInstance(object):
    STATE_DOWN = state.NamedEventState("down")
    STATE_UP = state.NamedEventState("up")
    STATE_FAILED = state.NamedEventState("failed", stop=STATE_DOWN, up=STATE_UP)
    STATE_STOPPING = state.NamedEventState("stopping", down=STATE_DOWN)
    STATE_MONITORING = state.NamedEventState("monitoring", down=STATE_FAILED, stop=STATE_STOPPING, up=STATE_UP)
    STATE_STARTING = state.NamedEventState("starting", down=STATE_FAILED, monitor=STATE_MONITORING, stop=STATE_STOPPING)

    STATE_UNKNOWN = state.NamedEventState("unknown", monitor=STATE_MONITORING)
    STATE_MONITORING['monitor_fail'] = STATE_UNKNOWN

    STATE_UP['stop'] = STATE_STOPPING
    STATE_UP['monitor'] = STATE_MONITORING
    STATE_DOWN['start'] = STATE_STARTING
    
    def __init__(self, service, node, instance_number):
        self.service = service
        self.instance_number = instance_number
        self.node = node

        self.id = "%s.%s" % (service.name, self.instance_number)
        
        self.machine = state.StateMachine(ServiceInstance.STATE_DOWN)
        
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
            self.machine.transition("down")
        else:
            self.machine.transition("up")
            self._queue_monitor()

        self.monitor_action = None

    def _monitor_complete_failstart(self):
        """Callback when our monitor failed to even start"""
        self.machine.transition("monitor_fail")
        self._queue_monitor()

        self.monitor_action = None
        
    def start(self):
        if self.machine.state != self.STATE_DOWN:
            raise InvalidStateError("Instance must be marked DOWN to start")

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
            self.machine.transition("down")
        elif self.machine.state == self.STATE_STOPPING:
            # Someone tried to stop us while we were just getting going. 
            # Go ahead and kick of the kill operation now that we're up.
            self.kill_instance()
        else:
            self._queue_monitor()

        self.start_action = None

    def _start_complete_failstart(self):
        log.warning("Failed to start service %s (%s)", self.id, self.node.hostname)
        self.machine.transition("down")
        self.start_action = None

    def stop(self):
        if self.machine.state not in (self.STATE_UP, self.STATE_FAILED, self.STATE_STARTING):
            raise InvalidStateError("Instance must be up or failed to stop")
        
        self.machine.transition("stop")
        
        if self.machine.state == self.STATE_STOPPING:
            self.kill_instance()

    def kill_instance(self):
        assert self.pid_file, self.pid_file
        
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
    STATE_STOPPING = state.NamedEventState("stopping", all_down=STATE_DOWN)
    STATE_FAILED = state.NamedEventState("failed", stop=STATE_STOPPING)
    STATE_DEGRADED = state.NamedEventState("degraded", stop=STATE_STOPPING, all_up=STATE_UP, all_failed=STATE_FAILED)
    STATE_STARTING = state.NamedEventState("starting", all_up=STATE_UP, failed=STATE_DEGRADED, stop=STATE_STOPPING)
    
    STATE_DOWN['start'] = STATE_STARTING
    STATE_UP['stop'] = STATE_STOPPING
    STATE_UP['failed'] = STATE_DEGRADED
    STATE_UP['down'] = STATE_DEGRADED
    
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
        self.machine.transition("start")
        while len(self.instances) < self.count:
            instance = self.build_instance()
            instance.start()

    def stop(self):
        self.machine.transition("stop")

        while self.instances:
            instance = self.instances.pop()
            try:
                instance.stop()
            except InvalidStateError:
                # We don't really care what they are doing as long as they arn't up
                pass

    def _create_instance(self, node, instance_number):
        service_instance = ServiceInstance(self, node, instance_number)
        self.instances.append(service_instance)

        service_instance.listen(ServiceInstance.STATE_UP, self._instance_up)
        service_instance.listen(ServiceInstance.STATE_DOWN, self._instance_down)
        service_instance.listen(ServiceInstance.STATE_FAILED, self._instance_failed)

        return service_instance

    def build_instance(self):
        node = self.node_pool.next()

        if self._last_instance_number is None:
            self._last_instance_number = 0
        else:
            self._last_instance_number += 1

        instance_number = self._last_instance_number

        service_instance = self._create_instance(node, instance_number)

        # This instance starts off as being down, so we better inform whoever might care.
        service_instance.machine.transition("down")

        return service_instance
    
    def _instance_up(self):
        """Callback for service instance to inform us it is up"""
        if all([instance.state == ServiceInstance.STATE_UP for instance in self.instances]):
            self.machine.transition("all_up")
        
    def _instance_down(self):
        """Callback for service instance to inform us it is down (gracefully)"""
        self.machine.transition("down")

        # Filter out our instance because we don't store downed instances
        self.instances = [inst for inst in self.instances if inst.state != inst.STATE_DOWN]

        if not self.instances:
            self.machine.transition("all_down")
            # Reset our instances
            self._last_instance_number = None

    def _instance_failed(self):
        """Callback to indicate an instance failed"""
        self.machine.transition("failed")
        if all([instance.state == ServiceInstance.STATE_FAILED for instance in self.instances]):
            self.machine.transition("all_failed")

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
            while len(self.instances) < self.count:
                new_instance = self.build_instance()
                if self.machine.state == self.STATE_DEGRADED:
                    new_instance.start()

            while self.count < len(self.instances):
                old_instance = self.instances.pop()
                # This will fire off an action, we could do something with the result rather than just forget it ever existed.
                # Also note that if this stop fails, we'll never know.
                try:
                    old_instance.stop()
                except InvalidStateError:
                    pass
        
        
    @property
    def data(self):
        data = {
            'state': str(self.machine.state),
            'last_instance_number': self._last_instance_number,
            
        }
        data['instances'] = []
        for instance in self.instances:
            service_data = {
                'node': instance.node.hostname,
                'instance_number': instance.instance_number,
                'state': str(instance.state),
            }

            data['instances'].append(service_data)

        return data
        
    def restore(self, data):
        """Restore state of this service from datafile"""
        # The state of a service is more easier than for jobs. There are just a few things we want to guarantee:
        #  1. If service instances are up, they can continue to be up. We'll just start monitoring from where we left off.
        #  2. Failures are maintained and have to be cleared.
        
        # Start our machine from where it left off
        self.machine.state = state.named_event_by_name(Service.STATE_DOWN, data['state'])
        self._last_instance_number = data['last_instance_number']

        # Restore all the instances
        for instance in data['instances']:
            node = self.node_pool[instance['node']]
            service_instance = self._create_instance(node, instance['instance_number'])
            if instance['state'] == ServiceInstance.STATE_FAILED.name:
                service_instance.machine.state = ServiceInstance.STATE_FAILED
            else:
                service_instance.machine.state = ServiceInstance.STATE_UP
                service_instance._run_monitor()
