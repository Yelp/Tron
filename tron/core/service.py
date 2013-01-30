import logging

from tron import event, node, eventloop
from tron.core import serviceinstance
from tron.core.serviceinstance import ServiceInstance
from tron.utils import observer
from tron.utils import state


log = logging.getLogger(__name__)


class ServiceState(state.NamedEventState):
    """Named event state subclass for services"""


# TODO: object to record failures/stdout/stderr
# TODO: get failure messages from instance stderr/stdout

class Service(observer.Observer):

    STATE_DISABLED      = "disabled"
    STATE_STARTING      = "starting"
    STATE_UP            = "up"
    STATE_DEGRADED      = "degraded"
    STATE_FAILED        = "failed"
    STATE_STOPPING      = "stopping"
    STATE_UNKNOWN       = "unknown"

    def __init__(self, config, instance_collection):
        self.config             = config
        self.instances          = instance_collection
        self.enabled            = False
        self.monitor            = ServiceMonitor(self, config.restart_interval)
        self.event_recorder     = event.get_recorder(str(self))

    @classmethod
    def from_config(cls, config, base_context):
        node_store          = node.NodePoolStore.get_instance()
        node_pool           = node_store[config.node]
        args                = config, node_pool, base_context
        instance_collection = serviceinstance.ServiceInstanceCollection(*args)
        return cls(config, instance_collection)

    @property
    def state(self):
        if not self.enabled:
            if not len(self.instances):
                return self.STATE_DISABLED

            if self.instances.all_states(ServiceInstance.STATE_STOPPING):
                return self.STATE_STOPPING

            return self.STATE_UNKNOWN

        if self.instances.all_states(ServiceInstance.STATE_UP):
            return self.STATE_UP

        if self.instances.is_starting():
            return self.STATE_STARTING

        if self.instances.all_states(ServiceInstance.STATE_FAILED):
            return self.STATE_FAILED

        return self.STATE_DEGRADED

    def enable(self):
        """Enable the service."""
        self.enabled = True
        self.monitor.start()
        self.event_recorder.ok('enabled')

    # TODO: update api, used to be stop
    def disable(self):
        self.enabled = False
        self.instances.stop()
        self.monitor.cancel()
        self.event_recorder.ok('disabled')

    def repair(self):
        """Repair the service by restarting instances."""
        # TODO:
        self.instances.clear_failed()
        for instance in self.instances.create_missing():
            self.watch(instance.get_observable())

        self.event_recorder.ok('repairing')
        self.instances.start()

    def _handle_instance_state_change(self, instance, event):
        """Handle any changes to the state of this service's instances."""
        self.instances.clear_down()
        self.record_events()

        # TODO: trigger repair?
        # TODO: record failures to a ServiceFailures

    handler = _handle_instance_state_change

    def record_events(self):
        """Record an event when the state changes."""
        state = self.state
        if state in (self.STATE_FAILED, self.STATE_DEGRADED):
            return self.event_recorder.critical(state)

        if state == self.STATE_UP:
            return self.event_recorder.ok(state)

    @property
    def state_data(self):
        """Data used to serialize the state of this service."""
        # TODO: backwards compatibility
        return dict(enabled=self.enabled, instances=self.instances.state_data)

    def __eq__(self, other):
        if other is None or not isinstance(other, Service):
            return False

        return self.config == other.config

    def __str__(self):
        return "Service:%s" % self.config.name


class ServiceRestore(object):
    """Restore a service from state, or after reconfig."""

    def restore_service_state(self, service_state_data):
        """Restore state of this service. If service instances are up,
        restart monitoring.
        """
        instance_state_data = service_state_data['instances']
        for instance in self.instances.restore_state(instance_state_data):
            self.watch(instance)
        self._handle_instance_state_change()
        self.event_recorder.info("restored")

    def update_from_service(self, service):
        """Update this service from the new config."""
        old_config, self.config  = self.config, service.config
        self.instances.update_config(service.config)

        if service.config.restart_interval != old_config.restart_interval:
            self.monitor.stop_watching(self)
            self.monitor = ServiceMonitor(self, service.config.restart_interval)

        diff_node_pool = service.instances.node_pool != self.instances.node_pool
        diff_command   = service.config.command      != old_config.command
        diff_count     = service.config.count        != old_config.count
        if diff_node_pool or diff_command or diff_count:
            self.instances.node_pool = service.instances.node_pool
            self.stop()

        # TODO: can this auto-restart the instances that need to start?
        self.monitor.start()



class ServiceMonitor(observer.Observer, observer.Observable):
    """Observe a service and restart it when it fails."""

    FAILURE_STATES = (Service.STATE_DEGRADED, Service.STATE_FAILED)

    def __init__(self, service, restart_interval):
        super(ServiceMonitor, self).__init__()
        self.service            = service
        self.restart_interval   = restart_interval
        self.timer              = eventloop.NullCallback

    def start(self):
        """Start watching the service if restart_interval is Truthy."""
        if self.restart_interval:
            self.watch(self.service)

    def restart_service(self):
        if self.service.state not in self.FAILURE_STATES:
            return
        log.info("Restarting failed instances for %s" % self.service)
        # TODO: check service.enabled
        self.service.repair()

    def _set_restart_callback(self):
        if self.timer.active():
            return
        func            = self.restart_service
        self.timer      = eventloop.call_later(self.restart_interval, func)

    def handle_service_state_change(self, _service, event):
        if event in self.FAILURE_STATES:
            self._set_restart_callback()

        if event == Service.STATE_STARTING:
            self.timer.cancel()

    def cancel(self):
        self.timer.cancel()

    handler = handle_service_state_change
