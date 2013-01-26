import logging

from twisted.internet import reactor
import weakref

from tron import event, node
from tron.core import serviceinstance
from tron.utils import observer
from tron.utils import state
from tron.utils.state import NamedEventState


log = logging.getLogger(__name__)


class ServiceMonitor(observer.Observer):
    """Observe a service and restart it when it fails."""

    def __init__(self, service, restart_interval):
        self.service            = weakref.proxy(service)
        self.restart_interval   = restart_interval
        self.timer              = None

    def start(self):
        """Start watching the service.  If restart_interval is None then
        there is no reason to start.
        """
        if self.restart_interval is not None:
            self.watch(self.service)

    def _restart_after_failure(self):
        self._clear_timer()

        if self.service.state in (Service.STATE_DEGRADED, Service.STATE_FAILED):
            msg = "Restarting failed instances for service %s"
            log.info(msg % self.service.name)
            self.service.start()

    def _clear_timer(self):
        self.timer = None

    def _set_restart_callback(self):
        if self.timer:
            return
        func            = self._restart_after_failure
        self.timer      = reactor.callLater(self.restart_interval, func)

    def handle_service_state_change(self, _observable, event):
        if event in (Service.STATE_DEGRADED, Service.STATE_FAILED):
            self._set_restart_callback()

        if event == Service.STATE_STARTING:
            self._clear_timer()

    handler = handle_service_state_change


class Service(observer.Observer):

    class ServiceState(NamedEventState):
        """Named event state subclass for services"""

    STATE_DOWN          = ServiceState("down")
    STATE_UP            = ServiceState("up")
    STATE_DEGRADED      = ServiceState("degraded")
    STATE_STOPPING      = ServiceState("stopping",
                            all_down=STATE_DOWN)
    STATE_FAILED        = ServiceState("failed")
    STATE_STARTING      = ServiceState("starting",
                            all_up=STATE_UP,
                            failed=STATE_DEGRADED,
                            stop=STATE_STOPPING)

    STATE_DOWN['start'] = STATE_STARTING

    STATE_DEGRADED.update(dict(
                            stop=STATE_STOPPING,
                            all_up=STATE_UP,
                            all_failed=STATE_FAILED))

    STATE_FAILED.update(dict(
                            stop=STATE_STOPPING,
                            up=STATE_DEGRADED,
                            start=STATE_STARTING))

    STATE_UP.update(dict(
                            stop=STATE_STOPPING,
                            failed=STATE_DEGRADED,
                            down=STATE_DEGRADED))

    def __init__(self, config, instance_collection):
        self.config             = config
        self.name               = config.name
        self.instances          = instance_collection
        self.machine            = state.StateMachine(
                                    Service.STATE_DOWN, delegate=self)
        self.monitor            = None
        self.event_recorder     = event.get_recorder(str(self))

        self.watch(self.machine)

    @classmethod
    def from_config(cls, config, base_context):
        node_store          = node.NodePoolStore.get_instance()
        node_pool           = node_store[config.node] if config.node else None
        instance_collection = serviceinstance.ServiceInstanceCollection(
                                config, node_pool, base_context)
        service             = cls(config, instance_collection)

        service.monitor     = ServiceMonitor(service, config.restart_interval)
        service.monitor.start()
        return service

    @property
    def state(self):
        return self.machine.state

    @property
    def attach(self):
        return self.machine.attach

    def start(self):
        """Start the service."""
        self.instances.clear_failed()
        for instance in self.instances.create_missing():
            self.watch(instance)

        return self.instances.start() and self.machine.transition("start")

    def stop(self):
        if not self.machine.transition("stop"):
            return False

        if not self.instances.stop():
            return False
        return True

    def zap(self):
        """Force down the service."""
        self.machine.transition("stop")
        self.instances.zap()

    def _handle_instance_state_change(self):
        """Handle any changes to the state of this service's instances."""
        self.instances.clear_down()

        if not len(self.instances):
            return self.machine.transition("all_down")

        if any(self.instances.get_failed()):
            self.machine.transition("failed")

            if all(self.instances.get_failed()):
                self.machine.transition("all_failed")
            return

        if self.instances.missing:
            msg = "Found %s instances are missing from %s."
            log.warn(msg % (self.instances.missing, self.name))
            return self.machine.transition("down")

        if all(self.instances.get_up()):
            return self.machine.transition("all_up")

    def _record_state_changes(self):
        """Record an event when the state changes."""
        if self.machine.state in (self.STATE_FAILED, self.STATE_DEGRADED):
            func = self.event_recorder.critical
        elif self.machine.state in (self.STATE_UP, self.STATE_DOWN):
            func = self.event_recorder.ok
        else:
            func = self.event_recorder.info

        func(str(self.machine.state))

    def handler(self, observable, _event):
        if observable == self:
            return self._record_state_changes()

        self._handle_instance_state_change()

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

    @property
    def state_data(self):
        """Data used to serialize the state of this service."""
        return {
            # TODO: this state is probably not useful, since it is
            # derived from the states of instances. Remove it
            'state':        str(self.machine.state),
            'instances':    self.instances.state_data
        }

    def restore_service_state(self, service_state_data):
        """Restore state of this service. If service instances are up,
        restart monitoring.
        """
        instance_state_data = service_state_data['instances']
        for instance in self.instances.restore_state(instance_state_data):
            self.watch(instance)
        self._handle_instance_state_change()
        self.event_recorder.info("restored")

    def __eq__(self, other):
        if other is None or not isinstance(other, Service):
            return False

        return self.config == other.config

    def __str__(self):
        return "SERVICE:%s" % self.name