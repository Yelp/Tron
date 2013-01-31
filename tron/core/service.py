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
    """Manage a collection of service instances."""

    STATE_DISABLED      = "disabled"
    STATE_STARTING      = "starting"
    STATE_UP            = "up"
    STATE_DEGRADED      = "degraded"
    STATE_FAILED        = "failed"
    STATE_STOPPING      = "stopping"
    STATE_UNKNOWN       = "unknown"

    FAILURE_STATES = set([STATE_DEGRADED, STATE_FAILED])

    def __init__(self, config, instance_collection):
        self.config             = config
        self.instances          = instance_collection
        self.enabled            = False
        self.repair_callback    = ServiceRepairCallback(
                                    self.repair, config.restart_interval)
        self.event_recorder     = event.get_recorder(str(self))

    @classmethod
    def from_config(cls, config, base_context):
        node_store          = node.NodePoolStore.get_instance()
        node_pool           = node_store[config.node]
        args                = config, node_pool, base_context
        instance_collection = serviceinstance.ServiceInstanceCollection(*args)
        return cls(config, instance_collection)

    def get_state(self):
        if not self.enabled:
            if not len(self.instances):
                return self.STATE_DISABLED

            if self.instances.all(ServiceInstance.STATE_STOPPING):
                return self.STATE_STOPPING

            return self.STATE_UNKNOWN

        if self.instances.all(ServiceInstance.STATE_UP):
            return self.STATE_UP

        if self.instances.is_starting():
            return self.STATE_STARTING

        if self.instances.all(ServiceInstance.STATE_FAILED):
            return self.STATE_FAILED

        return self.STATE_DEGRADED

    def enable(self):
        """Enable the service."""
        self.enabled = True
        self.event_recorder.ok('enabled')
        self.repair()

    # TODO: update api, used to be stop
    def disable(self):
        self.enabled = False
        self.instances.stop()
        self.repair_callback.cancel()
        self.event_recorder.ok('disabled')

    def repair(self):
        """Repair the service by restarting instances."""
        self.instances.clear_failed()
        self.watch_instances(self.instances.create_missing())
        self.event_recorder.ok('repairing')
        self.instances.start()

    def _handle_instance_state_change(self, instance, event):
        """Handle any changes to the state of this service's instances."""
        self.instances.clear_down()
        self.record_events()

        if self.get_state() in self.FAILURE_STATES:
            self.repair_callback.start()
        # TODO: record failures to a ServiceFailures

    handler = _handle_instance_state_change

    def record_events(self):
        """Record an event when the state changes."""
        state = self.get_state()
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

        return self.config == other.config and self.instances == other.instances

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Service:%s" % self.config.name

    def watch_instances(self, instances):
        for instance in instances:
            self.watch(instance.get_observable())

    def restore_state(self, state_data):
        instances = self.instances.restore_state(state_data['instances'])
        self.watch_instances(instances)

        (self.enable if state_data['enabled'] else self.disable)()
        self.event_recorder.info("restored")


class ServiceRepairCallback(object):
    """Monitor a callback to ensure that only a single instance is active."""

    def __init__(self, callback, restart_interval):
        super(ServiceRepairCallback, self).__init__()
        self.callback           = callback
        self.restart_interval   = restart_interval
        self.timer              = eventloop.NullCallback

    def start(self):
        """Start the callback if restart_interval is Truthy and the current
        timer is not active.
        """
        if not self.restart_interval or self.timer.active():
            return

        func            = self.run_callback
        self.timer      = eventloop.call_later(self.restart_interval, func)

    def run_callback(self):
        self.callback()

    def cancel(self):
        self.timer.cancel()