import logging

from tron import event, node, eventloop
from tron.core import serviceinstance
from tron.core.serviceinstance import ServiceInstance
from tron.utils import observer


log = logging.getLogger(__name__)


class ServiceState(object):
    """Determine the state of a Service."""
    DISABLED      = "DISABLED"
    STARTING      = "STARTING"
    UP            = "UP"
    DEGRADED      = "DEGRADED"
    FAILED        = "FAILED"
    STOPPING      = "STOPPING"
    UNKNOWN       = "UNKNOWN"

    FAILURE_STATES = set([DEGRADED, FAILED])

    @classmethod
    def from_service(cls, service):
        if not service.enabled:
            return cls.disabled_states(service)

        if service.instances.all(ServiceInstance.STATE_UP):
            return cls.UP

        if service.instances.is_starting():
            return cls.STARTING

        if service.instances.all(ServiceInstance.STATE_FAILED):
            return cls.FAILED

        return cls.DEGRADED

    @classmethod
    def disabled_states(cls, service):
        if not len(service.instances):
            return cls.DISABLED

        if service.instances.all(ServiceInstance.STATE_STOPPING):
            return cls.STOPPING

        return cls.UNKNOWN


class Service(observer.Observer, observer.Observable):
    """Manage a collection of service instances."""

    NOTIFY_STATE_CHANGE = 'event_state_changed'

    def __init__(self, config, instance_collection):
        super(Service, self).__init__()
        self.config             = config
        self.instances          = instance_collection
        self.enabled            = False
        args                    = config.restart_interval, self.repair
        self.repair_callback    = eventloop.UniqueCallback(*args)
        self.event_recorder     = event.get_recorder(str(self))

    @classmethod
    def from_config(cls, config, base_context):
        node_store          = node.NodePoolStore.get_instance()
        node_pool           = node_store[config.node]
        args                = config, node_pool, base_context
        instance_collection = serviceinstance.ServiceInstanceCollection(*args)
        return cls(config, instance_collection)

    def get_name(self):
        return self.config.name

    name = property(get_name)

    def get_state(self):
        return ServiceState.from_service(self)

    def enable(self):
        """Enable the service."""
        self.enabled = True
        self.event_recorder.ok('enabled')
        self.repair()

    def disable(self):
        self.enabled = False
        self.instances.stop()
        self.repair_callback.cancel()
        self.event_recorder.ok('disabled')

    def repair(self):
        """Repair the service by restarting instances."""
        self.instances.clear_failed()
        self.instances.restore()
        self.watch_instances(self.instances.create_missing())
        self.notify(self.NOTIFY_STATE_CHANGE)
        self.event_recorder.ok('repairing')
        self.instances.start()

    def _handle_instance_state_change(self, _instance, event):
        """Handle any changes to the state of this service's instances."""
        if event == serviceinstance.ServiceInstance.STATE_DOWN:
            self.instances.clear_down()
            self.notify(self.NOTIFY_STATE_CHANGE)

        if event in (serviceinstance.ServiceInstance.STATE_FAILED,
                     serviceinstance.ServiceInstance.STATE_UP):
            self.record_events()

        if self.get_state() in ServiceState.FAILURE_STATES:
            log.info("Starting service repair for %s", self)
            self.repair_callback.start()

    handler = _handle_instance_state_change

    def record_events(self):
        """Record an event when the state changes."""
        state = self.get_state()
        if state in ServiceState.FAILURE_STATES:
            return self.event_recorder.critical(state)

        if state == ServiceState.UP:
            return self.event_recorder.ok(state)

    @property
    def state_data(self):
        """Data used to serialize the state of this service."""
        return dict(enabled=self.enabled, instances=self.instances.state_data)

    def __eq__(self, other):
        if other is None or not isinstance(other, Service):
            return False

        return self.config == other.config and self.instances == other.instances

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Service:%s" % self.name

    def watch_instances(self, instances):
        self.watch_all(instance.get_observable() for instance in instances)

    def restore_state(self, state_data):
        instances = self.instances.restore_state(state_data['instances'])
        self.watch_instances(instances)

        (self.enable if state_data.get('enabled') else self.disable)()
        self.event_recorder.info("restored")


class ServiceCollection(object):
    """A collection of services."""

    def __init__(self):
        self.services = {}

    def load_from_config(self, service_configs, context):
        """Apply a configuration to this collection and return a generator of
        services which were added.
        """
        self._filter_by_name(service_configs.keys())

        for service_config in service_configs.itervalues():
            log.debug("Building new services %s", service_config.name)
            service = Service.from_config(service_config, context)
            if self.add(service):
                yield service

    def add(self, service):
        """Add a new service or update an existing service."""
        if self._service_exists(service):
            return False

        log.info("Adding new service %s" % service.name)
        self.services[service.name] = service
        return True

    def _service_exists(self, service):
        """Return True if the service is already in the collection and it's
        equal to service. Otherwise remove it from the collection and return
        False.
        """
        if service == self.services.get(service.name):
            return True

        if service.name in self.services:
            self.remove(service.name)

        return False

    def _filter_by_name(self, service_names):
        """Remove all services which are not named in service_names."""
        for name in set(self.services.keys()) - set(service_names):
            self.remove(name)

    def remove(self, service_name):
        if service_name not in self.services:
            raise ValueError("Service %s unknown", service_name)

        log.info("Removing service %s", service_name)
        self.services.pop(service_name).disable()

    def restore_state(self, service_state_data):
        for name, state_data in service_state_data.iteritems():
            self.services[name].restore_state(state_data)
        log.info("Loaded state for %d services", len(service_state_data))

    def get_by_name(self, name):
        return self.services.get(name)

    def get_names(self):
        return self.services.keys()

    def __iter__(self):
        return self.services.itervalues()