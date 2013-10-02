import logging
import itertools

from tron import event, node, eventloop
from tron.core import serviceinstance
from tron.core.serviceinstance import ServiceInstance
from tron.utils import observer, collections


log = logging.getLogger(__name__)


class ServiceState(object):
    """Determine the state of a Service."""
    DISABLED      = "disabled"
    STARTING      = "starting"
    UP            = "up"
    DEGRADED      = "degraded"
    FAILED        = "failed"
    STOPPING      = "stopping"
    UNKNOWN       = "unknown"

    FAILURE_STATES = set([DEGRADED, FAILED])

    @classmethod
    def from_service(cls, service):
        if not service.enabled:
            return cls.disabled_states(service)

        if service.instances.is_up():
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
        args                    = config.restart_delay, self.repair
        self.repair_callback    = eventloop.UniqueCallback(*args)
        self.event_recorder     = event.get_recorder(str(self))

    @classmethod
    def from_config(cls, config, base_context):
        node_repo           = node.NodePoolRepository.get_instance()
        node_pool           = node_repo.get_by_name(config.node)
        args                = config, node_pool, base_context
        instance_collection = serviceinstance.ServiceInstanceCollection(*args)
        return cls(config, instance_collection)

    def get_name(self):
        return self.config.name

    name = property(get_name)

    @property
    def id(self):
        return self.name

    def get_state(self):
        return ServiceState.from_service(self)

    def enable(self):
        """Enable the service."""
        self.enabled = True
        self.event_recorder.ok('enabled')
        self.repair()

    def disable(self, force=False):
        self.enabled = False
        (self.instances.kill if force else self.instances.stop)()
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

    def update_node_pool(self, node_pool):
        self.instances.update_node_pool(node_pool)
        self.instances.clear_extra()
        if self.enabled:
            self.repair()


class ServiceCollection(object):
    """A collection of services."""

    def __init__(self):
        self.services = collections.MappingCollection('services')

    def _build(self, new_service):
        """A method to be used as an update function for MappingCollection.add.
        This function attempts to load an old Service object, and if one
        exists, see if we don't actually have to use an entirely new
        Service object on reconfiguration.

        To do this, we first check if the number of instances (config.count) is
        different, as we have a method to fix this when updating the service's
        node pool. Then, if the configs are now equal, we can simply update
        the node pool of the old Service object and be done- no need for the
        new Service object. Otherwise, we use the new object as normal.
        """
        old_service = self.get_by_name(new_service.config.name)

        if not old_service:
            log.debug("Building new service %s", new_service.config.name)
            return False

        if old_service.config.count != new_service.config.count:
            old_service.config = old_service.config._replace(
                                    count=new_service.config.count)

        if old_service.config == new_service.config:
            log.debug("Updating service %s\'s node pool" % new_service.config.name)
            old_service.instances.context = new_service.instances.context
            old_service.update_node_pool(new_service.instances.node_pool)
            return True
        else:
            log.debug("Building new service %s", new_service.config.name)
            old_service.disable()
            return False

    def load_from_config(self, service_configs, context):
        """Apply a configuration to this collection and return a generator of
        services which were added.
        """
        self.services.filter_by_name(service_configs.keys())

        seq = (Service.from_config(config, context)
            for config in service_configs.itervalues())
        return itertools.ifilter(lambda e: self.services.add(e, self._build), seq)

    def restore_state(self, service_state_data):
        self.services.restore_state(service_state_data)

    def get_by_name(self, name):
        return self.services.get(name)

    def get_services_by_namespace(self, namespace):
        return [service for service in self
            if service.config.namespace == namespace]

    def get_names(self):
        return self.services.keys()

    def __iter__(self):
        return self.services.itervalues()
