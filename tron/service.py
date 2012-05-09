import collections
import logging

from twisted.internet import reactor
import weakref
import operator

from tron import command_context
from tron import event
from tron import node
from tron.actioncommand import ActionCommand
from tron.utils import observer, proxy
from tron.utils import state
from tron.utils import timeutils
from tron.utils.state import NamedEventState


log = logging.getLogger(__name__)

MIN_MONITOR_HANG_TIME = 10


class Error(Exception):
    """Generic service error"""


class InvalidStateError(Error):
    """Invalid state error"""


class ServiceInstanceMonitor(observer.Observable):

    def __init__(self, monitor_interval):
        super(ServiceInstanceMonitor, self).__init__()
        self.monitor_interval   = monitor_interval


class ServiceInstanceKiller(object):
    pass


class ServiceInstance(observer.Observer):

    class ServiceInstanceState(NamedEventState):
        """Event state subclass for service instances"""

    STATE_DOWN          = ServiceInstanceState("down")
    STATE_UP            = ServiceInstanceState("up")
    STATE_FAILED        = ServiceInstanceState("failed",
                            stop=STATE_DOWN,
                            up=STATE_UP)
    STATE_STOPPING      = ServiceInstanceState("stopping",
                            down=STATE_DOWN)
    STATE_MONITORING    = ServiceInstanceState("monitoring",
                            down=STATE_FAILED,
                            stop=STATE_STOPPING,
                            up=STATE_UP)
    STATE_STARTING      = ServiceInstanceState("starting",
                            down=STATE_FAILED,
                            monitor=STATE_MONITORING,
                            stop=STATE_STOPPING)
    STATE_UNKNOWN       = ServiceInstanceState("unknown",
                            monitor=STATE_MONITORING)

    STATE_MONITORING['monitor_fail']    = STATE_UNKNOWN
    STATE_UP['stop']                    = STATE_STOPPING
    STATE_UP['monitor']                 = STATE_MONITORING
    STATE_DOWN['start']                 = STATE_STARTING

    def __init__(self, service_name, node, instance_number, context,
            pid_file_template, bare_command):
        self.instance_number    = instance_number
        self.node               = node
        self.id                 = "%s.%s" % (service_name, self.instance_number)

        self.machine            = state.StateMachine(
                                    ServiceInstance.STATE_DOWN, delegate=self)

        self.context            = command_context.CommandContext(self, context)
        self.pid_file           = self._create_pid_file(pid_file_template)
        self.bare_command       = bare_command

        self.monitor_action     = None
        self.start_action       = None
        self.stop_action        = None

        # Store the Twisted delayed call objects here for later cancellation
        self._monitor_delayed_call = None
        self._hanging_monitor_check_delayed_call = None

    @property
    def state(self):
        return self.machine.state

    @property
    def attach(self):
        return self.machine.attach

    @classmethod
    def from_config(cls, config, node, instance_number, context):
        service_instance = cls(
            config.name,
            node,
            instance_number,
            context,
            config.pid_file_template,
            config.command)
        return service_instance

    @classmethod
    def from_state(cls, config, node, inst_number, context, state):
        service_instance = cls.from_config(config, node, inst_number, context)

        # TODO: old code set this to monitoring and started the monitoring,
        # if we maintain this behaviour, remove state as an arg
        service_instance.machine.state = ServiceInstance.STATE_MONITORING
        service_instance._run_monitor()

        return service_instance

    def _queue_monitor(self):
        self.monitor_action = None
        if self.service.monitor_interval > 0:
            self._monitor_delayed_call = reactor.callLater(
                self.service.monitor_interval, self._run_monitor)

    def _queue_monitor_hang_check(self):
        """Since our monitor cycle is controlled by monitors actually
        completing, we need a check to ensure the monitor doesn't hang.

        We aren't going to make this monitor interval configurable right now,
        but just peg it to a factor of the interval.
        """

        current_action = self.monitor_action
        hang_monitor_duration = max((self.service.monitor_interval or 0) * 0.8,
            MIN_MONITOR_HANG_TIME)

        self._hanging_monitor_check_delayed_call = reactor.callLater(
            hang_monitor_duration,
            lambda: self._monitor_hang_check(current_action))

    def _run_monitor(self):
        self._monitor_delayed_call = None

        if self.monitor_action:
            log.warning("Monitor action already exists, old callLater ?")
            return

        self.machine.transition("monitor")
        pid_file = self.pid_file

        if pid_file is None:
            # If our pid file doesn't exist or failed to be generated, we
            # can't really monitor
            self._monitor_complete_failstart()
            return

        monitor_command = "cat %(pid_file)s | xargs kill -0" % self.context
        log.debug("Executing '%s' on %s for %s", monitor_command,
            self.node.hostname, self.id)
        self.monitor_action = ActionCommand("%s.monitor" % self.id,
            monitor_command)
        self.watch(self.monitor_action)

        try:
            self.node.run(self.monitor_action)
        except node.Error, e:
            log.error("Failed to run monitor: %r", e)
            return

        self._queue_monitor_hang_check()

    def _monitor_hang_check(self, action):
        self._hanging_monitor_check_delayed_call = None
        if self.monitor_action is action:
            log.warning("Monitor for %s is still running", self.id)
            self.machine.transition("monitor_fail")
            self._queue_monitor_hang_check()

    def _monitor_complete_callback(self):
        """Callback when our monitor has completed"""
        if not self.monitor_action:
            # This actually happened. I suspect it was a cascading failure
            # caused by a crash somewhere else leaving us in an inconsistent
            # state, but perhaps there is a reasonable explanation. Either way,
            # we don't really care about this monitor anymore.
            log.warning("Monitor for %s complete, but we don't see to care...",
                self.id)
            return

        self.last_check = timeutils.current_time()
        log.debug("Monitor callback with exit %r",
            self.monitor_action.exit_status)
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

    def kill_instance(self):
        assert self.pid_file, self.pid_file

        kill_command = "cat %(pid_file)s | xargs kill" % self.context

        self.stop_action = ActionCommand("%s.stop" % self.id,
            kill_command)
        self.watch(self.stop_action)
        try:
            self.node.run(self.stop_action)
        except node.Error, e:
            log.warning("Failed to kill instance %s: %r", self.id, e)

    def _stop_complete_callback(self):
        if self.stop_action.exit_status != 0:
            log.error("Failed to stop service instance %s: Exit %r", self.id,
                self.stop_action.exit_status)

        self._queue_monitor()
        self.stop_action = None

    def _stop_complete_failstart(self):
        log.warning("Failed to start kill command for %s", self.id)
        self._queue_monitor()


    def _create_pid_file(self, pid_file_template):
        try:
            return pid_file_template % self.context
        except KeyError:
            msg = "Failed to render pid file template: %r" % pid_file_template
            log.error(msg)
        return None

    @property
    def command(self):
        try:
            return self.bare_command % self.context
        except KeyError:
            msg = "Failed to render service command for service %s: %s"
            log.error(msg % (self.service_name, self.bare_command))

        return None

    def start(self):
        if self.machine.state != self.STATE_DOWN:
            raise InvalidStateError("Instance must be marked DOWN to start")

        self.machine.transition("start")

        command = self.command
        if command is None:
            self._start_complete_failstart()
            return

        self.start_action = ActionCommand("%s.start" % self.id, command)
        self.watch(self.start_action)

        try:
            self.node.run(self.start_action)
        except node.Error, e:
            log.warning("Failed to start %s: %r", self.id, e)

    def handler(self, observable, event):
        # TODO: this no longer requires setting the start action as a field
        # TODO: This can be easily cleaned up

        if observable == self.monitor_action:
            # We use exiting instead of complete because all we really need is
            # the exit status
            if event == ActionCommand.EXITING:
                return self._monitor_complete_callback()
            if event == ActionCommand.FAILSTART:
                return self._monitor_complete_failstart()

        elif observable == self.start_action:
            if event == ActionCommand.EXITING:
                return self._start_complete_callback()
            if event == ActionCommand.FAILSTART:
                return self._start_complete_failstart()

        elif observable == self.stop_action:
            if event == ActionCommand.COMPLETE:
                return self._stop_complete_callback()
            if event == ActionCommand.FAILSTART:
                return self._stop_complete_failstart()

    def _start_complete_callback(self):
        if self.start_action.exit_status != 0:
            self.machine.transition("down")
        elif self.machine.state == self.STATE_STOPPING:
            # Someone tried to stop us while we were just getting going. Go
            # ahead and kick of the kill operation now that we're up.
            if self.stop_action:
                log.warning("Stopping %s while stop already in progress",
                            self.id)
            else:
                self.kill_instance()
        else:
            log.info("Start for %s complete, checking monitor", self.id)
            self._queue_monitor()

        self.start_action = None

    def _start_complete_failstart(self):
        log.warning("Failed to start service %s (%s)",
                    self.id, self.node.hostname)

        # We may have failed but long since not mattered
        if None in (self.machine, self.start_action):
            return

        self.machine.transition("down")
        self.start_action = None

    def stop(self):
        if self.machine.check('stop'):
            self.kill_instance()
            return self.machine.transition("stop")

    def zap(self):
        self.machine.transition("stop")
        self.machine.transition("down")

        # Kill the monitors so they don't put us back in the UP state
        if self._monitor_delayed_call is not None:
            self._monitor_delayed_call.cancel()
            self._monitor_delayed_call = None

        if self._hanging_monitor_check_delayed_call is not None:
            self._hanging_monitor_check_delayed_call.cancel()
            self._hanging_monitor_check_delayed_call = None

    @property
    def state_data(self):
        """This data is used to serialize the state of this service instance."""
        return {
            'node':             self.node.hostname,
            'instance_number':  self.instance_number,
            'state':            str(self.state),
        }

    def __str__(self):
        return "SERVICE:%s" % self.id


class ServiceInstanceCollection(object):
    """A collection of ServiceInstances."""

    def __init__(self, config, node_pool, context):
        self.count              = config.count
        self.config             = config
        self.node_pool          = node_pool
        self.instances          = []
        self.context            = command_context.CommandContext(next=context)

        self.instances_proxy    = proxy.CollectionProxy(
            lambda: self.instances,
            [
                ('stop',    all,    True),
                ('zap',     all,    True),
                ('start',   all,    True)
            ]
        )

    # TODO: test
    def restore_state(self, instances_state_data):
        """Restore state of the instances."""
        created_instances = []
        for state_data in instances_state_data:
            node_name = state_data['node']
            if node_name not in self.node_pool:
                msg = "Failed to find node %s in node_pool for %s"
                log.error(msg % (node_name, self.config.name))
                continue

            node            = self.node_pool[node_name]
            instance_num    = state_data['instance_number']
            instances_state = state_data['state']
            instance        = ServiceInstance.from_state(
                self.config, node, instance_num, self.context, instances_state)

            created_instances.append(instance)
            self.instances.append(instance)

        self.instances.sort()
        return created_instances

    def clear_failed(self):
        """Remove and cleanup any instances that have failed."""
        self._clear(ServiceInstance.STATE_FAILED)

    def clear_down(self):
        self._clear(ServiceInstance.STATE_DOWN)

    def _clear(self, state):
        self.instances = [i for i in self.instances if i.state != state]

    def get_failed(self):
        return self._filter(ServiceInstance.STATE_FAILED)

    def get_up(self):
        return self._filter(ServiceInstance.STATE_UP)

    def _filter(self, state):
        return (i for i in self.instances if i.state == state)

    def create_missing(self):
        """Create instances until this collection contains the configured
        number of instances.
        """
        created_instances = []
        while self.missing:
            instance = self.build_instance()
            created_instances.append(instance)
            self.instances.append(instance)
        self.sort()
        return created_instances

    def sort(self):
        self.instances.sort(key=operator.attrgetter('instance_number'))

    def build_instance(self):
        node                = self.node_pool.next_round_robin()
        instance_number     = self.next_instance_number()
        service_instance    = ServiceInstance.from_config(
                                self.config, node, instance_number, self.context)
        return service_instance

    def next_instance_number(self):
        """Return the next available instance number."""
        instance_nums = set(inst.instance_number for inst in self.instances)
        for num in xrange(self.count):
            if num not in instance_nums:
                return num

    @property
    def missing(self):
        return self.count - len(self.instances)

    @property
    def state_data(self):
        return [inst.state_data for inst in self.instances]

    def __len__(self):
        return len(self.instances)

    def __getattr__(self, item):
        return self.instances_proxy.perform(item)


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


class Service(observer.Observable, observer.Observer):

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
        super(Service, self).__init__()
        self.config             = config
        self.name               = config.name
        self.instances          = instance_collection
        self.machine            = state.StateMachine(
                                    Service.STATE_DOWN, delegate=self)

        # TODO: fix up events, parent should be mcp
        self.event_recorder = event.EventRecorder(self, parent=None)
        # TODO: this is a little weird, should get cleaned up
        self.watch(self.machine)

    @classmethod
    def from_config(cls, config, node_pools, base_context):
        node_pool           = node_pools[config.node] if config.node else None
        instance_collection = ServiceInstanceCollection(
                                config, node_pool, base_context)
        service             = cls(config, base_context, instance_collection)

        ServiceMonitor(service, config.restart_interval).start()
        return service

    @property
    def state(self):
        return self.machine.state

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
            func = self.event_recorder.emit_critical
        elif self.machine.state in (self.STATE_UP, self.STATE_DOWN):
            func = self.event_recorder.emit_ok
        else:
            func = self.event_recorder.emit_info

        func(str(self.machine.state))

    def handler(self, observable, _event):
        if observable == self:
            return self._record_state_changes()

        self._handle_instance_state_change()

    # TODO: clean this up
    def absorb_previous(self, prev_service):
        # Some changes we need to worry about:
        # * Changing instance counts
        # * Changing the command
        # * Changing the node pool
        # * Changes to the context ?
        # * Restart counts for downed services ?


        assert self.node_pool, "Missing node pool for %s" % self.name
        removed_instances = 0

        rebuild_all_instances = any([
            self.command != prev_service.command,
            self.pid_file_template != prev_service.pid_file_template])

        # Since we are inheriting all the existing instances, it's safe to also
        # inherit the previous state machine as well.
        self.machine = prev_service.machine

        # To permanently disable the older service, remove it's machine.
        prev_service.machine = None

        # Copy over all the old instances
        self.instances += prev_service.instances
        for service_instance in prev_service.instances:
            service_instance.machine.clear_observers()
            self.watch(service_instance)

            if rebuild_all_instances:
                # For some configuration changes, we'll just stop all the
                # previous instances. When those services stop, we should be in
                # a degraded mode, triggering a restart of the newer generation
                # of instances.
                service_instance.stop()
                removed_instances += 1

        prev_service.instances = []

        self.instances.sort()

        current_instances = [i for i in self.instances if i.state not in
                             (ServiceInstance.STATE_STOPPING,
                              ServiceInstance.STATE_DOWN,
                              ServiceInstance.STATE_FAILED)]

        # Now that we've inherited some instances, let's trigger an update to
        # our state machine.
        self._handle_instance_state_change()

        # We have special handling for node pool changes. This would cover the
        # case of removing (or subsituting) a node in a pool which would
        # require rebalancing services.

        if self.node_pool != prev_service.node_pool:
            # How many instances per node should we have ?
            optimal_instances_per_node = self.count / len(self.node_pool.nodes)
            instance_count_per_node = collections.defaultdict(int)

            for service_instance in current_instances:
                # First we'll stop any instances on nodes that are no longer
                # part of our pool
                try:
                    hostname = service_instance.node.hostname
                    service_instance.node = self.node_pool[hostname]
                except KeyError:
                    log.info("Stopping instance %r because it's not on a"
                             " current node (%r)",
                             service_instance.id,
                             service_instance.node.hostname)
                    service_instance.stop()
                    removed_instances += 1
                    continue

                instance_count_per_node[service_instance.node] += 1
                if (instance_count_per_node[service_instance.node] >
                        optimal_instances_per_node):
                    log.info("Stopping instance %r because node %s has too"
                             " many instances",
                             service_instance.id,
                             service_instance.node.hostname)
                    service_instance.stop()
                    removed_instances += 1
                    continue

        current_instances = [i for i in self.instances if i.state not in
                             (ServiceInstance.STATE_STOPPING,
                              ServiceInstance.STATE_DOWN,
                              ServiceInstance.STATE_FAILED)]

        count_to_remove = ((len(self.instances) - removed_instances) -
                           self.count)
        if count_to_remove > 0:
            instances_to_remove = current_instances[-count_to_remove:]
            for service_instance in instances_to_remove:
                service_instance.stop()
                removed_instances += 1

        self.instances.create_missing()
        self.event_recorder.emit_notice("reconfigured")

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
        self.event_recorder.emit_info("restored")

    def __eq__(self, other):
        if other is None or not isinstance(other, Service):
            return False

        return self.config == other.config

    def __str__(self):
        return "SERVICE:%s" % self.name
