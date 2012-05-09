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
from tron.utils.state import NamedEventState


log = logging.getLogger(__name__)


class ServiceInstanceMonitorTask(observer.Observable, observer.Observer):
    """ServiceInstance task which monitors the service process and
    notifies observers if the process is up or down.
    """

    MIN_HANG_CHECK_SECONDS  = 10

    NOTIFY_START            = 'monitor_task_notify_start'
    NOTIFY_FAILED           = 'monitor_task_notify_failed'
    NOTIFY_UP               = 'monitor_task_notify_up'
    NOTIFY_DOWN             = 'monitor_task_notify_down'

    def __init__(self, id, node, interval, pid_filename):
        super(ServiceInstanceMonitorTask, self).__init__()
        self.interval       = interval or 0
        self.node           = node
        self.id             = id
        self.pid_filename   = pid_filename
        self.action         = None

    def queue(self):
        """Queue this task to run after monitor_interval."""
        if not self.interval or self.callback:
            return
        self.callback = reactor.callLater(self.interval, self.run)

    def run(self):
        """Run the monitoring command."""
        self.callback = None

        # TODO: do we really need this?
        # TODO: this could happen if the monitor_interval is set to less then
        # MIN_HANG_CHECK_SECONDS (10) and the monitor action is hanging
        if self.action:
            log.warning("Monitor action already exists, old callLater ?")
            return

        self.notify(self.NOTIFY_START)
        self.action = self._build_action()

        if self._run_action():
            self._queue_hang_check()

    def _build_action(self):
        """Build and watch the monitor ActionCommand."""
        command         = "cat %s | xargs kill -0" % self.pid_filename
        action          = ActionCommand("%s.monitor" % self.id, command)
        msg             = "Executing '%s' on %s for %s"
        log.debug(msg % (command, self.node.hostname, self.id))
        self.watch(action)
        return action

    def _run_action(self):
        try:
            self.node.run(self.action)
        except node.Error, e:
            log.error("Failed to run monitor: %r", e)
            self.notify(self.NOTIFY_FAILED)
            return
        return True

    def handle_action_event(self, _action, event):
        if event == ActionCommand.EXITING:
            return self._handle_action_exit()
        if event == ActionCommand.FAILSTART:
            self.action = None
            self.notify(self.NOTIFY_FAILED)
            self.queue()

    handler = handle_action_event

    def _queue_hang_check(self):
        """Set a callback to verify this task has completed."""
        current_action  = self.action
        seconds         = max(self.interval * 0.8, self.MIN_HANG_CHECK_SECONDS)
        func            = self._run_hang_check
        callback        = reactor.callLater(seconds, func, current_action)
        self.hang_check_callback = callback

    def _run_hang_check(self, action):
        """If the monitor command is still running, notify the observers that
        this monitor has failed.
        """
        self.hang_check_callback = None
        if self.action is not action:
            return

        log.warning("Monitor for %s is still running", self.id)
        self.notify(self.NOTIFY_FAILED)
        self._queue_hang_check()

    def _handle_action_exit(self):
        log.debug("Monitor callback with exit %r" % self.action.exit_status)
        self.action = None
        if self.action.exit_status:
            self.notify(self.NOTIFY_DOWN)
            return

        self.notify(self.NOTIFY_UP)
        self.queue()

    def cancel(self):
        """Cancel the monitor callback and hang check."""
        if self.callback:
            self.callback.cancel()
            self.callback = None

        if self.hang_check_callback:
            self.hang_check_callback.cancel()
            self.hang_check_callback = None


class ServiceInstanceStopTask(observer.Observable, observer.Observer):
    """ServiceInstance task which kills the service process."""

    NOTIFY_DONE             = 'stop_task_notify_done'

    def __init__(self, id, node, pid_filename):
        super(ServiceInstanceStopTask, self).__init__()
        self.id             = id
        self.node           = node
        self.pid_filename   = pid_filename

    def kill(self):
        kill_command    = "cat %s | xargs kill" % self.pid_filename
        action          = ActionCommand("%s.stop" % self.id, kill_command)
        self.watch(action)

        try:
            self.node.run(action)
        except node.Error, e:
            log.warning("Failed to kill instance %s: %r", self.id, e)

    def handle_action_event(self, action, event):
        if event == ActionCommand.COMPLETE:
            return self._handle_complete(action)

        if event == ActionCommand.FAILSTART:
            log.warning("Failed to start kill command for %s", self.id)
            self.notify(self.NOTIFY_DONE)

    handler = handle_action_event

    def _handle_complete(self, action):
        if action.exit_status:
            msg = "Failed to stop service instance %s: Exit %r"
            log.error(msg % (self.id, action.exit_status))

        self.notify(self.NOTIFY_DONE)


class ServiceInstanceStartTask(observer.Observable, observer.Observer):
    """ServiceInstance task which starts the service process."""

    NOTIFY_DOWN             = 'start_task_notify_down'
    NOTIFY_MONITOR          = 'start_task_notify_monitor'

    def __init__(self, id, node):
        super(ServiceInstanceStartTask, self).__init__()
        self.id             = id
        self.node           = node

    def start(self, command):
        """Start the service command. It is important that command is passed
        in here when the start is called because it must be rendered using
        a context where the datetime is the current datetime.
        """
        action = ActionCommand("%s.start" % self.id, command)
        self.watch(action)

        try:
            self.node.run(action)
        except node.Error, e:
            log.warning("Failed to start %s: %r", self.id, e)
            self.notify(self.NOTIFY_DOWN)

    def handle_action_event(self, action, event):
        """Watch for events from the ActionCommand."""
        if event == ActionCommand.EXITING:
            return self._handle_action_exit(action)
        if event == ActionCommand.FAILSTART:
            return self._handle_action_failstart(action)
    handler = handle_action_event

    def _handle_action_failstart(self, _action):
        msg = "Failed to start service %s on %s."
        log.warning(msg % (self.id, self.node.hostname))
        self.notify(self.NOTIFY_DOWN)

    def _handle_action_exit(self, action):
        if action.exit_status:
            self.notify(self.NOTIFY_DOWN)
            return

        self.notify(self.NOTIFY_MONITOR)


class ServiceInstance(observer.Observer):

    class ServiceInstanceState(NamedEventState):
        """Event state subclass for service instances"""

    STATE_DOWN                  = ServiceInstanceState("down")
    STATE_UP                    = ServiceInstanceState("up")
    STATE_FAILED                = ServiceInstanceState("failed",
                                    stop=STATE_DOWN,
                                    up=STATE_UP)
    STATE_STOPPING              = ServiceInstanceState("stopping",
                                    down=STATE_DOWN)
    STATE_MONITORING            = ServiceInstanceState("monitoring",
                                    down=STATE_FAILED,
                                    stop=STATE_STOPPING,
                                    up=STATE_UP)
    STATE_STARTING              = ServiceInstanceState("starting",
                                    down=STATE_FAILED,
                                    monitor=STATE_MONITORING,
                                    stop=STATE_STOPPING)
    STATE_UNKNOWN               = ServiceInstanceState("unknown",
                                    monitor=STATE_MONITORING)

    STATE_MONITORING['monitor_fail']    = STATE_UNKNOWN
    STATE_UP['stop']                    = STATE_STOPPING
    STATE_UP['monitor']                 = STATE_MONITORING
    STATE_DOWN['start']                 = STATE_STARTING

    def __init__(self, service_name, node, instance_number, context,
            pid_file_template, bare_command, interval):
        self.service_name       = service_name
        self.instance_number    = instance_number
        self.node               = node
        self.id                 = "%s.%s" % (service_name, self.instance_number)

        start_state             = ServiceInstance.STATE_DOWN
        self.machine            = state.StateMachine(start_state, delegate=self)
        self.context            = command_context.CommandContext(self, context)
        self.bare_command       = bare_command
        self._create_tasks(pid_file_template, interval)

    def _create_tasks(self, pid_file_template, interval):
        """Create and watch tasks."""
        pid_file                = self._create_pid_file(pid_file_template)
        self.monitor_task       = ServiceInstanceMonitorTask(
                                    self.id, self.node, interval, pid_file)
        self.start_task         = ServiceInstanceStartTask(self.id, self.node)
        self.stop_task          = ServiceInstanceStopTask(
                                    self.id, self.node, pid_file)
        self.watch(self.monitor_task)
        self.watch(self.start_task)
        self.watch(self.stop_task)

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
            config.command,
            config.monitor_interval)
        return service_instance

    @classmethod
    def from_state(cls, config, node, inst_number, context, state):
        service_instance = cls.from_config(config, node, inst_number, context)

        # TODO: old code set this to monitoring and started the monitoring,
        # if we maintain this behaviour, remove state as an arg
        service_instance.monitor_task.run()
        return service_instance

    def _create_pid_file(self, pid_file_template):
        try:
            return pid_file_template % self.context
        except KeyError:
            msg = "Failed to render pid file template: %r" % pid_file_template
            log.error(msg)
            # TODO: put this instance in a disabled state so a check for None
            # does not have to be performed later
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
        if not self.machine.transition('start'):
            return False

        if self.command is None:
            self.machine.transition("down")
            return False

        self.start_task.start(self.command)

    def stop(self):
        if self.machine.check('stop'):
            self.stop_task.kill()
            return self.machine.transition("stop")

    def zap(self):
        """Force service instance into the down state and cancel monitor tasks
        so they do not restart the process.
        """
        self.machine.transition("stop")
        self.machine.transition("down")
        self.monitor_task.cancel()

    def handler(self, observable, event):
        """Handle events from ServiceInstance tasks."""
        if event == ServiceInstanceMonitorTask.NOTIFY_START:
            self.machine.transition("monitor")

        if event == ServiceInstanceMonitorTask.NOTIFY_FAILED:
            self.machine.transition("monitor_fail")

        if event == ServiceInstanceMonitorTask.NOTIFY_DOWN:
            self.machine.transition("down")

        if event == ServiceInstanceMonitorTask.NOTIFY_UP:
            self.machine.transition("up")

        if event == ServiceInstanceStartTask.NOTIFY_DOWN:
            self.machine.transition('down')

        if event == ServiceInstanceStartTask.NOTIFY_MONITOR:
            self._handle_start_task_complete()

        if event == ServiceInstanceStopTask.NOTIFY_DONE:
            self.monitor_task.queue()

    def _handle_start_task_complete(self):
        if self.machine.state == ServiceInstance.STATE_STARTING:
            log.info("Start for %s complete, starting monitor" % self.id)
            self.monitor_task.queue()
            return

        self.stop_task.kill()

    @property
    def state_data(self):
        """State data used to serialize the state of this ServiceInstance."""
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

    def update_config(self, config):
        self.config         = config
        self.count          = config.count

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
        while self.missing > 0:
            instance = self.build_instance()
            created_instances.append(instance)
            self.instances.append(instance)
        self.sort()
        return created_instances

    def sort(self):
        self.instances.sort(key=operator.attrgetter('instance_number'))

    def build_instance(self):
        # TODO: shouldn't this check which nodes are not used to properly
        # balance across nodes?
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
    def extra(self):
        return len(self.instances) - self.count

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

        # TODO: fix up events, parent should be mcp
        self.event_recorder = event.EventRecorder(self, parent=None)

        self.watch(self.machine)

    @classmethod
    def from_config(cls, config, node_pools, base_context):
        node_pool           = node_pools[config.node] if config.node else None
        instance_collection = ServiceInstanceCollection(
                                config, node_pool, base_context)
        service             = cls(config, base_context, instance_collection)

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
        self.event_recorder.emit_info("restored")

    def __eq__(self, other):
        if other is None or not isinstance(other, Service):
            return False

        return self.config == other.config

    def __str__(self):
        return "SERVICE:%s" % self.name
