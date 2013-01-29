import logging

import operator

from tron import command_context
from tron import eventloop
from tron import node
from tron.actioncommand import ActionCommand, CompletedActionCommand
from tron.utils import observer, proxy
from tron.utils import state


log = logging.getLogger(__name__)


class ServiceInstanceMonitorTask(observer.Observable, observer.Observer):
    """ServiceInstance task which monitors the service process and
    notifies observers if the process is up or down. Also monitors itself
    to ensure this check does not hang.

    This task will be a no-op if interval is Falsy.
    """

    # TODO: config setting
    MIN_HANG_CHECK_SECONDS  = 10

    NOTIFY_START            = 'monitor_task_notify_start'
    NOTIFY_FAILED           = 'monitor_task_notify_failed'
    NOTIFY_UP               = 'monitor_task_notify_up'
    NOTIFY_DOWN             = 'monitor_task_notify_down'

    command_template        = "cat %s | xargs kill -0"

    def __init__(self, id, node, interval, pid_filename):
        super(ServiceInstanceMonitorTask, self).__init__()
        self.interval               = interval or 0
        self.node                   = node
        self.id                     = id
        self.pid_filename           = pid_filename
        self.action                 = CompletedActionCommand
        self.callback               = eventloop.NullCallback
        self.hang_check_callback    = eventloop.NullCallback

    def queue(self):
        """Queue this task to run after monitor_interval."""
        if not self.interval or self.callback.active():
            return
        self.callback = eventloop.call_later(self.interval, self.run)

    def run(self):
        """Run the monitoring command."""
        if not self.action.is_complete:
            log.warn("%s: Monitor action already exists.", self)
            return

        self.notify(self.NOTIFY_START)
        self.action = self._build_action()

        if self._run_action():
            self._queue_hang_check()

    def _build_action(self):
        """Build and watch the monitor ActionCommand."""
        command         = self.command_template % self.pid_filename
        action          = ActionCommand("%s.monitor" % self.id, command)
        msg             = "Executing '%s' on %s for %s"
        log.debug(msg % (command, self.node, self.id))
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
        # TODO: check action matches self.action ?
        if event == ActionCommand.EXITING:
            return self._handle_action_exit()
        if event == ActionCommand.FAILSTART:
            self.notify(self.NOTIFY_FAILED)
            self.queue()

    handler = handle_action_event

    def _handle_action_exit(self):
        log.debug("%s with exit failure %r", self, self.action.has_failed)
        if self.action.has_failed:
            self.notify(self.NOTIFY_DOWN)
            return

        self.notify(self.NOTIFY_UP)
        self.queue()

    def cancel(self):
        """Cancel the monitor callback and hang check."""
        self.callback.cancel()
        self.hang_check_callback.cancel()

    def _queue_hang_check(self):
        """Set a callback to verify this task has completed."""
        current_action  = self.action
        # TODO: constant
        # TODO: fix interval (should be constant, not 80% ?)
        seconds         = max(self.interval * 0.8, self.MIN_HANG_CHECK_SECONDS)
        func            = self._run_hang_check
        callback        = eventloop.call_later(seconds, func, current_action)
        self.hang_check_callback = callback

    def _run_hang_check(self, action):
        """If the monitor command is still running, notify the observers that
        this monitor has failed.
        """
        if self.action is not action:
            return

        log.warning("Monitor for %s is still running", self.id)
        self.notify(self.NOTIFY_FAILED)
        # TODO: max hang checks, then fail
        self._queue_hang_check()

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)


class ServiceInstanceStopTask(observer.Observable, observer.Observer):
    """ServiceInstance task which kills the service process."""

    NOTIFY_SUCCESS              = 'stop_task_notify_success'
    NOTIFY_FAIL                 = 'stop_task_notify_fail'

    command_template            = "cat %s | xargs kill"

    def __init__(self, id, node, pid_filename):
        super(ServiceInstanceStopTask, self).__init__()
        self.id             = id
        self.node           = node
        self.pid_filename   = pid_filename

    def kill(self):
        kill_command    =  self.command_template % self.pid_filename
        action          = ActionCommand("%s.stop" % self.id, kill_command)
        self.watch(action)

        try:
            return self.node.run(action)
        except node.Error, e:
            log.warn("Failed to kill instance %s: %r", self.id, e)

    def handle_action_event(self, action, event):
        if event == ActionCommand.COMPLETE:
            return self._handle_complete(action)

        if event == ActionCommand.FAILSTART:
            log.warn("Failed to start kill command for %s", self.id)
            self.notify(self.NOTIFY_FAIL)

    handler = handle_action_event

    def _handle_complete(self, action):
        if action.has_failed:
            log.error("Failed to stop service instance %s", self)

        self.notify(self.NOTIFY_SUCCESS)

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)


class ServiceInstanceStartTask(observer.Observable, observer.Observer):
    """ServiceInstance task which starts the service process."""

    NOTIFY_DOWN             = 'start_task_notify_down'
    NOTIFY_STARTED          = 'start_task_notify_started'

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
            log.warn("Failed to start %s: %r", self.id, e)
            self.notify(self.NOTIFY_DOWN)

    def handle_action_event(self, action, event):
        """Watch for events from the ActionCommand."""
        if event == ActionCommand.EXITING:
            return self._handle_action_exit(action)
        if event == ActionCommand.FAILSTART:
            log.warn("Failed to start service %s on %s.", self.id, self.node)
            self.notify(self.NOTIFY_DOWN)
    handler = handle_action_event

    def _handle_action_exit(self, action):
        event = self.NOTIFY_DOWN if action.has_failed else self.NOTIFY_STARTED
        self.notify(event)


# TODO: object to record failures/stdout/stderr


def build_instance_context(name, node, number, parent_context):
    context = {
        'instance_number': number,
        'name': name,
        'node': node.hostname
    }
    return command_context.CommandContext(context, parent_context)


class ServiceInstanceState(state.NamedEventState):
    """Event state subclass for service instances"""


class ServiceInstance(observer.Observer):
    """An instance of a service."""

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
    STATE_STOPPING['stop_fail']         = STATE_UNKNOWN
    STATE_UP['stop']                    = STATE_STOPPING
    STATE_UP['monitor']                 = STATE_MONITORING
    STATE_DOWN['start']                 = STATE_STARTING

    def __init__(self, config, node, instance_number, context):
        self.config             = config
        self.node               = node
        self.instance_number    = instance_number
        self.context            = context
        self.id                 = "%s.%s" % (config.name, self.instance_number)

        start_state             = ServiceInstance.STATE_DOWN
        self.machine            = state.StateMachine(start_state, delegate=self)

    def create_tasks(self):
        """Create and watch tasks."""
        pid_file_template       = self.config.pid_file
        interval                = self.config.monitor_interval
        pid_file                = self._create_pid_file(pid_file_template)
        self.monitor_task       = ServiceInstanceMonitorTask(
                                    self.id, self.node, interval, pid_file)
        self.start_task         = ServiceInstanceStartTask(self.id, self.node)
        self.stop_task          = ServiceInstanceStopTask(
                                    self.id, self.node, pid_file)
        self.watch(self.monitor_task)
        self.watch(self.start_task)
        self.watch(self.stop_task)

    @classmethod
    def create(cls, config, node, instance_number, context):
        instance = cls(config, node, instance_number, context)
        instance.create_tasks()
        return instance

    # TODO: remove once moved down stack
    def _create_pid_file(self, pid_file_template):
        try:
            return pid_file_template % self.context
        except KeyError:
            msg = "Failed to render pid file template: %r" % pid_file_template
            log.error(msg)
            # TODO: put this instance in a disabled state so a check for None
            # does not have to be performed later
            # TODO: register error somewhere
        return None

    # TODO: remove error handling once moved down stack
    @property
    def command(self):
        try:
            return self.config.command % self.context
        except KeyError:
            msg = "Failed to render service command for service %s: %s"
            log.error(msg % (self.id, self.config.command))

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

    event_to_transition_map = {
        ServiceInstanceMonitorTask.NOTIFY_START:        "monitor",
        ServiceInstanceMonitorTask.NOTIFY_FAILED:       "monitor_fail",
        ServiceInstanceMonitorTask.NOTIFY_DOWN:         "down",
        ServiceInstanceMonitorTask.NOTIFY_UP:           "up",
        ServiceInstanceStartTask.NOTIFY_DOWN:           "down",
        ServiceInstanceStopTask.NOTIFY_FAIL:            "stop_fail",
    }

    def handler(self, _, event):
        """Handle events from ServiceInstance tasks."""
        if event in self.event_to_transition_map:
            self.machine.transition(self.event_to_transition_map[event])

        if event == ServiceInstanceStartTask.NOTIFY_STARTED:
            self._handle_start_task_complete()

        if event == ServiceInstanceStopTask.NOTIFY_SUCCESS:
            self.monitor_task.cancel()

    def _handle_start_task_complete(self):
        if self.machine.state == ServiceInstance.STATE_STARTING:
            log.info("Start for %s complete, starting monitor" % self.id)
            self.monitor_task.queue()
            return

        self.stop_task.kill()

    @property
    def state_data(self):
        return {
            'instance_number': self.instance_number,
            'node': self.node.hostname
        }

    def __str__(self):
        return "%s:%s" % (self.__class__.__name__, self.id)


class ServiceInstanceCollection(object):
    """A collection of ServiceInstances."""

    def __init__(self, config, node_pool, context):
        self.config             = config
        self.node_pool          = node_pool
        self.instances          = []
        self.context            = command_context.CommandContext(next=context)

        self.instances_proxy    = proxy.CollectionProxy(
            lambda: self.instances,
            [
                proxy.func_proxy('stop',    all),
                proxy.func_proxy('zap',     all),
                proxy.func_proxy('start',   all)
            ])

    def load_state(self):
        pass
        # TODO:

    def update_config(self, config):
        self.config = config
        # TODO: restart instances

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
        context             = build_instance_context(
                        self.config.name, node, instance_number, self.context)
        service_instance    = ServiceInstance.create(
                        self.config, node, instance_number, context)
        return service_instance

    def next_instance_number(self):
        """Return the next available instance number."""
        instance_nums = set(inst.instance_number for inst in self.instances)
        for num in xrange(self.config.count):
            if num not in instance_nums:
                return num

    @property
    def missing(self):
        return self.config.count - len(self.instances)

    @property
    def extra(self):
        return len(self.instances) - self.config.count

    def __len__(self):
        return len(self.instances)

    def __getattr__(self, item):
        return self.instances_proxy.perform(item)