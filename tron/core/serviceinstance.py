import logging

import operator

from tron import command_context, actioncommand
from tron import eventloop
from tron import node
from tron.actioncommand import ActionCommand
from tron.utils import observer, proxy, iteration
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
        self.action                 = actioncommand.CompletedActionCommand
        self.callback               = eventloop.NullCallback
        self.hang_check_callback    = eventloop.NullCallback

    # TODO: a fast queue for starting/restoring
    def queue(self):
        """Queue this task to run after monitor_interval."""
        if not self.interval or self.callback.active():
            return
        log.info("Queueing %s" % self)
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

    # TODO: add tracking for stderr
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
            return True
        except node.Error, e:
            log.error("Failed to run %s: %r", self, e)
            self.notify(self.NOTIFY_FAILED)

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

    NOTIFY_FAILED           = 'start_task_notify_failed'
    NOTIFY_STARTED          = 'start_task_notify_started'

    def __init__(self, id, node):
        super(ServiceInstanceStartTask, self).__init__()
        self.id             = id
        self.node           = node
        self.buffer_store   = actioncommand.StringBufferStore()

    def _build_and_watch(self, command):
        action = actioncommand.ActionCommand(
                    "%s.start" % self.id, command, serializer=self.buffer_store)
        self.watch(action)
        return action

    def start(self, command):
        """Start the service command. It is important that command is passed
        in here when the start is called because it must be rendered using
        a context where the datetime is the current datetime.
        """
        action = self._build_and_watch(command)

        try:
            self.node.run(action)
            return True
        except node.Error, e:
            log.warn("Failed to start %s: %r", self.id, e)
            self.notify(self.NOTIFY_FAILED)

    def handle_action_event(self, action, event):
        """Watch for events from the ActionCommand."""
        if event == ActionCommand.EXITING:
            return self._handle_action_exit(action)
        if event == ActionCommand.FAILSTART:
            log.warn("Failed to start service %s on %s.", self.id, self.node)
            self.notify(self.NOTIFY_FAILED)
    handler = handle_action_event

    def _handle_action_exit(self, action):
        event = self.NOTIFY_FAILED if action.has_failed else self.NOTIFY_STARTED
        self.notify(event)

    def get_failure_message(self):
        return self.buffer_store.get_stream(actioncommand.ActionCommand.STDERR)


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
                            down=STATE_DOWN,
                            stop_fail=STATE_FAILED)
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
    STATE_DOWN['monitor']               = STATE_MONITORING

    context_class               = command_context.ServiceInstanceContext

    # TODO: add start time?
    def __init__(self, config, node, instance_number, parent_context):
        self.config             = config
        self.node               = node
        self.instance_number    = instance_number
        self.id                 = "%s.%s" % (config.name, self.instance_number)

        start_state             = ServiceInstance.STATE_DOWN
        self.machine            = state.StateMachine(start_state, delegate=self)
        self.context = command_context.build_context(self, parent_context)
        self.failures           = []

    def create_tasks(self):
        """Create and watch tasks."""
        interval                = self.config.monitor_interval
        pid_file                = self.config.pid_file % self.context
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

    @property
    def command(self):
        return self.config.command % self.context

    def start(self):
        if not self.machine.transition('start'):
            return False
        return self.start_task.start(self.command)

    def stop(self):
        if self.machine.check('stop'):
            self.stop_task.kill()
            self.monitor_task.cancel()
            return self.machine.transition('stop')

    def restore(self):
        self.monitor_task.run()

    event_to_transition_map = {
        ServiceInstanceMonitorTask.NOTIFY_START:        'monitor',
        ServiceInstanceMonitorTask.NOTIFY_FAILED:       'monitor_fail',
        ServiceInstanceMonitorTask.NOTIFY_DOWN:         'down',
        ServiceInstanceMonitorTask.NOTIFY_UP:           'up',
        ServiceInstanceStopTask.NOTIFY_FAIL:            'stop_fail',
        ServiceInstanceStopTask.NOTIFY_SUCCESS:         'down',
    }

    def handler(self, _, event):
        """Handle events from ServiceInstance tasks."""
        if event in self.event_to_transition_map:
            self.machine.transition(self.event_to_transition_map[event])

        if event == ServiceInstanceStartTask.NOTIFY_STARTED:
            self._handle_start_task_complete()

        if event == ServiceInstanceStartTask.NOTIFY_FAILED:
            self.machine.transition('down')
            self.failures.append(self.start_task.get_failure_message())

    def _handle_start_task_complete(self):
        if self.machine.state != ServiceInstance.STATE_STARTING:
            self.stop_task.kill()
            return

        log.info("Start for %s complete, starting monitor" % self.id)
        self.monitor_task.queue()

    @property
    def state_data(self):
        return dict(instance_number=self.instance_number,
                    node=self.node.hostname)

    def get_observable(self):
        return self.machine

    def get_state(self):
        return self.machine.state

    def __str__(self):
        return "%s:%s" % (self.__class__.__name__, self.id)


# TODO: shouldn't this check which nodes are not used to properly
# balance across nodes? But doing this makes it less resilient to
# failures of a node
def node_selector(node_pool, hostname=None):
    """Attempt to retrieve the node by hostname.  If that node is not
    available, or hostname is None, then pick one the next node.
    """
    next_node = node_pool.next_round_robin
    if not hostname:
        return next_node()

    return node_pool.get_by_hostname(hostname) or next_node()


class ServiceInstanceCollection(object):
    """A collection of ServiceInstances."""

    def __init__(self, config, node_pool, context):
        self.config             = config
        self.node_pool          = node_pool
        self.instances          = []
        self.context            = context

        self.instances_proxy    = proxy.CollectionProxy(
            lambda: self.instances, [
                proxy.func_proxy('stop',    iteration.list_all),
                proxy.func_proxy('start',   iteration.list_all),
                proxy.func_proxy('restore', iteration.list_all),
                proxy.attr_proxy('state_data', list)
            ])

    def clear_failed(self):
        self._clear(ServiceInstance.STATE_FAILED)

    def clear_down(self):
        self._clear(ServiceInstance.STATE_DOWN)

    def _clear(self, state):
        log.info("clear instances in state %s from %s", state, self)
        self.instances = [i for i in self.instances if i.get_state() != state]

    def sort(self):
        self.instances.sort(key=operator.attrgetter('instance_number'))

    def create_missing(self):
        """Create instances until this collection contains the configured
        number of instances.
        """
        def builder(_):
            node = node_selector(self.node_pool)
            return self._build_instance(node, self.next_instance_number())
        log.info("Creating %s instances for %s" % (self.missing, self))
        return self._build_and_sort(builder, xrange(self.missing))

    def _build_instance(self, node, number):
        return ServiceInstance.create(self.config, node, number, self.context)

    def restore_state(self, state_data):
        assert not self.instances
        def builder(instance_state):
            node = node_selector(self.node_pool, instance_state['node'])
            return self._build_instance(node, instance_state['instance_number'])

        return self._build_and_sort(builder, state_data)

    def _build_and_sort(self, builder, seq):
        def build_and_add(item):
            instance = builder(item)
            log.info("Building and adding %s to %s" % (instance, self))
            self.instances.append(instance)
            return instance
        instances = list(build_and_add(item) for item in seq)
        self.sort()
        return instances

    def next_instance_number(self):
        """Return the next available instance number."""
        instance_nums = set(inst.instance_number for inst in self.instances)
        for num in xrange(self.config.count):
            if num not in instance_nums:
                return num

    # TODO: test case
    def get_by_number(self, instance_number):
        for instance in self.instances:
            if instance.instance_number == instance_number:
                return instance

    @property
    def missing(self):
        return self.config.count - len(self.instances)

    @property
    def extra(self):
        return len(self.instances) - self.config.count

    def all(self, state):
        if len(self.instances) != self.config.count:
            return False
        return self._all_states_match([state])

    def is_starting(self):
        states = set([ServiceInstance.STATE_STARTING,
                      ServiceInstance.STATE_MONITORING,
                      ServiceInstance.STATE_UP])
        return self._all_states_match(states)

    def _all_states_match(self, states):
        return all(inst.get_state() in states for inst in self.instances)

    def __len__(self):
        return len(self.instances)

    def __getattr__(self, item):
        return self.instances_proxy.perform(item)

    # TODO: I believe context can be removed from here because underlying next
    # objects are replaced
    def __eq__(self, other):
        return (self.node_pool == other.node_pool and
                self.config == other.config and
                self.context == other.context)

    def __ne__(self, other):
        return not self == other

    def __iter__(self):
        return iter(self.instances)

    def __str__(self):
        return "ServiceInstanceCollection:%s" % self.config.name