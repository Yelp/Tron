from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import operator
import signal

from tron import actioncommand
from tron import command_context
from tron import eventloop
from tron import node
from tron.actioncommand import ActionCommand
from tron.utils import iteration
from tron.utils import observer
from tron.utils import proxy
from tron.utils import state


log = logging.getLogger(__name__)


MIN_HANG_CHECK_SECONDS = 10
HANG_CHECK_DELAY_RATIO = 0.9


def create_hang_check(delay, func):
    delay = max(delay * HANG_CHECK_DELAY_RATIO, MIN_HANG_CHECK_SECONDS)
    return eventloop.UniqueCallback(delay, func)


def build_action(task, command=None):
    """Create an action for a task which is an Observer, and which has
    properties 'task_name', 'command', and 'buffer_store'.
    """
    name = '%s.%s' % (task.id, task.task_name)
    command = command or task.command
    action = ActionCommand(name, command, serializer=task.buffer_store)
    task.watch(action)
    return action


def run_action(task, action):
    """Run an action for a task which is an Observable and has has properties
    'node' and 'NOTIFY_FAILED'. Returns True on success, and calls
    task.notify(NOTIF_FAILURE) and returns False on failure.
    """
    log.debug("Executing %s on %s for %s" % (action, task.node, task))
    try:
        task.node.submit_command(action)
        return True
    except node.Error as e:
        log.error("Failed to run %s on %s: %s", action, task.node, e)
        stream = task.buffer_store.open(actioncommand.ActionCommand.STDERR)
        stream.write("Node run failure for %s: %s" % (task.task_name, e))
        task.notify(task.NOTIFY_FAILED)


def get_failures_from_task(task):
    return task.buffer_store.get_stream(actioncommand.ActionCommand.STDERR)


class ServiceInstanceMonitorTask(observer.Observable, observer.Observer):
    """ServiceInstance task which monitors the service process and
    notifies observers if the process is up or down. Also monitors itself
    to ensure this check does not hang.

    This task will be a no-op if interval is Falsy.
    """
    NOTIFY_START = 'monitor_task_notify_start'
    NOTIFY_FAILED = 'monitor_task_notify_failed'
    NOTIFY_UP = 'monitor_task_notify_up'
    NOTIFY_DOWN = 'monitor_task_notify_down'

    command_template = "cat %s | xargs kill -0"
    task_name = 'monitor'

    def __init__(self, id, node, interval, pid_filename):
        super(ServiceInstanceMonitorTask, self).__init__()
        self.interval = interval or 0
        self.node = node
        self.id = id
        self.pid_filename = pid_filename
        self.action = actioncommand.CompletedActionCommand
        self.callback = eventloop.UniqueCallback(
            self.interval, self.run,
        )
        self.hang_check_callback = create_hang_check(self.interval, self.fail)
        self.buffer_store = actioncommand.StringBufferStore()

    def queue(self):
        """Queue this task to run after monitor_interval."""
        log.info("Queueing %s" % self)
        self.callback.start()

    def run(self):
        """Run the monitoring command."""
        if not self.action.is_done:
            log.warn("%s: Monitor action already exists.", self)
            return

        self.notify(self.NOTIFY_START)
        self.action = build_action(self)
        if run_action(self, self.action):
            self.hang_check_callback.start()

    @property
    def command(self):
        return self.command_template % self.pid_filename

    def handle_action_event(self, action, event):
        if action != self.action:
            msg = "Ignoring %s %s, action was cleared due to hang check."
            log.warn(msg % (action, event))
            return

        if event == ActionCommand.EXITING:
            self.hang_check_callback.cancel()
            self._handle_action_exit()
        if event == ActionCommand.FAILSTART:
            self.hang_check_callback.cancel()
            self.queue()
            self.notify(self.NOTIFY_FAILED)

    handler = handle_action_event

    def _handle_action_exit(self):
        log.debug("%s exit, failure: %r", self, self.action.is_failed)
        if self.action.is_unknown:
            self.queue()
            self.notify(self.NOTIFY_FAILED)
            return
        if self.action.is_failed:
            self.notify(self.NOTIFY_DOWN)
            return

        self.notify(self.NOTIFY_UP)
        self.queue()
        self.buffer_store.clear()

    def cancel(self):
        """Cancel the monitor callback and hang check."""
        self.callback.cancel()
        self.hang_check_callback.cancel()

    def fail(self):
        log.warning("%s is still running %s.", self, self.action)
        self.node.stop(self.action)
        self.action.write_stderr("Monitoring failed")
        self.notify(self.NOTIFY_FAILED)
        self.action = actioncommand.CompletedActionCommand

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)


class ServiceInstanceStopTask(observer.Observable, observer.Observer):
    """ServiceInstance task which kills the service process."""

    NOTIFY_SUCCESS = 'stop_task_notify_success'
    NOTIFY_FAILED = 'stop_task_notify_fail'

    command_template = "cat %s | xargs kill -%s"
    task_name = 'stop'

    def __init__(self, id, node, pid_filename):
        super(ServiceInstanceStopTask, self).__init__()
        self.id = id
        self.node = node
        self.pid_filename = pid_filename
        self.buffer_store = actioncommand.StringBufferStore()
        self.command = None

    def get_command(self, signal):
        return self.command_template % (self.pid_filename, signal)

    def stop(self):
        self.command = self.get_command(signal.SIGTERM)
        return run_action(self, build_action(self))

    def kill(self):
        self.command = self.get_command(signal.SIGKILL)
        return run_action(self, build_action(self))

    def handle_action_event(self, action, event):
        if event == ActionCommand.COMPLETE:
            return self._handle_complete(action)

        if event == ActionCommand.FAILSTART:
            log.warn("Failed to start kill command for %s", self.id)
            self.notify(self.NOTIFY_FAILED)

    handler = handle_action_event

    def _handle_complete(self, action):
        if action.is_failed:
            log.error("Failed to stop %s", self)

        self.notify(self.NOTIFY_SUCCESS)

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)


class ServiceInstanceStartTask(observer.Observable, observer.Observer):
    """ServiceInstance task which starts the service process."""

    NOTIFY_FAILED = 'start_task_notify_failed'
    NOTIFY_STARTED = 'start_task_notify_started'

    task_name = 'start'

    def __init__(self, id, node):
        super(ServiceInstanceStartTask, self).__init__()
        self.id = id
        self.node = node
        self.buffer_store = actioncommand.StringBufferStore()

    def start(self, command):
        """Start the service command. command is rendered by the caller."""
        return run_action(self,  build_action(self, command=command))

    def handle_action_event(self, action, event):
        """Watch for events from the ActionCommand."""
        if event == ActionCommand.EXITING:
            return self._handle_action_exit(action)
        if event == ActionCommand.FAILSTART:
            log.warn("Failed to start service %s on %s.", self.id, self.node)
            self.notify(self.NOTIFY_FAILED)
    handler = handle_action_event

    def _handle_action_exit(self, action):
        event = self.NOTIFY_FAILED if action.is_failed else self.NOTIFY_STARTED
        self.notify(event)

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self.id)


class ServiceInstanceState(state.NamedEventState):
    """Event state subclass for service instances"""


class ServiceInstance(observer.Observer):
    """An instance of a service."""

    STATE_DOWN = ServiceInstanceState("down")
    STATE_UP = ServiceInstanceState("up")
    STATE_FAILED = ServiceInstanceState(
        "failed",
        stop=STATE_DOWN,
        up=STATE_UP,
    )
    STATE_STOPPING = ServiceInstanceState(
        "stopping",
        down=STATE_DOWN,
        stop_fail=STATE_FAILED,
    )
    STATE_MONITORING = ServiceInstanceState(
        "monitoring",
        down=STATE_FAILED,
        stop=STATE_STOPPING,
        up=STATE_UP,
    )
    STATE_STARTING = ServiceInstanceState(
        "starting",
        down=STATE_FAILED,
        monitor=STATE_MONITORING,
        stop=STATE_STOPPING,
    )
    STATE_UNKNOWN = ServiceInstanceState(
        "unknown",
        monitor=STATE_MONITORING,
        stop=STATE_DOWN,
    )

    STATE_MONITORING['monitor_fail'] = STATE_UNKNOWN
    STATE_UP['stop'] = STATE_STOPPING
    STATE_UP['monitor'] = STATE_MONITORING
    STATE_DOWN['start'] = STATE_STARTING
    STATE_DOWN['monitor'] = STATE_MONITORING

    context_class = command_context.ServiceInstanceContext

    def __init__(self, config, node, instance_number, parent_context):
        self.config = config
        self.node = node
        self.instance_number = instance_number
        self.id = "%s.%s" % (config.name, self.instance_number)

        start_state = ServiceInstance.STATE_DOWN
        self.machine = state.StateMachine(start_state, delegate=self)
        self.parent_context = parent_context
        self.context = command_context.build_context(self, parent_context)
        self.failures = []

    def create_tasks(self):
        """Create and watch tasks."""
        interval = self.config.monitor_interval
        pid_file = self.pid_filename
        self.monitor_task = ServiceInstanceMonitorTask(
            self.id, self.node, interval, pid_file,
        )
        self.start_task = ServiceInstanceStartTask(self.id, self.node)
        self.stop_task = ServiceInstanceStopTask(
            self.id, self.node, pid_file,
        )
        self.watch(self.monitor_task)
        self.watch(self.start_task)
        self.watch(self.stop_task)

    @classmethod
    def create(cls, config, node, instance_number, context):
        instance = cls(config, node, instance_number, context)
        instance.create_tasks()
        return instance

    @property
    def pid_filename(self):
        return self.config.pid_file % self.context

    @property
    def command(self):
        return self.config.command % self.context

    def start(self):
        if not self.machine.transition('start'):
            return False
        return self.start_task.start(self.command)

    def stop(self):
        return self.perform_stop_task(self.stop_task.stop)

    def kill(self):
        return self.perform_stop_task(self.stop_task.kill)

    def perform_stop_task(self, method):
        if self.machine.check('stop'):
            method()
            self.monitor_task.cancel()
            return self.machine.transition('stop')

    def restore(self):
        self.monitor_task.run()

    event_to_transition_map = {
        ServiceInstanceMonitorTask.NOTIFY_START:        'monitor',
        ServiceInstanceMonitorTask.NOTIFY_FAILED:       'monitor_fail',
        ServiceInstanceMonitorTask.NOTIFY_DOWN:         'down',
        ServiceInstanceMonitorTask.NOTIFY_UP:           'up',
        ServiceInstanceStopTask.NOTIFY_FAILED:          'stop_fail',
        ServiceInstanceStopTask.NOTIFY_SUCCESS:         'down',
    }

    def handler(self, task, event):
        """Handle events from ServiceInstance tasks."""
        log.debug("Service instance event %s on task %s" % (event, task))
        if event in self.event_to_transition_map:
            self.machine.transition(self.event_to_transition_map[event])

        if event in (ServiceInstanceStartTask.NOTIFY_STARTED, ServiceInstanceStartTask.NOTIFY_FAILED):
            self._handle_start_task_complete()

        if event == task.NOTIFY_FAILED:
            self.failures.append(get_failures_from_task(task))

        if event == ServiceInstanceMonitorTask.NOTIFY_FAILED and self.config.monitor_retries and self.config.monitor_retries < len(self.failures):
            log.info("Too many monitor failures(%d) of %s" %
                     (len(self.failures), task))
            self.monitor_task.cancel()
            self.machine.transition('stop')

        if event == ServiceInstanceMonitorTask.NOTIFY_UP:
            self.failures = []

    def _handle_start_task_complete(self):
        if self.machine.state != ServiceInstance.STATE_STARTING:
            self.stop_task.stop()
            return

        log.info("Start for %s complete, starting monitor" % self.id)
        self.monitor_task.queue()

    @property
    def state_data(self):
        return dict(
            instance_number=self.instance_number,
            node=self.node.hostname,
        )

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
        self.config = config
        self.node_pool = node_pool
        self.instances = []
        self.context = context

        self.instances_proxy = proxy.CollectionProxy(
            lambda: self.instances, [
                proxy.func_proxy('stop',    iteration.list_all),
                proxy.func_proxy('kill',    iteration.list_all),
                proxy.func_proxy('start',   iteration.list_all),
                proxy.func_proxy('restore', iteration.list_all),
                proxy.attr_proxy('state_data', list),
            ],
        )

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
        log.info("Creating {} instances for {}".format(self.missing, self))
        return self._build_and_sort(builder, range(self.missing))

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
            log.info("Building and adding {} to {}".format(instance, self))
            self.instances.append(instance)
            return instance
        instances = list(build_and_add(item) for item in seq)
        self.sort()
        return instances

    def next_instance_number(self):
        """Return the next available instance number."""
        instance_nums = {inst.instance_number for inst in self.instances}
        for num in range(self.config.count):
            if num not in instance_nums:
                return num

    def get_by_number(self, instance_number):
        for instance in self.instances:
            if instance.instance_number == instance_number:
                return instance

    @property
    def missing(self):
        return self.config.count - len(self.instances)

    def all(self, state):
        if len(self.instances) != self.config.count:
            return False
        return self._all_states_match([state])

    def is_starting(self):
        states = {
            ServiceInstance.STATE_STARTING,
            ServiceInstance.STATE_MONITORING,
            ServiceInstance.STATE_UP,
        }
        return self._all_states_match(states)

    def is_up(self):
        states = {
            ServiceInstance.STATE_MONITORING,
            ServiceInstance.STATE_UP,
        }
        return self._all_states_match(states)

    def _all_states_match(self, states):
        return all(inst.get_state() in states for inst in self.instances)

    def __len__(self):
        return len(self.instances)

    def __getattr__(self, item):
        return self.instances_proxy.perform(item)

    def __eq__(self, other):
        return (
            self.node_pool == other.node_pool and
            self.config == other.config
        )

    def __ne__(self, other):
        return not self == other

    def __iter__(self):
        return iter(self.instances)

    def __str__(self):
        return "ServiceInstanceCollection:%s" % self.config.name
