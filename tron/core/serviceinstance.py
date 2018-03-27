from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import signal

from tron import actioncommand
from tron import eventloop
from tron import node
from tron.actioncommand import ActionCommand
from tron.utils import observer
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
