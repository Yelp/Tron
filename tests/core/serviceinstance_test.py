import contextlib
import mock
from testify import setup, assert_equal, TestCase, run, setup_teardown
from testify.assertions import assert_in
from tests.assertions import assert_length

from tron import node, eventloop, command_context
from tron.actioncommand import ActionCommand
from tron.core import serviceinstance
from tron.utils import state


class ServiceInstanceMonitorTaskTestCase(TestCase):

    @setup_teardown
    def setup_task(self):
        self.interval = 20
        self.filename = "/tmp/filename"
        mock_node = mock.create_autospec(node.Node)
        self.task = serviceinstance.ServiceInstanceMonitorTask(
            "id", mock_node, self.interval, self.filename)
        self.task.notify = mock.create_autospec(self.task.notify)
        self.task.watch = mock.create_autospec(self.task.watch)
        self.mock_eventloop = None
        with mock.patch('tron.core.serviceinstance.eventloop') as self.mock_eventloop:
            yield

    def test_queue(self):
        self.task.queue()
        self.mock_eventloop.call_later.assert_called_with(self.interval, self.task.run)

    def test_queue_no_interval(self):
        self.task.interval = 0
        self.task.queue()
        assert_equal(self.mock_eventloop.call_later.call_count, 0)

    def test_queue_has_active_callback(self):
        self.task.callback = mock.create_autospec(eventloop.Callback)
        self.task.callback.active.return_value = True
        self.task.queue()
        assert_equal(self.mock_eventloop.call_later.call_count, 0)

    def test_run(self):
        self.task.run()

        self.task.notify.assert_called_with(self.task.NOTIFY_START)
        self.task.node.run.assert_called_with(self.task.action)
        self.mock_eventloop.call_later.assert_called_with(
            self.interval * 0.8, self.task._run_hang_check, self.task.action)

    def test_run_action_exists(self):
        self.task.action = mock.create_autospec(ActionCommand, is_complete=False)
        with mock.patch('tron.core.serviceinstance.log', autospec=True) as mock_log:
            self.task.run()
            assert_equal(mock_log.warn.call_count, 1)

    def test_run_failed(self):
        self.task._run_action = mock.create_autospec(
            self.task._run_action, return_value=False)
        self.task.run()
        assert_equal(self.mock_eventloop.call_later.call_count, 0)

    def test_build_action(self):
        action = self.task._build_action()
        self.task.watch.assert_called_with(action)
        assert_in(self.filename, action.command)

    def test_run_action(self):
        self.task.action = True
        assert self.task._run_action()
        self.task.node.run.assert_called_with(self.task.action)

    def test_run_action_failed(self):
        self.task.node.run.side_effect = node.Error
        assert not self.task._run_action()
        self.task.notify.assert_called_with(self.task.NOTIFY_FAILED)

    def test_handle_action_event_failstart(self):
        self.task.handle_action_event(None, ActionCommand.FAILSTART)
        self.task.notify.assert_called_with(self.task.NOTIFY_FAILED)
        self.mock_eventloop.call_later.assert_called_with(self.interval, self.task.run)

    def test_queue_hang_check(self):
        self.task._queue_hang_check()
        assert_equal(self.task.hang_check_callback,
                self.mock_eventloop.call_later.return_value)

    def test_run_hang_check(self):
        self.task.hang_check_callback = True
        action = mock.create_autospec(ActionCommand)
        self.task._run_hang_check(action)
        assert_equal(self.task.notify.call_count, 0)

    def test_run_hang_check_failed(self):
        self.task.hang_check_callback = True
        self.task.action = action = mock.create_autospec(ActionCommand)
        self.task._run_hang_check(action)
        assert_equal(self.task.hang_check_callback,
            self.mock_eventloop.call_later.return_value)
        self.task.notify.assert_called_with(self.task.NOTIFY_FAILED)

    def test_handle_action_exit_up(self):
        self.task.action = mock.create_autospec(ActionCommand)
        self.task.action.has_failed = False
        self.task.queue = mock.create_autospec(self.task.queue)
        self.task._handle_action_exit()
        self.task.notify.assert_called_with(self.task.NOTIFY_UP)
        self.task.queue.assert_called_with()

    def test_handle_action_exit_down(self):
        self.task.action = mock.create_autospec(ActionCommand)
        self.task.queue = mock.create_autospec(self.task.queue)
        self.task._handle_action_exit()
        self.task.notify.assert_called_with(self.task.NOTIFY_DOWN)
        assert_equal(self.task.queue.call_count, 0)


class ServiceInstanceStopTaskTestCase(TestCase):

    @setup
    def setup_task(self):
        self.node = mock.create_autospec(node.Node)
        self.pid_filename = '/tmp/filename'
        self.task = serviceinstance.ServiceInstanceStopTask(
            'id', self.node, self.pid_filename)
        self.task.watch = mock.create_autospec(self.task.watch)
        self.task.notify = mock.create_autospec(self.task.notify)

    def test_kill_success(self):
        patcher = mock.patch('tron.core.serviceinstance.log', autospec=True)
        with patcher as mock_log:
            deferred = self.task.kill()
            assert_equal(mock_log.warn.call_count, 0)
            assert_equal(deferred, self.node.run.return_value)

    def test_kill_failed(self):
        self.node.run.side_effect = node.Error
        patcher = mock.patch('tron.core.serviceinstance.log', autospec=True)
        with patcher as mock_log:
            assert not self.task.kill()
            assert_equal(mock_log.warn.call_count, 1)

    def test_handle_action_event_complete(self):
        action = mock.create_autospec(ActionCommand)
        event = ActionCommand.COMPLETE
        self.task.handle_action_event(action, event)
        self.task.notify.assert_called_with(self.task.NOTIFY_SUCCESS)

    def test_handle_action_event_failstart(self):
        action = mock.create_autospec(ActionCommand)
        event = ActionCommand.FAILSTART
        self.task.handle_action_event(action, event)
        self.task.notify.assert_called_with(self.task.NOTIFY_FAIL)

    def test_handle_complete_failed(self):
        action = mock.create_autospec(ActionCommand, has_failed=True)
        with mock.patch('tron.core.serviceinstance.log', autospec=True) as mock_log:
            self.task._handle_complete(action)
            assert_equal(mock_log.error.call_count, 1)

        self.task.notify.assert_called_with(self.task.NOTIFY_SUCCESS)

    def test_handle_complete(self):
        action = mock.create_autospec(ActionCommand, has_failed=False)
        self.task._handle_complete(action)
        self.task.notify.assert_called_with(self.task.NOTIFY_SUCCESS)


class ServiceInstanceStartTaskTestCase(TestCase):

    @setup
    def setup_task(self):
        self.node = mock.create_autospec(node.Node)
        self.task = serviceinstance.ServiceInstanceStartTask('id', self.node)
        self.task.notify = mock.create_autospec(self.task.notify)
        self.task.watch = mock.create_autospec(self.task.watch)

    def test_start(self):
        command = 'the command'
        with mock.patch('tron.core.serviceinstance.ActionCommand') as mock_ac:
            self.task.start(command)
            self.task.watch.assert_called_with(mock_ac.return_value)
            self.node.run.assert_called_with(mock_ac.return_value)

    def test_start_failed(self):
        command = 'the command'
        self.node.run.side_effect = node.Error
        self.task.start(command)
        self.task.notify.assert_called_with(self.task.NOTIFY_DOWN)

    def test_handle_action_event_exit(self):
        action = mock.create_autospec(ActionCommand)
        event = ActionCommand.EXITING
        self.task.handle_action_event(action, event)
        self.task.notify(self.task.NOTIFY_STARTED)

    def test_handle_action_event_failstart(self):
        action = mock.create_autospec(ActionCommand)
        event = ActionCommand.FAILSTART
        patcher = mock.patch('tron.core.serviceinstance.log', autospec=True)
        with patcher as mock_log:
            self.task.handle_action_event(action, event)
            assert_equal(mock_log.warn.call_count, 1)

    def test_handle_action_exit_fail(self):
        action = mock.create_autospec(ActionCommand, has_failed=True)
        self.task._handle_action_exit(action)
        self.task.notify.assert_called_with(self.task.NOTIFY_DOWN)

    def test_handle_action_exit_success(self):
        action = mock.create_autospec(ActionCommand, has_failed=False)
        self.task._handle_action_exit(action)
        self.task.notify.assert_called_with(self.task.NOTIFY_STARTED)


class ServiceInstanceTestCase(TestCase):

    @setup
    def setup_instance(self):
        self.config = mock.MagicMock()
        self.node = mock.create_autospec(node.Node, hostname='hostname')
        self.number = 5
        self.context = mock.create_autospec(command_context.CommandContext)
        self.instance = serviceinstance.ServiceInstance(
            self.config, self.node, self.number, self.context)
        self.instance.machine = mock.create_autospec(state.StateMachine, state=None)
        self.instance.start_task = mock.create_autospec(
            serviceinstance.ServiceInstanceStartTask)
        self.instance.stop_task = mock.create_autospec(
            serviceinstance.ServiceInstanceStopTask)
        self.instance.monitor_task = mock.create_autospec(
            serviceinstance.ServiceInstanceMonitorTask)
        self.instance.watch = mock.create_autospec(self.instance.watch)

    def test_create_tasks(self):
        self.instance.create_tasks()
        assert_equal(self.instance.watch.mock_calls, [
            mock.call(self.instance.monitor_task),
            mock.call(self.instance.start_task),
            mock.call(self.instance.stop_task),
        ])

    def test_start_invalid_state(self):
        self.instance.machine.transition.return_value = False
        self.instance.start()
        assert_equal(self.instance.start_task.start.call_count, 0)

    def test_start(self):
        self.instance.start()
        self.instance.start_task.start.assert_called_with(self.instance.command)

    def test_stop_invalid_state(self):
        self.instance.machine.check.return_value = False
        self.instance.stop()
        assert not self.instance.machine.transition.call_count

    def test_stop(self):
        self.instance.stop()
        self.instance.stop_task.kill.assert_called_with()
        self.instance.machine.transition.assert_called_with('stop')

    def test_zap(self):
        self.instance.zap()
        assert_equal(self.instance.machine.transition.mock_calls, [
            mock.call('stop'), mock.call('down')])
        self.instance.monitor_task.cancel.assert_called_with()

    def test_handler_transition_map(self):
        obs = mock.Mock()
        event = serviceinstance.ServiceInstanceMonitorTask.NOTIFY_START
        self.instance.handler(obs, event)
        self.instance.machine.transition.assert_called_with("monitor")

    def test_handler_notify_started(self):
        obs = mock.Mock()
        event = serviceinstance.ServiceInstanceStartTask.NOTIFY_STARTED
        self.instance._handle_start_task_complete = mock.create_autospec(
            self.instance._handle_start_task_complete)
        self.instance.handler(obs, event)
        self.instance._handle_start_task_complete.assert_called_with()

    def test_handler_notify_success(self):
        obs = mock.Mock()
        event = serviceinstance.ServiceInstanceStopTask.NOTIFY_SUCCESS
        self.instance.handler(obs, event)
        self.instance.monitor_task.cancel.assert_called_with()

    def test_handle_start_task_complete(self):
        self.instance.machine = mock.Mock(
            state=serviceinstance.ServiceInstance.STATE_STARTING)
        self.instance._handle_start_task_complete()
        self.instance.monitor_task.queue.assert_called_with()

    def test_handle_start_task_complete_from_unknown(self):
        self.instance._handle_start_task_complete()
        self.instance.stop_task.kill.assert_called_with()

    def test_state_data(self):
        expected = {
            'instance_number': self.number,
            'node': self.node.hostname
        }
        assert_equal(self.instance.state_data, expected)


def create_mock_instance(**kwargs):
    return mock.create_autospec(serviceinstance.ServiceInstance, **kwargs)

class ServiceInstanceCollectionTestCase(TestCase):

    @setup
    def setup_collection(self):
        self.node_pool      = mock.create_autospec(node.NodePool)
        self.config         = mock.Mock()
        context             = mock.Mock()
        self.collection     = serviceinstance.ServiceInstanceCollection(
            self.config, self.node_pool, context)

    def test__init__(self):
        assert_equal(self.collection.config.count, self.config.count)
        assert_equal(self.collection.config, self.config)
        assert_equal(self.collection.instances,
            self.collection.instances_proxy.obj_list_getter())

    def test_clear_failed(self):
        instances = [
            create_mock_instance(state=serviceinstance.ServiceInstance.STATE_FAILED),
            create_mock_instance(state=serviceinstance.ServiceInstance.STATE_UP),
            ]
        self.collection.instances.extend(instances)
        self.collection.clear_failed()
        assert_equal(self.collection.instances, instances[1:])

    def test_clear_failed_none(self):
        instances = [create_mock_instance(state=serviceinstance.ServiceInstance.STATE_UP)]
        self.collection.instances.extend(instances)
        self.collection.clear_failed()
        assert_equal(self.collection.instances, instances)

    def test_get_up(self):
        instances = [
            create_mock_instance(state=serviceinstance.ServiceInstance.STATE_UP),
            create_mock_instance(state=serviceinstance.ServiceInstance.STATE_FAILED)
        ]
        self.collection.instances.extend(instances)
        up_instances = list(self.collection.get_up())
        assert_equal(up_instances, instances[:1])

    def test_get_up_none(self):
        instances = [
            create_mock_instance(state=serviceinstance.ServiceInstance.STATE_DOWN),
            create_mock_instance(state=serviceinstance.ServiceInstance.STATE_FAILED)
        ]
        self.collection.instances.extend(instances)
        up_instances = list(self.collection.get_up())
        assert_equal(up_instances, [])

    def test_create_missing(self):
        self.collection.config.count = 5
        self.collection.build_instance = mock.create_autospec(self.collection.build_instance)
        created = self.collection.create_missing()
        assert_length(created, 5)
        assert_equal(set(created), set(self.collection.instances))

    def test_create_missing_none(self):
        self.collection.config.count = 2
        self.collection.instances = [create_mock_instance(instance_number=i) for i in range(2)]
        created = self.collection.create_missing()
        assert_length(created, 0)

    def test_build_instance(self):
        self.collection.next_instance_number = mock.create_autospec(
            self.collection.next_instance_number)
        patcher = mock.patch('tron.core.serviceinstance.ServiceInstance', autospec=True)
        context_patcher = mock.patch(
            'tron.core.serviceinstance.build_instance_context', autospec=True)
        with contextlib.nested(patcher, context_patcher ) as (
            mock_service_instance_class, mock_build_context):
            instance = self.collection.build_instance()
            factory = mock_service_instance_class
            assert_equal(instance, factory.return_value)
            factory.assert_called_with(self.config,
                self.node_pool.next_round_robin.return_value,
                self.collection.next_instance_number.return_value,
                mock_build_context.return_value)

    def test_next_instance_number(self):
        self.collection.config.count = 6
        self.collection.instances = [create_mock_instance(instance_number=i) for i in range(5)]
        assert_equal(self.collection.next_instance_number(), 5)

    def test_next_instance_number_in_middle(self):
        self.collection.config.count = 6
        self.collection.instances = [
            create_mock_instance(instance_number=i) for i in range(6) if i != 3]
        assert_equal(self.collection.next_instance_number(), 3)

    def test_missing(self):
        self.collection.config.count = 5
        assert_equal(self.collection.missing, 5)

        self.collection.instances = range(5)
        assert_equal(self.collection.missing, 0)

if __name__ == "__main__":
    run()