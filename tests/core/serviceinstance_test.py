import mock
from testify import setup, assert_equal, TestCase, run, setup_teardown
from testify.assertions import assert_in
from tests.assertions import assert_length

from tron import node, eventloop
from tron.actioncommand import ActionCommand
from tron.core import serviceinstance


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
        def raise_error(_):
            raise node.Error()
        self.task.node.run = raise_error
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


# TODO: Start task test case
# TODO: stop task test case
# TODO: instance test case

def create_mock_instance(**kwargs):
    return mock.create_autospec(serviceinstance.ServiceInstance, **kwargs)

class ServiceInstanceCollectionTestCase(TestCase):

    @setup
    def setup_collection(self):
        self.node_pool      = mock.create_autospec(node.NodePool)
        self.config         = mock.Mock()
        context             = mock.Mock()
        self.collection = serviceinstance.ServiceInstanceCollection(
            self.config, self.node_pool, context)

    def test__init__(self):
        assert_equal(self.collection.count, self.config.count)
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
        self.collection.count = 5
        self.collection.build_instance = mock.create_autospec(self.collection.build_instance)
        created = self.collection.create_missing()
        assert_length(created, 5)
        assert_equal(set(created), set(self.collection.instances))

    def test_create_missing_none(self):
        self.collection.count = 2
        self.collection.instances = [create_mock_instance(instance_number=i) for i in range(2)]
        created = self.collection.create_missing()
        assert_length(created, 0)

    def test_build_instance(self):
        self.collection.next_instance_number = mock.create_autospec(
            self.collection.next_instance_number)
        patcher = mock.patch('tron.core.serviceinstance.ServiceInstance', autospec=True)
        with patcher as mock_service_instance_class:
            instance = self.collection.build_instance()
            factory = mock_service_instance_class.from_config
            assert_equal(instance, factory.return_value)
            factory.assert_called_with(self.config,
                self.node_pool.next_round_robin.return_value,
                self.collection.next_instance_number.return_value,
                self.collection.context)

    def test_next_instance_number(self):
        self.collection.count = 6
        self.collection.instances = [create_mock_instance(instance_number=i) for i in range(5)]
        assert_equal(self.collection.next_instance_number(), 5)

    def test_next_instance_number_in_middle(self):
        self.collection.count = 6
        self.collection.instances = [
            create_mock_instance(instance_number=i) for i in range(6) if i != 3]
        assert_equal(self.collection.next_instance_number(), 3)

    def test_missing(self):
        self.collection.count = 5
        assert_equal(self.collection.missing, 5)

        self.collection.instances = range(5)
        assert_equal(self.collection.missing, 0)

if __name__ == "__main__":
    run()