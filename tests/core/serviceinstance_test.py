from testify import setup, assert_equal, TestCase, run
from testify.assertions import assert_in
from tests import mocks
from tests.assertions import assert_length, assert_call
from tests.testingutils import Turtle, MockReactorTestCase
from tron import node
from tron.actioncommand import ActionCommand

from tron.core import serviceinstance


class ServiceInstanceMonitorTaskTestCase(MockReactorTestCase):

    module_to_mock = serviceinstance

    @setup
    def setup_task(self):
        self.interval = 20
        self.filename = "/tmp/filename"
        self.task = serviceinstance.ServiceInstanceMonitorTask(
            "id", Turtle(), self.interval, self.filename)
        self.task.notify = Turtle()
        self.task.watch = Turtle()

    def test_queue(self):
        self.task.queue()
        assert_call(self.reactor.callLater, 0, self.interval, self.task.run)

    def test_queue_no_interval(self):
        self.task.interval = 0
        assert_length(self.reactor.callLater.calls, 0)

    def test_queue_has_callback(self):
        self.task.callback = True
        assert_length(self.reactor.callLater.calls, 0)

    def test_run(self):
        self.task.callback = True
        self.task.run()

        assert not self.task.callback
        assert_call(self.task.notify, 0, self.task.NOTIFY_START)
        assert_call(self.task.node.run, 0, self.task.action)
        assert_call(self.reactor.callLater, 0,
            self.interval * 0.8, self.task._run_hang_check, self.task.action)

    def test_run_failed(self):
        self.task._run_action = lambda: False
        self.task.run()
        assert_length(self.reactor.callLater.calls, 0)

    def test_build_action(self):
        action = self.task._build_action()
        assert_call(self.task.watch, 0, action)
        assert_in(self.filename, action.command)

    def test_run_action(self):
        self.task.action = True
        assert self.task._run_action()
        assert_call(self.task.node.run, 0, self.task.action)

    def test_run_action_failed(self):
        def raise_error(_):
            raise node.Error()
        self.task.node.run = raise_error
        assert not self.task._run_action()
        assert_call(self.task.notify, 0, self.task.NOTIFY_FAILED)

    def test_handle_action_event_failstart(self):
        self.task.action = True
        self.task.handle_action_event(None, ActionCommand.FAILSTART)
        assert not self.task.action
        assert_call(self.task.notify, 0, self.task.NOTIFY_FAILED)
        assert_call(self.reactor.callLater, 0, self.interval, self.task.run)

    def test_queue_hang_check(self):
        self.task._queue_hang_check()
        assert_equal(self.task.hang_check_callback,
                self.reactor.callLater.returns[0])

    def test_run_hang_check(self):
        self.task.hang_check_callback = True
        action = Turtle()
        self.task._run_hang_check(action)
        assert not self.task.hang_check_callback
        assert_length(self.task.notify.calls, 0)


    def test_run_hang_check_failed(self):
        self.task.hang_check_callback = True
        self.task.action = action = Turtle()
        self.task._run_hang_check(action)
        assert_equal(self.task.hang_check_callback,
            self.reactor.callLater.returns[0])
        assert_call(self.task.notify, 0 ,self.task.NOTIFY_FAILED)

    def test_handle_action_exit_up(self):
        self.task.action = Turtle(exit_status=0)
        self.task.queue = Turtle()
        self.task._handle_action_exit()
        assert not self.task.action
        assert_call(self.task.notify, 0, self.task.NOTIFY_UP)
        assert_call(self.task.queue, 0)

    def test_handle_action_exit_down(self):
        self.task.action = Turtle(exit_status=1)
        self.task.queue = Turtle()
        self.task._handle_action_exit()
        assert not self.task.action
        assert_call(self.task.notify, 0, self.task.NOTIFY_DOWN)
        assert_length(self.task.queue.calls, 0)


class ServiceInstanceCollectionTestCase(TestCase):

    @setup
    def setup_collection(self):
        self.node_pool   = mocks.MockNodePool()
        self.config = Turtle()
        context     = Turtle()
        self.collection = serviceinstance.ServiceInstanceCollection(
            self.config, self.node_pool, context)

    def test__init__(self):
        assert_equal(self.collection.count, self.config.count)
        assert_equal(self.collection.config, self.config)
        assert_equal(self.collection.instances,
            self.collection.instances_proxy.obj_list_getter())

    def test_clear_failed(self):
        instances = [
            Turtle(state=serviceinstance.ServiceInstance.STATE_FAILED),
            Turtle(state=serviceinstance.ServiceInstance.STATE_UP),
            ]
        self.collection.instances.extend(instances)
        self.collection.clear_failed()
        assert_equal(self.collection.instances, instances[1:])

    def test_clear_failed_none(self):
        instances = [Turtle(state=serviceinstance.ServiceInstance.STATE_UP)]
        self.collection.instances.extend(instances)
        self.collection.clear_failed()
        assert_equal(self.collection.instances, instances)

    def test_get_up(self):
        instances = [
            Turtle(state=serviceinstance.ServiceInstance.STATE_UP),
            Turtle(state=serviceinstance.ServiceInstance.STATE_FAILED)
        ]
        self.collection.instances.extend(instances)
        up_instances = list(self.collection.get_up())
        assert_equal(up_instances, instances[:1])

    def test_get_up_none(self):
        instances = [
            Turtle(state=serviceinstance.ServiceInstance.STATE_DOWN),
            Turtle(state=serviceinstance.ServiceInstance.STATE_FAILED)
        ]
        self.collection.instances.extend(instances)
        up_instances = list(self.collection.get_up())
        assert_equal(up_instances, [])

    def test_create_missing(self):
        self.collection.count = 5
        self.collection.build_instance = Turtle()
        created = self.collection.create_missing()
        assert_length(created, 5)
        assert_equal(set(created), set(self.collection.instances))

    def test_create_missing_none(self):
        self.collection.count = 2
        self.collection.instances = [Turtle(), Turtle()]
        created = self.collection.create_missing()
        assert_length(created, 0)

    # TODO: test build_instance

    def test_next_instance_number(self):
        self.collection.count = 6
        self.collection.instances = [Turtle(instance_number=i) for i in range(5)]
        assert_equal(self.collection.next_instance_number(), 5)

    def test_next_instance_number_in_middle(self):
        self.collection.count = 6
        self.collection.instances = [
        Turtle(instance_number=i) for i in range(6) if i != 3]
        assert_equal(self.collection.next_instance_number(), 3)

    def test_missing(self):
        self.collection.count = 5
        assert_equal(self.collection.missing, 5)

        self.collection.instances = range(5)
        assert_equal(self.collection.missing, 0)

if __name__ == "__main__":
    run()