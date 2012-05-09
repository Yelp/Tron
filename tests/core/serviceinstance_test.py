from testify import setup, assert_equal, TestCase, run
from tests import mocks
from tests.assertions import assert_length
from tests.testingutils import Turtle

from tron.core import serviceinstance

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