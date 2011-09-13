from testify import *

from tron import service
from tron import node
from tron.utils import testingutils

def set_instance_up(service_instance):
    service_instance.start_action.exit_status = 0
    service_instance._start_complete_callback()

    service_instance._run_monitor()
    service_instance.monitor_action.exit_status = 0
    service_instance._monitor_complete_callback()

class SimpleTest(TestCase):
    @setup
    def build_service(self):
        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=testingutils.TestPool())
        self.service.pid_file_template = "/var/run/service.pid"
        self.service.count = 2
        
    def test_start(self):
        assert_equal(self.service.state, service.Service.STATE_DOWN)
        
        self.service.start()
        assert_equal(self.service.machine.state, service.Service.STATE_STARTING)
        for instance in self.service.instances:
            assert_equal(instance.state, service.ServiceInstance.STATE_STARTING)
        
            set_instance_up(instance)
    
            assert_equal(instance.state, service.ServiceInstance.STATE_UP)
        assert_equal(self.service.state, service.Service.STATE_UP)

    def test_instance_up(self):
        self.service.start()
        instance1, instance2 = self.service.instances
        
        instance1._run_monitor()
        assert_equal(instance1.state, service.ServiceInstance.STATE_MONITORING)

        instance1.monitor_action.exit_status = 0
        instance1._monitor_complete_callback()
        assert_equal(instance1.state, service.ServiceInstance.STATE_UP)
        assert_equal(self.service.state, service.Service.STATE_STARTING)

        instance2._run_monitor()
        instance2.monitor_action.exit_status = 0
        instance2._monitor_complete_callback()

        assert_equal(self.service.state, service.Service.STATE_UP)

    def test_instance_failure(self):
        self.service.start()
        instance1, instance2 = self.service.instances

        instance1.machine.state = service.ServiceInstance.STATE_UP
        instance2.machine.state = service.ServiceInstance.STATE_UP
        self.service.machine.state = service.Service.STATE_UP

        # Fail an instance
        instance2._run_monitor()
        instance2.monitor_action.exit_status = 1
        instance2._monitor_complete_callback()
        
        assert_equal(instance2.state, service.ServiceInstance.STATE_FAILED)
        assert_equal(self.service.state, service.Service.STATE_DEGRADED)
        assert_equal(len(self.service.instances), 2)
        
        instance2.stop()
        assert_equal(len(self.service.instances), 1)
        assert_equal(instance2.state, service.ServiceInstance.STATE_DOWN)
        
        # Bring a new instance back up
        instance3 = self.service.build_instance()
        
        instance3._run_monitor()
        instance3.monitor_action.exit_status = 0
        instance3._monitor_complete_callback()
        
        assert_equal(instance3.state, service.ServiceInstance.STATE_UP)
        assert_equal(self.service.state, service.Service.STATE_UP)

        # Fail both
        instance1._run_monitor()
        instance1.monitor_action.exit_status = 1
        instance1._monitor_complete_callback()

        instance3._run_monitor()
        instance3.monitor_action.exit_status = 1
        instance3._monitor_complete_callback()


        assert_equal(instance1.state, service.ServiceInstance.STATE_FAILED)
        assert_equal(self.service.state, service.Service.STATE_FAILED)
        
class ReconfigTest(TestCase):
    @setup
    def build_service(self):
        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=testingutils.TestPool())
        self.service.pid_file_template = "/tmp/pid"
        self.service.count = 2
    
    def test_absorb_state(self):
        self.service.start()

        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.service.node_pool)
        new_service.count = self.service.count     

        new_service.absorb_previous(self.service)
        
        assert_equal(len(new_service.instances), 2)
        assert_equal(self.service.machine, None)
        
    
    def test_absorb_count_incr(self):
        self.service.start()
        
        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.service.node_pool)
        new_service.count = 3
        self.service.machine.state = service.Service.STATE_DEGRADED
        
        new_service.absorb_previous(self.service)
        assert_equal(len(new_service.instances), new_service.count)
        
        assert_equal(len(new_service.instances), 3)
        
    def test_absorb_count_decr(self):
        self.service.start()

        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.service.node_pool)
        new_service.pid_file_template = "/tmp/pid"
        new_service.count = 1

        new_service.absorb_previous(self.service)
        assert_equal(new_service.instances[-1].state, service.ServiceInstance.STATE_STOPPING)

    def test_rapid_change(self):
        self.service.start()

        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.service.node_pool)
        new_service.pid_file_template = "/tmp/pid"
        new_service.count = 1

        new_service.absorb_previous(self.service)
        assert_equal(new_service.instances[-1].state, service.ServiceInstance.STATE_STOPPING)

        another_new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.service.node_pool)
        another_new_service.pid_file_template = "/tmp/pid"
        another_new_service.count = 3

        another_new_service.absorb_previous(new_service)
        assert_equal(len(another_new_service.instances), 3)
        assert_equal(another_new_service.instances[-2].state, service.ServiceInstance.STATE_STOPPING)

class ReconfigNodePoolTest(TestCase):
    @setup
    def build_current_service(self):
        self.node_pool = testingutils.TestPool("node0", "node1")
        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=self.node_pool)
        self.service.pid_file_template = "/tmp/pid"
        self.service.count = 4

    @setup
    def build_new_service(self):
        self.new_node_pool = testingutils.TestPool("node0")
        self.new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.new_node_pool)
        self.new_service.pid_file_template = "/tmp/pid"
        self.new_service.count = 4
    

    def test_node_pool_rebalance(self):
        self.service.start()

        failing_node_instances = [i for i in self.service.instances if i.node.hostname == "node1"]
        self.new_service.absorb_previous(self.service)

        assert all(i.state == service.ServiceInstance.STATE_STOPPING for i in failing_node_instances)

class ReconfigRebuildAllTest(TestCase):
    @setup
    def build_current_service(self):
        self.node_pool = testingutils.TestPool("node0")
        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=self.node_pool)
        self.service.pid_file_template = "/tmp/pid"
        self.service.count = 4

    @setup
    def build_new_service(self):
        self.new_service = service.Service("Sample Service", "sleep 120 &", node_pool=self.node_pool)
        self.new_service.pid_file_template = "/tmp/pid"
        self.new_service.count = 4
    

    def test(self):
        self.service.start()

        self.new_service.absorb_previous(self.service)
        
        assert all(i.state == service.ServiceInstance.STATE_STOPPING for i in self.new_service.instances)

        for i in self.new_service.instances:
            i.machine.transition("down")

        assert all(i.state == service.ServiceInstance.STATE_STARTING for i in self.new_service.instances), [i.state for i in self.new_service.instances]


class SimpleRestoreTest(TestCase):
    @setup
    def build_service(self):
        self.node_pool = node.NodePool()
        test_node = testingutils.TestNode()
        self.node_pool.nodes.append(test_node)

        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=self.node_pool)
        self.service.pid_file_template = "/tmp/pid"
        self.service.count = 2

        self.service.start()
        instance1, instance2 = self.service.instances
        instance1.machine.state = service.ServiceInstance.STATE_UP
        instance2.machine.state = service.ServiceInstance.STATE_UP
        self.service.machine.state = service.Service.STATE_UP
    
    def test(self):
        data = self.service.data
        
        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.node_pool)
        new_service.pid_file_template = "/tmp/pid"
        new_service.count = 2
        new_service.restore(data)
        
        assert_equal(new_service.machine.state, service.Service.STATE_UP)
        assert_equal(len(new_service.instances), 2)
        for instance in new_service.instances:
            assert_equal(instance.state, service.ServiceInstance.STATE_MONITORING)


class FailureRestoreTest(TestCase):
    @setup
    def build_service(self):
        self.node_pool = node.NodePool()
        test_node = testingutils.TestNode()
        self.node_pool.nodes.append(test_node)

        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=self.node_pool)
        self.service.pid_file_template = "/tmp/pid"
        self.service.count = 2

        self.service.start()
        instance1, instance2 = self.service.instances
        instance1.machine.state = service.ServiceInstance.STATE_UP
        instance2.machine.state = service.ServiceInstance.STATE_FAILED
        self.service.machine.state = service.Service.STATE_DEGRADED
    
    def test(self):
        data = self.service.data
        
        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=self.node_pool)
        new_service.pid_file_template = "/tmp/pid"
        new_service.count = 2
        new_service.restore(data)
        
        assert_equal(new_service.machine.state, service.Service.STATE_DEGRADED)
        assert_equal(len(new_service.instances), 2)
        instance1, instance2 = new_service.instances
        assert_equal(instance1.state, service.ServiceInstance.STATE_MONITORING)
        assert_equal(instance2.state, service.ServiceInstance.STATE_MONITORING)


class MonitorFailureTest(TestCase):
    @setup
    def build_service(self):
        self.service = service.Service("Sample Service", "sleep 60 &", node_pool=testingutils.TestPool())
        self.service.pid_file_template = "/var/run/service.pid"
        self.service.count = 2
    
    @setup
    def start_service(self):
        self.service.start()
        instance1, instance2 = self.service.instances
        
        def run_fail(runnable):
            instance1._monitor_complete_failstart()
            raise node.ConnectError("Failed to connect")

        instance1.node.run = run_fail

    def test_instance_up(self):
        self.service.start()
        instance1, instance2 = self.service.instances
        
        instance1._run_monitor()
        assert_equal(instance1.state, service.ServiceInstance.STATE_UNKNOWN)
    