from testify import *

from tron import service

class SimpleTest(TestCase):
    def test(self):
        new_service = service.Service("Sample Service", "sleep 60 &", node_pool=turtle.Turtle())
        new_service.count = 2
        assert_equal(new_service.state, service.Service.STATE_DOWN)
        
        new_service.start()
        assert_equal(new_service.machine.state, service.Service.STATE_STARTING)
        for instance in new_service.instances:
            assert_equal(instance.state, service.ServiceInstance.STATE_STARTING)
        
        
    