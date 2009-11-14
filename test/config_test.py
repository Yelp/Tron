"""Tests for our configuration system"""
import StringIO
import datetime

from testify import *
from tron import config, mcp, scheduler

class SimpleTest(TestCase):
    sample_config = """
--- !TronConfiguration
nodes:
    - &nodeBatch00 !Node
        hostname: batch0
jobs:  
    - &jobFoo !Job
        name: "Foo Job"
        node: *nodeBatch00
        command: "$BATCH_DIR/foo.py --test"
        schedule: !IntervalScheduler
            interval: 1hr
"""
    def test(self):
        test_config = config.load_config(StringIO.StringIO(self.sample_config))
        assert hasattr(test_config, "nodes")
        assert hasattr(test_config, "jobs")
        assert_equal(len(test_config.jobs), 1)
        assert_equal(len(test_config.nodes), 1)
        
        my_mcp = mcp.MasterControlProgram()
        test_config.apply(my_mcp)
        
        my_job = my_mcp.jobs["Foo Job"]
        assert_equal(my_job.node.hostname, "batch0")
        
        assert isinstance(my_job.scheduler, scheduler.IntervalScheduler)
        assert_equal(my_job.scheduler.interval, datetime.timedelta(hours=1))