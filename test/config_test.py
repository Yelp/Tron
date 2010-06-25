"""Tests for our configuration system"""
import StringIO
import datetime

from testify import *
from tron import config, mcp, scheduler

class ConfigTest(TestCase):
    sample_config = """
--- !TronConfiguration
ssh_options: !SSHOptions
    agent: true

nodes:
    - &nodeBatch00 !Node
        hostname: batch0
jobs:  
    - &jobFoo !Job
        name: "Foo Job"
        node: *nodeBatch00
        command: "$BATCH_DIR/foo.py --test"
        output_dir: "/home/user/"
        schedule: !IntervalScheduler
            interval: 1hr

    - &jobBar !Job
        name: "Bar Job"
        node: *nodeBatch00
        command: "echo oh oh oh"
        dependant_on: *jobFoo
"""
    @setup
    def setup(self):
        self.test_config = config.load_config(StringIO.StringIO(self.sample_config))
        self.my_mcp = mcp.MasterControlProgram()
        self.test_config.apply(self.my_mcp)
        self.foo_job = self.my_mcp.jobs["Foo Job"]
        self.bar_job = self.my_mcp.jobs["Bar Job"]

    def test_attributes(self):
        assert hasattr(self.test_config, "nodes")
        assert hasattr(self.test_config, "jobs")
        assert hasattr(self.test_config, "ssh_options")

        assert_equal(len(self.test_config.jobs), 2)
        assert_equal(len(self.test_config.nodes), 1)
        
    def test_node_attribute(self):
        assert_equal(self.foo_job.node.hostname, "batch0")
        assert_equal(self.foo_job.node.conch_options['noagent'], False)
    
    def test_command_attribute(self):
        assert hasattr(self.foo_job, "command")
        assert hasattr(self.bar_job, "command")
        assert_equal(self.foo_job.command, "$BATCH_DIR/foo.py --test")
        assert_equal(self.bar_job.command, "echo oh oh oh")

    def test_output_dir_attribute(self):
        assert hasattr(self.foo_job, "output_dir")
        assert_equal(self.foo_job.output_dir, "/home/user/")
        
        assert self.bar_job.output_dir is None

    def test_schedule_attribute(self):
        assert isinstance(self.foo_job.scheduler, scheduler.IntervalScheduler)
        assert_equal(self.foo_job.scheduler.interval, datetime.timedelta(hours=1))
    
    def test_dependant_on_attribute(self):
        assert_equal(len(self.foo_job.dependants), 1)
        assert_equal(self.foo_job.dependants[0], self.bar_job)

