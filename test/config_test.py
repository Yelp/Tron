"""Tests for our configuration system"""
import StringIO
import datetime

from testify import *
from tron import config, mcp, scheduler

class ConfigTest(TestCase):
    config = """
--- !TronConfiguration
state_dir: "."

ssh_options: !SSHOptions
    agent: true

nodes:
    - &node0 !Node
        hostname: batch0
    - &node1 !Node
        hostname: batch1

jobs:
    - &job0 !Job
        name: "job0"
        node: *node0
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &intAction !Action
                name: "task0.0"
                command: "test_command0.0"
                output_dir: "output_dir0.0"
                
    - &job1 !Job
        name: "job1"
        node: *node0
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &intAction2 !Action
                name: "task1.0"
                command: "test_command1.0"
                output_dir: "output_dir1.0"
            - &actionBar !Action
                name: "task1.1"
                command: "test_command1.1"
                output_dir: "output_dir1.1"
                requires: *intAction2

    - &job2 !Job
        name: "job2"
        node: *node1
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &actionFail !Action
                name: "task2.0"
                output_dir: "output_dir2.0"
                command: "test_command2.0"

    - &job3 !Job
        name: "job3"
        node: *node1
        schedule: "constant"
        actions:
            - &actionConstant !Action
                name: "task3.0"
                command: "test_command3.0"
                output_dir: "output_dir3.0"
            - &actionFollow !Action
                name: "task3.1"
                node: *node0
                command: "test_command3.1"
                requires: *actionConstant
                output_dir: "output_dir3.1"

    - &job4 !Job
        name: "job4"
        node: *node1
        schedule: "daily"
        actions:
            - &actionDaily !Action
                name: "task4.0"
                command: "test_command4.0"
                output_dir: "output_dir4.0"
"""
    
    @setup
    def setup(self):
        self.test_config = config.load_config(StringIO.StringIO(self.config))
        self.my_mcp = mcp.MasterControlProgram('state')
        self.test_config.apply(self.my_mcp)

        self.node0 = self.my_mcp.nodes[0]
        self.node1 = self.my_mcp.nodes[1]

        self.job0 = self.my_mcp.jobs['job0']
        self.job1 = self.my_mcp.jobs['job1']
        self.job2 = self.my_mcp.jobs['job2']
        self.job3 = self.my_mcp.jobs['job3']
        self.job4 = self.my_mcp.jobs['job4']

        self.all_jobs = [self.job0, self.job1, self.job2, self.job3, self.job4]

    def test_attributes(self):
        assert hasattr(self.test_config, "state_dir")
        assert hasattr(self.test_config, "nodes")
        assert hasattr(self.test_config, "jobs")
        assert hasattr(self.test_config, "ssh_options")

        assert_equal(len(self.test_config.jobs), 5)
        assert_equal(len(self.test_config.nodes), 2)
        
    def test_node_attribute(self):
        assert_equal(len(self.my_mcp.nodes), 2)
        assert_equal(self.my_mcp.nodes[0].hostname, "batch0")
        assert_equal(self.my_mcp.nodes[1].hostname, "batch1")

        assert_equal(self.my_mcp.nodes[0].conch_options['noagent'], False)
        assert_equal(self.my_mcp.nodes[1].conch_options['noagent'], False)
    
    def test_job_name_attribute(self):
        for j in self.all_jobs:
            assert hasattr(j, "name")
            
        assert_equal(self.job0.name, "job0")
        assert_equal(self.job1.name, "job1")
        assert_equal(self.job2.name, "job2")
        assert_equal(self.job3.name, "job3")
        assert_equal(self.job4.name, "job4")
    
    def test_job_node_attribute(self):
        for j in self.all_jobs:
            assert hasattr(j, "node")
        
        assert_equal(self.job0.node, self.node0)
        assert_equal(self.job1.node, self.node0)
        assert_equal(self.job2.node, self.node1)
        assert_equal(self.job3.node, self.node1)
        assert_equal(self.job4.node, self.node1)

    def test_job_schedule_attribute(self):
        for j in self.all_jobs:
            assert hasattr(j, "scheduler")

        assert isinstance(self.job0.scheduler, scheduler.IntervalScheduler)
        assert_equal(self.job0.scheduler.interval, datetime.timedelta(seconds=20))
        assert isinstance(self.job1.scheduler, scheduler.IntervalScheduler)
        assert_equal(self.job1.scheduler.interval, datetime.timedelta(seconds=20))
        assert isinstance(self.job2.scheduler, scheduler.IntervalScheduler)
        assert_equal(self.job2.scheduler.interval, datetime.timedelta(seconds=20))
        assert isinstance(self.job3.scheduler, scheduler.ConstantScheduler)
        assert isinstance(self.job4.scheduler, scheduler.DailyScheduler)
     
    def test_actions_name_attribute(self): 
        for job_count in range(len(self.all_jobs)):
            j = self.all_jobs[job_count]

            for act_count in range(len(j.topo_actions)):
                a = j.topo_actions[act_count]
                assert hasattr(a, "name")
                assert_equal(a.name, "task%s.%s" % (job_count, act_count))
    
    def test_actions_command_attribute(self): 
        for job_count in range(len(self.all_jobs)):
            j = self.all_jobs[job_count]

            for act_count in range(len(j.topo_actions)):
                a = j.topo_actions[act_count]
                assert hasattr(a, "command")
                assert_equal(a.command, "test_command%s.%s" % (job_count, act_count))
      
    def test_actions_output_dir_attribute(self): 
        for job_count in range(len(self.all_jobs)):
            j = self.all_jobs[job_count]

            for act_count in range(len(j.topo_actions)):
                a = j.topo_actions[act_count]
                assert hasattr(a, "output_dir")
                assert_equal(a.output_dir, "output_dir%s.%s" % (job_count, act_count))

    def test_actions_requirements(self):
        dep0 = self.job1.topo_actions[1]
        dep1 = self.job3.topo_actions[1]
        req0 = self.job1.topo_actions[0]
        req1 = self.job3.topo_actions[0]

        assert hasattr(dep0, 'required_actions')
        assert hasattr(dep1, 'required_actions')
        
        assert_equals(len(dep0.required_actions), 1)
        assert_equals(len(dep1.required_actions), 1)
        assert_equals(len(req0.required_actions), 0)
        assert_equals(len(req1.required_actions), 0)

        assert_equals(dep0.required_actions[0], req0)
        assert_equals(dep1.required_actions[0], req1)

