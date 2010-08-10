"""Tests for our configuration system"""
import StringIO
import datetime
import os
import shutil

from testify import *
from tron import config, mcp, scheduler

class ConfigTest(TestCase):
    config = """
--- !TronConfiguration
working_dir: "./config_test_dir"

ssh_options: !SSHOptions
    agent: true
    identities: 
        - test/test_id_rsa

nodes:
    - &node0 !Node
        hostname: 'batch0'
    - &node1 !Node
        hostname: 'batch1'
    - &nodePool !NodePool
        hostnames: ['batch2', 'batch3']
jobs:
    - &job0 !Job
        name: "test_job0"
        node: *node0
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &intAction !Action
                name: "action0.0"
                command: "test_command0.0"
                
    - &job1 !Job
        name: "test_job1"
        node: *node0
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &intAction2 !Action
                name: "action1.0"
                command: "test_command1.0"
            - &actionBar !Action
                name: "action1.1"
                command: "test_command1.1"
                requires: *intAction2

    - &job2 !Job
        name: "test_job2"
        node: *node1
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &actionFail !Action
                name: "action2.0"
                command: "test_command2.0"

    - &job3 !Job
        name: "test_job3"
        node: *node1
        schedule: "constant"
        actions:
            - &actionConstant0 !Action
                name: "action3.0"
                command: "test_command3.0"
            - &actionConstant1 !Action
                name: "action3.1"
                command: "test_command3.1"
            - &actionFollow !Action
                name: "action3.2"
                node: *node0
                command: "test_command3.2"
                requires: [*actionConstant0, *actionConstant1]

    - &job4 !Job
        name: "test_job4"
        node: *nodePool
        schedule: "daily"
        actions:
            - &actionDaily !Action
                name: "action4.0"
                command: "test_command4.0"
"""
    @class_setup
    def class_setup(self):
        os.mkdir('./config_test_dir')

    @setup
    def setup(self):
        self.test_config = config.load_config(StringIO.StringIO(self.config))
        self.my_mcp = mcp.MasterControlProgram('./config_test_dir')
        self.test_config.apply(self.my_mcp)

        self.node0 = self.my_mcp.nodes[0]
        self.node1 = self.my_mcp.nodes[1]
        
        self.job0 = self.my_mcp.jobs['test_job0']
        self.job1 = self.my_mcp.jobs['test_job1']
        self.job2 = self.my_mcp.jobs['test_job2']
        self.job3 = self.my_mcp.jobs['test_job3']
        self.job4 = self.my_mcp.jobs['test_job4']

        self.all_jobs = [self.job0, self.job1, self.job2, self.job3, self.job4]

    @class_teardown
    def teardown(self):
        shutil.rmtree('./config_test_dir')

    def test_attributes(self):
        assert hasattr(self.test_config, "working_dir")
        assert hasattr(self.test_config, "nodes")
        assert hasattr(self.test_config, "jobs")
        assert hasattr(self.test_config, "ssh_options")

        assert_equal(len(self.test_config.jobs), 5)
        assert_equal(len(self.test_config.nodes), 3)
        
    def test_node_attribute(self):
        assert_equal(len(self.my_mcp.nodes), 4)
        assert_equal(self.my_mcp.nodes[0].hostname, "batch0")
        assert_equal(self.my_mcp.nodes[1].hostname, "batch1")

        assert_equal(self.my_mcp.nodes[0].conch_options['noagent'], False)
        assert_equal(self.my_mcp.nodes[1].conch_options['noagent'], False)
    
    def test_job_name_attribute(self):
        for j in self.all_jobs:
            assert hasattr(j, "name")
            
        assert_equal(self.job0.name, "test_job0")
        assert_equal(self.job1.name, "test_job1")
        assert_equal(self.job2.name, "test_job2")
        assert_equal(self.job3.name, "test_job3")
        assert_equal(self.job4.name, "test_job4")
    
    def test_job_node_attribute(self):
        for j in self.all_jobs:
            assert hasattr(j, "node_pool")
        
        node2 = self.my_mcp.nodes[2]
        node3 = self.my_mcp.nodes[3]

        assert_equal(self.job0.node_pool.nodes[0], self.node0)
        assert_equal(self.job1.node_pool.nodes[0], self.node0)
        assert_equal(self.job2.node_pool.nodes[0], self.node1)
        assert_equal(self.job3.node_pool.nodes[0], self.node1)
        assert_equal(self.job4.node_pool.nodes[0], node2)
        assert_equal(self.job4.node_pool.nodes[1], node3)

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
                assert_equal(a.name, "action%s.%s" % (job_count, act_count))
    
    def test_actions_command_attribute(self): 
        for job_count in range(len(self.all_jobs)):
            j = self.all_jobs[job_count]

            for act_count in range(len(j.topo_actions)):
                a = j.topo_actions[act_count]
                assert hasattr(a, "command")
                assert_equal(a.command, "test_command%s.%s" % (job_count, act_count))
      
    def test_actions_requirements(self):
        dep0 = self.job1.topo_actions[1]
        dep1 = self.job3.topo_actions[2]
        req0 = self.job1.topo_actions[0]
        req1 = self.job3.topo_actions[0]

        assert hasattr(dep0, 'required_actions')
        assert hasattr(dep1, 'required_actions')
        
        assert_equals(len(dep0.required_actions), 1)
        assert_equals(len(dep1.required_actions), 2)
        assert_equals(len(req0.required_actions), 0)
        assert_equals(len(req1.required_actions), 0)

        assert_equals(dep0.required_actions[0], req0)
        assert_equals(dep1.required_actions[0], req1)

