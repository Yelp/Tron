"""Tests for our configuration system"""
import StringIO
import datetime
import os
import shutil
import tempfile

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

command_context:
    batch_dir: /tron/batch/test/foo
    python: /usr/bin/python
    
nodes:
    - &node0 !Node
        hostname: 'batch0'
    - &node1
        hostname: 'batch1'
    - &nodePool !NodePool
        nodes: [*node0, *node1]
jobs:
    - &job0 !Job
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            - &intAction !Action
                name: "action0_0"
                command: "test_command0.0"
                
    - &job1
        name: "test_job1"
        node: *node0
        schedule: "daily 00:30:00 MWF"
        actions:
            - &intAction2
                name: "action1_0"
                command: "test_command1.0"
            - &actionBar
                name: "action1_1"
                command: "test_command1.1"
                requires: *intAction2

    - &job2
        name: "test_job2"
        node: *node1
        schedule: "daily 16:30:00"
        actions:
            - &actionFail !Action
                name: "action2_0"
                command: "test_command2.0"

    - &job3
        name: "test_job3"
        node: *node1
        schedule: "constant"
        actions:
            - &actionConstant0
                name: "action3_0"
                command: "test_command3.0"
            - &actionConstant1
                name: "action3_1"
                command: "test_command3.1"
            - &actionFollow
                name: "action3_2"
                node: *node0
                command: "test_command3.2"
                requires: [*actionConstant0, *actionConstant1]

    - &job4
        name: "test_job4"
        node: *nodePool
        all_nodes: True
        schedule: "daily"
        actions:
            - &actionDaily
                name: "action4_0"
                command: "test_command4.0"

services:
    -
        name: "service0"
        node: *nodePool
        enable:
            command: "service_command0"
        disable:
            command: "service_command1"
        monitor:
            schedule: "interval 5 mins"
            actions:
                - &mon0
                    name: "mon0"
                    command: "service_command2"
                -
                    name: "mon1"
                    command: "service_command3"
                    requires: *mon0

"""
    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_config = config.load_config(StringIO.StringIO(self.config))
        self.my_mcp = mcp.MasterControlProgram(self.test_dir, 'config')
        self.test_config.apply(self.my_mcp)

        self.node0 = self.my_mcp.nodes[0]
        self.node1 = self.my_mcp.nodes[1]
        
        self.job0 = self.my_mcp.jobs['test_job0']
        self.job1 = self.my_mcp.jobs['test_job1']
        self.job2 = self.my_mcp.jobs['test_job2']
        self.job3 = self.my_mcp.jobs['test_job3']
        self.job4 = self.my_mcp.jobs['test_job4']

        self.serv = self.my_mcp.jobs['service0']

        self.all_jobs = [self.job0, self.job1, self.job2, self.job3, self.job4]

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_attributes(self):
        assert hasattr(self.test_config, "working_dir")
        assert hasattr(self.test_config, "nodes")
        assert hasattr(self.test_config, "jobs")
        assert hasattr(self.test_config, "ssh_options")

        assert_equal(len(self.test_config.jobs), 5)
        assert_equal(len(self.test_config.services), 1)
        assert_equal(len(self.test_config.nodes), 3)
        
    def test_node_attribute(self):
        assert_equal(len(self.my_mcp.nodes), 2)
        assert_equal(self.my_mcp.nodes[0].hostname, "batch0")
        assert_equal(self.my_mcp.nodes[1].hostname, "batch1")

        assert_equal(self.my_mcp.nodes[0].conch_options['noagent'], False)
        assert_equal(self.my_mcp.nodes[1].conch_options['noagent'], False)
        
        assert self.job4.node_pool.nodes[0] is self.my_mcp.nodes[0]
    
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
        
        assert_equal(self.job0.node_pool.nodes[0], self.node0)
        assert_equal(self.job1.node_pool.nodes[0], self.node0)
        assert_equal(self.job2.node_pool.nodes[0], self.node1)
        assert_equal(self.job3.node_pool.nodes[0], self.node1)

        assert_equal(self.job4.node_pool.nodes[0], self.node0)
        assert_equal(self.job4.node_pool.nodes[1], self.node1)

    def test_job_schedule_attribute(self):
        for j in self.all_jobs:
            assert hasattr(j, "scheduler")

        assert isinstance(self.job0.scheduler, scheduler.IntervalScheduler)
        assert_equal(self.job0.scheduler.interval, datetime.timedelta(seconds=20))

        assert isinstance(self.job1.scheduler, scheduler.DailyScheduler)
        assert_equal(self.job1.scheduler.start_time, datetime.time(hour=0, minute=30, second=0))
        assert self.job1.scheduler.wait_days

        assert isinstance(self.job2.scheduler, scheduler.DailyScheduler)
        assert_equal(self.job2.scheduler.start_time, datetime.time(hour=16, minute=30, second=0))

        assert isinstance(self.job3.scheduler, scheduler.ConstantScheduler)
        assert isinstance(self.job4.scheduler, scheduler.DailyScheduler)

    def test_actions_name_attribute(self): 
        for job_count in range(len(self.all_jobs)):
            j = self.all_jobs[job_count]

            for act_count in range(len(j.topo_actions)):
                a = j.topo_actions[act_count]
                assert hasattr(a, "name")
                assert_equal(a.name, "action%s_%s" % (job_count, act_count))

    def test_all_nodes_attribute(self):
        assert self.job4.all_nodes
        assert not self.job3.all_nodes
    
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

        assert dep0.required_actions[0] is req0
        assert dep1.required_actions[0] is req1

    def test_command_context(self):
        assert hasattr(self.test_config, "command_context")
        assert_equal(self.test_config.command_context['python'], "/usr/bin/python")
        assert_equal(self.my_mcp.context['python'], "/usr/bin/python")
        assert_equal(self.job1.context['python'], "/usr/bin/python")

    def test_service_attributes(self):
        assert_equals(self.serv.name, 'service0')

        assert self.serv.enable_act
        assert_equals(self.serv.enable_act.name, 'enable')
        assert_equals(self.serv.enable_act.command, 'service_command0')

        assert self.serv.disable_act
        assert_equals(self.serv.disable_act.name, 'disable')
        assert_equals(self.serv.disable_act.command, 'service_command1')

        assert_equals(len(self.serv.topo_actions), 2)
        assert_equals(self.serv.topo_actions[0].name, 'mon0')
        assert_equals(self.serv.topo_actions[0].command, 'service_command2')
        assert_equals(len(self.serv.topo_actions[0].required_actions), 0)

        assert_equals(self.serv.topo_actions[1].name, 'mon1')
        assert_equals(self.serv.topo_actions[1].command, 'service_command3')
        assert_equals(len(self.serv.topo_actions[1].required_actions), 1)
        assert_equals(self.serv.topo_actions[1].required_actions[0], self.serv.topo_actions[0])
        
if __name__ == '__main__':
    run()
