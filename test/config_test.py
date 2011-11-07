"""Tests for our configuration system"""
import datetime
import logging
from logging import handlers
import os
import platform
import shutil
import StringIO
import tempfile

from testify import *
from tron import config, mcp, scheduler


def syslog_address_for_platform():
    if platform.system() == 'Darwin':
        return '/var/run/syslog'
    elif platform.system() == 'Windows':
        return ['localhost', 514]
    else:
        return '/dev/log'


BASE_CONFIG = """
--- !TronConfiguration
working_dir: "./config_test_dir"

ssh_options: !SSHOptions
    agent: true
    identities: 
        - test/test_id_rsa

nodes:
    - &node0 !Node
        hostname: 'batch0'
    - &node1
        hostname: 'batch1'
    - &nodePool !NodePool
        nodes: [*node0, *node1]
"""


class ConfigTest(TestCase):
    config = BASE_CONFIG + """

command_context:
    batch_dir: /tron/batch/test/foo
    python: /usr/bin/python
    
jobs:
    - &job0 !Job
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            - &intAction !Action
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action: !CleanupAction
            command: "test_command0.1"
                
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
        command: "service_command0"
        count: 2
        pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
        monitor_interval: 20
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

        self.serv = self.my_mcp.services['service0']

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

            if j.cleanup_action is not None:
                assert_equal(j.cleanup_action.name, config.CLEANUP_ACTION_NAME)


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
        assert_equal(self.all_jobs[0].cleanup_action.command, "test_command0.1")
      
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
        assert_equal(self.serv.name, 'service0')
        assert_equal(self.serv.monitor_interval, 20)
        assert_equal(self.serv.count, 2)
        assert self.serv.pid_file_template
        assert self.serv.command
        assert self.serv.context


class LoggingConfigTest(TestCase):

    config = BASE_CONFIG

    reconfig = BASE_CONFIG + """
syslog_address: %s""" % syslog_address_for_platform()

    bad_config = BASE_CONFIG + """
syslog_address: /does/not/exist"""

    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_config = config.load_config(StringIO.StringIO(self.config))
        self.my_mcp = mcp.MasterControlProgram(self.test_dir, 'config')
        self.test_config.apply(self.my_mcp)

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_add_syslog(self):
        root = logging.getLogger('')
        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)
        assert_equal(len(root.handlers), 2)
        assert_equal(type(root.handlers[-1]), handlers.SysLogHandler)

        test_reconfig = config.load_config(StringIO.StringIO(self.config))
        test_reconfig.apply(self.my_mcp)
        assert_equal(len(root.handlers), 1)
        assert_equal(type(root.handlers[0]), logging.StreamHandler)

    def test_bad_syslog(self):
        root = logging.getLogger('')
        test_reconfig = config.load_config(StringIO.StringIO(self.bad_config))
        assert_raises(config.ConfigError, test_reconfig.apply, self.my_mcp)


class BadJobConfigTest(TestCase):
    @setup
    def build_env(self):
        self.test_dir = tempfile.mkdtemp()
        self.my_mcp = mcp.MasterControlProgram(self.test_dir, 'config')

    def test_no_actions(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        """
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

    def test_empty_actions(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
        """
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

    def test_dupe_names(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"                
            -
                name: "action0_0"
                command: "test_command0.0"                

        """
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)
    
    def test_bad_requires(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            - &action0_0
                name: "action0_0"
                command: "test_command0.0"                
            - &action0_1
                name: "action0_1"
                command: "test_command0.1"              

    - &job1
        name: "test_job1"
        node: *node0
        schedule: "interval 20s"
        actions:
            -
                name: "action1_0"
                command: "test_command1.0"
                requires: *action0_0

        """
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

    def test_config_name_collision(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            -
                name: "%s"
                command: "test_command0.0"                

        """ % config.CLEANUP_ACTION_NAME
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

    def test_config_name(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"                
        cleanup_action:
            name: "gerald"
            command: "test_command0.1"
        """
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

    def test_config_requires(self):
        test_config = BASE_CONFIG + """
jobs:
    - &job0
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            -   &action0_0
                name: "action0_0"
                command: "test_command0.0"                
        cleanup_action:
            command: "test_command0.1"
            requires: *action0_0
        """
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

    def test_job_in_services(self):
        test_config = BASE_CONFIG + """
services:
    - !Job
        name: "test_job0"
        node: *node0
        schedule: "interval 20s"
        actions:
            - &intAction !Action
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action: !CleanupAction
            command: "test_command0.1"
"""
        test_config = config.load_config(StringIO.StringIO(test_config))
        assert_raises(config.ConfigError, test_config.apply, self.my_mcp)

if __name__ == '__main__':
    run()
