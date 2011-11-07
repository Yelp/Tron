"""Tests for our configuration system"""
import StringIO
import datetime
import os
import tempfile
import shutil

from testify import *
from tron import config, mcp, scheduler
from tron.utils import testingutils
from test.config_test import syslog_address_for_platform

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
        nodes: [*node0, *node1]
jobs:
    - &job0 !Job
        name: "test_unchanged"
        node: *node0
        schedule: "daily"
        actions:
            - &action0 !Action
                name: "action_unchanged"
                command: "command_unchanged"

    - &job1 !Job
        name: "test_remove"
        node: *node1
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &action1 !Action
                name: "action_remove"
                command: "command_remove"

    - &job2 !Job
        name: "test_change"
        node: *nodePool
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &action2 !Action
                name: "action_change"
                command: "command_change"
            - &action3 !Action
                name: "action_remove2"
                command: "command_remove2"
                requires: *action2

    - &job3 !Job
        name: "test_daily_change"
        node: *node0
        schedule: "daily"
        actions:
            - &action4 !Action
                name: "action_daily_change"
                command: "command"

"""
    reconfig = """

--- !TronConfiguration
working_dir: "./config_test_dir"

ssh_options: !SSHOptions
    agent: true
    identities:
        - test/test_id_rsa

syslog_address: %s

nodes:
    - &node0 !Node
        hostname: 'batch0'
    - &node1 !Node
        hostname: 'batch1'

    - &nodePool !NodePool
        nodes: [*node0, *node1]
jobs:
    - &job0 !Job
        name: "test_unchanged"
        node: *node0
        schedule: "daily"
        actions:
            - &action0 !Action
                name: "action_unchanged"
                command: "command_unchanged"

    - &job2 !Job
        name: "test_change"
        node: *node0
        schedule: "daily"
        actions:
            - &action2 !Action
                name: "action_change"
                command: "command_changed"

    - &job3 !Job
        name: "test_daily_change"
        node: *node0
        schedule: "daily"
        actions:
            - &action4 !Action
                name: "action_daily_change"
                command: "command_changed"

    - &job4 !Job
        name: "test_new"
        node: *nodePool
        schedule: !IntervalScheduler
            interval: 20s
        actions:
            - &action1 !Action
                name: "action_new"
                command: "command_new"


""" % syslog_address_for_platform()

    @setup
    def setup(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_config = config.load_config(StringIO.StringIO(self.config))
        self.my_mcp = mcp.MasterControlProgram(self.test_dir, 'config')
        self.test_config.apply(self.my_mcp)
        
    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_job_list(self):
        assert_equal(len(self.my_mcp.jobs), 4)

        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)

        assert_equal(len(self.my_mcp.jobs), 4)

    def test_job_unchanged(self):
        assert 'test_unchanged' in self.my_mcp.jobs
        job0 = self.my_mcp.jobs['test_unchanged']
        run0 = job0.next_runs()[0]
        run0.start()
        run1 = job0.next_runs()[0]

        assert_equal(job0.name, "test_unchanged")
        assert_equal(len(job0.topo_actions), 1)
        assert_equal(job0.topo_actions[0].name, 'action_unchanged')
        assert_equal(str(job0.scheduler), "DAILY")

        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)
        job0 = self.my_mcp.jobs['test_unchanged']

        assert_equal(job0.name, "test_unchanged")
        assert_equal(len(job0.topo_actions), 1)
        assert_equal(job0.topo_actions[0].name, 'action_unchanged')
        assert_equal(str(job0.scheduler), "DAILY")

        assert_equal(len(job0.runs), 2)
        assert_equal(job0.runs[1], run0)
        assert_equal(job0.runs[0], run1)
        assert run1.is_scheduled

    def test_job_removed(self):
        assert 'test_remove' in self.my_mcp.jobs
        job1 = self.my_mcp.jobs['test_remove']
        run0 = job1.next_runs()[0]
        run0.start()
        run1 = job1.next_runs()[0]

        assert_equal(job1.name, "test_remove")
        assert_equal(len(job1.topo_actions), 1)
        assert_equal(job1.topo_actions[0].name, 'action_remove')

        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)

        assert not 'test_remove' in self.my_mcp.jobs
        assert not job1.enabled
        assert not run1.is_scheduled

    def test_job_changed(self):
        assert 'test_change' in self.my_mcp.jobs
        job2 = self.my_mcp.jobs['test_change']
        run0 = job2.next_runs()[0]
        run0.start()
        run1 = job2.next_runs()[0]
        assert_equal(len(job2.runs), 2)

        assert_equal(job2.name, "test_change")
        assert_equal(len(job2.topo_actions), 2)
        assert_equal(job2.topo_actions[0].name, 'action_change')
        assert_equal(job2.topo_actions[1].name, 'action_remove2')
        assert_equal(job2.topo_actions[0].command, 'command_change')
        assert_equal(job2.topo_actions[1].command, 'command_remove2')

        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)
        job2 = self.my_mcp.jobs['test_change']

        assert_equal(job2.name, "test_change")
        assert_equal(len(job2.topo_actions), 1)
        assert_equal(job2.topo_actions[0].name, 'action_change')
        assert_equal(job2.topo_actions[0].command, 'command_changed')

        assert_equal(len(job2.runs), 2)
        assert job2.runs[1].is_starting, job2.runs[1].action_runs[0].state
        assert job2.runs[0].is_scheduled

    def test_job_new(self):
        assert not 'test_new' in self.my_mcp.jobs
        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)

        assert 'test_new' in self.my_mcp.jobs
        job3 = self.my_mcp.jobs['test_new']

        assert_equal(job3.name, "test_new")
        assert_equal(len(job3.topo_actions), 1)
        assert_equal(job3.topo_actions[0].name, 'action_new')
        assert_equal(job3.topo_actions[0].command, 'command_new')

    def test_daily_reschedule(self):
        job4 = self.my_mcp.jobs['test_daily_change']

        job4.next_runs()

        assert_equal(len(job4.runs), 1)
        run = job4.runs[0]
        assert run.is_scheduled

        test_reconfig = config.load_config(StringIO.StringIO(self.reconfig))
        test_reconfig.apply(self.my_mcp)

        assert run.job is None

        assert_equal(len(job4.runs), 1)
        next_run = job4.runs[0]
        assert next_run is not run
        assert next_run.is_scheduled
        assert_equal(run.run_time, next_run.run_time)


if __name__ == '__main__':
    run()
