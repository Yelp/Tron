"""Tests for our configuration system"""
import shutil
import tempfile

import yaml

from testify import *
from tron import config_parse
from tron import mcp
from tests.config_parse_test import syslog_address_for_platform

class ConfigTest(TestCase):

    def config_1(self, wd):
        config = dict(
            working_dir=wd,
            ssh_options=dict(
                agent=True,
                identities=['tests/test_id_rsa'],
            ),
            nodes=[
                dict(name='node0', hostname='batch0'),
                dict(name='node1', hostname='batch1'),
            ],
            node_pools=[dict(name='nodePool', nodes=['node0', 'node1'])],
            jobs=[
                dict(
                    name='test_unchanged',
                    node='node0',
                    schedule='daily',
                    actions=[dict(name='action_unchanged',
                                  command='command_unchanged') ]
                ),
                dict(
                    name='test_remove',
                    node='node1',
                    schedule=dict(interval='20s'),
                    actions=[dict(name='action_remove',
                                  command='command_remove')],
                ),
                dict(
                    name='test_change',
                    node='nodePool',
                    schedule=dict(interval='20s'),
                    actions=[
                        dict(name='action_change',
                             command='command_change'),
                        dict(name='action_remove2',
                             command='command_remove2',
                             requires=['action_change']),
                    ],
                ),
                dict(
                    name='test_daily_change',
                    node='node0',
                    schedule='daily',
                            actions=[dict(name='action_daily_change',
                                          command='command')],
                ),
            ])
        # TODO: skip the yaml dump
        return yaml.dump(config)

    def config_2(self, wd):
        config = dict(
            working_dir=wd,
            ssh_options=dict(
                agent=True,
                identities=['tests/test_id_rsa'],
            ),
            syslog_address=syslog_address_for_platform(),
            nodes=[
                dict(name='node0', hostname='batch0'),
                dict(name='node1', hostname='batch1'),
            ],
            node_pools=[dict(name='nodePool', nodes=['node0', 'node1'])],
            jobs=[
                dict(
                    name='test_unchanged',
                    node='node0',
                    schedule='daily',
                    actions=[dict(name='action_unchanged',
                                  command='command_unchanged') ]
                ),
                dict(
                    name='test_change',
                    node='nodePool',
                    schedule='daily',
                    actions=[
                        dict(name='action_change',
                             command='command_changed'),
                    ],
                ),
                dict(
                    name='test_daily_change',
                    node='node0',
                    schedule='daily',
                            actions=[dict(name='action_daily_change',
                                          command='command_changed')],
                ),
                dict(
                    name='test_new',
                    node='nodePool',
                    schedule=dict(interval='20s'),
                    actions=[dict(name='action_new',
                                  command='command_new')]
                ),
            ])
        return yaml.dump(config)

    @setup
    def setup_mcp(self):
        self.test_dir = tempfile.mkdtemp()
        self.my_mcp = mcp.MasterControlProgram(self.test_dir, 'config')
        config = self.config_1(self.test_dir)
        self.my_mcp.apply_config(config_parse.load_config(config))

    @teardown
    def teardown_mcp(self):
        shutil.rmtree(self.test_dir)

    def reconfigure(self):
        config = self.config_2(self.test_dir)
        self.my_mcp.apply_config(config_parse.load_config(config))

    def test_job_list(self):
        assert_equal(len(self.my_mcp.jobs), 4)
        self.reconfigure()
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

        self.reconfigure()
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

        self.reconfigure()

        assert not 'test_remove' in self.my_mcp.jobs
        assert not job1.enabled
        assert not run1.is_scheduled

    def test_job_changed(self):
        assert 'test_change' in self.my_mcp.jobs
        job2 = self.my_mcp.jobs['test_change']
        run0 = job2.next_runs()[0]
        run0.start()
        job2.next_runs()
        assert_equal(len(job2.runs), 2)

        assert_equal(job2.name, "test_change")
        assert_equal(len(job2.topo_actions), 2)
        assert_equal(job2.topo_actions[0].name, 'action_change')
        assert_equal(job2.topo_actions[1].name, 'action_remove2')
        assert_equal(job2.topo_actions[0].command, 'command_change')
        assert_equal(job2.topo_actions[1].command, 'command_remove2')

        self.reconfigure()
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
        self.reconfigure()

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

        self.reconfigure()

        assert run.job is None

        assert_equal(len(job4.runs), 1)
        next_run = job4.runs[0]
        assert next_run is not run
        assert next_run.is_scheduled
        assert_equal(run.run_time, next_run.run_time)


if __name__ == '__main__':
    run()
