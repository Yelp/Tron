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
from tron.utils import timeutils


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


class TimeZoneConfigTest(TestCase):
    """This test is the sibling of scheduler_test.DailySchedulerDSTTest."""

    config = BASE_CONFIG + """
time_zone: US/Pacific
jobs:
    -
        name: "tz_test_job"
        node: *node0
        schedule: "every day at 00:00"
        actions:
            -
                name: "action1_0"
                command: "test_command1.0"
    -
        name: "tz_config_test_job_spring_forward"
        node: *node0
        schedule: "every day at 02:30"
        actions:
            -
                name: "action2_0"
                command: "test_command2.0"
    -
        name: "tz_config_test_job_fall_back"
        node: *node0
        schedule: "every day at 01:00"
        actions:
            -
                name: "action3_0"
                command: "test_command3.0"
    """

    @setup
    def setup(self):
        self.tmp_dirs = []

    @teardown
    def unset_time(self):
        timeutils.override_current_time(None)

    @teardown
    def teardown(self):
        for tmp_dir in self.tmp_dirs:
            shutil.rmtree(tmp_dir)

    def hours_to_job_at_datetime(self, job_name, *args, **kwargs):
        """Return the number of hours until the next *two* runs of a job with
        the given scheduler
        """
        # if you need to print a datetime with tz info, use this:
        #   fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        #   my_datetime.strftime(fmt)

        test_dir = tempfile.mkdtemp()
        self.tmp_dirs.append(test_dir)
        test_config = config.load_config(StringIO.StringIO(self.config))
        my_mcp = mcp.MasterControlProgram(test_dir, 'config')

        test_config.apply(my_mcp)
        now = datetime.datetime(*args, **kwargs)
        timeutils.override_current_time(now)
        next_run = my_mcp.jobs[job_name].next_runs()[0]
        t1 = round(next_run.seconds_until_run_time()/60/60, 1)
        next_run = my_mcp.jobs[job_name].next_runs()[0]
        t2 = round(next_run.seconds_until_run_time()/60/60, 1)
        return t1, t2

    def _assert_range(self, x, lower, upper):
        assert_gt(x, lower)
        assert_lt(x, upper)

    def test_fall_back(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'fall back' point, when the system time zone changes
        from (e.g.) PDT to PST.
        """
        # Exact crossover time:
        # datetime.datetime(2011, 11, 6, 2, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PDT vantage point, the run time is 24.2 and 48.2 hours away:
        s1a, s1b = self.hours_to_job_at_datetime(
            'tz_test_job', 2011, 11, 6, 0, 50, 0)

        # From the PST vantage point, the run time is 21.8  and 45.8 hours away:
        s2a, s2b = self.hours_to_job_at_datetime(
            'tz_test_job', 2011, 11, 6, 2, 10, 0)

        # Make sure the measurements are consistent for each vantage point,
        # meaning that each run is 24 hours apart no matter where you measure
        # from, even if the start time appears different for each vantage
        # point.
        self._assert_range(s1b - s1a, 23.99, 24.01)
        self._assert_range(s2b - s2a, 23.99, 24.01)

        # Start times should differ by 2.4 hours.
        self._assert_range(s1a - s2a, 2.39, 2.41)

    def test_fall_back_2(self):
        """Identical to test_fall_back, but checks the behavior of jobs
        scheduled at an ambiguous time.
        """
        # Exact crossover time:
        # datetime.datetime(2011, 11, 6, 2, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PDT vantage point, the run time is 1.2 and 25.2 hours away:
        s1a, s1b = self.hours_to_job_at_datetime(
            'tz_config_test_job_fall_back', 2011, 11, 6, 0, 50, 0)

        # From the PST vantage point, the run time is 23.8 and 47.8 hours away.
        # This is an ambiguous time because 1 AM occurs twice. Tron will always
        # act as if it is in the first block.
        s2a, s2b = self.hours_to_job_at_datetime(
            'tz_config_test_job_fall_back', 2011, 11, 6, 1, 10, 0)

        # By this method, the first hour of the repeated 1 AM block is
        # effectively ignored.

        self._assert_range(s1b - s1a, 23.99, 24.01)
        self._assert_range(s2b - s2a, 23.99, 24.01)

        # Since the second measurement is taken after the job runs, we check
        # the 'second' run from the first measurement against the 'first' run
        # of the first measurement.
        # Like test_fall_back, start times should differ by 2.4 hours.
        self._assert_range(s1b - s2a, 1.39, 1.41)

    def test_spring_forward(self):
        """This test checks the behavior of the scheduler at the daylight
        savings time 'spring forward' point, when the system time zone changes
        from (e.g.) PST to PDT.
        """
        # Exact crossover time:
        # datetime.datetime(2011, 3, 13, 2, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PST vantage point, the run time is 21.1 hours away:
        s1a, s1b = self.hours_to_job_at_datetime(
            'tz_test_job', 2011, 3, 13, 1, 55, 0)

        # From the PDT vantage point, the run time is 20.9 hours away:
        s2a, s2b = self.hours_to_job_at_datetime(
            'tz_test_job', 2011, 3, 13, 3, 05, 0)

        self._assert_range(s1b - s1a, 23.99, 24.01)
        self._assert_range(s2b - s2a, 23.99, 24.01)

        # So we lose an hour here. The 2 AM block does not exist.
        # If this were not a DST crossover, this difference would be
        # 1.2, not 0.2.
        self._assert_range(s1a - s2a, 0.19, 0.21)

    def test_spring_forward_2(self):
        """Identical to test_spring_forward, but checks the behavior of jobs
        scheduled at a nonexistent time.
        """
        # Exact crossover time:
        # datetime.datetime(2011, 3, 13, 2, 0, 0, tzinfo=pytz.utc)
        # This test will use times on either side of it.

        # From the PST vantage point, the run time is 0.6 hours away:
        s1a, s1b = self.hours_to_job_at_datetime(
            'tz_config_test_job_spring_forward', 2011, 3, 13, 1, 55, 0)

        # This means the job at the nonexistent time 2:30 will be run at the
        # "new" 3:30
        self._assert_range(s1a, 0.59, 0.61)

        # From the PDT vantage point, the next run time is 23.4 hours away,
        # because the job has already been run.
        s2a, s2b = self.hours_to_job_at_datetime(
            'tz_config_test_job_spring_forward', 2011, 3, 13, 3, 05, 0)

        self._assert_range(s1b - s1a, 22.99, 23.01)
        self._assert_range(s2b - s2a, 23.99, 24.01)

        # Since the second measurement is taken after the job runs, we check
        # the 'second' run from the first measurement against the 'first' run
        # of the first measurement.
        # Like test_spring_forward, the times should differ by 0.2 hours.
        self._assert_range(s1b - s2a, 0.19, 0.21)


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
