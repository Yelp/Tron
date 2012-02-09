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
from tron import config2, mcp, scheduler
from tron.config2 import *
from tron.utils import timeutils


BASE_CONFIG = """
---
working_dir: "/tmp"

ssh_options: !SSHOptions
    agent: true
    identities:
        - tests/test_id_rsa

nodes:
    - &node0 !Node
        hostname: 'batch0'
    - &node1
        hostname: 'batch1'
    - &nodePool !NodePool
        nodes: [*node0, *node1]
"""


def syslog_address_for_platform():
    if platform.system() == 'Darwin':
        return '/var/run/syslog'
    elif platform.system() == 'Windows':
        return ['localhost', 514]
    else:
        return '/dev/log'


class OldConfigTest(TestCase):
    OLD_BASE_CONFIG = """
--- !TronConfiguration
working_dir: "/tmp"

ssh_options: !SSHOptions
    agent: true
    identities:
        - tests/test_id_rsa

nodes:
    - &node0 !Node
        hostname: 'batch0'
    - &node1
        hostname: 'batch1'
    - &nodePool !NodePool
        nodes: [*node0, *node1]
    """

    config = OLD_BASE_CONFIG + """

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

    @teardown
    def teardown(self):
        shutil.rmtree(self.test_dir)

    def test_attributes(self):
        test_config = config2.load_config(StringIO.StringIO(self.config))
        expected = TronConfig(
            working_dir='/tmp',
            syslog_address=None,
            command_context=FrozenDict(**{
                'python': '/usr/bin/python',
                'batch_dir': '/tron/batch/test/foo'
            }),
            ssh_options={'ciphers': None,
                         'macs': None,
                         'option': None,
                         'host-key-algorithms': None,
                         'user-authentications': None,
                         'noagent': 0,
                         'compress': 0,
                         'agent': True,
                         'known-hosts': None,
                         'user': None,
                         'reconnect': 0,
                         'logfile': None,
                         'port': None,
                         'identity': None,
                         'log': 0,
                         'nox11': 0,
                         'version': 0},
            notification_options=None,
            time_zone=None,
            nodes=FrozenDict(**{
                'batch0': ConfigNode(name='batch0', hostname='batch0'),
                'batch1': ConfigNode(name='batch1', hostname='batch1')
            }),
            node_pools=FrozenDict(**{
                'batch0_batch1': ConfigNodePool(nodes=['batch0', 'batch1'],
                                                name='batch0_batch1')
            }),
            jobs=FrozenDict(**{
                'test_job0': ConfigJob(
                    name='test_job0',
                    node='batch0',
                    schedule='interval 20s',
                    actions=FrozenDict(**{
                        'action0_0': ConfigAction(
                            name='action0_0',
                            command='test_command0.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=ConfigAction(
                        name='cleanup_action',
                        command='test_command0.1',
                        requires=(),
                        node=None)),
                'test_job1': ConfigJob(
                    name='test_job1',
                    node='batch0',
                    schedule='daily 00:30:00 MWF',
                    actions=FrozenDict(**{
                        'action1_1': ConfigAction(
                            name='action1_1',
                            command='test_command1.1',
                            requires=('command', 'name'),
                            node=None),
                        'action1_0': ConfigAction(
                            name='action1_0',
                            command='test_command1.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None),
                'test_job2': ConfigJob(
                    name='test_job2',
                    node='batch1',
                    schedule='daily 16:30:00',
                    actions=FrozenDict(**{
                        'action2_0': ConfigAction(
                            name='action2_0',
                            command='test_command2.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None),
                'test_job3': ConfigJob(
                    name='test_job3',
                    node='batch1',
                    schedule='constant',
                    actions=FrozenDict(**{
                        'action3_1': ConfigAction(
                            name='action3_1',
                            command='test_command3.1',
                            requires=(),
                            node=None),
                        'action3_0': ConfigAction(
                            name='action3_0',
                            command='test_command3.0',
                            requires=(),
                            node=None),
                        'action3_2': ConfigAction(
                            name='action3_2',
                            command='test_command3.2',
                            requires=('action3_0', 'action3_1'),
                            node='batch0')
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None),
                'test_job4': ConfigJob(
                    name='test_job4',
                    node='batch0_batch1',
                    schedule='daily',
                    actions=FrozenDict(**{
                        'action4_0': ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None)}),
                    queueing=True,
                    run_limit=50,
                    all_nodes=True,
                    cleanup_action=None)
                }),
                services=FrozenDict(**{
                    'service0': ConfigService(
                        name='service0',
                        node='batch0_batch1',
                        pid_file='/var/run/%(name)s-%(instance_number)s.pid',
                        command='service_command0',
                        monitor_interval=20,
                        restart_interval=None,
                        count=2)
                }
            )
        )

        assert_equal(test_config, expected)


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

