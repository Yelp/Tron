import datetime
import platform
import shutil
import StringIO
import tempfile
from textwrap import dedent

from testify import assert_equal, assert_in
from testify import run, setup, teardown, TestCase
from tron.config import config_parse
from tron.config.config_parse import TronConfig, load_config, ConfigSSHOptions, valid_job
from tron.config.config_parse import ConfigNode, ConfigNodePool, ConfigJob
from tron.config.config_parse import ConfigAction, ConfigCleanupAction
from tron.config.config_parse import ConfigService, ConfigError
from tron.config.config_parse import CLEANUP_ACTION_NAME
from tron.config.config_parse import valid_node_pool, valid_config
from tron.config.schedule_parse import ConfigConstantScheduler
from tron.config.schedule_parse import ConfigDailyScheduler
from tron.config.schedule_parse import ConfigIntervalScheduler
from tests.testingutils import assert_raises
from tron.utils.dicts import FrozenDict


BASE_CONFIG = """
working_dir: "/tmp"

ssh_options:
    agent: true
    identities:
        - tests/test_id_rsa

nodes:
    - name: node0
      hostname: 'batch0'
    - name: node1
      hostname: 'batch1'

node_pools:
    - name: NodePool
      nodes: [node0, node1]
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
        enabled: False
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
    def setup_testdir(self):
        self.test_dir = tempfile.mkdtemp()

    @teardown
    def teardown_testdir(self):
        shutil.rmtree(self.test_dir)

    def test_attributes(self):
        test_config = load_config(StringIO.StringIO(self.config))
        expected = TronConfig(
            working_dir='/tmp',
            syslog_address=None,
            command_context=FrozenDict(**{
                'python': '/usr/bin/python',
                'batch_dir': '/tron/batch/test/foo'
            }),
            ssh_options=ConfigSSHOptions(
                agent=True,
                identities=['tests/test_id_rsa'],
            ),
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
                    schedule=ConfigIntervalScheduler(
                        timedelta=datetime.timedelta(0, 20)),
                    actions=FrozenDict(**{
                        'action0_0': ConfigAction(
                            name='action0_0',
                            command='test_command0.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=False,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=ConfigCleanupAction(
                        name='cleanup',
                        command='test_command0.1',
                        requires=(),
                        node=None),
                    enabled=True),
                'test_job1': ConfigJob(
                    name='test_job1',
                    node='batch0',
                    enabled=True,
                    schedule=ConfigDailyScheduler(
                        ordinals=None,
                        weekdays=set([0, 2, 4]),
                        monthdays=None,
                        months=None,
                        timestr='00:30',
                    ),
                    actions=FrozenDict(**{
                        'action1_1': ConfigAction(
                            name='action1_1',
                            command='test_command1.1',
                            requires=('action1_0',),
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
                    enabled=True,
                    schedule=ConfigDailyScheduler(
                        ordinals=None,
                        weekdays=None,
                        monthdays=None,
                        months=None,
                        timestr='16:30',
                    ),
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
                    schedule=ConfigConstantScheduler(),
                    enabled=True,
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
                    queueing=False,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None),
                'test_job4': ConfigJob(
                    name='test_job4',
                    node='batch0_batch1',
                    schedule=ConfigDailyScheduler(
                        ordinals=None,
                        weekdays=None,
                        monthdays=None,
                        months=None,
                        timestr='00:00',
                    ),
                    actions=FrozenDict(**{
                        'action4_0': ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None)}),
                    queueing=True,
                    run_limit=50,
                    all_nodes=True,
                    cleanup_action=None,
                    enabled=False)
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

        # we could just do a big assert_equal here, but it would be hella hard
        # to debug failures that way.
        assert_equal(test_config.working_dir, expected.working_dir)
        assert_equal(test_config.syslog_address, expected.syslog_address)
        assert_equal(test_config.command_context, expected.command_context)
        assert_equal(test_config.ssh_options, expected.ssh_options)
        assert_equal(test_config.notification_options, expected.notification_options)
        assert_equal(test_config.time_zone, expected.time_zone)
        assert_equal(test_config.nodes, expected.nodes)
        assert_equal(test_config.node_pools, expected.node_pools)
        assert_equal(test_config.jobs['test_job0'], expected.jobs['test_job0'])
        assert_equal(test_config.jobs['test_job1'], expected.jobs['test_job1'])
        assert_equal(test_config.jobs['test_job2'], expected.jobs['test_job2'])
        assert_equal(test_config.jobs['test_job3'], expected.jobs['test_job3'])
        assert_equal(test_config.jobs['test_job4'], expected.jobs['test_job4'])
        assert_equal(test_config.jobs, expected.jobs)
        assert_equal(test_config.services, expected.services)
        assert_equal(test_config, expected)
        assert_equal(test_config.jobs['test_job4'].enabled, False)


class JobConfigTestCase(TestCase):

    def test_no_actions(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        """
        expected_message = "Job test_job0 is missing options: actions"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_message, str(exception))

    def test_empty_actions(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
        """
        expected_message = "Value at Job.test_job0 is not a list with items"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_message, str(exception))

    def test_dupe_names(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
            -
                name: "action0_0"
                command: "test_command0.0"

        """
        expected_message = "Action name action0_0 on job test_job0 used twice"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_message, str(exception))

    def test_bad_requires(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
            -
                name: "action0_1"
                command: "test_command0.1"

    -
        name: "test_job1"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action1_0"
                command: "test_command1.0"
                requires: action0_0

        """
        expected_message = ('jobs.test_job1.action1_0 has a dependency '
                '"action0_0" that is not in the same job!')
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_message, str(exception))


    def test_circular_dependency(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
                requires: action0_1
            -
                name: "action0_1"
                command: "test_command0.1"
                requires: action0_0
        """
        expect = "Circular dependency in job.test_job0: action0_0 -> action0_1"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expect, exception)

    def test_config_cleanup_name_collision(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "%s"
                command: "test_command0.0"

        """ % CLEANUP_ACTION_NAME
        expected_message = "Bad action name at Action.cleanup: cleanup"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_message, str(exception))

    def test_config_cleanup_action_name(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action:
            name: "gerald"
            command: "test_command0.1"
        """
        expected_msg = "Cleanup actions cannot have custom names"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_msg, str(exception))

    def test_config_cleanup_requires(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action:
            command: "test_command0.1"
            requires: [action0_0]
        """
        expected_msg = "can not have requires"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_msg, str(exception))

    def test_job_in_services(self):
        test_config = BASE_CONFIG + """
services:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                command: "test_command0.0"
        cleanup_action:
            command: "test_command0.1"
"""
        expected_msg = "Service test_job0 is missing options:"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_msg, str(exception))

    def test_overlap_job_service_names(self):
        tron_config = dict(
            jobs=[
                dict(
                    name="sameName",
                    node="localhost",
                    schedule="interval 20s",
                    actions=[dict(name="someAction", command="something")]
                )
            ],
            services=[
                dict(
                    name="sameName",
                    node="localhost",
                    pid_file="file",
                    command="something",
                    monitor_interval="20"
                )
            ]
        )
        expected_message = "Job and Service names must be unique sameName"
        exception = assert_raises(ConfigError, valid_config, tron_config)
        assert_in(expected_message, str(exception))

    def test_validate_job_no_actions(self):
        job_config = dict(
            name="job_name",
            node="localhost",
            schedule="constant",
            actions=[]
        )
        expected_msg = "Value at Job.job_name is not a list with items"
        exception = assert_raises(ConfigError, valid_job, job_config)
        assert_in(expected_msg, str(exception))


class NodeConfigTestCase(TestCase):

    def test_validate_node_pool(self):
        config_node_pool = valid_node_pool(
            dict(name="theName", nodes=["node1", "node2"])
        )
        assert_equal(config_node_pool.name, "theName")
        assert_equal(len(config_node_pool.nodes), 2)

    def test_overlap_node_and_node_pools(self):
        tron_config = dict(
            nodes=[
                dict(name="sameName", hostname="localhost")
            ],
            node_pools=[
                dict(name="sameName", nodes=["sameNode"])
            ]
        )
        expected_msg = "Node and NodePool names must be unique sameName"
        exception = assert_raises(ConfigError, valid_config, tron_config)
        assert_in(expected_msg, str(exception))

    def test_invalid_node_name(self):
        test_config = BASE_CONFIG + dedent("""
            jobs:
                -
                    name: "test_job0"
                    node: "some_unknown_node"
                    schedule: "interval 20s"
                    actions:
                        -
                            name: "action0_0"
                            command: "test_command0.0"
            """)
        expected_msg = "some_unknown_node configured for ConfigJob test_job0"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_msg, str(exception))

    def test_invalid_nested_node_pools(self):
        test_config = dedent("""
            working_dir: "/tmp"

            nodes:
                - name: node0
                  hostname: batch0

            node_pools:
                - name: pool0
                  nodes: [batch1]
                - name: pool1
                  nodes: [node0, pool0]
            jobs:
                - name: somejob
                  node: pool1
                  schedule: "interval 30s"
                  actions:
                    - name: first
                      command: "echo 1"
        """)
        expected_msg = "NodePool pool1 contains another NodePool pool0"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_msg, str(exception))

    def test_invalid_node_pool_config(self):
        test_config = dedent("""
            working_dir: "/tmp"

            nodes:
                - name: node0
                  hostname: batch0

            node_pools:
                - name: pool0
                  hostname: batch1
                - name: pool1
                  nodes: [node0, pool0]
            jobs:
                - name: somejob
                  node: pool1
                  schedule: "interval 30s"
                  actions:
                    - name: first
                      command: "echo 1"
        """)
        expected_msg = "NodePool pool0 is missing options"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_msg, str(exception))


StubConfigObject = config_parse.config_object_factory(
    'StubConfigObject',
    ['req1', 'req2'],
    ['opt1', 'opt2']
)

class StubValidator(config_parse.Validator):
    config_class = StubConfigObject

class ValidatorTestCase(TestCase):

    @setup
    def setup_validator(self):
        self.validator = StubValidator()

    def test_validate_with_none(self):
        expected_msg = "A StubObject is required"
        exception = assert_raises(ConfigError, self.validator.validate, None)
        assert_in(expected_msg, str(exception))

    def test_validate_optional_with_none(self):
        self.validator.optional = True
        assert_equal(self.validator.validate(None), None)

if __name__ == '__main__':
    run()
