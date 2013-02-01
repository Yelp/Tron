import datetime
import os
import shutil
import StringIO
import stat
import tempfile
from textwrap import dedent

import mock

from testify import assert_equal, assert_in
from testify import run, setup, teardown, TestCase
from tron.config import config_parse, schema
from tron.config.config_parse import *
from tron.config.config_parse import _initialize_original_config, _initialize_namespaced_update
from tron.config.schedule_parse import ConfigConstantScheduler
from tron.config.schedule_parse import ConfigGrocScheduler
from tron.config.schedule_parse import ConfigIntervalScheduler
from tron.config.schema import MASTER_NAMESPACE
from tests.assertions import assert_raises
from tron.utils.dicts import FrozenDict


BASE_CONFIG = """
ssh_options:
    agent: true
    identities:
        - tests/test_id_rsa

nodes:
    - name: node0
      hostname: 'node0'
    - name: node1
      hostname: 'node1'

node_pools:
    - name: NodePool
      nodes: [node0, node1]
"""


class ConfigTestCase(TestCase):
    BASE_CONFIG = """
output_stream_dir: "/tmp"

ssh_options:
    agent: true
    identities:
        - tests/test_id_rsa

nodes:
    -   name: node0
        hostname: 'node0'
    -   name: node1
        hostname: 'node1'
node_pools:
    -   name: nodePool
        nodes: [node0, node1]
    """

    config = BASE_CONFIG + """

command_context:
    batch_dir: /tron/batch/test/foo
    python: /usr/bin/python

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

    -
        name: "test_job1"
        node: node0
        schedule: "daily 00:30:00 MWF"
        allow_overlap: True
        actions:
            -
                name: "action1_0"
                command: "test_command1.0"
            -
                name: "action1_1"
                command: "test_command1.1"
                requires: [action1_0]

    -
        name: "test_job2"
        node: node1
        schedule: "daily 16:30:00"
        actions:
            -
                name: "action2_0"
                command: "test_command2.0"

    -
        name: "test_job3"
        node: node1
        schedule: "constant"
        actions:
            -
                name: "action3_0"
                command: "test_command3.0"
            -
                name: "action3_1"
                command: "test_command3.1"
            -
                name: "action3_2"
                node: node0
                command: "test_command3.2"
                requires: [action3_0, action3_1]

    -
        name: "test_job4"
        node: nodePool
        all_nodes: True
        schedule: "daily"
        enabled: False
        actions:
            -
                name: "action4_0"
                command: "test_command4.0"

services:
    -
        name: "service0"
        node: nodePool
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
            config_name=MASTER_NAMESPACE,
            output_stream_dir='/tmp',
            command_context=FrozenDict({
                'python': '/usr/bin/python',
                'batch_dir': '/tron/batch/test/foo'
            }),
            ssh_options=ConfigSSHOptions(
                agent=True,
                identities=['tests/test_id_rsa'],
            ),
            notification_options=None,
            time_zone=None,
            state_persistence=config_parse.DEFAULT_STATE_PERSISTENCE,
            nodes=FrozenDict({
                'node0': ConfigNode(name='node0', username=os.environ['USER'], hostname='node0'),
                'node1': ConfigNode(name='node1', username=os.environ['USER'], hostname='node1')
            }),
            node_pools=FrozenDict({
                'nodePool': ConfigNodePool(nodes=['node0', 'node1'],
                                                name='nodePool')
            }),
            jobs=FrozenDict({
                'test_job0': ConfigJob(
                    name='test_job0',
                    namespace='MASTER',
                    node='node0',
                    schedule=ConfigIntervalScheduler(
                        timedelta=datetime.timedelta(0, 20)),
                    actions=FrozenDict({
                        'action0_0': ConfigAction(
                            name='action0_0',
                            command='test_command0.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=ConfigCleanupAction(
                        name='cleanup',
                        command='test_command0.1',
                        requires=(),
                        node=None),
                    enabled=True,
                    allow_overlap=False),
                'test_job1': ConfigJob(
                    name='test_job1',
                    namespace='MASTER',
                    node='node0',
                    enabled=True,
                    schedule=ConfigGrocScheduler(
                        ordinals=None,
                        weekdays=set([1, 3, 5]),
                        monthdays=None,
                        months=None,
                        timestr='00:30',
                    ),
                    actions=FrozenDict({
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
                    cleanup_action=None,
                    allow_overlap=True),
                'test_job2': ConfigJob(
                    name='test_job2',
                    namespace='MASTER',
                    node='node1',
                    enabled=True,
                    schedule=ConfigGrocScheduler(
                        ordinals=None,
                        weekdays=None,
                        monthdays=None,
                        months=None,
                        timestr='16:30',
                    ),
                    actions=FrozenDict({
                        'action2_0': ConfigAction(
                            name='action2_0',
                            command='test_command2.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    allow_overlap=False),
                'test_job3': ConfigJob(
                    name='test_job3',
                    namespace='MASTER',
                    node='node1',
                    schedule=ConfigConstantScheduler(),
                    enabled=True,
                    actions=FrozenDict({
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
                            node='node0')
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    allow_overlap=False),
                'test_job4': ConfigJob(
                    name='test_job4',
                    namespace='MASTER',
                    node='nodePool',
                    schedule=ConfigGrocScheduler(
                        ordinals=None,
                        weekdays=None,
                        monthdays=None,
                        months=None,
                        timestr='00:00',
                    ),
                    actions=FrozenDict({
                        'action4_0': ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None)}),
                    queueing=True,
                    run_limit=50,
                    all_nodes=True,
                    cleanup_action=None,
                    enabled=False,
                    allow_overlap=False)
                }),
                services=FrozenDict({
                    'service0': ConfigService(
                        name='service0',
                        namespace='MASTER',
                        node='nodePool',
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
        assert_equal(test_config[MASTER_NAMESPACE].command_context, expected.command_context)
        assert_equal(test_config[MASTER_NAMESPACE].ssh_options, expected.ssh_options)
        assert_equal(test_config[MASTER_NAMESPACE].notification_options, expected.notification_options)
        assert_equal(test_config[MASTER_NAMESPACE].time_zone, expected.time_zone)
        assert_equal(test_config[MASTER_NAMESPACE].nodes, expected.nodes)
        assert_equal(test_config[MASTER_NAMESPACE].node_pools, expected.node_pools)
        assert_equal(test_config[MASTER_NAMESPACE].jobs['test_job0'], expected.jobs['test_job0'])
        assert_equal(test_config[MASTER_NAMESPACE].jobs['test_job1'], expected.jobs['test_job1'])
        assert_equal(test_config[MASTER_NAMESPACE].jobs['test_job2'], expected.jobs['test_job2'])
        assert_equal(test_config[MASTER_NAMESPACE].jobs['test_job3'], expected.jobs['test_job3'])
        assert_equal(test_config[MASTER_NAMESPACE].jobs['test_job4'], expected.jobs['test_job4'])
        assert_equal(test_config[MASTER_NAMESPACE].jobs, expected.jobs)
        assert_equal(test_config[MASTER_NAMESPACE].services, expected.services)
        assert_equal(test_config[MASTER_NAMESPACE], expected)
        assert_equal(test_config[MASTER_NAMESPACE].jobs['test_job4'].enabled, False)


class NamedConfigTestCase(TestCase):
    config = """
config_name: "test_namespace"
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

    -
        name: "test_job1"
        node: node0
        schedule: "daily 00:30:00 MWF"
        allow_overlap: True
        actions:
            -
                name: "action1_0"
                command: "test_command1.0"
            -
                name: "action1_1"
                command: "test_command1.1"
                requires: [action1_0]

    -
        name: "test_job2"
        node: node1
        schedule: "daily 16:30:00"
        actions:
            -
                name: "action2_0"
                command: "test_command2.0"

    -
        name: "test_job3"
        node: node1
        schedule: "constant"
        actions:
            -
                name: "action3_0"
                command: "test_command3.0"
            -
                name: "action3_1"
                command: "test_command3.1"
            -
                name: "action3_2"
                node: node0
                command: "test_command3.2"
                requires: [action3_0, action3_1]

    -
        name: "test_job4"
        node: nodePool
        all_nodes: True
        schedule: "daily"
        enabled: False
        actions:
            -
                name: "action4_0"
                command: "test_command4.0"

services:
    -
        name: "service0"
        node: nodePool
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
        expected = NamedTronConfig(
            config_name='test_namespace',
            jobs=FrozenDict({
                'test_job0': ConfigJob(
                    name='test_job0',
                    namespace='test_namespace',
                    node='node0',
                    schedule=ConfigIntervalScheduler(
                        timedelta=datetime.timedelta(0, 20)),
                    actions=FrozenDict({
                        'action0_0': ConfigAction(
                            name='action0_0',
                            command='test_command0.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=ConfigCleanupAction(
                        name='cleanup',
                        command='test_command0.1',
                        requires=(),
                        node=None),
                    enabled=True,
                    allow_overlap=False),
                'test_job1': ConfigJob(
                    name='test_job1',
                    namespace='test_namespace',
                    node='node0',
                    enabled=True,
                    schedule=ConfigGrocScheduler(
                        ordinals=None,
                        weekdays=set([1, 3, 5]),
                        monthdays=None,
                        months=None,
                        timestr='00:30',
                    ),
                    actions=FrozenDict({
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
                    cleanup_action=None,
                    allow_overlap=True),
                'test_job2': ConfigJob(
                    name='test_job2',
                    namespace='test_namespace',
                    node='node1',
                    enabled=True,
                    schedule=ConfigGrocScheduler(
                        ordinals=None,
                        weekdays=None,
                        monthdays=None,
                        months=None,
                        timestr='16:30',
                    ),
                    actions=FrozenDict({
                        'action2_0': ConfigAction(
                            name='action2_0',
                            command='test_command2.0',
                            requires=(),
                            node=None)
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    allow_overlap=False),
                'test_job3': ConfigJob(
                    name='test_job3',
                    namespace='test_namespace',
                    node='node1',
                    schedule=ConfigConstantScheduler(),
                    enabled=True,
                    actions=FrozenDict({
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
                            node='node0')
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    allow_overlap=False),
                'test_job4': ConfigJob(
                    name='test_job4',
                    namespace='test_namespace',
                    node='nodePool',
                    schedule=ConfigGrocScheduler(
                        ordinals=None,
                        weekdays=None,
                        monthdays=None,
                        months=None,
                        timestr='00:00',
                    ),
                    actions=FrozenDict({
                        'action4_0': ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None)}),
                    queueing=True,
                    run_limit=50,
                    all_nodes=True,
                    cleanup_action=None,
                    enabled=False,
                    allow_overlap=False)
                }),
                services=FrozenDict({
                    'service0': ConfigService(
                        name='service0',
                        namespace='test_namespace',
                        node='nodePool',
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
        assert_equal(test_config["test_namespace"].jobs['test_job0'], expected.jobs['test_job0'])
        assert_equal(test_config["test_namespace"].jobs['test_job1'], expected.jobs['test_job1'])
        assert_equal(test_config["test_namespace"].jobs['test_job2'], expected.jobs['test_job2'])
        assert_equal(test_config["test_namespace"].jobs['test_job3'], expected.jobs['test_job3'])
        assert_equal(test_config["test_namespace"].jobs['test_job4'], expected.jobs['test_job4'])
        assert_equal(test_config["test_namespace"].jobs, expected.jobs)
        assert_equal(test_config["test_namespace"].services, expected.services)
        assert_equal(test_config["test_namespace"], expected)
        assert_equal(test_config["test_namespace"].jobs['test_job4'].enabled, False)


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
        expected_message = "Value at config.Job.test_job0.actions"
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
        expected_message = "config.Job.test_job0.actions.Action.cleanup.name"
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
                    monitor_interval=20
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
        config_context = config_parse.ConfigContext('config', None, None)
        expected_msg = "Value at config.Job.job_name.actions is not a list with items"
        exception = assert_raises(ConfigError, valid_job, job_config, config_context)
        assert_in(expected_msg, str(exception))


class NodeConfigTestCase(TestCase):

    def test_validate_node_pool(self):
        config_node_pool = valid_node_pool(
            dict(name="theName", nodes=["node1", "node2"]))
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
            nodes:
                - name: node0
                  hostname: node0

            node_pools:
                - name: pool0
                  nodes: [node1]
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
            nodes:
                - name: node0
                  hostname: node0

            node_pools:
                - name: pool0
                  hostname: node1
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

    def test_missing_original_config(self):
        assert_equal({}, _initialize_original_config('/test/bogusssss'))


class InitializeNamespacedUpdateTestCase(TestCase):

    def test_valid_unnamed_update(self):
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
services:
    -
        name: "test_service0"
        node: node0
        command: "service_command0"
        count: 2
        pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
        monitor_interval: 20
        """
        expected_result = ('MASTER',
            {'config_name': 'MASTER',
            'jobs': [{'node': 'node0',
                      'namespace': 'MASTER',
                      'schedule': 'interval 20s',
                      'name': 'test_job0',
                      'actions': [{'command': 'test_command0.0',
                                   'name': 'action0_0'}],
                      'cleanup_action': {'command': 'test_command0.1'}}],
            'node_pools': [{'nodes': ['node0',
                                      'node1'],
                            'name': 'NodePool'}],
            'ssh_options': {'identities': ['tests/test_id_rsa'],
                            'agent': True},
            'services': [{'node': 'node0',
                          'namespace': 'MASTER',
                          'count': 2,
                          'command': 'service_command0',
                          'name': 'test_service0',
                          'monitor_interval': 20,
                          'pid_file': '/var/run/%(name)s-%(instance_number)s.pid'}],
            'nodes': [{'hostname': 'node0',
                       'name': 'node0'},
                      {'hostname': 'node1',
                       'name': 'node1'}]})
        assert_equal(expected_result, _initialize_namespaced_update(test_config))

    def test_invalid_unnamed_update(self):
        test_config = BASE_CONFIG + """
foobar:
"""
        expected_message = "Unknown options in Tron : foobar"
        exception = assert_raises(ConfigError, _initialize_namespaced_update, test_config)
        assert_in(expected_message, str(exception))

    def test_valid_named_update(self):
        test_config = """
config_name: test_config
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
services:
    -
        name: "test_service0"
        node: node0
        command: "service_command0"
        count: 2
        pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
        monitor_interval: 20
        """
        expected_result = ('test_config',
                           {'services': [{'node': 'node0',
                                          'namespace': 'test_config',
                                          'count': 2,
                                          'command': 'service_command0',
                                          'name': 'test_service0',
                                          'monitor_interval': 20,
                                          'pid_file': '/var/run/%(name)s-%(instance_number)s.pid'}],
                            'config_name': 'test_config',
                            'jobs': [{'node': 'node0',
                                      'namespace': 'test_config',
                                      'schedule': 'interval 20s',
                                      'name': 'test_job0',
                                      'actions': [{'command': 'test_command0.0',
                                                   'name': 'action0_0'}],
                                      'cleanup_action': {'command': 'test_command0.1'}}]})
        assert_equal(expected_result, _initialize_namespaced_update(test_config))

    def test_invalid_named_update(self):
        test_config = """
config_name: "foo"
bozray:
        """
        expected_message = "Unknown options in NamedTron : bozray"
        exception = assert_raises(ConfigError, load_config, test_config)
        assert_in(expected_message, str(exception))

    def test_valid_job_collation(self):
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
services:
    -
        name: "test_service0"
        node: node0
        command: "service_command0"
        count: 2
        pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
        monitor_interval: 20
        """
        expected_collated_jobs = {'MASTER_test_job0':
                ConfigJob(name='test_job0',
                          namespace='MASTER',
                          node='node0',
                          schedule=ConfigIntervalScheduler(timedelta=datetime.timedelta(0, 20)),
                          actions=FrozenDict({'action0_0':
                                  ConfigAction(name='action0_0',
                                               command='test_command0.0',
                                               requires=(),
                                               node=None)}),
                          queueing=True,
                          run_limit=50,
                          all_nodes=False,
                          cleanup_action=ConfigCleanupAction(command='test_command0.1',
                                 requires=(),
                                 name='cleanup',
                                 node=None),
                          enabled=True,
                          allow_overlap=False)
                }

        expected_collated_services = {'MASTER_test_service0':
                ConfigService(name='test_service0',
                              namespace='MASTER',
                              node='node0',
                              pid_file='/var/run/%(name)s-%(instance_number)s.pid',
                              command='service_command0',
                              monitor_interval=20,
                              restart_interval=None,
                              count=2)
                }

        config_container = load_config(test_config)
        jobs, services = collate_jobs_and_services(config_container)
        assert_equal(expected_collated_jobs, jobs)
        assert_equal(expected_collated_services, services)

    def test_invalid_job_collation(self):
        jobs = FrozenDict({'test_collision0': ConfigJob(
            name='test_collision0',
            node='node0',
            namespace='MASTER',
            schedule=ConfigIntervalScheduler(
                timedelta=datetime.timedelta(0, 20)),
            actions=FrozenDict({'action0_0': ConfigAction(name='action0_0',
                command='test_command0.0',
                requires=(),
                node=None)}),
            queueing=True,
            run_limit=50,
            all_nodes=False,
            cleanup_action=ConfigCleanupAction(command='test_command0.1',
                requires=(),
                name='cleanup',
                node=None),
            enabled=True,
            allow_overlap=False)})

        services = FrozenDict({'test_collision0': ConfigService(name='test_collision0',
            namespace='MASTER',
            node='node0',
            pid_file='/var/run/%(name)s-%(instance_number)s.pid',
            command='service_command0',
            monitor_interval=20,
            restart_interval=None,
            count=2)})
        fake_config = mock.Mock()
        setattr(fake_config, 'jobs', jobs)
        setattr(fake_config, 'services', services)
        expected_message = "Collision found for identifier 'MASTER_test_collision0'"
        exception = assert_raises(ConfigError, collate_jobs_and_services, {'MASTER': fake_config})
        assert_in(expected_message, str(exception))


StubConfigObject = schema.config_object_factory(
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


class ValidOutputStreamDirTestCase(TestCase):

    @setup
    def setup_dir(self):
        self.dir = tempfile.mkdtemp()

    @teardown
    def teardown_dir(self):
        shutil.rmtree(self.dir)

    def test_valid_dir(self):
        assert_equal(self.dir, valid_output_stream_dir(self.dir, None))

    def test_missing_dir(self):
        exception = assert_raises(ConfigError, valid_output_stream_dir, 'bogus-dir', None)
        assert_in("is not a directory", str(exception))

    def test_no_ro_dir(self):
        os.chmod(self.dir, stat.S_IRUSR)
        exception = assert_raises(ConfigError, valid_output_stream_dir, self.dir, None)
        assert_in("is not writable", str(exception))


class ValidatorIdentifierTestCase(TestCase):

    def test_valid_identifier_too_long(self):
        assert_raises(ConfigError, valid_identifier, 'a' * 256, mock.Mock())

    def test_valid_identifier(self):
        name = 'avalidname'
        assert_equal(name, valid_identifier(name, mock.Mock()))

    def test_valid_identifier_invalid_character(self):
        for name in ['invalid space', '*name', '1numberstarted', 123, '']:
            assert_raises(ConfigError, valid_identifier, name, mock.Mock())


class BuildFormatStringValidatorTestCase(TestCase):

    @setup
    def setup_keys(self):
        self.keys = ['one', 'seven', 'stars']
        self.validator = build_format_string_validator(self.keys)

    def test_validator_passes(self):
        template = "The %(one)s thing I %(seven)s is %(stars)s"
        assert self.validator(template, NullConfigContext)

    def test_validator_error(self):
        template = "The %(one)s thing I %(seven)s is %(unknown)s"
        assert_raises(ConfigError, self.validator, template, NullConfigContext)


if __name__ == '__main__':
    run()
