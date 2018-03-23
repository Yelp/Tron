from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import os
import shutil
import tempfile
import textwrap

import mock
import pytz
from testify import assert_equal
from testify import assert_in
from testify import run
from testify import setup
from testify import teardown
from testify import TestCase

from tests.assertions import assert_raises
from tron import yaml
from tron.config import config_parse
from tron.config import config_utils
from tron.config import ConfigError
from tron.config import manager
from tron.config import schedule_parse
from tron.config import schema
from tron.config.config_parse import build_format_string_validator
from tron.config.config_parse import CLEANUP_ACTION_NAME
from tron.config.config_parse import valid_cleanup_action_name
from tron.config.config_parse import valid_config
from tron.config.config_parse import valid_job
from tron.config.config_parse import valid_node_pool
from tron.config.config_parse import valid_output_stream_dir
from tron.config.config_parse import validate_fragment
from tron.config.config_utils import NullConfigContext
from tron.config.schedule_parse import ConfigConstantScheduler
from tron.config.schedule_parse import ConfigIntervalScheduler
from tron.config.schema import MASTER_NAMESPACE
from tron.utils.dicts import FrozenDict


BASE_CONFIG = """
ssh_options:
    agent: false
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

clusters:
    - cluster-one
    - cluster-two
"""


def valid_config_from_yaml(config_content):
    return valid_config(manager.from_string(config_content))


class ConfigTestCase(TestCase):
    BASE_CONFIG = """
output_stream_dir: "/tmp"

time_zone: "EST"

ssh_options:
    agent: false
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

clusters:
    - cluster-one
    - cluster-two
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
        time_zone: "Pacific/Auckland"
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
    -
        name: "test_job_paasta"
        node: nodePool
        service: my_service
        deploy_group: prod.non_canary
        schedule: "daily"
        actions:
            -
                name: "action4_0"
                executor: paasta
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

    @mock.patch.dict('tron.config.config_parse.ValidateNode.defaults')
    def test_attributes(self):
        config_parse.ValidateNode.defaults['username'] = 'foo'
        expected = schema.TronConfig(
            action_runner=FrozenDict(),
            output_stream_dir='/tmp',
            command_context=FrozenDict({
                'python': '/usr/bin/python',
                'batch_dir': '/tron/batch/test/foo',
            }),
            ssh_options=schema.ConfigSSHOptions(
                agent=False,
                identities=('tests/test_id_rsa',),
                known_hosts_file=None,
                connect_timeout=30,
                idle_connection_timeout=3600,
                jitter_min_load=4,
                jitter_max_delay=20,
                jitter_load_factor=1,
            ),
            notification_options=None,
            time_zone=pytz.timezone("EST"),
            state_persistence=config_parse.DEFAULT_STATE_PERSISTENCE,
            nodes=FrozenDict({
                'node0': schema.ConfigNode(
                    name='node0',
                    username='foo', hostname='node0', port=22,
                ),
                'node1': schema.ConfigNode(
                    name='node1',
                    username='foo', hostname='node1', port=22,
                ),
            }),
            node_pools=FrozenDict({
                'nodePool': schema.ConfigNodePool(
                    nodes=('node0', 'node1'),
                    name='nodePool',
                ),
            }),
            clusters=('cluster-one', 'cluster-two'),
            jobs=FrozenDict({
                'MASTER.test_job0': schema.ConfigJob(
                    name='MASTER.test_job0',
                    namespace='MASTER',
                    node='node0',
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=ConfigIntervalScheduler(
                        timedelta=datetime.timedelta(0, 20), jitter=None,
                    ),
                    actions=FrozenDict({
                        'action0_0': schema.ConfigAction(
                            name='action0_0',
                            command='test_command0.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=schema.ConfigCleanupAction(
                        name='cleanup',
                        command='test_command0.1',
                        node=None,
                        executor='ssh',
                        cluster=None,
                        pool=None,
                        cpus=None,
                        mem=None,
                        service=None,
                        deploy_group=None,
                    ),
                    enabled=True,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'MASTER.test_job1': schema.ConfigJob(
                    name='MASTER.test_job1',
                    namespace='MASTER',
                    node='node0',
                    enabled=True,
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days={1, 3, 5},
                        hour=0, minute=30, second=0,
                        original="00:30:00 MWF",
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action1_1': schema.ConfigAction(
                            name='action1_1',
                            command='test_command1.1',
                            requires=('action1_0',),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                        'action1_0': schema.ConfigAction(
                            name='action1_0',
                            command='test_command1.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    max_runtime=None,
                    allow_overlap=True,
                    time_zone=pytz.timezone("Pacific/Auckland"),
                ),
                'MASTER.test_job2': schema.ConfigJob(
                    name='MASTER.test_job2',
                    namespace='MASTER',
                    node='node1',
                    enabled=True,
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days=set(),
                        hour=16, minute=30, second=0,
                        original="16:30:00 ",
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action2_0': schema.ConfigAction(
                            name='action2_0',
                            command='test_command2.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'MASTER.test_job3': schema.ConfigJob(
                    name='MASTER.test_job3',
                    namespace='MASTER',
                    node='node1',
                    schedule=ConfigConstantScheduler(),
                    enabled=True,
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    actions=FrozenDict({
                        'action3_1': schema.ConfigAction(
                            name='action3_1',
                            command='test_command3.1',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                        'action3_0': schema.ConfigAction(
                            name='action3_0',
                            command='test_command3.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                        'action3_2': schema.ConfigAction(
                            name='action3_2',
                            command='test_command3.2',
                            requires=('action3_0', 'action3_1'),
                            node='node0',
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'MASTER.test_job4': schema.ConfigJob(
                    name='MASTER.test_job4',
                    namespace='MASTER',
                    node='nodePool',
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days=set(),
                        hour=0, minute=0, second=0,
                        original='00:00:00 ',
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action4_0': schema.ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=True,
                    cleanup_action=None,
                    enabled=False,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'MASTER.test_job_paasta': schema.ConfigJob(
                    name='MASTER.test_job_paasta',
                    namespace='MASTER',
                    node='nodePool',
                    monitoring={},
                    service='my_service',
                    deploy_group='prod.non_canary',
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days=set(),
                        hour=0, minute=0, second=0,
                        original='00:00:00 ',
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action4_0': schema.ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None,
                            executor='paasta',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    enabled=True,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
            }),
            services=FrozenDict(
                {
                    'MASTER.service0': schema.ConfigService(
                        name='MASTER.service0',
                        namespace='MASTER',
                        node='nodePool',
                        pid_file='/var/run/%(name)s-%(instance_number)s.pid',
                        command='service_command0',
                        monitor_interval=20,
                        monitor_retries=5,
                        restart_delay=None,
                        count=2,
                    ),
                },
            ),
        )

        test_config = valid_config_from_yaml(self.config)
        assert_equal(test_config.command_context, expected.command_context)
        assert_equal(test_config.ssh_options, expected.ssh_options)
        assert_equal(
            test_config.notification_options,
            expected.notification_options,
        )
        assert_equal(test_config.time_zone, expected.time_zone)
        assert_equal(test_config.nodes, expected.nodes)
        assert_equal(test_config.node_pools, expected.node_pools)
        assert_equal(
            test_config.jobs['MASTER.test_job0'],
            expected.jobs['MASTER.test_job0'],
        )
        assert_equal(
            test_config.jobs['MASTER.test_job1'],
            expected.jobs['MASTER.test_job1'],
        )
        assert_equal(
            test_config.jobs['MASTER.test_job2'],
            expected.jobs['MASTER.test_job2'],
        )
        assert_equal(
            test_config.jobs['MASTER.test_job3'],
            expected.jobs['MASTER.test_job3'],
        )
        assert_equal(
            test_config.jobs['MASTER.test_job4'],
            expected.jobs['MASTER.test_job4'],
        )
        assert_equal(
            test_config.jobs['MASTER.test_job_paasta'],
            expected.jobs['MASTER.test_job_paasta'],
        )
        assert_equal(test_config.jobs, expected.jobs)
        assert_equal(test_config.services, expected.services)
        assert_equal(test_config, expected)
        assert_equal(test_config.jobs['MASTER.test_job4'].enabled, False)

    def test_empty_node_test(self):
        valid_config_from_yaml("""nodes:""")


class NamedConfigTestCase(TestCase):
    config = """
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
                command: "test_command1.0 %(some_var)s"
            -
                name: "action1_1"
                command: "test_command1.1"
                requires: [action1_0]

    -
        name: "test_job2"
        node: node1
        schedule: "daily 16:30:00"
        monitoring: {}
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
        node: NodePool
        all_nodes: True
        schedule: "daily"
        enabled: False
        actions:
            -
                name: "action4_0"
                command: "test_command4.0"
    -
        name: "test_job_paasta"
        node: NodePool
        service: my_service
        deploy_group: prod.non_canary
        schedule: "daily"
        actions:
            -
                name: "action4_0"
                executor: paasta
                command: "test_command4.0"

services:
    -
        name: "service0"
        node: NodePool
        command: "service_command0"
        count: 2
        pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
        monitor_interval: 20
    -
        name: "service1"
        node: NodePool
        command: "service_command1"
        count: 20
        pid_file: "/var/run/%(name)s-%(instance_number)s.pid"
        monitor_interval: 40
"""

    def test_attributes(self):
        expected = schema.NamedTronConfig(
            jobs=FrozenDict({
                'test_job0': schema.ConfigJob(
                    name='test_job0',
                    namespace='test_namespace',
                    node='node0',
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=ConfigIntervalScheduler(
                        timedelta=datetime.timedelta(0, 20),
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action0_0': schema.ConfigAction(
                            name='action0_0',
                            command='test_command0.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=schema.ConfigCleanupAction(
                        name='cleanup',
                        command='test_command0.1',
                        node=None,
                        executor='ssh',
                        cluster=None,
                        pool=None,
                        cpus=None,
                        mem=None,
                        service=None,
                        deploy_group=None,
                    ),
                    enabled=True,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'test_job1': schema.ConfigJob(
                    name='test_job1',
                    namespace='test_namespace',
                    node='node0',
                    enabled=True,
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days={1, 3, 5},
                        hour=0,
                        minute=30,
                        second=0,
                        original="00:30:00 MWF",
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action1_1': schema.ConfigAction(
                            name='action1_1',
                            command='test_command1.1',
                            requires=('action1_0',),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                        'action1_0': schema.ConfigAction(
                            name='action1_0',
                            command='test_command1.0 %(some_var)s',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    max_runtime=None,
                    allow_overlap=True,
                    time_zone=None,
                ),
                'test_job2': schema.ConfigJob(
                    name='test_job2',
                    namespace='test_namespace',
                    node='node1',
                    enabled=True,
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days=set(),
                        hour=16,
                        minute=30,
                        second=0,
                        original="16:30:00 ",
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action2_0': schema.ConfigAction(
                            name='action2_0',
                            command='test_command2.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'test_job3': schema.ConfigJob(
                    name='test_job3',
                    namespace='test_namespace',
                    node='node1',
                    schedule=ConfigConstantScheduler(),
                    enabled=True,
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    actions=FrozenDict({
                        'action3_1': schema.ConfigAction(
                            name='action3_1',
                            command='test_command3.1',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                        'action3_0': schema.ConfigAction(
                            name='action3_0',
                            command='test_command3.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                        'action3_2': schema.ConfigAction(
                            name='action3_2',
                            command='test_command3.2',
                            requires=('action3_0', 'action3_1'),
                            node='node0',
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'test_job4': schema.ConfigJob(
                    name='test_job4',
                    namespace='test_namespace',
                    node='NodePool',
                    monitoring={},
                    service=None,
                    deploy_group=None,
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days=set(),
                        hour=0, minute=0, second=0,
                        original="00:00:00 ",
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action4_0': schema.ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None,
                            executor='ssh',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=True,
                    cleanup_action=None,
                    enabled=False,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
                'test_job_paasta': schema.ConfigJob(
                    name='test_job_paasta',
                    namespace='test_namespace',
                    node='NodePool',
                    monitoring={},
                    service='my_service',
                    deploy_group='prod.non_canary',
                    schedule=schedule_parse.ConfigDailyScheduler(
                        days=set(),
                        hour=0, minute=0, second=0,
                        original='00:00:00 ',
                        jitter=None,
                    ),
                    actions=FrozenDict({
                        'action4_0': schema.ConfigAction(
                            name='action4_0',
                            command='test_command4.0',
                            requires=(),
                            node=None,
                            executor='paasta',
                            cluster=None,
                            pool=None,
                            cpus=None,
                            mem=None,
                            service=None,
                            deploy_group=None,
                        ),
                    }),
                    queueing=True,
                    run_limit=50,
                    all_nodes=False,
                    cleanup_action=None,
                    enabled=True,
                    max_runtime=None,
                    allow_overlap=False,
                    time_zone=None,
                ),
            }),
            services=FrozenDict(
                {
                    'service0': schema.ConfigService(
                        namespace='test_namespace',
                        name='service0',
                        node='NodePool',
                        pid_file='/var/run/%(name)s-%(instance_number)s.pid',
                        command='service_command0',
                        monitor_interval=20,
                        monitor_retries=5,
                        restart_delay=None,
                        count=2,
                    ),
                    'service1': schema.ConfigService(
                        namespace='test_namespace',
                        name='service1',
                        node='NodePool',
                        pid_file='/var/run/%(name)s-%(instance_number)s.pid',
                        command='service_command1',
                        monitor_interval=40.0,
                        monitor_retries=5,
                        restart_delay=None,
                        count=20,
                    ),
                },
            ),
        )

        test_config = validate_fragment(
            'test_namespace', yaml.load(self.config),
        )
        assert_equal(test_config.jobs['test_job0'], expected.jobs['test_job0'])
        assert_equal(test_config.jobs['test_job1'], expected.jobs['test_job1'])
        assert_equal(test_config.jobs['test_job2'], expected.jobs['test_job2'])
        assert_equal(test_config.jobs['test_job3'], expected.jobs['test_job3'])
        assert_equal(test_config.jobs['test_job4'], expected.jobs['test_job4'])
        assert_equal(
            test_config.jobs['test_job_paasta'],
            expected.jobs['test_job_paasta'],
        )
        assert_equal(
            test_config.services['service0'], expected.services['service0'],
        )
        assert_equal(
            test_config.services['service1'], expected.services['service1'],
        )
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
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
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
        expected_message = "Value at config.jobs.Job.test_job0.actions"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
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
        expected = "Duplicate name action0_0 at config.jobs.Job.test_job0.actions"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected, str(exception))

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
                requires: [action0_0]

        """
        expected_message = (
            'jobs.MASTER.test_job1.action1_0 has a dependency '
            '"action0_0" that is not in the same job!'
        )
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
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
                requires: [action0_1]
            -
                name: "action0_1"
                command: "test_command0.1"
                requires: [action0_0]
        """
        expect = "Circular dependency in job.MASTER.test_job0: action0_0 -> action0_1"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expect, str(exception))

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
        expected_message = "config.jobs.Job.test_job0.actions.Action.cleanup.name"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
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
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
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
        expected_msg = "Unknown keys in CleanupAction : requires"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_equal(expected_msg, str(exception))

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
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_job_with_invalid_cluster(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        service: foo
        deploy_group: prod
        actions:
            -
                name: "action0_0"
                executor: paasta
                cluster: unknown-cluster
                command: "test_command0.0"
"""
        expected_msg = "Unknown cluster name unknown-cluster"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_job_with_missing_service_for_paasta_action(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                executor: paasta
                cluster: cluster-one
                command: "test_command0.0"
"""
        expected_msg = "need a service and deploy_group"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_job_with_missing_service_for_paasta_cleanup_action(self):
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
                executor: ssh
        cleanup_action:
            command: "test_command0.1"
            executor: paasta
"""
        expected_msg = "need a service and deploy_group"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_job_with_service_in_paasta_action_only_is_valid(self):
        test_config = BASE_CONFIG + """
jobs:
    -
        name: "test_job0"
        node: node0
        schedule: "interval 20s"
        actions:
            -
                name: "action0_0"
                executor: paasta
                cluster: cluster-one
                service: baz
                deploy_group: prod
                command: "test_command0.0"
"""
        expected = schema.ConfigJob(
            name='MASTER.test_job0',
            namespace='MASTER',
            node='node0',
            monitoring={},
            service=None,
            deploy_group=None,
            schedule=ConfigIntervalScheduler(
                timedelta=datetime.timedelta(0, 20), jitter=None,
            ),
            actions=FrozenDict({
                'action0_0': schema.ConfigAction(
                    name='action0_0',
                    command='test_command0.0',
                    requires=(),
                    node=None,
                    executor='paasta',
                    cluster='cluster-one',
                    pool=None,
                    cpus=None,
                    mem=None,
                    service='baz',
                    deploy_group='prod',
                ),
            }),
            queueing=True,
            run_limit=50,
            all_nodes=False,
            cleanup_action=None,
            enabled=True,
            max_runtime=None,
            allow_overlap=False,
            time_zone=None,
        )
        parsed_config = valid_config_from_yaml(test_config)
        assert_equal(parsed_config.jobs['MASTER.test_job0'], expected)

    def test_overlap_job_service_names(self):
        tron_config = dict(
            nodes=['localhost'],
            jobs=[
                dict(
                    name="sameName",
                    node="localhost",
                    schedule="interval 20s",
                    actions=[dict(name="someAction", command="something")],
                ),
            ],
            services=[
                dict(
                    name="sameName",
                    node="localhost",
                    pid_file="file",
                    command="something",
                    monitor_interval=20,
                ),
            ],
        )
        expected_message = "Job and Service names must be unique MASTER.sameName"
        exception = assert_raises(ConfigError, valid_config, tron_config)
        assert_in(expected_message, str(exception))

    def test_validate_job_no_actions(self):
        job_config = dict(
            name="job_name",
            node="localhost",
            schedule="constant",
            actions=[],
        )
        config_context = config_utils.ConfigContext(
            'config', ['localhost'], ['cluster'], None, None,
        )
        expected_msg = "Required non-empty list at config.Job.job_name.actions"
        exception = assert_raises(
            ConfigError, valid_job, job_config, config_context,
        )
        assert_in(expected_msg, str(exception))


class NodeConfigTestCase(TestCase):

    def test_validate_node_pool(self):
        config_node_pool = valid_node_pool(
            dict(name="theName", nodes=["node1", "node2"]),
        )
        assert_equal(config_node_pool.name, "theName")
        assert_equal(len(config_node_pool.nodes), 2)

    def test_overlap_node_and_node_pools(self):
        tron_config = dict(
            nodes=[
                dict(name="sameName", hostname="localhost"),
            ],
            node_pools=[
                dict(name="sameName", nodes=["sameNode"]),
            ],
        )
        expected_msg = "Node and NodePool names must be unique sameName"
        exception = assert_raises(ConfigError, valid_config, tron_config)
        assert_in(expected_msg, str(exception))

    def test_invalid_node_name(self):
        test_config = BASE_CONFIG + textwrap.dedent("""
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
        expected_msg = "Unknown node name some_unknown_node at config.jobs.Job.test_job0.node"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_equal(expected_msg, str(exception))

    def test_invalid_nested_node_pools(self):
        test_config = textwrap.dedent("""
            nodes:
                - name: node0
                  hostname: node0
                - name: node1
                  hostname: node1

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
        expected_msg = "NodePool pool1 contains other NodePools: pool0"
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_invalid_node_pool_config(self):
        test_config = textwrap.dedent("""
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
        exception = assert_raises(
            ConfigError, valid_config_from_yaml, test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_invalid_named_update(self):
        test_config = """bozray:"""
        test_config = yaml.load(test_config)
        expected_message = "Unknown keys in NamedConfigFragment : bozray"
        exception = assert_raises(
            ConfigError, validate_fragment, 'foo', test_config,
        )
        assert_in(expected_message, str(exception))


class ValidateJobsAndServicesTestCase(TestCase):

    def test_valid_jobs_and_services_success(self):
        test_config = BASE_CONFIG + textwrap.dedent("""
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
                    """)
        expected_jobs = {
            'MASTER.test_job0':
            schema.ConfigJob(
                name='MASTER.test_job0',
                namespace='MASTER',
                node='node0',
                monitoring={},
                service=None,
                deploy_group=None,
                schedule=ConfigIntervalScheduler(
                    timedelta=datetime.timedelta(0, 20), jitter=None,
                ),
                actions=FrozenDict({'action0_0':
                                    schema.ConfigAction(
                                        name='action0_0',
                                        command='test_command0.0',
                                        requires=(),
                                        node=None,
                                        executor='ssh',
                                        cluster=None,
                                        pool=None,
                                        cpus=None,
                                        mem=None,
                                        service=None,
                                        deploy_group=None,
                                    )}),
                queueing=True,
                run_limit=50,
                all_nodes=False,
                cleanup_action=schema.ConfigCleanupAction(
                    command='test_command0.1',
                    name='cleanup',
                    node=None,
                    executor='ssh',
                    cluster=None,
                    pool=None,
                    cpus=None,
                    mem=None,
                    service=None,
                    deploy_group=None,
                ),
                enabled=True,
                allow_overlap=False,
                max_runtime=None,
                time_zone=None,
            ),
        }

        expected_services = {
            'MASTER.test_service0':
            schema.ConfigService(
                name='MASTER.test_service0',
                namespace='MASTER',
                node='node0',
                pid_file='/var/run/%(name)s-%(instance_number)s.pid',
                command='service_command0',
                monitor_interval=20,
                monitor_retries=5,
                restart_delay=None,
                count=2,
            ),
        }

        config = manager.from_string(test_config)
        context = config_utils.ConfigContext(
            'config', ['node0'], ['unused-cluster'], None, MASTER_NAMESPACE,
        )
        config_parse.validate_jobs_and_services(config, context)
        assert_equal(expected_jobs, config['jobs'])
        assert_equal(expected_services, config['services'])


class ValidCleanupActionNameTestCase(TestCase):

    def test_valid_cleanup_action_name_pass(self):
        name = valid_cleanup_action_name(CLEANUP_ACTION_NAME, None)
        assert_equal(CLEANUP_ACTION_NAME, name)

    def test_valid_cleanup_action_name_fail(self):
        assert_raises(
            ConfigError,
            valid_cleanup_action_name, 'other', NullConfigContext,
        )


class ValidOutputStreamDirTestCase(TestCase):

    @setup
    def setup_dir(self):
        self.dir = tempfile.mkdtemp()

    @teardown
    def teardown_dir(self):
        shutil.rmtree(self.dir)

    def test_valid_dir(self):
        path = valid_output_stream_dir(self.dir, NullConfigContext)
        assert_equal(self.dir, path)

    def test_missing_dir(self):
        exception = assert_raises(
            ConfigError,
            valid_output_stream_dir, 'bogus-dir', NullConfigContext,
        )
        assert_in("is not a directory", str(exception))

    # TODO: docker tests run as root so everything is writeable
    # def test_no_ro_dir(self):
    #     os.chmod(self.dir, stat.S_IRUSR)
    #     exception = assert_raises(
    #         ConfigError,
    #         valid_output_stream_dir, self.dir, NullConfigContext,
    #     )
    #     assert_in("is not writable", str(exception))

    def test_missing_with_partial_context(self):
        dir = '/bogus/path/does/not/exist'
        context = config_utils.PartialConfigContext('path', 'MASTER')
        path = config_parse.valid_output_stream_dir(dir, context)
        assert_equal(path, dir)


class BuildFormatStringValidatorTestCase(TestCase):

    @setup
    def setup_keys(self):
        self.context = dict.fromkeys(['one', 'seven', 'stars'])
        self.validator = build_format_string_validator(self.context)

    def test_validator_passes(self):
        template = "The %(one)s thing I %(seven)s is %(stars)s"
        assert self.validator(template, NullConfigContext)

    def test_validator_error(self):
        template = "The %(one)s thing I %(seven)s is %(unknown)s"
        exception = assert_raises(
            ConfigError,
            self.validator, template, NullConfigContext,
        )
        assert_in("Unknown context variable", str(exception))

    def test_validator_passes_with_context(self):
        template = "The %(one)s thing I %(seven)s is %(mars)s"
        context = config_utils.ConfigContext(
            None, None, None, {'mars': 'ok'}, None,
        )
        assert self.validator(template, context)


class ValidateConfigMappingTestCase(TestCase):

    config = BASE_CONFIG + textwrap.dedent(
        """
        command_context:
            some_var: "The string"
        """
    )

    def test_validate_config_mapping_missing_master(self):
        config_mapping = {'other': mock.Mock()}
        seq = config_parse.validate_config_mapping(config_mapping)
        exception = assert_raises(ConfigError, list, seq)
        assert_in('requires a MASTER namespace', str(exception))

    def test_validate_config_mapping(self):
        master_config = manager.from_string(self.config)
        other_config = manager.from_string(NamedConfigTestCase.config)
        config_mapping = {
            'other': other_config,
            MASTER_NAMESPACE: master_config,
        }
        result = list(config_parse.validate_config_mapping(config_mapping))
        assert_equal(len(result), 2)
        assert_equal(result[0][0], MASTER_NAMESPACE)
        assert_equal(result[1][0], 'other')


class ConfigContainerTestCase(TestCase):

    config = BASE_CONFIG + textwrap.dedent(
        """
        command_context:
            some_var: "The string"
        """
    )

    @setup
    def setup_container(self):
        other_config = yaml.load(NamedConfigTestCase.config)
        self.config_mapping = {
            MASTER_NAMESPACE: valid_config(yaml.load(self.config)),
            'other': validate_fragment('other', other_config),
        }
        self.container = config_parse.ConfigContainer(self.config_mapping)

    def test_create(self):
        config_mapping = {
            MASTER_NAMESPACE: yaml.load(self.config),
            'other': yaml.load(NamedConfigTestCase.config),
        }

        container = config_parse.ConfigContainer.create(config_mapping)
        assert_equal(set(container.configs.keys()), {'MASTER', 'other'})

    def test_create_missing_master(self):
        config_mapping = {'other': mock.Mock()}
        assert_raises(
            ConfigError,
            config_parse.ConfigContainer.create, config_mapping,
        )

    def test_get_job_and_service_names(self):
        job_names, service_names = self.container.get_job_and_service_names()
        expected = [
            'test_job1', 'test_job0',
            'test_job3', 'test_job2', 'test_job4',
            'test_job_paasta',
        ]
        assert_equal(set(job_names), set(expected))
        assert_equal(set(service_names), {'service1', 'service0'})

    def test_get_jobs(self):
        expected = [
            'test_job1', 'test_job0',
            'test_job3', 'test_job2', 'test_job4',
            'test_job_paasta',
        ]
        assert_equal(set(expected), set(self.container.get_jobs().keys()))

    def test_get_services(self):
        assert_equal(
            set(self.container.get_services().keys()),
            {'service1', 'service0'},
        )

    def test_get_node_names(self):
        node_names = self.container.get_node_names()
        expected = {'node0', 'node1', 'NodePool'}
        assert_equal(node_names, expected)


class ValidateSSHOptionsTestCase(TestCase):

    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext
        self.config = {'agent': True, 'identities': []}

    @mock.patch.dict('tron.config.config_parse.os.environ')
    def test_post_validation_failed(self):
        if 'SSH_AUTH_SOCK' in os.environ:
            del os.environ['SSH_AUTH_SOCK']
        assert_raises(
            ConfigError, config_parse.valid_ssh_options.validate,
            self.config, self.context,
        )

    @mock.patch.dict('tron.config.config_parse.os.environ')
    def test_post_validation_success(self):
        os.environ['SSH_AUTH_SOCK'] = 'something'
        config = config_parse.valid_ssh_options.validate(
            self.config, self.context,
        )
        assert_equal(config.agent, True)


class ValidateIdentityFileTestCase(TestCase):

    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext
        self.private_file = tempfile.NamedTemporaryFile()

    def test_valid_identity_file_missing_private_key(self):
        exception = assert_raises(
            ConfigError,
            config_parse.valid_identity_file, '/file/not/exist', self.context,
        )
        assert_in("Private key file", str(exception))

    def test_valid_identity_files_missing_public_key(self):
        filename = self.private_file.name
        exception = assert_raises(
            ConfigError,
            config_parse.valid_identity_file, filename, self.context,
        )
        assert_in("Public key file", str(exception))

    def test_valid_identity_files_valid(self):
        filename = self.private_file.name
        fh_private = open(filename + '.pub', 'w')
        try:
            config = config_parse.valid_identity_file(filename, self.context)
        finally:
            fh_private.close()
            os.unlink(fh_private.name)
        assert_equal(config, filename)

    def test_valid_identity_files_missing_with_partial_context(self):
        path = '/bogus/file/does/not/exist'
        context = config_utils.PartialConfigContext('path', 'MASTER')
        file_path = config_parse.valid_identity_file(path, context)
        assert_equal(path, file_path)


class ValidKnownHostsFileTestCase(TestCase):

    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext
        self.known_hosts_file = tempfile.NamedTemporaryFile()

    def test_valid_known_hosts_file_exists(self):
        filename = config_parse.valid_known_hosts_file(
            self.known_hosts_file.name, self.context,
        )
        assert_equal(filename, self.known_hosts_file.name)

    def test_valid_known_hosts_file_missing(self):
        exception = assert_raises(
            ConfigError,
            config_parse.valid_known_hosts_file, '/bogus/path', self.context,
        )
        assert_in('Known hosts file /bogus/path', str(exception))

    def test_valid_known_hosts_file_missing_partial_context(self):
        context = config_utils.PartialConfigContext
        expected = '/bogus/does/not/exist'
        filename = config_parse.valid_known_hosts_file(
            expected, context,
        )
        assert_equal(filename, expected)


if __name__ == '__main__':
    run()
