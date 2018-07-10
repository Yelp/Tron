import datetime
import shutil
import tempfile

import mock
import pytz
from testify import assert_equal
from testify import assert_in
from testify import run
from testify import setup
from testify import teardown
from testify import TestCase

from tests.assertions import assert_raises
from tron.config import config_parse
from tron.config import schedule_parse
from tron.config.action_runner import ActionRunner
from tron.config.job import Job
from tron.config.job import JobMap
from tron.config.mesos_options import MesosOptions
from tron.config.node import Node
from tron.config.node import NodeMap
from tron.config.node import NodePool
from tron.config.node import NodePoolMap
from tron.config.schedule_parse import ConfigGenericSchedule
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.config.schema import MASTER_NAMESPACE
from tron.config.ssh_options import SSHOptions
from tron.config.state_persistence import StatePersistence
from tron.config.tron_config import NamedTronConfig
from tron.config.tron_config import TronConfig
from tron.core.action import Action
from tron.core.action import ActionMap
from tron.core.action import ExecutorTypes
from tron.core.action import Volume
from tron.core.action import VolumeModes
from tron.utils.dicts import FrozenDict


def make_base_config():
    return dict(
        ssh_options=dict(agent=False, identities=['tests/test_id_rsa']),
        time_zone="EST",
        output_stream_dir="/tmp",
        # TODO: fix mocking username
        nodes=[
            dict(name='node0', hostname='node0', username='foo'),
            dict(name='node1', hostname='node1', username='foo'),
        ],
        node_pools=[dict(name='NodePool', nodes=['node0', 'node1'])]
    )


def make_ssh_options():
    return SSHOptions(
        agent=False,
        identities=('tests/test_id_rsa', ),
        known_hosts_file=None,
        connect_timeout=30,
        idle_connection_timeout=3600,
        jitter_min_load=4,
        jitter_max_delay=20,
        jitter_load_factor=1,
    )


def make_command_context():
    return FrozenDict({
        'python': '/usr/bin/python',
        'batch_dir': '/tron/batch/test/foo',
    })


def make_nodes():
    return NodeMap({
        'node0':
            Node(
                name='node0',
                username='foo',
                hostname='node0',
                port=22,
            ),
        'node1':
            Node(
                name='node1',
                username='foo',
                hostname='node1',
                port=22,
            ),
    })


def make_node_pools():
    return NodePoolMap.from_config([{
        'name': 'NodePool',
        'nodes': ['node0', 'node1']
    }])


def make_action(**kwargs):
    kwargs.setdefault('name', 'action'),
    kwargs.setdefault('command', 'command')
    kwargs.setdefault('executor', 'ssh')
    kwargs.setdefault('requires', ())
    kwargs.setdefault('expected_runtime', datetime.timedelta(1))
    return Action.from_config(config=kwargs)


def make_cleanup_action(**kwargs):
    kwargs.setdefault('name', 'cleanup'),
    kwargs.setdefault('command', 'command')
    kwargs.setdefault('executor', 'ssh')
    kwargs.setdefault('expected_runtime', datetime.timedelta(1))
    return Action.from_config(config=kwargs)


def make_job(**kwargs):
    kwargs.setdefault('namespace', 'MASTER')
    kwargs.setdefault('name', f"{kwargs['namespace']}.job_name")
    kwargs.setdefault('node', 'node0')
    kwargs.setdefault('enabled', True)
    kwargs.setdefault('monitoring', {})
    kwargs.setdefault(
        'schedule',
        schedule_parse.ConfigDailyScheduler(
            scheduler='daily',
            original="16:30:00",
            days=set(),
            hour=16,
            minute=30,
            second=0,
        )
    )
    kwargs.setdefault('actions', ActionMap.create(dict(action=make_action())))
    kwargs.setdefault('queueing', True)
    kwargs.setdefault('run_limit', 50)
    kwargs.setdefault('all_nodes', False)
    kwargs.setdefault('cleanup_action', make_cleanup_action())
    kwargs.setdefault('max_runtime')
    kwargs.setdefault('allow_overlap', False)
    kwargs.setdefault('time_zone', None)
    kwargs.setdefault('expected_runtime', datetime.timedelta(0, 3600))
    return Job.create(kwargs)


def make_master_jobs():
    return JobMap({
        'MASTER.test_job0':
            make_job(
                name='MASTER.test_job0',
                schedule=ConfigGenericSchedule.from_config('interval 20s'),
                expected_runtime=datetime.timedelta(1)
            ),
        'MASTER.test_job1':
            make_job(
                name='MASTER.test_job1',
                schedule=schedule_parse.ConfigDailyScheduler(
                    days={1, 3, 5},
                    hour=0,
                    minute=30,
                    second=0,
                    scheduler='daily',
                    original="00:30:00 MWF",
                ),
                actions=FrozenDict({
                    'action':
                        make_action(
                            requires=('action1', ),
                            expected_runtime=datetime.timedelta(0, 7200)
                        ),
                    'action1':
                        make_action(
                            name='action1',
                            expected_runtime=datetime.timedelta(0, 7200)
                        ),
                }),
                time_zone=pytz.timezone("Pacific/Auckland"),
                expected_runtime=datetime.timedelta(1),
                cleanup_action=None,
                allow_overlap=True,
            ),
        'MASTER.test_job2':
            make_job(
                name='MASTER.test_job2',
                node='node1',
                actions=ActionMap({
                    'action2_0':
                        make_action(
                            name='action2_0',
                            command='test_command2.0',
                        )
                }),
                time_zone=pytz.timezone("Pacific/Auckland"),
                expected_runtime=datetime.timedelta(1),
                cleanup_action=None,
            ),
        'MASTER.test_job3':
            make_job(
                name='MASTER.test_job3',
                node='node1',
                schedule=ConfigGenericSchedule.from_config('constant'),
                actions=ActionMap({
                    'action':
                        make_action(),
                    'action1':
                        make_action(name='action1'),
                    'action2':
                        make_action(
                            name='action2',
                            requires=('action', 'action1'),
                            node='node0',
                        ),
                }),
                cleanup_action=None,
                expected_runtime=datetime.timedelta(1),
            ),
        'MASTER.test_job4':
            make_job(
                name='MASTER.test_job4',
                node='NodePool',
                schedule=schedule_parse.ConfigDailyScheduler(
                    scheduler='daily',
                    original="",
                    hour=0,
                    minute=0,
                    second=0,
                    days=set(),
                ),
                all_nodes=True,
                enabled=False,
                cleanup_action=None,
                expected_runtime=datetime.timedelta(1),
            ),
        'MASTER.test_job_mesos':
            make_job(
                name='MASTER.test_job_mesos',
                node='NodePool',
                schedule=schedule_parse.ConfigDailyScheduler(
                    scheduler='daily',
                    original="",
                    hour=0,
                    minute=0,
                    second=0,
                    days=set(),
                ),
                actions=ActionMap({
                    'action_mesos':
                        make_action(
                            name='action_mesos',
                            command='test_command_mesos',
                            executor='mesos',
                            cpus=0.1,
                            mem=100,
                            mesos_address='the-master.mesos',
                            docker_image='container:latest',
                        ),
                }),
                cleanup_action=None,
                expected_runtime=datetime.timedelta(1),
            ),
    })


def make_tron_config(
    action_runner=None,
    output_stream_dir='/tmp',
    command_context=None,
    ssh_options=None,
    notification_options=None,
    time_zone=pytz.timezone("EST"),
    state_persistence=StatePersistence(name='tron_state'),
    nodes=None,
    node_pools=None,
    jobs=None,
    mesos_options=None,
):
    return TronConfig(
        action_runner=action_runner or ActionRunner(),
        output_stream_dir=output_stream_dir,
        command_context=command_context or
        dict(batch_dir='/tron/batch/test/foo', python='/usr/bin/python'),
        ssh_options=ssh_options or make_ssh_options(),
        notification_options=None,
        time_zone=time_zone,
        state_persistence=state_persistence,
        nodes=nodes or make_nodes(),
        node_pools=node_pools or make_node_pools(),
        jobs=jobs or make_master_jobs(),
        mesos_options=mesos_options or MesosOptions(),
    )


def make_named_tron_config(**kwargs):
    kwargs.setdefault('jobs', make_master_jobs())
    kwargs.setdefault('nodes', ['node0'])
    return NamedTronConfig(**kwargs)


class ConfigTestCase(TestCase):
    JOBS_CONFIG = dict(
        jobs=[
            dict(
                name="test_job0",
                node='node0',
                schedule="interval 20s",
                actions=[dict(name="action", command="command")],
                cleanup_action=dict(command="command"),
            ),
            dict(
                name="test_job1",
                node='node0',
                schedule="daily 00:30:00 MWF",
                allow_overlap=True,
                time_zone="Pacific/Auckland",
                actions=[
                    dict(
                        name="action",
                        command="command",
                        requires=['action1'],
                        expected_runtime="2h",
                    ),
                    dict(
                        name="action1",
                        command="command",
                        expected_runtime="2h",
                    )
                ]
            ),
            dict(
                name="test_job2",
                node='node1',
                schedule="daily 16:30:00",
                expected_runtime="1d",
                time_zone="Pacific/Auckland",
                actions=[dict(name="action2_0", command="test_command2.0")]
            ),
            dict(
                name="test_job3",
                node='node1',
                schedule="constant",
                actions=[
                    dict(name="action", command="command"),
                    dict(name="action1", command="command"),
                    dict(
                        name="action2",
                        node='node0',
                        command="command",
                        requires=['action', 'action1']
                    )
                ]
            ),
            dict(
                name="test_job4",
                node='NodePool',
                all_nodes=True,
                schedule="daily",
                enabled=False,
                actions=[dict(name='action', command='command')]
            ),
            dict(
                name="test_job_mesos",
                node='NodePool',
                schedule="daily",
                actions=[
                    dict(
                        name="action_mesos",
                        executor='mesos',
                        command="test_command_mesos",
                        cpus=.1,
                        mem=100,
                        mesos_address='the-master.mesos',
                        docker_image='container:latest',
                    )
                ]
            )
        ]
    )

    config = dict(
        command_context=dict(
            batch_dir='/tron/batch/test/foo', python='/usr/bin/python'
        ),
        **make_base_config(),
        **JOBS_CONFIG
    )

    def test_attributes(self):
        expected = make_tron_config()

        test_config = TronConfig.from_config(self.config)
        assert_equal(test_config.command_context, expected.command_context)
        assert_equal(test_config.ssh_options, expected.ssh_options)
        assert_equal(
            test_config.notification_options,
            expected.notification_options,
        )
        assert_equal(test_config.time_zone, expected.time_zone)
        assert_equal(test_config.nodes, expected.nodes)
        assert_equal(test_config.node_pools, expected.node_pools)
        for key in ['0', '1', '2', '3', '4', '_mesos']:
            assert f"test_job{key}" in test_config.jobs, f"{key} in test_config.jobs"
            assert f"MASTER.test_job{key}" in expected.jobs, f"{key} in test_config.jobs"
            assert_equal(
                test_config.jobs[f"test_job{key}"],
                expected.jobs[f"MASTER.test_job{key}"]
            )

    def test_empty_node_test(self):
        TronConfig.from_config(dict(nodes=None))

    def test_substitution(self):
        def test_config():
            return dict(
                nodes=['node0'],
                jobs=[
                    dict(
                        name="test_job",
                        namespace='MASTER',
                        node='node0',
                        schedule="interval 20s",
                        actions=[dict(name="action", command="%(hello)s")],
                    )
                ]
            )

        msg = assert_raises(ValueError, TronConfig.from_config, test_config())
        assert_in("substitution", str(msg))

        try:
            TronConfig.from_config(
                dict(command_context=dict(hello=123), **test_config())
            )
        except Exception as e:
            assert False, f"Unexpected exception: {e}"


class NamedConfigTestCase(TestCase):
    config = ConfigTestCase.JOBS_CONFIG

    def test_attributes(self):
        expected = make_named_tron_config(
            namespace='test_namespace',
            jobs=JobMap({
                'test_job':
                    make_job(
                        name="test_job",
                        namespace='test_namespace',
                        schedule=ConfigGenericSchedule.
                        from_config('interval 20s'),
                        expected_runtime=datetime.timedelta(1),
                    )
            })
        )
        test_config = NamedTronConfig.from_config(
            dict(
                namespace='test_namespace',
                nodes=['node0'],
                jobs=[
                    dict(
                        name="test_job",
                        namespace='test_namespace',
                        node="node0",
                        schedule="interval 20s",
                        actions=[dict(name="action", command="command")],
                        cleanup_action=dict(command="command"),
                    )
                ]
            )
        )
        assert_equal(test_config, expected)

    def test_substitution(self):
        test_config = dict(
            namespace='test_namespace',
            nodes=['node0'],
            jobs=[
                dict(
                    name="test_job",
                    namespace='test_namespace',
                    node="node0",
                    schedule="interval 20s",
                    actions=[dict(name="action", command="%(hello)s")],
                )
            ]
        )
        msg = assert_raises(
            ValueError, NamedTronConfig.from_config, test_config
        )
        assert_in("substitution", str(msg))

        try:
            NamedTronConfig.from_config(
                dict(command_context=dict(hello=123), **test_config)
            )
        except Exception as e:
            assert False, f"Unexpected exception: {e}"


class JobConfigTestCase(TestCase):
    def test_no_actions(self):
        test_config = dict(
            jobs=[
                dict(name='test_job0', node='node0', schedule='interval 20s')
            ],
            **make_base_config()
        )

        expected_message = "Job.actions"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_message, str(exception))

    def test_empty_actions(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=None
                )
            ],
            **make_base_config()
        )

        expected_message = "Invalid type for field Job.actions"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_message, str(exception))

    def test_dupe_names(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=[
                        dict(name='action', command='cmd'),
                        dict(name='action', command='cmd'),
                    ]
                )
            ],
            **make_base_config()
        )

        expected = "Duplicate action names found: ['action']"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected, str(exception))

    def test_bad_requires(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=[dict(name='action', command='cmd')]
                ),
                dict(
                    name='test_job1',
                    node='node0',
                    schedule='interval 20s',
                    actions=[
                        dict(
                            name='action1', command='cmd', requires=['action']
                        )
                    ]
                )
            ],
            **make_base_config()
        )

        expected_message = 'contains external dependency'
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_message, str(exception))

    def test_circular_dependency(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=[
                        dict(
                            name='action1',
                            command='cmd',
                            requires=['action2']
                        ),
                        dict(
                            name='action2',
                            command='cmd',
                            requires=['action1']
                        ),
                    ]
                )
            ],
            **make_base_config()
        )

        expect = "contains circular dependency"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expect, str(exception))

    def test_config_cleanup_name_collision(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=[
                        dict(name=CLEANUP_ACTION_NAME, command='cmd'),
                    ]
                )
            ],
            **make_base_config()
        )
        expected_message = "reserved for cleanup action"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_message, str(exception))

    def test_config_cleanup_action_name(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=[
                        dict(name='action', command='cmd'),
                    ],
                    cleanup_action=dict(name='gerald', command='cmd')
                )
            ],
            **make_base_config()
        )

        expected_msg = "cleanup_action cannot have name"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_config_cleanup_requires(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='node0',
                    schedule='interval 20s',
                    actions=[
                        dict(name='action', command='cmd'),
                    ],
                    cleanup_action=dict(command='cmd', requires=['action'])
                )
            ],
            **make_base_config()
        )

        expected_msg = "Cleanup action cannot have dependencies, has ['action']"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_validate_job_no_actions(self):
        job_config = dict(
            name="job_name",
            node="localhost",
            schedule="constant",
            actions=[],
        )
        expected_msg = "`actions` can't be empty"
        exception = assert_raises(
            ValueError,
            Job.from_config,
            job_config,
        )
        assert_in(expected_msg, str(exception))


class NodeConfigTestCase(TestCase):
    def test_validate_node_pool(self):
        config_node_pool = NodePool.from_config(
            dict(name="theName", nodes=["node1", "node2"])
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
        expected_msg = "both node and node pool"
        exception = assert_raises(
            ValueError, TronConfig.from_config, tron_config
        )
        assert_in(expected_msg, str(exception))

    def test_invalid_node_name(self):
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='unknown_node',
                    schedule='interval 20s',
                    actions=[dict(name='action', command='cmd')]
                )
            ],
            **make_base_config()
        )

        expected_msg = "unknown node unknown_node"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_invalid_nested_node_pools(self):
        test_config = dict(
            nodes=[
                dict(name='node0', hostname='node0'),
                dict(name='node1', hostname='node1')
            ],
            node_pools=[
                dict(name='pool0', nodes=['node1']),
                dict(name='pool1', nodes=['node0', 'pool0'])
            ],
            jobs=[
                dict(
                    name='test_job0',
                    node='pool1',
                    schedule='interval 20s',
                    actions=[dict(name='action', command='cmd')]
                )
            ]
        )

        expected_msg = "node_pools.pool1: contains other node pools: {'pool0'}"
        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_invalid_node_pool_config(self):
        test_config = dict(
            nodes=[
                dict(name='node0', hostname='node0'),
                dict(name='node1', hostname='node1')
            ],
            node_pools=[
                dict(name='pool0', hostname=['node1']),
                dict(name='pool1', nodes=['node0', 'pool0'])
            ],
            jobs=[
                dict(
                    name='test_job0',
                    node='pool1',
                    schedule='interval 20s',
                    actions=[dict(name='action', command='cmd')]
                )
            ]
        )

        exception = assert_raises(
            ValueError,
            TronConfig.from_config,
            test_config,
        )
        assert_in("hostname", str(exception))

    def test_invalid_named_update(self):
        test_config = dict(bozray=None, namespace='foobar')
        expected_message = "Namespace foobar 'bozray' are not among"
        exception = assert_raises(
            ValueError,
            NamedTronConfig.from_config,
            test_config,
        )
        assert_in(expected_message, str(exception))


class ValidateJobsTestCase(TestCase):
    def test_valid_jobs_success(self):
        test_config = dict(
            jobs=[
                dict(
                    name="test_job0",
                    node='localhost',
                    schedule="interval 20s",
                    expected_runtime="20m",
                    actions=[
                        dict(
                            name="action",
                            command="command",
                            expected_runtime="20m"
                        ),
                        dict(
                            name="action_mesos",
                            command="command",
                            executor='mesos',
                            cpus=4,
                            mem=300,
                            constraints=[
                                dict(
                                    attribute='pool',
                                    operator='LIKE',
                                    value='default'
                                )
                            ],
                            docker_image='my_container:latest',
                            docker_parameters=[
                                dict(key='label', value='labelA'),
                                dict(key='label', value='labelB')
                            ],
                            env=dict(USER='batch'),
                            extra_volumes=[
                                dict(
                                    container_path='/tmp',
                                    host_path='/home/tmp',
                                    mode='RO'
                                )
                            ],
                            mesos_address='http://my-mesos-master.com'
                        )
                    ],
                    cleanup_action=dict(command="command")
                )
            ]
        )

        expected_jobs = JobMap.create({
            'test_job0':
                make_job(
                    node='localhost',
                    name='MASTER.test_job0',
                    schedule=ConfigGenericSchedule.from_config('interval 20s'),
                    actions=ActionMap.create({
                        'action':
                            make_action(
                                expected_runtime=datetime.timedelta(0, 1200),
                            ),
                        'action_mesos':
                            make_action(
                                name='action_mesos',
                                executor='mesos',
                                cpus=4.0,
                                mem=300.0,
                                constraints=(
                                    dict(
                                        attribute='pool',
                                        operator='LIKE',
                                        value='default',
                                    ),
                                ),
                                docker_image='my_container:latest',
                                docker_parameters=(
                                    dict(
                                        key='label',
                                        value='labelA',
                                    ),
                                    dict(
                                        key='label',
                                        value='labelB',
                                    ),
                                ),
                                env={'USER': 'batch'},
                                extra_volumes=(
                                    dict(
                                        container_path='/tmp',
                                        host_path='/home/tmp',
                                        mode=VolumeModes.RO,
                                    ),
                                ),
                                mesos_address='http://my-mesos-master.com',
                                expected_runtime=datetime.timedelta(hours=24),
                            ),
                    }),
                    expected_runtime=datetime.timedelta(0, 1200),
                ),
        })
        parsed_config = TronConfig.from_config(test_config)
        assert_equal(expected_jobs, parsed_config.jobs)


class ValidMesosActionTestCase(TestCase):
    def test_missing_docker_image(self):
        config = dict(
            name='test_missing',
            command='echo hello',
            executor=ExecutorTypes.mesos,
            cpus=0.2,
            mem=150,
            mesos_address='http://hello.org',
        )
        assert_raises(
            ValueError,
            Action.from_config,
            config,
        )

    def test_cleanup_missing_docker_image(self):
        config = dict(
            name='cleanup',
            command='echo hello',
            executor=ExecutorTypes.mesos,
            cpus=0.2,
            mem=150,
            mesos_address='http://hello.org',
        )
        assert_raises(
            ValueError,
            Action.from_config,
            config,
        )


class ValidOutputStreamDirTestCase(TestCase):
    @setup
    def setup_dir(self):
        self.dir = tempfile.mkdtemp()

    @teardown
    def teardown_dir(self):
        shutil.rmtree(self.dir)

    def test_valid_dir(self):
        config = make_tron_config(output_stream_dir=self.dir)
        assert_equal(config.output_stream_dir, self.dir)

    def test_missing_dir(self):
        exception = assert_raises(
            Exception,
            make_tron_config,
            output_stream_dir='bogus-dir',
        )
        assert_in("is writable", str(exception))


class ValidateConfigMappingTestCase(TestCase):
    config = dict(
        **make_base_config(), command_context=dict(some_var="The string")
    )

    def test_validate_config_mapping_missing_master(self):
        config_mapping = {'other': mock.Mock()}
        seq = config_parse.validate_config_mapping(config_mapping)
        exception = assert_raises(ValueError, list, seq)
        assert_in('requires a MASTER namespace', str(exception))

    def test_validate_config_mapping(self):
        master_config = self.config
        other_config = NamedConfigTestCase.config
        config_mapping = {
            'other': other_config,
            MASTER_NAMESPACE: master_config,
        }
        result = list(config_parse.validate_config_mapping(config_mapping))
        assert_equal(len(result), 2)
        assert_equal(result[0][0], MASTER_NAMESPACE)
        assert_equal(result[1][0], 'other')


class ConfigContainerTestCase(TestCase):
    config = make_base_config()

    @setup
    def setup_container(self):
        other_config = NamedConfigTestCase.config
        self.config_mapping = {
            MASTER_NAMESPACE:
                TronConfig.from_config(self.config),
            'other':
                NamedTronConfig.from_config(
                    dict(
                        namespace='other',
                        nodes=['node0', 'node1', 'NodePool'],
                        **other_config
                    )
                ),
        }
        self.container = config_parse.ConfigContainer(self.config_mapping)

    def test_create(self):
        config_mapping = {
            MASTER_NAMESPACE: self.config,
            'other': NamedConfigTestCase.config,
        }

        container = config_parse.ConfigContainer.create(config_mapping)
        assert_equal(set(container.configs.keys()), {'MASTER', 'other'})

    def test_create_missing_master(self):
        config_mapping = {'other': mock.Mock()}
        assert_raises(
            ValueError,
            config_parse.ConfigContainer.create,
            config_mapping,
        )

    def test_get_job_names(self):
        job_names = self.container.get_job_names()
        expected = [
            'test_job1',
            'test_job0',
            'test_job3',
            'test_job2',
            'test_job4',
            'test_job_mesos',
        ]
        assert_equal(set(job_names), set(expected))

    def test_get_jobs(self):
        expected = [
            'test_job1',
            'test_job0',
            'test_job3',
            'test_job2',
            'test_job4',
            'test_job_mesos',
        ]
        assert_equal(set(expected), set(self.container.get_jobs().keys()))

    def test_get_node_names(self):
        node_names = self.container.get_node_names()
        expected = {'node0', 'node1', 'NodePool'}
        assert_equal(node_names, expected)


class ValidateVolumeTestCase(TestCase):
    def test_missing_container_path(self):
        config = {
            'container_path_typo': '/nail/srv',
            'host_path': '/tmp',
            'mode': 'RO',
        }
        assert_raises(
            AttributeError,
            Volume.create,
            config,
        )

    def test_missing_host_path(self):
        config = {
            'container_path': '/nail/srv',
            'hostPath': '/tmp',
            'mode': 'RO',
        }
        assert_raises(
            AttributeError,
            Volume.create,
            config,
        )

    def test_invalid_mode(self):
        config = {
            'container_path': '/nail/srv',
            'host_path': '/tmp',
            'mode': 'RA',
        }
        assert_raises(
            ValueError,
            Volume.create,
            config,
        )

    def test_valid(self):
        config = {
            'container_path': '/nail/srv',
            'host_path': '/tmp',
            'mode': 'RO',
        }
        assert (Volume.create(config))

    def test_mesos_default_volumes(self):
        mesos_options = {}
        mesos_options['default_volumes'] = [
            {
                'container_path': '/nail/srv',
                'host_path': '/tmp',
                'mode': 'RO',
            },
            {
                'container_path': '/nail/srv',
                'host_path': '/tmp',
                'mode': 'invalid',
            },
        ]
        assert_raises(
            ValueError,
            MesosOptions.from_config,
            mesos_options,
        )
        # After we fix the error, expect error to go away.
        mesos_options['default_volumes'][1]['mode'] = 'RW'
        assert MesosOptions.from_config(mesos_options)


if __name__ == '__main__':
    run()
