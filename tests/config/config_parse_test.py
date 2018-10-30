from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import os
import shutil
import tempfile

import mock
import pytest
import pytz

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import run
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tests.assertions import assert_raises
from tron.config import config_parse
from tron.config import config_utils
from tron.config import ConfigError
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

BASE_CONFIG = dict(
    ssh_options=dict(agent=False, identities=['tests/test_id_rsa']),
    time_zone="EST",
    output_stream_dir="/tmp",
    nodes=[
        dict(name='node0', hostname='node0'),
        dict(name='node1', hostname='node1'),
    ],
    node_pools=[dict(name='NodePool', nodes=['node0', 'node1'])]
)


def make_ssh_options():
    return schema.ConfigSSHOptions(
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
    return FrozenDict({
        'node0':
            schema.ConfigNode(
                name='node0',
                username='foo',
                hostname='node0',
                port=22,
            ),
        'node1':
            schema.ConfigNode(
                name='node1',
                username='foo',
                hostname='node1',
                port=22,
            ),
    })


def make_node_pools():
    return FrozenDict({
        'NodePool':
            schema.ConfigNodePool(
                nodes=('node0', 'node1'),
                name='NodePool',
            ),
    })


def make_mesos_options():
    return schema.ConfigMesos(
        master_address=None,
        master_port=5050,
        secret_file=None,
        role='*',
        principal="tron",
        enabled=False,
        default_volumes=(),
        dockercfg_location=None,
        offer_timeout=300,
    )


def make_action(**kwargs):
    kwargs.setdefault('name', 'action'),
    kwargs.setdefault('command', 'command')
    kwargs.setdefault('executor', 'ssh')
    kwargs.setdefault('requires', ())
    kwargs.setdefault('expected_runtime', datetime.timedelta(1))
    return schema.ConfigAction(**kwargs)


def make_cleanup_action(**kwargs):
    kwargs.setdefault('name', 'cleanup'),
    kwargs.setdefault('command', 'command')
    kwargs.setdefault('executor', 'ssh')
    kwargs.setdefault('expected_runtime', datetime.timedelta(1))
    return schema.ConfigCleanupAction(**kwargs)


def make_job(**kwargs):
    kwargs.setdefault('namespace', 'MASTER')
    kwargs.setdefault('name', f"{kwargs['namespace']}.job_name")
    kwargs.setdefault('node', 'node0')
    kwargs.setdefault('enabled', True)
    kwargs.setdefault('monitoring', {})
    kwargs.setdefault(
        'schedule',
        schedule_parse.ConfigDailyScheduler(
            days=set(),
            hour=16,
            minute=30,
            second=0,
            original="16:30:00 ",
            jitter=None,
        )
    )
    kwargs.setdefault('actions', FrozenDict({'action': make_action()}))
    kwargs.setdefault('queueing', True)
    kwargs.setdefault('run_limit', 50)
    kwargs.setdefault('all_nodes', False)
    kwargs.setdefault('cleanup_action', make_cleanup_action())
    kwargs.setdefault('max_runtime')
    kwargs.setdefault('allow_overlap', False)
    kwargs.setdefault('time_zone', None)
    kwargs.setdefault('expected_runtime', datetime.timedelta(0, 3600))
    return schema.ConfigJob(**kwargs)


def make_master_jobs():
    return FrozenDict({
        'MASTER.test_job0':
            make_job(
                name='MASTER.test_job0',
                schedule=schedule_parse.ConfigIntervalScheduler(
                    timedelta=datetime.timedelta(0, 20),
                    jitter=None,
                ),
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
                    original="00:30:00 MWF",
                    jitter=None,
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
                actions=FrozenDict({
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
        'MASTER.test_job_actions_dict':
            make_job(
                name='MASTER.test_job_actions_dict',
                node='node1',
                schedule=ConfigConstantScheduler(),
                actions=FrozenDict({
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
                    original="00:00:00 ",
                    hour=0,
                    minute=0,
                    second=0,
                    days=set(),
                    jitter=None,
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
                    original="00:00:00 ",
                    hour=0,
                    minute=0,
                    second=0,
                    days=set(),
                    jitter=None,
                ),
                actions=FrozenDict({
                    'action_mesos':
                        make_action(
                            name='action_mesos',
                            command='test_command_mesos',
                            executor='mesos',
                            cpus=0.1,
                            mem=100,
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
    time_zone=pytz.timezone("EST"),
    state_persistence=config_parse.DEFAULT_STATE_PERSISTENCE,
    nodes=None,
    node_pools=None,
    jobs=None,
    mesos_options=None,
):
    return schema.TronConfig(
        action_runner=action_runner or FrozenDict(),
        output_stream_dir=output_stream_dir,
        command_context=command_context or
        FrozenDict(batch_dir='/tron/batch/test/foo', python='/usr/bin/python'),
        ssh_options=ssh_options or make_ssh_options(),
        time_zone=time_zone,
        state_persistence=state_persistence,
        nodes=nodes or make_nodes(),
        node_pools=node_pools or make_node_pools(),
        jobs=jobs or make_master_jobs(),
        mesos_options=mesos_options or make_mesos_options(),
    )


def make_named_tron_config(jobs=None):
    return schema.NamedTronConfig(jobs=jobs or make_master_jobs())


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
                name="test_job_actions_dict",
                node='node1',
                schedule="constant",
                actions=dict(
                    action=dict(command="command"),
                    action1=dict(command="command"),
                    action2=dict(
                        node='node0',
                        command="command",
                        requires=['action', 'action1']
                    )
                )
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
        **BASE_CONFIG,
        **JOBS_CONFIG
    )

    @mock.patch.dict('tron.config.config_parse.ValidateNode.defaults')
    def test_attributes(self):
        config_parse.ValidateNode.defaults['username'] = 'foo'
        expected = make_tron_config()

        test_config = valid_config(self.config)

        assert test_config.command_context == expected.command_context
        assert test_config.ssh_options == expected.ssh_options
        assert test_config.mesos_options == expected.mesos_options
        assert test_config.time_zone == expected.time_zone
        assert test_config.nodes == expected.nodes
        assert test_config.node_pools == expected.node_pools
        for key in ['0', '1', '2', '_actions_dict', '4', '_mesos']:
            job_name = f"MASTER.test_job{key}"
            assert job_name in test_config.jobs, f"{job_name} in test_config.jobs"
            assert job_name in expected.jobs, f"{job_name} in test_config.jobs"
            assert_equal(test_config.jobs[job_name], expected.jobs[job_name])

        assert test_config == expected

    def test_empty_node_test(self):
        valid_config(dict(nodes=None))


class TestNamedConfig(TestCase):
    config = ConfigTestCase.JOBS_CONFIG

    def test_attributes(self):
        expected = make_named_tron_config(
            jobs=FrozenDict({
                'test_job':
                    make_job(
                        name="test_job",
                        namespace='test_namespace',
                        schedule=ConfigIntervalScheduler(
                            timedelta=datetime.timedelta(0, 20),
                            jitter=None,
                        ),
                        expected_runtime=datetime.timedelta(1),
                    )
            })
        )
        test_config = validate_fragment(
            'test_namespace',
            dict(
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

    def test_attributes_with_master_context(self):
        expected = make_named_tron_config(
            jobs=FrozenDict({
                'test_namespace.test_job':
                    make_job(
                        name="test_namespace.test_job",
                        namespace="test_namespace",
                        schedule=ConfigIntervalScheduler(
                            timedelta=datetime.timedelta(0, 20),
                            jitter=None,
                        ),
                        expected_runtime=datetime.timedelta(1),
                    )
            })
        )
        master_config = dict(
            nodes=[dict(
                name="node0",
                hostname="node0",
            )],
            node_pools=[dict(
                name="nodepool0",
                nodes=["node0"],
            )]
        )
        test_config = validate_fragment(
            'test_namespace',
            dict(
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
            ),
            master_config=master_config
        )
        assert_equal(test_config, expected)

    def test_invalid_job_node_with_master_context(self):
        master_config = dict(
            nodes=[dict(
                name="node0",
                hostname="node0",
            )],
        )
        test_config = dict(
            jobs=[
                dict(
                    name="test_job",
                    namespace='test_namespace',
                    node="node1",
                    schedule="interval 20s",
                    actions=[dict(name="action", command="command")],
                    cleanup_action=dict(command="command"),
                )
            ]
        )
        expected_message = "Unknown node name node1 at test_namespace.NamedConfigFragment.jobs.Job.test_job.node"
        exception = assert_raises(
            ConfigError,
            validate_fragment,
            'test_namespace',
            test_config,
            master_config,
        )
        assert_in(expected_message, str(exception))

    def test_invalid_action_node_with_master_context(self):
        master_config = dict(
            nodes=[dict(
                name="node0",
                hostname="node0",
            )],
            node_pools=[dict(
                name="nodepool0",
                nodes=["node0"],
            )]
        )
        test_config = dict(
            jobs=[
                dict(
                    name="test_job",
                    namespace='test_namespace',
                    node="node0",
                    schedule="interval 20s",
                    actions=
                    [dict(name="action", node="nodepool1", command="command")],
                    cleanup_action=dict(command="command"),
                )
            ]
        )
        expected_message = "Unknown node name nodepool1 at test_namespace.NamedConfigFragment.jobs.Job.test_job.actions.Action.action.node"

        exception = assert_raises(
            ConfigError,
            validate_fragment,
            'test_namespace',
            test_config,
            master_config,
        )
        assert_in(expected_message, str(exception))


class TestJobConfig(TestCase):
    def test_no_actions(self):
        test_config = dict(
            jobs=[
                dict(name='test_job0', node='node0', schedule='interval 20s')
            ],
            **BASE_CONFIG
        )

        expected_message = "Job test_job0 is missing options: actions"
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )

        expected_message = "Value at config.jobs.Job.test_job0.actions"
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )

        expected = "Duplicate name action at config.jobs.Job.test_job0.actions"
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )

        expected_message = (
            'jobs.MASTER.test_job1.action1 has a dependency '
            '"action" that is not in the same job!'
        )
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )

        expect = "Circular dependency in job.MASTER.test_job0: action1 -> action2"
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )
        expected_message = "config.jobs.Job.test_job0.actions.Action.cleanup.name"
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )

        expected_msg = "Cleanup actions cannot have custom names"
        exception = assert_raises(
            ConfigError,
            valid_config,
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
            **BASE_CONFIG
        )

        expected_msg = "Unknown keys in CleanupAction : requires"
        exception = assert_raises(
            ConfigError,
            valid_config,
            test_config,
        )
        assert_equal(expected_msg, str(exception))

    def test_validate_job_no_actions(self):
        job_config = dict(
            name="job_name",
            node="localhost",
            schedule="constant",
            actions=[],
        )
        config_context = config_utils.ConfigContext(
            'config',
            ['localhost'],
            None,
            None,
        )
        expected_msg = "Required non-empty list at config.Job.job_name.actions"
        exception = assert_raises(
            ConfigError,
            valid_job,
            job_config,
            config_context,
        )
        assert_in(expected_msg, str(exception))


class TestNodeConfig(TestCase):
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
        test_config = dict(
            jobs=[
                dict(
                    name='test_job0',
                    node='unknown_node',
                    schedule='interval 20s',
                    actions=[dict(name='action', command='cmd')]
                )
            ],
            **BASE_CONFIG
        )

        expected_msg = "Unknown node name unknown_node at config.jobs.Job.test_job0.node"
        exception = assert_raises(
            ConfigError,
            valid_config,
            test_config,
        )
        assert_equal(expected_msg, str(exception))

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

        expected_msg = "NodePool pool1 contains other NodePools: pool0"
        exception = assert_raises(
            ConfigError,
            valid_config,
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

        expected_msg = "NodePool pool0 is missing options"
        exception = assert_raises(
            ConfigError,
            valid_config,
            test_config,
        )
        assert_in(expected_msg, str(exception))

    def test_invalid_named_update(self):
        test_config = dict(bozray=None)
        expected_message = "Unknown keys in NamedConfigFragment : bozray"
        exception = assert_raises(
            ConfigError,
            validate_fragment,
            'foo',
            test_config,
        )
        assert_in(expected_message, str(exception))


class TestValidateJobs(TestCase):
    def test_valid_jobs_success(self):
        test_config = dict(
            jobs=[
                dict(
                    name="test_job0",
                    node='node0',
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
                        ),
                        dict(
                            name="test_trigger_attrs",
                            command="foo",
                            triggered_by=["foo.bar"],
                            trigger_downstreams=True,
                        ),
                    ],
                    cleanup_action=dict(command="command")
                )
            ],
            **BASE_CONFIG
        )

        expected_jobs = FrozenDict({
            'MASTER.test_job0':
                make_job(
                    name='MASTER.test_job0',
                    schedule=ConfigIntervalScheduler(
                        timedelta=datetime.timedelta(0, 20),
                        jitter=None,
                    ),
                    actions=FrozenDict({
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
                                    schema.ConfigConstraint(
                                        attribute='pool',
                                        operator='LIKE',
                                        value='default',
                                    ),
                                ),
                                docker_image='my_container:latest',
                                docker_parameters=(
                                    schema.ConfigParameter(
                                        key='label',
                                        value='labelA',
                                    ),
                                    schema.ConfigParameter(
                                        key='label',
                                        value='labelB',
                                    ),
                                ),
                                env={'USER': 'batch'},
                                extra_volumes=(
                                    schema.ConfigVolume(
                                        container_path='/tmp',
                                        host_path='/home/tmp',
                                        mode='RO',
                                    ),
                                ),
                                expected_runtime=datetime.timedelta(hours=24),
                            ),
                        'test_trigger_attrs':
                            make_action(
                                name="test_trigger_attrs",
                                command="foo",
                                triggered_by=("foo.bar", ),
                                trigger_downstreams=True,
                            ),
                    }),
                    expected_runtime=datetime.timedelta(0, 1200),
                ),
        })

        context = config_utils.ConfigContext(
            'config',
            ['node0'],
            None,
            MASTER_NAMESPACE,
        )
        config_parse.validate_jobs(test_config, context)
        assert_equal(expected_jobs, test_config['jobs'])


class TestValidMesosAction(TestCase):
    def test_missing_docker_image(self):
        config = dict(
            name='test_missing',
            command='echo hello',
            executor=schema.ExecutorTypes.mesos,
            cpus=0.2,
            mem=150,
        )
        assert_raises(
            ConfigError,
            config_parse.valid_action,
            config,
            NullConfigContext,
        )

    def test_cleanup_missing_docker_image(self):
        config = dict(
            command='echo hello',
            executor=schema.ExecutorTypes.mesos,
            cpus=0.2,
            mem=150,
        )
        assert_raises(
            ConfigError,
            config_parse.valid_action,
            config,
            NullConfigContext,
        )


class TestValidCleanupActionName(TestCase):
    def test_valid_cleanup_action_name_pass(self):
        name = valid_cleanup_action_name(CLEANUP_ACTION_NAME, None)
        assert_equal(CLEANUP_ACTION_NAME, name)

    def test_valid_cleanup_action_name_fail(self):
        assert_raises(
            ConfigError,
            valid_cleanup_action_name,
            'other',
            NullConfigContext,
        )


class TestValidOutputStreamDir(TestCase):
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
            valid_output_stream_dir,
            'bogus-dir',
            NullConfigContext,
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


class TestBuildFormatStringValidator(TestCase):
    @setup
    def setup_keys(self):
        self.context = dict.fromkeys(['one', 'seven', 'stars'])
        self.validator = build_format_string_validator(self.context)

    def test_validator_passes(self):
        template = "The {one} thing I {seven} is {stars}"
        assert self.validator(template, NullConfigContext)

    def test_validator_unknown_variable_error(self):
        template = "The {one} thing I {seven} is {unknown}"
        exception = assert_raises(
            ConfigError,
            self.validator,
            template,
            NullConfigContext,
        )
        assert_in("Unknown context variable", str(exception))

    def test_validator_passes_with_context(self):
        template = "The {one} thing I {seven} is {mars}"
        context = config_utils.ConfigContext(
            None,
            None,
            {'mars': 'ok'},
            None,
        )
        assert self.validator(template, context) == template

    def test_validator_valid_string_without_no_percent_escape(self):
        template = "The {one} {seven} thing is {mars} --year %Y"
        context = config_utils.ConfigContext(
            path=None,
            nodes=None,
            command_context={'mars': 'ok'},
            namespace=None,
        )
        assert self.validator(template, context)


class TestValidateConfigMapping(TestCase):
    config = dict(**BASE_CONFIG, command_context=dict(some_var="The string"))

    def test_validate_config_mapping_missing_master(self):
        config_mapping = {'other': mock.Mock()}
        seq = config_parse.validate_config_mapping(config_mapping)
        exception = assert_raises(ConfigError, list, seq)
        assert_in('requires a MASTER namespace', str(exception))

    def test_validate_config_mapping(self):
        master_config = self.config
        other_config = TestNamedConfig.config
        config_mapping = {
            'other': other_config,
            MASTER_NAMESPACE: master_config,
        }
        result = list(config_parse.validate_config_mapping(config_mapping))
        assert_equal(len(result), 2)
        assert_equal(result[0][0], MASTER_NAMESPACE)
        assert_equal(result[1][0], 'other')


class TestConfigContainer(TestCase):
    config = BASE_CONFIG

    @setup
    def setup_container(self):
        other_config = TestNamedConfig.config
        self.config_mapping = {
            MASTER_NAMESPACE: valid_config(self.config),
            'other': validate_fragment('other', other_config),
        }
        self.container = config_parse.ConfigContainer(self.config_mapping)

    def test_create(self):
        config_mapping = {
            MASTER_NAMESPACE: self.config,
            'other': TestNamedConfig.config,
        }

        container = config_parse.ConfigContainer.create(config_mapping)
        assert_equal(set(container.configs.keys()), {'MASTER', 'other'})

    def test_create_missing_master(self):
        config_mapping = {'other': mock.Mock()}
        assert_raises(
            ConfigError,
            config_parse.ConfigContainer.create,
            config_mapping,
        )

    def test_get_job_names(self):
        job_names = self.container.get_job_names()
        expected = [
            'test_job1',
            'test_job0',
            'test_job_actions_dict',
            'test_job2',
            'test_job4',
            'test_job_mesos',
        ]
        assert_equal(set(job_names), set(expected))

    def test_get_jobs(self):
        expected = [
            'test_job1',
            'test_job0',
            'test_job_actions_dict',
            'test_job2',
            'test_job4',
            'test_job_mesos',
        ]
        assert_equal(set(expected), set(self.container.get_jobs().keys()))

    def test_get_node_names(self):
        node_names = self.container.get_node_names()
        expected = {'node0', 'node1', 'NodePool'}
        assert_equal(node_names, expected)


class TestValidateSSHOptions(TestCase):
    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext
        self.config = {'agent': True, 'identities': []}

    @mock.patch.dict('tron.config.config_parse.os.environ')
    def test_post_validation_failed(self):
        if 'SSH_AUTH_SOCK' in os.environ:
            del os.environ['SSH_AUTH_SOCK']
        assert_raises(
            ConfigError,
            config_parse.valid_ssh_options.validate,
            self.config,
            self.context,
        )

    @mock.patch.dict('tron.config.config_parse.os.environ')
    def test_post_validation_success(self):
        os.environ['SSH_AUTH_SOCK'] = 'something'
        config = config_parse.valid_ssh_options.validate(
            self.config,
            self.context,
        )
        assert_equal(config.agent, True)


class TestValidateIdentityFile(TestCase):
    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext
        self.private_file = tempfile.NamedTemporaryFile()

    def test_valid_identity_file_missing_private_key(self):
        exception = assert_raises(
            ConfigError,
            config_parse.valid_identity_file,
            '/file/not/exist',
            self.context,
        )
        assert_in("Private key file", str(exception))

    def test_valid_identity_files_missing_public_key(self):
        filename = self.private_file.name
        exception = assert_raises(
            ConfigError,
            config_parse.valid_identity_file,
            filename,
            self.context,
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


class TestValidKnownHostsFile(TestCase):
    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext
        self.known_hosts_file = tempfile.NamedTemporaryFile()

    def test_valid_known_hosts_file_exists(self):
        filename = config_parse.valid_known_hosts_file(
            self.known_hosts_file.name,
            self.context,
        )
        assert_equal(filename, self.known_hosts_file.name)

    def test_valid_known_hosts_file_missing(self):
        exception = assert_raises(
            ConfigError,
            config_parse.valid_known_hosts_file,
            '/bogus/path',
            self.context,
        )
        assert_in('Known hosts file /bogus/path', str(exception))

    def test_valid_known_hosts_file_missing_partial_context(self):
        context = config_utils.PartialConfigContext
        expected = '/bogus/does/not/exist'
        filename = config_parse.valid_known_hosts_file(
            expected,
            context,
        )
        assert_equal(filename, expected)


class TestValidateVolume(TestCase):
    @setup
    def setup_context(self):
        self.context = config_utils.NullConfigContext

    def test_missing_container_path(self):
        config = {
            'container_path_typo': '/nail/srv',
            'host_path': '/tmp',
            'mode': 'RO',
        }
        assert_raises(
            ConfigError,
            config_parse.valid_volume.validate,
            config,
            self.context,
        )

    def test_missing_host_path(self):
        config = {
            'container_path': '/nail/srv',
            'hostPath': '/tmp',
            'mode': 'RO',
        }
        assert_raises(
            ConfigError,
            config_parse.valid_volume.validate,
            config,
            self.context,
        )

    def test_invalid_mode(self):
        config = {
            'container_path': '/nail/srv',
            'host_path': '/tmp',
            'mode': 'RA',
        }
        assert_raises(
            ConfigError,
            config_parse.valid_volume.validate,
            config,
            self.context,
        )

    def test_valid(self):
        config = {
            'container_path': '/nail/srv',
            'host_path': '/tmp',
            'mode': 'RO',
        }
        assert_equal(
            schema.ConfigVolume(**config),
            config_parse.valid_volume.validate(config, self.context),
        )

    def test_mesos_default_volumes(self):
        mesos_options = {'master_address': 'mesos_master'}
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
            ConfigError,
            config_parse.valid_mesos_options.validate,
            mesos_options,
            self.context,
        )
        # After we fix the error, expect error to go away.
        mesos_options['default_volumes'][1]['mode'] = 'RW'
        assert config_parse.valid_mesos_options.validate(
            mesos_options,
            self.context,
        )


class TestValidMasterAddress:
    @pytest.fixture
    def context(self):
        return config_utils.NullConfigContext

    @pytest.mark.parametrize(
        'url', [
            'http://blah.com',
            'http://blah.com/',
            'blah.com',
            'blah.com/',
        ]
    )
    def test_valid(self, url, context):
        normalized = 'http://blah.com'
        result = config_parse.valid_master_address(url, context)
        assert result == normalized

    @pytest.mark.parametrize(
        'url', [
            'https://blah.com',
            'http://blah.com/something',
            'blah.com/other',
            'http://',
            'blah.com?a=1',
        ]
    )
    def test_invalid(self, url, context):
        with pytest.raises(ConfigError):
            config_parse.valid_master_address(url, context)


if __name__ == '__main__':
    run()
