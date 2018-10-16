"""
 Immutable config schema objects.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

from tron.utils.collections import Enum

MASTER_NAMESPACE = "MASTER"

CLEANUP_ACTION_NAME = 'cleanup'


def config_object_factory(name, required=None, optional=None):
    """
    Creates a namedtuple which has two additional attributes:
        required_keys:
            all keys required to be set on this configuration object
        optional keys:
            optional keys for this configuration object

    The tuple is created from required + optional
    """
    required = required or []
    optional = optional or []

    config_class = namedtuple(name, required + optional)

    # make last len(optional) args actually optional
    config_class.__new__.__defaults__ = (None, ) * len(optional)
    config_class.required_keys = required
    config_class.optional_keys = optional

    return config_class


TronConfig = config_object_factory(
    name='TronConfig',
    optional=[
        'output_stream_dir',  # str
        'action_runner',  # ConfigActionRunner
        'state_persistence',  # ConfigState
        'command_context',  # FrozenDict of str
        'ssh_options',  # ConfigSSHOptions
        'time_zone',  # pytz time zone
        'nodes',  # FrozenDict of ConfigNode
        'node_pools',  # FrozenDict of ConfigNodePool
        'jobs',  # FrozenDict of ConfigJob
        'mesos_options',  # ConfigMesos
        'eventbus_enabled',  # bool or None
    ],
)

NamedTronConfig = config_object_factory(
    name='NamedTronConfig',
    optional=[
        'jobs',  # FrozenDict of ConfigJob
    ],
)

ConfigActionRunner = config_object_factory(
    'ConfigActionRunner',
    optional=['runner_type', 'remote_status_path', 'remote_exec_path'],
)

ConfigSSHOptions = config_object_factory(
    name='ConfigSSHOptions',
    optional=[
        'agent',
        'identities',
        'known_hosts_file',
        'connect_timeout',
        'idle_connection_timeout',
        'jitter_min_load',
        'jitter_max_delay',
        'jitter_load_factor',
    ],
)

ConfigNode = config_object_factory(
    name='ConfigNode',
    required=['hostname'],
    optional=['name', 'username', 'port'],
)

ConfigNodePool = config_object_factory('ConfigNodePool', ['nodes'], ['name'])

ConfigState = config_object_factory(
    name='ConfigState',
    required=[
        'name',
        'store_type',
    ],
    optional=[
        'connection_details',
        'buffer_size',
    ],
)

ConfigMesos = config_object_factory(
    name='ConfigMesos',
    optional=[
        'master_address',
        'master_port',
        'secret_file',
        'principal',
        'role',
        'enabled',
        'default_volumes',
        'dockercfg_location',
        'offer_timeout',
    ],
)

ConfigJob = config_object_factory(
    name='ConfigJob',
    required=[
        'name',  # str
        'node',  # str
        'schedule',  # Config*Scheduler
        'actions',  # FrozenDict of ConfigAction
        'namespace',  # str
    ],
    optional=[
        'monitoring',  # dict
        'queueing',  # bool
        'run_limit',  # int
        'all_nodes',  # bool
        'cleanup_action',  # ConfigAction
        'enabled',  # bool
        'allow_overlap',  # bool
        'max_runtime',  # datetime.Timedelta
        'time_zone',  # pytz time zone
        'expected_runtime',  # datetime.Timedelta
    ],
)

ConfigAction = config_object_factory(
    name='ConfigAction',
    required=[
        'name',  # str
        'command',  # str
    ],
    optional=[
        'requires',  # tuple of str
        'node',  # str
        'retries',  # int
        'retries_delay',  # datetime.Timedelta
        'executor',  # str
        'cpus',  # float
        'mem',  # float
        'constraints',  # List of ConfigConstraint
        'docker_image',  # str
        'docker_parameters',  # List of ConfigParameter
        'env',  # dict
        'extra_volumes',  # List of ConfigVolume
        'expected_runtime',  # datetime.Timedelta
        'trigger_downstreams',  # None, bool or dict
        'triggered_by',  # list or None
        'on_upstream_rerun',  # ActionOnRerun or None
    ],
)

ConfigCleanupAction = config_object_factory(
    name='ConfigCleanupAction',
    required=[
        'command',  # str
    ],
    optional=[
        'name',  # str
        'node',  # str
        'retries',  # int
        'retries_delay',  # datetime.Timedelta
        'expected_runtime',  # datetime.Timedelta
        'executor',  # str
        'cpus',  # float
        'mem',  # float
        'constraints',  # List of ConfigConstraint
        'docker_image',  # str
        'docker_parameters',  # List of ConfigParameter
        'env',  # dict
        'extra_volumes',  # List of ConfigVolume
        'trigger_downstreams',  # None, bool or dict
        'triggered_by',  # list or None
        'on_upstream_rerun',  # ActionOnRerun or None
    ],
)

ConfigConstraint = config_object_factory(
    name='ConfigConstraint',
    required=[
        'attribute',
        'operator',
        'value',
    ],
    optional=[],
)

ConfigVolume = config_object_factory(
    name='ConfigVolume',
    required=[
        'container_path',
        'host_path',
        'mode',
    ],
    optional=[],
)

ConfigParameter = config_object_factory(
    name='ConfigParameter',
    required=[
        'key',
        'value',
    ],
    optional=[],
)

StatePersistenceTypes = Enum.create('shelve', 'sql', 'yaml')

ExecutorTypes = Enum.create('ssh', 'mesos')

ActionRunnerTypes = Enum.create('none', 'subprocess')

VolumeModes = Enum.create('RO', 'RW')

ActionOnRerun = Enum.create('rerun')
