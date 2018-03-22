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
    config_class.required_keys = required
    config_class.optional_keys = optional
    return config_class


TronConfig = config_object_factory(
    name='TronConfig',
    optional=[
        'output_stream_dir',   # str
        'action_runner',       # ConfigActionRunner
        'state_persistence',   # ConfigState
        'command_context',     # FrozenDict of str
        'ssh_options',         # ConfigSSHOptions
        'notification_options',  # NotificationOptions or None
        'time_zone',           # pytz time zone
        'nodes',               # FrozenDict of ConfigNode
        'node_pools',          # FrozenDict of ConfigNodePool
        'jobs',                # FrozenDict of ConfigJob
        'services',            # FrozenDict of ConfigService
        'clusters',            # tuple of str
    ],
)


NamedTronConfig = config_object_factory(
    name='NamedTronConfig',
    optional=[
        'jobs',                # FrozenDict of ConfigJob
        'services',             # FrozenDict of ConfigService
    ],
)


NotificationOptions = config_object_factory(
    name='NotificationOptions',
    required=[
        'smtp_host',            # str
        'notification_addr',    # str
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


ConfigJob = config_object_factory(
    name='ConfigJob',
    required=[
        'name',                 # str
        'node',                 # str
        'schedule',             # Config*Scheduler
        'actions',              # FrozenDict of ConfigAction
        'namespace',            # str
    ],
    optional=[
        'monitoring',           # dict
        'queueing',             # bool
        'run_limit',            # int
        'all_nodes',            # bool
        'cleanup_action',       # ConfigAction
        'enabled',              # bool
        'allow_overlap',        # bool
        'max_runtime',          # datetime.Timedelta
        'time_zone',            # pytz time zone
        'service',              # str
        'deploy_group',         # str
    ],
)


ConfigAction = config_object_factory(
    name='ConfigAction',
    required=[
        'name',                 # str
        'command',              # str
    ],
    optional=[
        'requires',             # tuple of str
        'node',                 # str
        'executor',             # str
        'cluster',              # str
        'pool',                 # str
        'cpus',                 # float
        'mem',                  # float
        'service',              # str
        'deploy_group',         # str
    ],
)

ConfigCleanupAction = config_object_factory(
    name='ConfigCleanupAction',
    required=[
        'command',              # str
    ],
    optional=[
        'name',                 # str
        'node',                 # str
        'executor',             # str
        'cluster',              # str
        'pool',                 # str
        'cpus',                 # float
        'mem',                  # float
        'service',              # str
        'deploy_group',         # str
    ],
)


ConfigService = config_object_factory(
    name='ConfigService',
    required=[
        'name',                 # str
        'node',                 # str
        'pid_file',             # str
        'command',              # str
        'monitor_interval',     # float
        'namespace',            # str
    ],
    optional=[
        'restart_delay',        # float
        'monitor_retries',      # int
        'count',                # int
    ],
)


StatePersistenceTypes = Enum.create('shelve', 'sql', 'yaml')


ExecutorTypes = Enum.create('ssh', 'paasta')


ActionRunnerTypes = Enum.create('none', 'subprocess')
