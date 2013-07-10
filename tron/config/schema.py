"""
 Immutable config schema objects.
"""
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
    'TronConfig',
    optional=[
        'output_stream_dir',   # str
        'action_runner',       # ConfigActionRunner
        'state_persistence',   # ConfigState
        'command_context',     # FrozenDict of str
        'ssh_options',         # ConfigSSHOptions
        'notification_options',# NotificationOptions or None
        'time_zone',           # pytz time zone
        'nodes',               # FrozenDict of ConfigNode
        'node_pools',          # FrozenDict of ConfigNodePool
        'jobs',                # FrozenDict of ConfigJob
        'services',            # FrozenDict of ConfigService
    ])


NamedTronConfig = config_object_factory(
    'NamedTronConfig',
    optional=[
        'jobs',                # FrozenDict of ConfigJob
        'services'             # FrozenDict of ConfigService
    ])


NotificationOptions = config_object_factory(
    'NotificationOptions',
    [
        'smtp_host',            # str
        'notification_addr',    # str
    ])


ConfigActionRunner = config_object_factory('ConfigActionRunner',
    optional=['runner_type', 'remote_status_path', 'remote_exec_path'])


ConfigSSHOptions = config_object_factory(
    'ConfigSSHOptions',
    optional=[
        'agent',
        'identities',
        'known_hosts_file',
        'connect_timeout',
        'idle_connection_timeout',
        'jitter_min_load',
        'jitter_max_delay',
        'jitter_load_factor',
    ])


ConfigNode = config_object_factory('ConfigNode',
    ['hostname'], ['name', 'username', 'port'])


ConfigNodePool = config_object_factory('ConfigNodePool', ['nodes'], ['name'])


ConfigState = config_object_factory(
    'ConfigState',
    [
        'name',
        'store_type',
        ],[
        'connection_details',
        'buffer_size'
    ])


ConfigJob = config_object_factory(
    'ConfigJob',
    [
        'name',                 # str
        'node',                 # str
        'schedule',             # Config*Scheduler
        'actions',              # FrozenDict of ConfigAction
        'namespace',            # str
    ],[
        'queueing',             # bool
        'run_limit',            # int
        'all_nodes',            # bool
        'cleanup_action',       # ConfigAction
        'enabled',              # bool
        'allow_overlap',        # bool
        'max_runtime',          # datetime.Timedelta
    ])


ConfigAction = config_object_factory(
    'ConfigAction',
    [
        'name',                 # str
        'command',              # str
    ],[
        'requires',             # tuple of str
        'node',                 # str
    ])

ConfigCleanupAction = config_object_factory(
    'ConfigCleanupAction',
    [
        'command',              # str
    ],[
        'name',                 # str
        'node',                 # str
    ])


ConfigService = config_object_factory(
    'ConfigService',
    [
        'name',                 # str
        'node',                 # str
        'pid_file',             # str
        'command',              # str
        'monitor_interval',     # float
        'namespace',            # str
    ],[
        'restart_delay',        # float
        'count',                # int
    ])


StatePersistenceTypes = Enum.create('shelve', 'sql', 'mongo', 'yaml')


ActionRunnerTypes = Enum.create('none', 'subprocess')
