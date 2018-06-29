"""
 Immutable config schema objects.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

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
        'action_runner',  # ActionRunner
        'state_persistence',  # StatePersistence
        'command_context',  # FrozenDict of str
        'ssh_options',  # SSHOptions
        'notification_options',  # [NotificationOptions]
        'time_zone',  # pytz time zone
        'nodes',  # NodeMap
        'node_pools',  # NodePoolMap
        'jobs',  # FrozenDict of ConfigJob
        'mesos_options',  # MesosOptions
    ],
)

NamedTronConfig = config_object_factory(
    name='NamedTronConfig',
    optional=[
        'jobs',  # FrozenDict of ConfigJob
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
