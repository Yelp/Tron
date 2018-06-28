"""
Parse a dictionary structure and return an immutable structure that
contain a validated configuration.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import itertools
import logging
import os

import pytz
import six

from tron import command_context
from tron.config import config_utils
from tron.config import ConfigError
from tron.config.action_runner import ActionRunner
from tron.config.config_utils import build_dict_name_validator
from tron.config.config_utils import ConfigContext
from tron.config.config_utils import PartialConfigContext
from tron.config.config_utils import valid_bool
from tron.config.config_utils import valid_dict
from tron.config.config_utils import valid_identifier
from tron.config.config_utils import valid_int
from tron.config.config_utils import valid_name_identifier
from tron.config.config_utils import valid_string
from tron.config.config_utils import Validator
from tron.config.mesos_options import MesosOptions
from tron.config.node import NodeMap
from tron.config.node import NodePoolMap
from tron.config.notification_options import NotificationOptions
from tron.config.schedule_parse import valid_schedule
from tron.config.schema import ConfigJob
from tron.config.schema import MASTER_NAMESPACE
from tron.config.schema import NamedTronConfig
from tron.config.schema import TronConfig
from tron.config.ssh_options import SSHOptions
from tron.config.state_persistence import StatePersistence
from tron.core.action import Action
from tron.core.action import ActionMap
from tron.utils.dicts import FrozenDict

log = logging.getLogger(__name__)


def build_format_string_validator(context_object):
    """Validate that a string does not contain any unexpected formatting keys.
        valid_keys - a sequence of strings
    """

    def validator(value, config_context):
        if config_context.partial:
            return valid_string(value, config_context)

        context = command_context.CommandContext(
            context_object,
            config_context.command_context,
        )

        try:
            value % context
            return value
        except (KeyError, ValueError) as e:
            error_msg = "Unknown context variable %s at %s: %s"
            raise ConfigError(error_msg % (e, config_context.path, value))

    return validator


def valid_output_stream_dir(output_dir, config_context):
    """Returns a valid string for the output directory, or raises ConfigError
    if the output_dir is not valid.
    """
    if not output_dir:
        return

    if config_context.partial:
        return output_dir

    valid_string(output_dir, config_context)
    if not os.path.isdir(output_dir):
        msg = "output_stream_dir '%s' is not a directory"
        raise ConfigError(msg % output_dir)

    if not os.access(output_dir, os.W_OK):
        raise ConfigError(
            "output_stream_dir '%s' is not writable" % output_dir,
        )

    return output_dir


def valid_identity_file(file_path, config_context):
    valid_string(file_path, config_context)

    if config_context.partial:
        return file_path

    file_path = os.path.expanduser(file_path)
    if not os.path.exists(file_path):
        raise ConfigError("Private key file %s doesn't exist" % file_path)

    public_key_path = file_path + '.pub'
    if not os.path.exists(public_key_path):
        raise ConfigError("Public key file %s doesn't exist" % public_key_path)
    return file_path


def valid_known_hosts_file(file_path, config_context):
    valid_string(file_path, config_context)

    if config_context.partial:
        return file_path

    file_path = os.path.expanduser(file_path)
    if not os.path.exists(file_path):
        raise ConfigError("Known hosts file %s doesn't exist" % file_path)
    return file_path


def valid_command_context(context, config_context):
    # context can be any dict.
    return FrozenDict(**valid_dict(context or {}, config_context))


def valid_time_zone(tz, config_context):
    if tz is None:
        return None
    valid_string(tz, config_context)
    try:
        return pytz.timezone(tz)
    except pytz.exceptions.UnknownTimeZoneError:
        raise ConfigError('%s is not a valid time zone' % tz)


def valid_node_name(value, config_context):
    valid_identifier(value, config_context)
    if not config_context.partial and value not in config_context.nodes:
        msg = "Unknown node name %s at %s"
        raise ConfigError(msg % (value, config_context.path))
    return value


action_context = command_context.build_filled_context(
    command_context.JobContext,
    command_context.JobRunContext,
    command_context.ActionRunContext,
)


class ValidateJob(Validator):
    """Validate jobs."""
    config_class = ConfigJob
    defaults = {
        'run_limit': 50,
        'all_nodes': False,
        'cleanup_action': None,
        'enabled': True,
        'queueing': True,
        'allow_overlap': False,
        'max_runtime': None,
        'monitoring': {},
        'time_zone': None,
        'expected_runtime': datetime.timedelta(hours=24),
    }

    validators = {
        'name': valid_name_identifier,
        'schedule': valid_schedule,
        'run_limit': valid_int,
        'all_nodes': valid_bool,
        'actions': ActionMap.from_config,
        'cleanup_action': Action.from_config,
        'node': valid_node_name,
        'queueing': valid_bool,
        'enabled': valid_bool,
        'allow_overlap': valid_bool,
        'max_runtime': config_utils.valid_time_delta,
        'monitoring': valid_dict,
        'time_zone': valid_time_zone,
        'expected_runtime': config_utils.valid_time_delta,
    }

    def cast(self, in_dict, config_context):
        in_dict['namespace'] = config_context.namespace
        return in_dict

    # TODO: extract common code to a util function
    def _validate_dependencies(
        self,
        job,
        actions,
        base_action,
        current_action=None,
        stack=None,
    ):
        """Check for circular or misspelled dependencies."""
        stack = stack or []
        current_action = current_action or base_action

        stack.append(current_action.name)
        for dep in current_action.requires:
            if dep == base_action.name and len(stack) > 0:
                msg = 'Circular dependency in job.%s: %s'
                raise ConfigError(msg % (job['name'], ' -> '.join(stack)))
            if dep not in actions:
                raise ConfigError(
                    'Action jobs.%s.%s has a dependency "%s"'
                    ' that is not in the same job!' %
                    (job['name'], current_action.name, dep),
                )
            self._validate_dependencies(
                job,
                actions,
                base_action,
                actions[dep],
                stack,
            )

        stack.pop()

    def post_validation(self, job, config_context):
        """Validate actions for the job."""
        if len(job['actions']) == 0:
            raise ConfigError(
                'Required non-empty list at {}.actions'.format(
                    config_context.path,
                ),
            )

        for _, action in six.iteritems(job['actions']):
            self._validate_dependencies(job, job['actions'], action)


valid_job = ValidateJob()


def validate_jobs(config, config_context):
    """Validate jobs"""
    valid_jobs = build_dict_name_validator(valid_job, allow_empty=True)
    validation = [('jobs', valid_jobs)]

    for config_name, valid in validation:
        child_context = config_context.build_child_context(config_name)
        config[config_name] = valid(config.get(config_name, []), child_context)

    fmt_string = 'Job names must be unique %s'
    config_utils.unique_names(fmt_string, config['jobs'])


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """
    config_class = TronConfig
    defaults = {
        'action_runner': {},
        'output_stream_dir':
            None,
        'command_context': {},
        'ssh_options':
            SSHOptions(),
        'notification_options':
            None,
        'time_zone':
            None,
        'state_persistence':
            StatePersistence(name='tron_state'),
        'nodes':
            NodeMap.from_config([dict(name='localhost', hostname='localhost')],
                                None),
        'node_pools':
            NodePoolMap(),
        'jobs': (),
        'mesos_options':
            MesosOptions(),
    }

    validators = {
        'action_runner': ActionRunner.from_config,
        'output_stream_dir': valid_output_stream_dir,
        'command_context': valid_command_context,
        'ssh_options': SSHOptions.from_config,
        'notification_options': NotificationOptions.from_config,
        'time_zone': valid_time_zone,
        'state_persistence': StatePersistence.from_config,
        'nodes': NodeMap.from_config,
        'node_pools': NodePoolMap.from_config,
        'mesos_options': MesosOptions.from_config,
    }
    optional = False

    def validate_node_pool_nodes(self, config):
        """Validate that each node in a node_pool is in fact a node, and not
        another pool.
        """
        all_node_names = set(config['nodes'])
        for node_pool in six.itervalues(config['node_pools']):
            invalid_names = set(node_pool.nodes) - all_node_names
            if invalid_names:
                msg = "NodePool %s contains other NodePools: " % node_pool.name
                raise ConfigError(msg + ",".join(invalid_names))

    def post_validation(self, config, _):
        """Validate a non-named config."""
        node_names = config_utils.unique_names(
            'Node and NodePool names must be unique %s',
            config['nodes'],
            config.get('node_pools', []),
        )

        if config.get('node_pools'):
            self.validate_node_pool_nodes(config)

        config_context = ConfigContext(
            'config',
            node_names,
            config.get('command_context'),
            MASTER_NAMESPACE,
        )
        validate_jobs(config, config_context)


class ValidateNamedConfig(Validator):
    """A shorter validator for named configurations, which allow for
    jobs to be defined as configuration fragments that
    are, in turn, reconciled by Tron.
    """
    config_class = NamedTronConfig
    type_name = "NamedConfigFragment"
    defaults = {
        'jobs': (),
    }

    optional = False

    def post_validation(self, config, config_context):
        validate_jobs(config, config_context)


valid_config = ValidateConfig()
valid_named_config = ValidateNamedConfig()


def validate_fragment(name, fragment):
    """Validate a fragment with a partial context."""
    config_context = PartialConfigContext(name, name)
    if name == MASTER_NAMESPACE:
        return valid_config(fragment, config_context=config_context)
    return valid_named_config(fragment, config_context=config_context)


def get_nodes_from_master_namespace(master):
    return set(itertools.chain(master.nodes, master.node_pools))


def validate_config_mapping(config_mapping):
    if MASTER_NAMESPACE not in config_mapping:
        msg = "A config mapping requires a %s namespace"
        raise ConfigError(msg % MASTER_NAMESPACE)

    master = valid_config(config_mapping.pop(MASTER_NAMESPACE))
    nodes = get_nodes_from_master_namespace(master)
    yield MASTER_NAMESPACE, master

    for name, content in six.iteritems(config_mapping):
        context = ConfigContext(
            name,
            nodes,
            master.command_context,
            name,
        )
        yield name, valid_named_config(content, config_context=context)


class ConfigContainer(object):
    """A container around configuration fragments (and master)."""

    def __init__(self, config_mapping):
        self.configs = config_mapping

    def items(self):
        return six.iteritems(self.configs)

    @classmethod
    def create(cls, config_mapping):
        return cls(dict(validate_config_mapping(config_mapping)))

    # TODO: DRY with get_jobs()
    def get_job_names(self):
        job_names = []
        for config in six.itervalues(self.configs):
            job_names.extend(config.jobs)
        return job_names

    def get_jobs(self):
        return dict(
            itertools.chain.from_iterable(
                six.iteritems(config.jobs)
                for _, config in self.configs.items()
            ),
        )

    def get_master(self):
        return self.configs[MASTER_NAMESPACE]

    def get_node_names(self):
        return get_nodes_from_master_namespace(self.get_master())

    def __getitem__(self, name):
        return self.configs[name]

    def __contains__(self, name):
        return name in self.configs
