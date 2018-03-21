"""
Parse a dictionary structure and return an immutable structure that
contain a validated configuration.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import getpass
import itertools
import logging
import os

import pytz
import six
from six import string_types

from tron import command_context
from tron.config import config_utils
from tron.config import ConfigError
from tron.config import schema
from tron.config.config_utils import build_dict_name_validator
from tron.config.config_utils import build_list_of_type_validator
from tron.config.config_utils import ConfigContext
from tron.config.config_utils import PartialConfigContext
from tron.config.config_utils import valid_bool
from tron.config.config_utils import valid_dict
from tron.config.config_utils import valid_float
from tron.config.config_utils import valid_identifier
from tron.config.config_utils import valid_int
from tron.config.config_utils import valid_name_identifier
from tron.config.config_utils import valid_string
from tron.config.config_utils import Validator
from tron.config.schedule_parse import valid_schedule
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.config.schema import ConfigAction
from tron.config.schema import ConfigCleanupAction
from tron.config.schema import ConfigJob
from tron.config.schema import ConfigService
from tron.config.schema import ConfigSSHOptions
from tron.config.schema import ConfigState
from tron.config.schema import MASTER_NAMESPACE
from tron.config.schema import NamedTronConfig
from tron.config.schema import NotificationOptions
from tron.config.schema import TronConfig
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
            context_object, config_context.command_context,
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


def valid_cluster_name(value, config_context):
    valid_string(value, config_context)
    if not config_context.partial and value not in config_context.clusters:
        msg = "Unknown cluster name %s at %s"
        raise ConfigError(msg % (value, config_context.path))
    return value


class ValidateSSHOptions(Validator):
    """Validate SSH options."""
    config_class = ConfigSSHOptions
    optional = True
    defaults = {
        'agent':                    False,
        'identities':               (),
        'known_hosts_file':         None,
        'connect_timeout':          30,
        'idle_connection_timeout':  3600,
        'jitter_min_load':          4,
        'jitter_max_delay':         20,
        'jitter_load_factor':       1,
    }

    validators = {
        'agent':                    valid_bool,
        # TODO: move this config and validations outside master namespace
        # 'identities':               build_list_of_type_validator(
        #                                 valid_identity_file, allow_empty=True),
        'identities':               build_list_of_type_validator(
            valid_string, allow_empty=True,
        ),
        # 'known_hosts_file':         valid_known_hosts_file,
        'known_hosts_file':         valid_string,
        'connect_timeout':          config_utils.valid_int,
        'idle_connection_timeout':  config_utils.valid_int,
        'jitter_min_load':          config_utils.valid_int,
        'jitter_max_delay':         config_utils.valid_int,
        'jitter_load_factor':       config_utils.valid_int,
    }

    def post_validation(self, valid_input, config_context):
        if config_context.partial:
            return

        if valid_input['agent'] and 'SSH_AUTH_SOCK' not in os.environ:
            raise ConfigError("No SSH Agent available ($SSH_AUTH_SOCK)")


valid_ssh_options = ValidateSSHOptions()


class ValidateNotificationOptions(Validator):
    """Validate notification options."""
    config_class = NotificationOptions
    optional = True


valid_notification_options = ValidateNotificationOptions()


class ValidateNode(Validator):
    config_class = schema.ConfigNode
    validators = {
        'name':                 config_utils.valid_identifier,
        'username':             config_utils.valid_string,
        'hostname':             config_utils.valid_string,
        'port':                 config_utils.valid_int,
    }

    defaults = {
        'port': 22,
        'username': getpass.getuser(),
    }

    def do_shortcut(self, node):
        """Nodes can be specified with just a hostname string."""
        if isinstance(node, string_types):
            return schema.ConfigNode(hostname=node, name=node, **self.defaults)

    def set_defaults(self, output_dict, config_context):
        super(ValidateNode, self).set_defaults(output_dict, config_context)
        output_dict.setdefault('name', output_dict['hostname'])


valid_node = ValidateNode()


class ValidateNodePool(Validator):
    config_class = schema.ConfigNodePool
    validators = {
        'name':                 valid_identifier,
        'nodes':                build_list_of_type_validator(valid_identifier),
    }

    def cast(self, node_pool, _context):
        if isinstance(node_pool, list):
            node_pool = dict(nodes=node_pool)
        return node_pool

    def set_defaults(self, node_pool, _):
        node_pool.setdefault('name', '_'.join(node_pool['nodes']))


valid_node_pool = ValidateNodePool()


def valid_action_name(value, config_context):
    valid_identifier(value, config_context)
    if value == CLEANUP_ACTION_NAME:
        error_msg = "Invalid action name %s at %s"
        raise ConfigError(error_msg % (value, config_context.path))
    return value


action_context = command_context.build_filled_context(
    command_context.JobContext,
    command_context.JobRunContext,
    command_context.ActionRunContext,
)


class ValidateAction(Validator):
    """Validate an action."""
    config_class = ConfigAction

    defaults = {
        'node':                 None,
        'requires':             (),
        'executor':             schema.ExecutorTypes.ssh,
        'cluster':              None,
        'pool':                 None,
        'cpus':                 None,
        'mem':                  None,
        'service':              None,
        'deploy_group':         None,
    }
    requires = build_list_of_type_validator(
        valid_action_name, allow_empty=True,
    )
    validators = {
        'name':                 valid_action_name,
        'command':              build_format_string_validator(action_context),
        'node':                 valid_node_name,
        'requires':             requires,
        'executor':             config_utils.build_enum_validator(
            schema.ExecutorTypes,
        ),
        'cluster':              valid_cluster_name,
        'pool':                 valid_string,
        'cpus':                 valid_float,
        'mem':                  valid_float,
        'service':              valid_string,
        'deploy_group':         valid_string,
    }


valid_action = ValidateAction()


def valid_cleanup_action_name(value, config_context):
    if value != CLEANUP_ACTION_NAME:
        msg = "Cleanup actions cannot have custom names %s.%s"
        raise ConfigError(msg % (config_context.path, value))
    return CLEANUP_ACTION_NAME


class ValidateCleanupAction(Validator):
    config_class = ConfigCleanupAction
    defaults = {
        'node':                 None,
        'name':                 CLEANUP_ACTION_NAME,
        'executor':             schema.ExecutorTypes.ssh,
        'cluster':              None,
        'pool':                 None,
        'cpus':                 None,
        'mem':                  None,
        'service':              None,
        'deploy_group':         None,
    }
    validators = {
        'name':                 valid_cleanup_action_name,
        'command':              build_format_string_validator(action_context),
        'node':                 valid_node_name,
        'executor':             config_utils.build_enum_validator(
            schema.ExecutorTypes,
        ),
        'cluster':              valid_cluster_name,
        'pool':                 valid_string,
        'cpus':                 valid_float,
        'mem':                  valid_float,
        'service':              valid_string,
        'deploy_group':         valid_string,
    }


valid_cleanup_action = ValidateCleanupAction()


class ValidateJob(Validator):
    """Validate jobs."""
    config_class = ConfigJob
    defaults = {
        'run_limit':            50,
        'all_nodes':            False,
        'cleanup_action':       None,
        'enabled':              True,
        'queueing':             True,
        'allow_overlap':        False,
        'max_runtime':          None,
        'monitoring':           {},
        'time_zone':            None,
        'service':              None,
        'deploy_group':         None,
    }

    validators = {
        'name':                 valid_name_identifier,
        'schedule':             valid_schedule,
        'run_limit':            valid_int,
        'all_nodes':            valid_bool,
        'actions':              build_dict_name_validator(valid_action),
        'cleanup_action':       valid_cleanup_action,
        'node':                 valid_node_name,
        'queueing':             valid_bool,
        'enabled':              valid_bool,
        'allow_overlap':        valid_bool,
        'max_runtime':          config_utils.valid_time_delta,
        'monitoring':           valid_dict,
        'time_zone':            valid_time_zone,
        'service':              valid_string,
        'deploy_group':         valid_string,
    }

    def cast(self, in_dict, config_context):
        in_dict['namespace'] = config_context.namespace
        return in_dict

    # TODO: extract common code to a util function
    def _validate_dependencies(
        self, job, actions,
        base_action, current_action=None, stack=None,
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
                job, actions, base_action, actions[dep], stack,
            )

        stack.pop()

    def post_validation(self, job, config_context):
        """Validate actions for the job."""
        incomplete_paasta_actions = []

        def is_incomplete_paasta_action(action):
            return (
                action.executor == schema.ExecutorTypes.paasta and (
                    action.service is None or
                    action.deploy_group is None
                )
            )

        for _, action in six.iteritems(job['actions']):
            self._validate_dependencies(job, job['actions'], action)
            if is_incomplete_paasta_action(action):
                incomplete_paasta_actions.append(action)

        cleanup_action = job.get('cleanup_action')
        if cleanup_action and is_incomplete_paasta_action(cleanup_action):
            incomplete_paasta_actions.append(action)

        if incomplete_paasta_actions and not(
            job.get('service') and job.get('deploy_group')
        ):
            raise ConfigError(
                'Either job {name} or PaaSTA actions {actions} need a service '
                'and deploy_group.'.format(
                    name=job['name'],
                    actions=incomplete_paasta_actions,
                ),
            )


valid_job = ValidateJob()


class ValidateService(Validator):
    """Validate a services configuration."""
    config_class = ConfigService

    service_context = command_context.build_filled_context(
        command_context.ServiceInstanceContext,
    )

    service_pid_context = command_context.build_filled_context(
        command_context.ServiceInstancePidContext,
    )

    defaults = {
        'count':                1,
        'monitor_retries':      5,
        'restart_delay':        None,
    }

    validators = {
        'name':                 valid_name_identifier,
        'pid_file':             build_format_string_validator(service_pid_context),
        'command':              build_format_string_validator(service_context),
        'monitor_interval':     valid_float,
        'monitor_retries':      valid_int,
        'count':                valid_int,
        'node':                 valid_node_name,
        'restart_delay':        valid_float,
    }

    def cast(self, in_dict, config_context):
        in_dict['namespace'] = config_context.namespace
        return in_dict


valid_service = ValidateService()


class ValidateActionRunner(Validator):
    config_class = schema.ConfigActionRunner
    optional = True
    defaults = {
        'runner_type':          None,
        'remote_exec_path':     '',
        'remote_status_path':   '/tmp',
    }

    validators = {
        'runner_type':          config_utils.build_enum_validator(
            schema.ActionRunnerTypes,
        ),
        'remote_status_path':   valid_string,
        'remote_exec_path':     valid_string,
    }


class ValidateStatePersistence(Validator):
    config_class = schema.ConfigState
    defaults = {
        'buffer_size':          1,
        'connection_details':   None,
    }

    validators = {
        'name':                 valid_string,
        'store_type':           config_utils.build_enum_validator(
            schema.StatePersistenceTypes,
        ),
        'connection_details':   valid_string,
        'buffer_size':          valid_int,
    }

    def post_validation(self, config, config_context):
        buffer_size = config.get('buffer_size')

        if buffer_size and buffer_size < 1:
            path = config_context.path
            raise ConfigError("%s buffer_size must be >= 1." % path)


valid_state_persistence = ValidateStatePersistence()


def validate_jobs_and_services(config, config_context):
    """Validate jobs and services."""
    valid_jobs = build_dict_name_validator(valid_job, allow_empty=True)
    valid_services = build_dict_name_validator(valid_service, allow_empty=True)
    validation = [('jobs', valid_jobs), ('services', valid_services)]

    for config_name, valid in validation:
        child_context = config_context.build_child_context(config_name)
        config[config_name] = valid(config.get(config_name, []), child_context)

    fmt_string = 'Job and Service names must be unique %s'
    config_utils.unique_names(fmt_string, config['jobs'], config['services'])


DEFAULT_STATE_PERSISTENCE = ConfigState('tron_state', 'shelve', None, 1)
DEFAULT_NODE = ValidateNode().do_shortcut(node='localhost')


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    FrozenDicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """
    config_class = TronConfig
    defaults = {
        'action_runner':        {},
        'output_stream_dir':    None,
        'command_context':      {},
        'ssh_options':          ValidateSSHOptions.defaults,
        'notification_options': None,
        'time_zone':            None,
        'state_persistence':    DEFAULT_STATE_PERSISTENCE,
        'nodes':                {'localhost': DEFAULT_NODE},
        'node_pools':           {},
        'jobs':                 (),
        'services':             (),
        'clusters':             (),
    }
    node_pools = build_dict_name_validator(valid_node_pool, allow_empty=True)
    nodes = build_dict_name_validator(valid_node, allow_empty=True)
    clusters = build_list_of_type_validator(valid_string, allow_empty=True)
    validators = {
        'action_runner':        ValidateActionRunner(),
        'output_stream_dir':    valid_output_stream_dir,
        'command_context':      valid_command_context,
        'ssh_options':          valid_ssh_options,
        'notification_options': valid_notification_options,
        'time_zone':            valid_time_zone,
        'state_persistence':    valid_state_persistence,
        'nodes':                nodes,
        'node_pools':           node_pools,
        'clusters':             clusters,
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
            config['nodes'], config.get('node_pools', []),
        )

        if config.get('node_pools'):
            self.validate_node_pool_nodes(config)

        config_context = ConfigContext(
            'config', node_names, config.get('clusters'),
            config.get('command_context'), MASTER_NAMESPACE,
        )
        validate_jobs_and_services(config, config_context)


class ValidateNamedConfig(Validator):
    """A shorter validator for named configurations, which allow for
    jobs and services to be defined as configuration fragments that
    are, in turn, reconciled by Tron.
    """
    config_class = NamedTronConfig
    type_name = "NamedConfigFragment"
    defaults = {
        'jobs':                 (),
        'services':             (),
    }

    optional = False

    def post_validation(self, config, config_context):
        validate_jobs_and_services(config, config_context)


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
            name, nodes, master.clusters, master.command_context, name,
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

    # TODO: DRY with get_jobs(), get_services()
    def get_job_and_service_names(self):
        job_names, service_names = [], []
        for config in six.itervalues(self.configs):
            job_names.extend(config.jobs)
            service_names.extend(config.services)
        return job_names, service_names

    def get_jobs(self):
        return dict(itertools.chain.from_iterable(
            six.iteritems(config.jobs) for _, config in self.configs.items()
        ))

    def get_services(self):
        return dict(itertools.chain.from_iterable(
            six.iteritems(config.services) for _, config in self.configs.items()
        ))

    def get_master(self):
        return self.configs[MASTER_NAMESPACE]

    def get_node_names(self):
        return get_nodes_from_master_namespace(self.get_master())

    def __getitem__(self, name):
        return self.configs[name]

    def __contains__(self, name):
        return name in self.configs
