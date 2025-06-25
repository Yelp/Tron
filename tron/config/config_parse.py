"""
Parse a dictionary structure and return an immutable structure that
contain a validated configuration.


WARNING: it is *NOT* safe to delete classes that are being validated (or their attributes) if there are any references to them in DynamoDB until TRON-2200 is complete! (See DAR-2328)
NOTE: this means that reverting a change that adds a new attribute is not safe :)
"""
import datetime
import getpass
import itertools
import logging
import os
from copy import deepcopy
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

import pytz
from task_processing.plugins.mesos.constraints import OPERATORS

from tron import command_context
from tron.config import config_utils
from tron.config import ConfigError
from tron.config import schema
from tron.config.config_utils import build_dict_name_validator
from tron.config.config_utils import build_dict_value_validator
from tron.config.config_utils import build_list_of_type_validator
from tron.config.config_utils import ConfigContext
from tron.config.config_utils import PartialConfigContext
from tron.config.config_utils import StringFormatter
from tron.config.config_utils import valid_bool
from tron.config.config_utils import valid_dict
from tron.config.config_utils import valid_exit_code
from tron.config.config_utils import valid_float
from tron.config.config_utils import valid_identifier
from tron.config.config_utils import valid_int
from tron.config.config_utils import valid_list
from tron.config.config_utils import valid_name_identifier
from tron.config.config_utils import valid_string
from tron.config.config_utils import Validator
from tron.config.schedule_parse import valid_schedule
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.config.schema import ConfigAction
from tron.config.schema import ConfigCleanupAction
from tron.config.schema import ConfigConstraint
from tron.config.schema import ConfigFieldSelectorSource
from tron.config.schema import ConfigJob
from tron.config.schema import ConfigKubernetes
from tron.config.schema import ConfigMesos
from tron.config.schema import ConfigNodeAffinity
from tron.config.schema import ConfigParameter
from tron.config.schema import ConfigProjectedSAVolume
from tron.config.schema import ConfigSecretSource
from tron.config.schema import ConfigSecretVolume
from tron.config.schema import ConfigSecretVolumeItem
from tron.config.schema import ConfigSSHOptions
from tron.config.schema import ConfigState
from tron.config.schema import ConfigTopologySpreadConstraints
from tron.config.schema import ConfigVolume
from tron.config.schema import MASTER_NAMESPACE
from tron.config.schema import NamedTronConfig
from tron.config.schema import TronConfig

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
            StringFormatter(context).format(value)
            return value
        except (KeyError, ValueError) as e:
            error_msg = "Unknown context variable %s at %s: %s"
            raise ConfigError(error_msg % (e, config_context.path, value))
        except (TypeError) as e:
            error_msg = "Wrong command format %s: %s at %s"
            raise ConfigError(error_msg % (value, e, config_context.path))

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

    public_key_path = file_path + ".pub"
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
    return valid_dict(context or {}, config_context)


def valid_time_zone(tz, config_context):
    if tz is None:
        return None
    valid_string(tz, config_context)
    try:
        return pytz.timezone(tz)
    except pytz.exceptions.UnknownTimeZoneError:
        raise ConfigError("%s is not a valid time zone" % tz)


def valid_node_name(value, config_context):
    valid_identifier(value, config_context)
    if not config_context.partial and value not in config_context.nodes:
        msg = "Unknown node name %s at %s"
        raise ConfigError(msg % (value, config_context.path))
    return value


def valid_master_address(value, config_context):
    """Validates and normalizes Mesos master address.

    Must be HTTP or not include a scheme, and only include
    a host, without any path components.
    """
    valid_string(value, config_context)

    # Parse with HTTP as default, only HTTP allowed.
    scheme, netloc, path, params, query, fragment = urlparse(value, "http")
    if scheme != "http":
        msg = f"Only HTTP supported for Mesos master address, got {value}"
        raise ConfigError(msg)

    if params or query or fragment:
        msg = f"Mesos master address may not contain path components, got {value}"
        raise ConfigError(msg)

    # Only one of netloc or path allowed, and no / except trailing ones.
    # netloc is empty if there's no scheme, then we try the path.
    path = path.rstrip("/")
    if (netloc and path) or "/" in path:
        msg = f"Mesos master address may not contain path components, got {value}"
        raise ConfigError(msg)

    if not netloc:
        netloc = path

    if not netloc:
        msg = f"Mesos master address is missing host, got {value}"
        raise ConfigError(msg)

    return f"{scheme}://{netloc}"


def valid_k8s_master_address(value: str, config_context: ConfigContext) -> str:
    """Validates and normalizes Kubernetes master address.

    Must be HTTP or not include a scheme, and only include
    a host, without any path components.
    """
    valid_string(value, config_context)

    # Parse with HTTP as default, only HTTPS allowed.
    scheme, netloc, path, params, query, fragment = urlparse(url=value, scheme="https")
    if scheme != "http":
        msg = f"Only HTTPS supported for Kubernetes master address, got {value}"
        raise ConfigError(msg)

    if params or query or fragment:
        msg = f"Kubernetes master address may not contain path components, got {value}"
        raise ConfigError(msg)

    # Only one of netloc or path allowed, and no / except trailing ones.
    path = path.rstrip("/")
    if (netloc and path) or "/" in path:
        msg = f"Kubernetes master address may not contain path components, got {value}"
        raise ConfigError(msg)

    # netloc is empty if there's no scheme, so we fallback to path.
    if not netloc and path:
        netloc = path

    if not netloc:
        msg = f"Kubernetes master address is missing host, got {value}"
        raise ConfigError(msg)

    return f"{scheme}://{netloc}"


class ValidateConstraint(Validator):
    config_class = ConfigConstraint
    validators = {
        "attribute": valid_string,
        "operator": config_utils.build_enum_validator(OPERATORS.keys()),
        "value": valid_string,
    }


valid_constraint = ValidateConstraint()


class ValidateDockerParameter(Validator):
    config_class = ConfigParameter
    validators = {
        "key": valid_string,
        "value": valid_string,
    }


valid_docker_parameter = ValidateDockerParameter()


class ValidateVolume(Validator):
    config_class = ConfigVolume
    validators = {
        "container_path": valid_string,
        "host_path": valid_string,
        "mode": config_utils.build_real_enum_validator(schema.VolumeModes),
    }


valid_volume = ValidateVolume()


class ValidateSecretSource(Validator):
    config_class = ConfigSecretSource
    validators = {
        "secret_name": valid_string,  # name of Kubernetes Secret
        "key": valid_string,  # key name in Secret data
    }


valid_secret_source = ValidateSecretSource()


def valid_permission_mode(value: Union[str, int], config_context: ConfigContext) -> str:
    try:
        decimal_value = int(
            str(value), base=8
        )  # take in permission mode as string or int representation of an octal number. Goes from 0 to 4095 in decimal.
    except ValueError:
        error_msg = "Could not parse {} as octal permission mode at {}"
        raise ConfigError(error_msg.format(value, config_context.path))
    if decimal_value > 4095 or decimal_value < 0:
        error_msg = "Octal permission mode {} out of bound at {}"
        raise ConfigError(error_msg.format(value, config_context.path))
    return str(value)


class ValidateSecretVolumeItem(Validator):
    config_class = ConfigSecretVolumeItem

    validators = {
        "key": valid_string,  # name of current secret
        "path": valid_string,  # New secret filename
        "mode": valid_permission_mode,  # Octal permission mode
    }


valid_secret_volume_item = ValidateSecretVolumeItem()


class ValidateSecretVolume(Validator):
    config_class = ConfigSecretVolume

    optional = True
    defaults = {
        "default_mode": "0644",
        "items": None,
    }

    validators = {
        "container_path": valid_string,
        "secret_volume_name": valid_string,
        "secret_name": valid_string,
        "default_mode": valid_permission_mode,
        "items": build_list_of_type_validator(valid_secret_volume_item, allow_empty=True),
    }

    def post_validation(self, valid_input, config_context):
        """Propagate default mode and enforce the secret-key match."""
        # Ensure 'items' is an iterable list, even if defaulted to None by set_defaults.
        # The 'or []' handles the case where valid_input.get('items') returns None.
        items = valid_input.get("items") or []

        # Our secrets will really only ever have one key, so weirdly we only care about a single
        # item of this array AND it must have the same name as the secret (which is the single key).
        if len(items) > 1:
            raise ConfigError(
                "There is more than one item in the items array. This is unsupported as we don't support multi-key secrets."
            )

        processed_items = []
        modified = False
        default_mode = valid_input.get("default_mode", self.defaults["default_mode"])
        secret_name = valid_input.get("secret_name")

        for item in items:
            if item.key != secret_name:
                raise ConfigError(f"Item key '{item.key}' does not match the volume's secret name '{secret_name}'")

            final_item = item
            if item.mode is None:
                # Apply volume's default_mode to items without an explicit mode.
                final_item = item._replace(mode=default_mode)
                modified = True
            processed_items.append(final_item)

        if modified:
            # Update valid_input with the (potentially) modified items tuple.
            # This ensures the final object reflects applied defaults.
            valid_input["items"] = tuple(processed_items)


valid_secret_volume = ValidateSecretVolume()


class ValidateProjectedSAVolume(Validator):
    config_class = ConfigProjectedSAVolume
    optional = True
    defaults = {
        "expiration_seconds": 1800,
    }
    validators = {
        "container_path": valid_string,
        "audience": valid_string,
        "expiration_seconds": valid_int,
    }


valid_projected_sa_volume = ValidateProjectedSAVolume()


class ValidateFieldSelectorSource(Validator):
    config_class = ConfigFieldSelectorSource
    validators = {
        "field_path": valid_string,  # k8s field path - e.g., `status.podIP`
    }


valid_field_selector_source = ValidateFieldSelectorSource()


def _valid_node_affinity_operator(value: str, config_context: ConfigContext) -> str:
    valid_operators = {"In", "NotIn", "Exists", "NotExists", "Gt", "Lt"}
    if value not in valid_operators:
        raise ConfigError(f"Got {value} as a node affinity operator, expected one of {valid_operators}")

    return value


class ValidateNodeAffinity(Validator):
    config_class = ConfigNodeAffinity
    validators = {
        "key": valid_string,
        "operator": _valid_node_affinity_operator,
        "value": build_list_of_type_validator(valid_string, allow_empty=True),
    }


valid_node_affinity = ValidateNodeAffinity()


def _valid_when_unsatisfiable(value: str, config_context: ConfigContext) -> str:
    valid_values = {"DoNotSchedule", "ScheduleAnyway"}
    if value not in valid_values:
        raise ConfigError(f"Got {value} as a when_unsatisfiable value, expected one of {valid_values}")

    return value


def _valid_topology_spread_label_selector(value: Dict[str, str], config_context: ConfigContext) -> Dict[str, str]:
    if not value:
        raise ConfigError("TopologySpreadConstraints must have a label_selector")

    # XXX: we probably also want to enforce k8s limits for label lengths and whatnot
    if not all(isinstance(k, str) for k in value.keys()):
        raise ConfigError("TopologySpreadConstraints label_selector keys must be strings")

    if not all(isinstance(s, str) for s in value.values()):
        raise ConfigError("TopologySpreadConstraints label_selector values must be strings")

    return value


class ValidateTopologySpreadConstraints(Validator):
    config_class = ConfigTopologySpreadConstraints
    validators = {
        "max_skew": valid_int,
        "when_unsatisfiable": _valid_when_unsatisfiable,
        "topology_key": valid_string,
        "label_selector": _valid_topology_spread_label_selector,
    }


valid_topology_spread_constraints = ValidateTopologySpreadConstraints()


class ValidateSSHOptions(Validator):
    """Validate SSH options."""

    config_class = ConfigSSHOptions
    optional = True
    defaults = {
        "agent": False,
        "identities": (),
        "known_hosts_file": None,
        "connect_timeout": 30,
        "idle_connection_timeout": 3600,
        "jitter_min_load": 4,
        "jitter_max_delay": 20,
        "jitter_load_factor": 1,
    }

    validators = {
        "agent": valid_bool,
        # TODO: move this config and validations outside master namespace
        # 'identities':               build_list_of_type_validator(
        #                                 valid_identity_file, allow_empty=True),
        "identities": build_list_of_type_validator(
            valid_string,
            allow_empty=True,
        ),
        # 'known_hosts_file':         valid_known_hosts_file,
        "known_hosts_file": valid_string,
        "connect_timeout": config_utils.valid_int,
        "idle_connection_timeout": config_utils.valid_int,
        "jitter_min_load": config_utils.valid_int,
        "jitter_max_delay": config_utils.valid_int,
        "jitter_load_factor": config_utils.valid_int,
    }

    def post_validation(self, valid_input, config_context):
        if config_context.partial:
            return

        if valid_input["agent"] and "SSH_AUTH_SOCK" not in os.environ:
            raise ConfigError("No SSH Agent available ($SSH_AUTH_SOCK)")


valid_ssh_options = ValidateSSHOptions()


class ValidateNode(Validator):
    config_class = schema.ConfigNode
    validators = {
        "name": config_utils.valid_identifier,
        "username": config_utils.valid_string,
        "hostname": config_utils.valid_string,
        "port": config_utils.valid_int,
    }

    defaults = {
        "port": 22,
        "username": getpass.getuser(),
    }

    def do_shortcut(self, node):
        """Nodes can be specified with just a hostname string."""
        if isinstance(node, str):
            return schema.ConfigNode(hostname=node, name=node, **self.defaults)

    def set_defaults(self, output_dict, config_context):
        super().set_defaults(output_dict, config_context)
        output_dict.setdefault("name", output_dict["hostname"])


valid_node = ValidateNode()


class ValidateNodePool(Validator):
    config_class = schema.ConfigNodePool
    validators = {
        "name": valid_identifier,
        "nodes": build_list_of_type_validator(valid_identifier),
    }

    def cast(self, node_pool, _context):
        if isinstance(node_pool, list):
            node_pool = dict(nodes=node_pool)
        return node_pool

    def set_defaults(self, node_pool, _):
        node_pool.setdefault("name", "_".join(node_pool["nodes"]))


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


def valid_mesos_action(action, config_context):
    required_keys = {"cpus", "mem", "docker_image"}
    if action.get("executor") == schema.ExecutorTypes.mesos.value:
        missing_keys = required_keys - set(action.keys())
        if missing_keys:
            raise ConfigError(
                "Mesos executor for action {id} is missing these required keys: {keys}".format(
                    id=action["name"],
                    keys=missing_keys,
                ),
            )


def valid_kubernetes_action(action, config_context):
    required_keys = {"cpus", "mem", "docker_image"}
    if action.get("executor") == schema.ExecutorTypes.kubernetes.value:
        missing_keys = required_keys - set(action.keys())
        if missing_keys:
            raise ConfigError(
                "Kubernetes executor for action {id} is missing these required keys: {keys}".format(
                    id=action["name"],
                    keys=missing_keys,
                ),
            )


def valid_trigger_downstreams(trigger_downstreams, config_context):
    if isinstance(trigger_downstreams, (type(None), bool, dict)):
        return trigger_downstreams
    raise ConfigError("must be None, bool or dict")


class ValidateAction(Validator):
    """Validate an action."""

    config_class = ConfigAction

    defaults = {
        "node": None,
        "requires": (),
        "retries": None,
        "retries_delay": None,
        "expected_runtime": datetime.timedelta(hours=24),
        "executor": schema.ExecutorTypes.ssh.value,  # type: ignore[attr-defined] # ExecutorTypes is an Enum
        "cpus": None,
        "mem": None,
        "disk": None,
        "cap_add": None,
        "cap_drop": None,
        "constraints": None,
        "docker_image": None,
        "docker_parameters": None,
        "env": None,
        "secret_env": None,
        "secret_volumes": None,
        "projected_sa_volumes": None,
        "field_selector_env": None,
        "extra_volumes": None,
        "trigger_downstreams": None,
        "triggered_by": None,
        "on_upstream_rerun": None,
        "trigger_timeout": None,
        "node_selectors": None,
        "node_affinities": None,
        "topology_spread_constraints": None,
        "labels": None,
        "annotations": None,
        "service_account_name": None,
        "ports": None,
    }
    requires = build_list_of_type_validator(
        valid_action_name,
        allow_empty=True,
    )
    validators = {
        "name": valid_action_name,
        "command": build_format_string_validator(action_context),
        "node": valid_node_name,
        "requires": requires,
        "retries": valid_int,
        "retries_delay": config_utils.valid_time_delta,
        "expected_runtime": config_utils.valid_time_delta,
        "executor": config_utils.build_real_enum_validator(schema.ExecutorTypes),
        "cpus": valid_float,
        "mem": valid_float,
        "disk": valid_float,
        "cap_add": valid_list,
        "cap_drop": valid_list,
        "constraints": build_list_of_type_validator(valid_constraint, allow_empty=True),
        "docker_image": valid_string,
        "docker_parameters": build_list_of_type_validator(
            valid_docker_parameter,
            allow_empty=True,
        ),
        "env": valid_dict,
        "secret_env": build_dict_value_validator(valid_secret_source),
        "secret_volumes": build_list_of_type_validator(valid_secret_volume, allow_empty=True),
        "projected_sa_volumes": build_list_of_type_validator(valid_projected_sa_volume, allow_empty=True),
        "field_selector_env": build_dict_value_validator(valid_field_selector_source),
        "extra_volumes": build_list_of_type_validator(valid_volume, allow_empty=True),
        "trigger_downstreams": valid_trigger_downstreams,
        "triggered_by": build_list_of_type_validator(valid_string, allow_empty=True),
        "on_upstream_rerun": config_utils.build_real_enum_validator(schema.ActionOnRerun),
        "trigger_timeout": config_utils.valid_time_delta,
        "node_selectors:": valid_dict,
        "node_affinities": build_list_of_type_validator(valid_node_affinity, allow_empty=True),
        "topology_spread_constraints": build_list_of_type_validator(
            valid_topology_spread_constraints, allow_empty=True
        ),
        "labels:": valid_dict,
        "annotations": valid_dict,
        "service_account_name": valid_string,
        "ports": build_list_of_type_validator(valid_int, allow_empty=True),
    }

    def post_validation(self, action, config_context):
        valid_mesos_action(action, config_context)
        valid_kubernetes_action(action, config_context)


valid_action = ValidateAction()


def valid_cleanup_action_name(value, config_context):
    if value != CLEANUP_ACTION_NAME:
        msg = "Cleanup actions cannot have custom names %s.%s"
        raise ConfigError(msg % (config_context.path, value))
    return CLEANUP_ACTION_NAME


class ValidateCleanupAction(Validator):
    config_class = ConfigCleanupAction
    defaults = {
        "node": None,
        "name": CLEANUP_ACTION_NAME,
        "retries": None,
        "retries_delay": None,
        "expected_runtime": datetime.timedelta(hours=24),
        "executor": schema.ExecutorTypes.ssh.value,  # type: ignore[attr-defined] # ExecutorTypes is an Enum
        "cpus": None,
        "mem": None,
        "disk": None,
        "cap_add": None,
        "cap_drop": None,
        "constraints": None,
        "docker_image": None,
        "docker_parameters": None,
        "env": None,
        "secret_env": None,
        "secret_volumes": None,
        "projected_sa_volumes": None,
        "field_selector_env": None,
        "extra_volumes": None,
        "trigger_downstreams": None,
        "triggered_by": None,
        "on_upstream_rerun": None,
        "trigger_timeout": None,
        "node_selectors": None,
        "node_affinities": None,
        "topology_spread_constraints": None,
        "labels": None,
        "annotations": None,
        "service_account_name": None,
        "ports": None,
    }
    validators = {
        "name": valid_cleanup_action_name,
        "command": build_format_string_validator(action_context),
        "node": valid_node_name,
        "retries": valid_int,
        "retries_delay": config_utils.valid_time_delta,
        "expected_runtime": config_utils.valid_time_delta,
        "executor": config_utils.build_real_enum_validator(schema.ExecutorTypes),
        "cpus": valid_float,
        "mem": valid_float,
        "disk": valid_float,
        "cap_add": valid_list,
        "cap_drop": valid_list,
        "constraints": build_list_of_type_validator(valid_constraint, allow_empty=True),
        "docker_image": valid_string,
        "docker_parameters": build_list_of_type_validator(
            valid_docker_parameter,
            allow_empty=True,
        ),
        "env": valid_dict,
        "secret_env": build_dict_value_validator(valid_secret_source),
        "secret_volumes": build_list_of_type_validator(valid_secret_volume, allow_empty=True),
        "projected_sa_volumes": build_list_of_type_validator(valid_projected_sa_volume, allow_empty=True),
        "field_selector_env": build_dict_value_validator(valid_field_selector_source),
        "extra_volumes": build_list_of_type_validator(valid_volume, allow_empty=True),
        "trigger_downstreams": valid_trigger_downstreams,
        "triggered_by": build_list_of_type_validator(valid_string, allow_empty=True),
        "on_upstream_rerun": config_utils.build_real_enum_validator(schema.ActionOnRerun),
        "trigger_timeout": config_utils.valid_time_delta,
        "node_selectors:": valid_dict,
        "node_affinities": build_list_of_type_validator(valid_node_affinity, allow_empty=True),
        "topology_spread_constraints": build_list_of_type_validator(
            valid_topology_spread_constraints, allow_empty=True
        ),
        "labels": valid_dict,
        "annotations": valid_dict,
        "service_account_name": valid_string,
        "ports": build_list_of_type_validator(valid_int, allow_empty=True),
    }

    def post_validation(self, action, config_context):
        valid_mesos_action(action, config_context)
        valid_kubernetes_action(action, config_context)


valid_cleanup_action = ValidateCleanupAction()


class ValidateJob(Validator):
    """Validate jobs."""

    config_class = ConfigJob
    defaults: Dict[str, Any] = {
        "run_limit": 50,
        "all_nodes": False,
        "cleanup_action": None,
        "enabled": True,
        "queueing": True,
        "allow_overlap": False,
        "max_runtime": None,
        "monitoring": {},
        "time_zone": None,
        "expected_runtime": datetime.timedelta(hours=24),
        "use_k8s": False,
    }

    validators = {
        "name": valid_name_identifier,
        "schedule": valid_schedule,
        "run_limit": valid_int,
        "all_nodes": valid_bool,
        "actions": build_dict_name_validator(valid_action),
        "cleanup_action": valid_cleanup_action,
        "node": valid_node_name,
        "queueing": valid_bool,
        "enabled": valid_bool,
        "allow_overlap": valid_bool,
        "max_runtime": config_utils.valid_time_delta,
        "monitoring": valid_dict,
        "time_zone": valid_time_zone,
        "expected_runtime": config_utils.valid_time_delta,
        "use_k8s": valid_bool,
    }

    def cast(self, in_dict, config_context):
        in_dict["namespace"] = config_context.namespace
        return in_dict

    # TODO: extract common code to a util function
    def _validate_dependencies(
        self,
        job: Dict[str, Any],  # TODO: create TypedDict for this
        # TODO: setup UniqueNameDict for use with mypy so that the following line
        # is not a lie
        actions: Dict[str, ConfigAction],
        base_action: ConfigAction,
        current_action: Optional[ConfigAction] = None,
        stack: Optional[List[str]] = None,
        already_validated: Optional[Set[Tuple[str, str]]] = None,
    ) -> None:
        """Check for circular or misspelled dependencies."""
        # for large graphs, we can end up validating the same jobs/actions repeatedly
        # this is unnecessary and we can skip a ton of work simply by caching what we've
        # already validated
        already_validated = already_validated or set()
        current_action = current_action or base_action
        validated = (job["name"], current_action.name)
        if validated in already_validated:
            return None
        else:
            already_validated.add(validated)

        stack = stack or []
        stack.append(current_action.name)
        for dep in current_action.requires:
            if dep == base_action.name and len(stack) > 0:
                msg = "Circular dependency in job.%s: %s"
                raise ConfigError(msg % (job["name"], " -> ".join(stack)))
            if dep not in actions:
                raise ConfigError(
                    'Action jobs.%s.%s has a dependency "%s"'
                    " that is not in the same job!" % (job["name"], current_action.name, dep),
                )
            self._validate_dependencies(job, actions, base_action, actions[dep], stack, already_validated)

        stack.pop()

    def post_validation(self, job, config_context):
        """Validate actions for the job."""
        for _, action in job["actions"].items():
            self._validate_dependencies(job, job["actions"], action)


valid_job = ValidateJob()


class ValidateActionRunner(Validator):
    config_class = schema.ConfigActionRunner
    optional = True
    defaults = {
        "runner_type": None,
        "remote_exec_path": "",
        "remote_status_path": "/tmp",
    }

    validators = {
        "runner_type": config_utils.build_real_enum_validator(schema.ActionRunnerTypes),
        "remote_status_path": valid_string,
        "remote_exec_path": valid_string,
    }


class ValidateStatePersistence(Validator):
    config_class = schema.ConfigState
    defaults = {
        "buffer_size": 1,
        "dynamodb_region": None,
        "table_name": None,
        "max_transact_write_items": 8,
    }

    validators = {
        "name": valid_string,
        "store_type": config_utils.build_real_enum_validator(schema.StatePersistenceTypes),
        "buffer_size": valid_int,
        "dynamodb_region": valid_string,
        "table_name": valid_string,
        "max_transact_write_items": valid_int,
    }

    def post_validation(self, config, config_context):
        buffer_size = config.get("buffer_size")

        if buffer_size and buffer_size < 1:
            path = config_context.path
            raise ConfigError("%s buffer_size must be >= 1." % path)

        store_type = config.get("store_type")

        if store_type == schema.StatePersistenceTypes.dynamodb.value:
            if not config.get("table_name"):
                raise ConfigError(f"{config_context.path} table_name is required when store_type is 'dynamodb'")
            if not config.get("dynamodb_region"):
                raise ConfigError(f"{config_context.path} dynamodb_region is required when store_type is 'dynamodb'")

            max_transact = config.get("max_transact_write_items")

            # Upper bound is based on boto3 transact_write_items limit
            if not 1 <= max_transact <= 100:
                raise ConfigError(
                    f"{config_context.path} max_transact_write_items must be between 1 and 100, got {max_transact}"
                )


valid_state_persistence = ValidateStatePersistence()


class ValidateMesos(Validator):
    config_class = ConfigMesos
    option = True
    defaults = {
        "master_address": None,
        "master_port": 5050,
        "secret_file": None,
        "role": "*",
        "principal": "tron",
        "enabled": False,
        "default_volumes": (),
        "dockercfg_location": None,
        "offer_timeout": 300,
    }

    validators = {
        "master_address": valid_master_address,
        "master_port": valid_int,
        "secret": valid_string,
        "role": valid_string,
        "enabled": valid_bool,
        "default_volumes": build_list_of_type_validator(valid_volume, allow_empty=True),
        "dockercfg_location": valid_string,
        "offer_timeout": valid_int,
    }


valid_mesos_options = ValidateMesos()


class ValidateKubernetes(Validator):
    config_class = ConfigKubernetes
    optional = True
    defaults = {
        "kubeconfig_path": None,
        "enabled": False,
        "non_retryable_exit_codes": (),
        "default_volumes": (),
    }

    validators = {
        "kubeconfig_path": valid_string,
        "enabled": valid_bool,
        "non_retryable_exit_codes": build_list_of_type_validator(valid_exit_code, allow_empty=True),
        "default_volumes": build_list_of_type_validator(valid_volume, allow_empty=True),
        "watcher_kubeconfig_paths": build_list_of_type_validator(valid_string, allow_empty=True),
    }


valid_kubernetes_options = ValidateKubernetes()


def validate_jobs(config, config_context):
    """Validate jobs"""
    valid_jobs = build_dict_name_validator(valid_job, allow_empty=True)
    validation = [("jobs", valid_jobs)]

    for config_name, valid in validation:
        child_context = config_context.build_child_context(config_name)
        config[config_name] = valid(config.get(config_name, []), child_context)

    fmt_string = "Job names must be unique %s"
    config_utils.unique_names(fmt_string, config["jobs"])


DEFAULT_STATE_PERSISTENCE = ConfigState(
    name="tron_state",
    store_type="shelve",
    buffer_size=1,
)
DEFAULT_NODE = ValidateNode().do_shortcut(node="localhost")


class ValidateConfig(Validator):
    """Given a parsed config file (should be only basic literals and
    containers), return an immutable, fully populated series of namedtuples and
    dicts with all defaults filled in, all valid values, and no unused
    values. Throws a ConfigError if any part of the input dict is invalid.
    """

    config_class = TronConfig
    defaults = {
        "action_runner": {},
        "output_stream_dir": None,
        "command_context": {},
        "ssh_options": ConfigSSHOptions(**ValidateSSHOptions.defaults),
        "time_zone": None,
        "state_persistence": DEFAULT_STATE_PERSISTENCE,
        "nodes": {
            "localhost": DEFAULT_NODE,
        },
        "node_pools": {},
        "jobs": (),
        "mesos_options": ConfigMesos(**ValidateMesos.defaults),
        "k8s_options": ConfigKubernetes(**ValidateKubernetes.defaults),
        "eventbus_enabled": None,
        "read_json": False,
    }
    node_pools = build_dict_name_validator(valid_node_pool, allow_empty=True)
    nodes = build_dict_name_validator(valid_node, allow_empty=True)
    validators = {
        "action_runner": ValidateActionRunner(),
        "output_stream_dir": valid_output_stream_dir,
        "command_context": valid_command_context,
        "ssh_options": valid_ssh_options,
        "time_zone": valid_time_zone,
        "state_persistence": valid_state_persistence,
        "nodes": nodes,
        "node_pools": node_pools,
        "mesos_options": valid_mesos_options,
        "k8s_options": valid_kubernetes_options,
        "eventbus_enabled": valid_bool,
        "read_json": valid_bool,
    }
    optional = False

    def validate_node_pool_nodes(self, config):
        """Validate that each node in a node_pool is in fact a node, and not
        another pool.
        """
        all_node_names = set(config["nodes"])
        for node_pool in config["node_pools"].values():
            invalid_names = set(node_pool.nodes) - all_node_names
            if invalid_names:
                msg = "NodePool %s contains other NodePools: " % node_pool.name
                raise ConfigError(msg + ",".join(invalid_names))

    def post_validation(self, config, _):
        """Validate a non-named config."""
        node_names = config_utils.unique_names(
            "Node and NodePool names must be unique %s",
            config["nodes"],
            config.get("node_pools", []),
        )

        if config.get("node_pools"):
            self.validate_node_pool_nodes(config)

        config_context = ConfigContext(
            "config",
            node_names,
            config.get("command_context"),
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
        "jobs": (),
    }

    optional = False

    def post_validation(self, config, config_context):
        validate_jobs(config, config_context)


valid_config = ValidateConfig()
valid_named_config = ValidateNamedConfig()


def validate_fragment(name, fragment, master_config=None):
    """Validate a fragment with a partial context."""
    config_context = PartialConfigContext(name, name)
    if name == MASTER_NAMESPACE:
        return valid_config(fragment, config_context=config_context)
    if master_config is None:
        return valid_named_config(fragment, config_context=config_context)

    config_mapping = {MASTER_NAMESPACE: master_config, name: fragment}
    for config_name, config in validate_config_mapping(config_mapping):
        if config_name == name:
            return config


def get_nodes_from_master_namespace(master):
    return set(itertools.chain(master.nodes, master.node_pools))


def validate_config_mapping(config_mapping):
    if MASTER_NAMESPACE not in config_mapping:
        msg = "A config mapping requires a %s namespace"
        raise ConfigError(msg % MASTER_NAMESPACE)

    # we mutate this mapping - so let's make sure that we're making a copy
    # in case the passed-in mapping is used elsewhere
    config_mapping_to_validate = deepcopy(config_mapping)
    master = valid_config(config_mapping_to_validate.pop(MASTER_NAMESPACE))
    nodes = get_nodes_from_master_namespace(master)
    yield MASTER_NAMESPACE, master

    for name, content in config_mapping_to_validate.items():
        context = ConfigContext(
            name,
            nodes,
            master.command_context,
            name,
        )
        yield name, valid_named_config(content, config_context=context)


class ConfigContainer:
    """A container around configuration fragments (and master)."""

    def __init__(self, config_mapping):
        self.configs = config_mapping

    def items(self):
        return self.configs.items()

    @classmethod
    def create(cls, config_mapping):
        return cls(dict(validate_config_mapping(config_mapping)))

    # TODO: DRY with get_jobs()
    def get_job_names(self):
        job_names = []
        for config in self.configs.values():
            job_names.extend(config.jobs)
        return job_names

    def get_jobs(self):
        return dict(
            itertools.chain.from_iterable(config.jobs.items() for _, config in self.configs.items()),
        )

    def get_master(self):
        return self.configs[MASTER_NAMESPACE]

    def get_node_names(self):
        return get_nodes_from_master_namespace(self.get_master())

    def __getitem__(self, name):
        return self.configs[name]

    def __contains__(self, name):
        return name in self.configs
