"""
 Immutable config schema objects.
 WARNING: it is *NOT* safe to delete these classes (or their attributes) if there are any references to them in DynamoDB! (See DAR-2328)
 NOTE: this means that reverting a change that adds a new attribute is not safe :)
"""
from collections import namedtuple
from enum import Enum

MASTER_NAMESPACE = "MASTER"

CLEANUP_ACTION_NAME = "cleanup"


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
    config_class.__new__.__defaults__ = (None,) * len(optional)
    config_class.required_keys = required
    config_class.optional_keys = optional

    return config_class


TronConfig = config_object_factory(
    name="TronConfig",
    optional=[
        "output_stream_dir",  # str
        "action_runner",  # ConfigActionRunner
        "state_persistence",  # ConfigState
        "command_context",  # dict of str
        "ssh_options",  # ConfigSSHOptions
        "time_zone",  # pytz time zone
        "nodes",  # dict of ConfigNode
        "node_pools",  # dict of ConfigNodePool
        "jobs",  # dict of ConfigJob
        "mesos_options",  # ConfigMesos
        "k8s_options",  # ConfigKubernetes
        "eventbus_enabled",  # bool or None
    ],
)

NamedTronConfig = config_object_factory(
    name="NamedTronConfig",
    optional=[
        "jobs",
    ],
)  # dict of ConfigJob

ConfigActionRunner = config_object_factory(
    "ConfigActionRunner",
    optional=["runner_type", "remote_status_path", "remote_exec_path"],
)

ConfigSSHOptions = config_object_factory(
    name="ConfigSSHOptions",
    optional=[
        "agent",
        "identities",
        "known_hosts_file",
        "connect_timeout",
        "idle_connection_timeout",
        "jitter_min_load",
        "jitter_max_delay",
        "jitter_load_factor",
    ],
)

ConfigNode = config_object_factory(
    name="ConfigNode",
    required=["hostname"],
    optional=["name", "username", "port"],
)

ConfigNodePool = config_object_factory("ConfigNodePool", ["nodes"], ["name"])

ConfigState = config_object_factory(
    name="ConfigState",
    required=[
        "name",
        "store_type",
    ],
    optional=[
        "buffer_size",
        "dynamodb_region",
        "table_name",
    ],
)

ConfigMesos = config_object_factory(
    name="ConfigMesos",
    optional=[
        "master_address",
        "master_port",
        "secret_file",
        "principal",
        "role",
        "enabled",
        "default_volumes",
        "dockercfg_location",
        "offer_timeout",
    ],
)

ConfigKubernetes = config_object_factory(
    name="ConfigKubernetes",
    optional=[
        "kubeconfig_path",
        "enabled",
        "default_volumes",
    ],
)

ConfigJob = config_object_factory(
    name="ConfigJob",
    required=[
        "name",  # str
        "node",  # str
        "schedule",  # Config*Scheduler
        "actions",  # dict of ConfigAction
        "namespace",  # str
    ],
    optional=[
        "monitoring",  # dict
        "queueing",  # bool
        "run_limit",  # int
        "all_nodes",  # bool
        "cleanup_action",  # ConfigAction
        "enabled",  # bool
        "allow_overlap",  # bool
        "max_runtime",  # datetime.Timedelta
        "time_zone",  # pytz time zone
        "expected_runtime",  # datetime.Timedelta
        # TODO: cleanup once we're fully off of Mesos and all non-SSH jobs *only* use k8s
        "use_k8s",  # bool
    ],
)

ConfigAction = config_object_factory(
    name="ConfigAction",
    required=[
        "name",
        "command",
    ],  # str  # str
    optional=[
        "requires",  # tuple of str
        "node",  # str
        "retries",  # int
        "retries_delay",  # datetime.Timedelta
        "executor",  # str
        "cpus",  # float
        "mem",  # float
        "disk",  # float
        "cap_add",  # List of str
        "cap_drop",  # List of str
        "constraints",  # List of ConfigConstraint
        "docker_image",  # str
        "docker_parameters",  # List of ConfigParameter
        "env",  # dict
        "secret_env",  # dict of str, ConfigSecretSource
        "secret_volumes",  # List of ConfigSecretVolume
        "projected_sa_volumes",  # List of ConfigProjectedSAVolume
        "field_selector_env",  # dict of str, ConfigFieldSelectorSource
        "extra_volumes",  # List of ConfigVolume
        "expected_runtime",  # datetime.Timedelta
        "trigger_downstreams",  # None, bool or dict
        "triggered_by",  # list or None
        "on_upstream_rerun",  # ActionOnRerun or None
        "trigger_timeout",  # datetime.deltatime or None
        "node_selectors",  # Dict of str, str
        "node_affinities",  # List of ConfigNodeAffinity
        "labels",  # Dict of str, str
        "annotations",  # Dict of str, str
        "service_account_name",  # str
        "ports",  # List of int
    ],
)

ConfigCleanupAction = config_object_factory(
    name="ConfigCleanupAction",
    required=[
        "command",
    ],  # str
    optional=[
        "name",  # str
        "node",  # str
        "retries",  # int
        "retries_delay",  # datetime.Timedelta
        "expected_runtime",  # datetime.Timedelta
        "executor",  # str
        "cpus",  # float
        "mem",  # float
        "disk",  # float
        "cap_add",  # List of str
        "cap_drop",  # List of str
        "constraints",  # List of ConfigConstraint
        "docker_image",  # str
        "docker_parameters",  # List of ConfigParameter
        "env",  # dict
        "secret_env",  # dict of str, ConfigSecretSource
        "secret_volumes",  # List of ConfigSecretVolume
        "projected_sa_volumes",  # List of ConfigProjectedSAVolume
        "field_selector_env",  # dict of str, ConfigFieldSelectorSource
        "extra_volumes",  # List of ConfigVolume
        "trigger_downstreams",  # None, bool or dict
        "triggered_by",  # list or None
        "on_upstream_rerun",  # ActionOnRerun or None
        "trigger_timeout",  # datetime.deltatime or None
        "node_selectors",  # Dict of str, str
        "node_affinities",  # List of ConfigNodeAffinity
        "labels",  # Dict of str, str
        "annotations",  # Dict of str, str
        "service_account_name",  # str
        "ports",  # List of int
    ],
)

ConfigConstraint = config_object_factory(
    name="ConfigConstraint",
    required=[
        "attribute",
        "operator",
        "value",
    ],
    optional=[],
)

ConfigVolume = config_object_factory(
    name="ConfigVolume",
    required=[
        "container_path",
        "host_path",
    ],
    optional=["mode"],
)


ConfigSecretVolumeItem = config_object_factory(
    name="ConfigSecretVolumeItem",
    required=[
        "key",
        "path",
    ],
    optional=["mode"],
)

_ConfigSecretVolume = config_object_factory(
    name="ConfigSecretVolume",
    required=["secret_volume_name", "secret_name", "container_path"],
    optional=["default_mode", "items"],
)


class ConfigSecretVolume(_ConfigSecretVolume):  # type: ignore
    def _asdict(self) -> dict:
        d = super()._asdict().copy()
        items = d.get("items", [])
        if items is not None and items:
            # the config parsing code appears to be turning arrays into tuples - however, updating the
            # code we think is at fault breaks a non-trivial amount of tests. in the interest of time, we're
            # just casting to a list here, but we should eventually circle back here
            # and either ensure that we always get a list from the config parse code OR document that we're
            # expecting Tron's config parsing code to return immutable data if this is behavior we want to depend on.
            d["items"] = list(d["items"])
            for i, item in enumerate(items):
                if isinstance(item, ConfigSecretVolumeItem):
                    d["items"][i] = item._asdict()
        return d  # type: ignore


ConfigSecretSource = config_object_factory(
    name="ConfigSecretSource",
    required=["secret_name", "key"],
    optional=[],
)

ConfigProjectedSAVolume = config_object_factory(
    name="ConfigProjectedSAVolume",
    required=["container_path", "audience"],
    optional=["expiration_seconds"],
)

ConfigFieldSelectorSource = config_object_factory(
    name="ConfigFieldSelectorSource",
    required=["field_path"],
    optional=[],
)

ConfigNodeAffinity = config_object_factory(
    name="ConfigNodeAffinity",
    required=["key", "operator", "value"],
    optional=[],
)

ConfigParameter = config_object_factory(
    name="ConfigParameter",
    required=[
        "key",
        "value",
    ],
    optional=[],
)

StatePersistenceTypes = Enum(  # type: ignore
    "StatePersistenceTypes",
    dict(shelve="shelve", yaml="yaml", dynamodb="dynamodb"),
)

ExecutorTypes = Enum("ExecutorTypes", dict(ssh="ssh", mesos="mesos", kubernetes="kubernetes", spark="spark"))  # type: ignore

ActionRunnerTypes = Enum("ActionRunnerTypes", dict(none="none", subprocess="subprocess"))  # type: ignore

VolumeModes = Enum("VolumeModes", dict(RO="RO", RW="RW"))  # type: ignore

ActionOnRerun = Enum("ActionOnRerun", dict(rerun="rerun"))  # type: ignore

# WARNING: it is *NOT* safe to delete these classes (or their attributes) if there are any references to them in DynamoDB! (See DAR-2328)
# NOTE: this means that reverting a change that adds a new attribute is not safe :)
