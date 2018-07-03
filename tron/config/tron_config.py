# pylint: disable=E1101
import datetime
import os

from pyrsistent import field
from pyrsistent import freeze
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import PSet
from pyrsistent import PVector
from pyrsistent import v

from tron.config import ConfigRecord
from tron.config.action_runner import ActionRunner
from tron.config.config_utils import valid_time_zone
from tron.config.job import JobMap
from tron.config.mesos_options import MesosOptions
from tron.config.node import NodeMap
from tron.config.node import NodePoolMap
from tron.config.notification_options import NotificationOptions
from tron.config.schema import MASTER_NAMESPACE
from tron.config.ssh_options import SSHOptions
from tron.config.state_persistence import StatePersistence


class TronConfig(ConfigRecord):
    output_stream_dir = field(
        type=(str, type(None)),
        initial=None,
        invariant=lambda osd: (
            not osd or os.path.isdir(osd) and os.access(osd, os.W_OK),
            "`output_stream_dir` is writable directory: '{}'".format(osd)
        )
    )
    action_runner = field(
        type=ActionRunner,
        initial=ActionRunner(),
        factory=ActionRunner.from_config
    )
    state_persistence = field(
        type=(StatePersistence, type(None)),
        initial=StatePersistence(name='tron_state'),
        factory=StatePersistence.from_config
    )
    command_context = field(type=PMap, initial=m(), factory=freeze)
    ssh_options = field(
        type=SSHOptions, initial=SSHOptions, factory=SSHOptions.from_config
    )
    notification_options = field(
        type=(NotificationOptions, type(None)),
        initial=None,
        factory=NotificationOptions.from_config
    )
    time_zone = field(
        type=(datetime.tzinfo, type(None)),
        initial=None,
        factory=valid_time_zone
    )
    nodes = field(
        type=NodeMap,
        initial=NodeMap.from_config(['localhost']),
        factory=NodeMap.from_config
    )
    node_pools = field(
        type=NodePoolMap,
        initial=NodePoolMap(),
        factory=NodePoolMap.from_config
    )
    jobs = field(type=JobMap, initial=JobMap(), factory=JobMap.from_config)
    mesos_options = field(
        type=MesosOptions,
        initial=MesosOptions(),
        factory=MesosOptions.from_config
    )

    def __invariant__(self):
        """Validate a non-named config."""
        non_unique = set(self.nodes.keys()).intersection(
            self.node_pools.keys()
        )
        if non_unique:
            return (
                False,
                "names present in nodes and node pools: {}".format(non_unique)
            )

        all_node_names = set(self.nodes.keys())
        for _, node_pool in self.node_pools.items():
            invalid_names = set(node_pool.nodes.keys()) - all_node_names
            if invalid_names:
                return (
                    False, "NodePool {} contains other NodePools: {}".format(
                        node_pool.name, ",".join(invalid_names)
                    )
                )

        nodes_and_pools = set(self.nodes.keys() + self.node_pools.keys())
        for _, job in self.jobs.items():
            if job.node not in nodes_and_pools:
                return (False, "Unknown node name {}".format(job.node))

        return (True, "all ok")

    @classmethod
    def from_config(kls, config):
        """ Given a parsed config file (should be only basic literals and
        containers), return an immutable, fully populated series of namedtuples and
        FrozenDicts with all defaults filled in, all valid values, and no unused
        values. Throws a ValueError if any part of the input dict is invalid.
        """
        try:
            jobs = {}
            for job in config.get('jobs') or []:
                name = job['name']
                job['name'] = "{}.{}".format(MASTER_NAMESPACE, name)
                job['namespace'] = MASTER_NAMESPACE
                jobs[name] = job
            config['jobs'] = jobs
            return kls.create(config)
        except Exception as e:
            raise ValueError(f"Config MASTER {e}").with_traceback(
                e.__traceback__
            )


class NamedTronConfig(ConfigRecord):
    namespace = field(type=str, mandatory=True)
    nodes = field(type=(PVector, PSet), initial=v(), factory=freeze)
    command_context = field(type=PMap, initial=m(), factory=freeze)
    jobs = field(type=JobMap, initial=JobMap(), factory=JobMap.from_config)

    @classmethod
    def from_config(kls, config):
        """ Given a parsed config file (should be only basic literals and
        containers), return an immutable, fully populated series of namedtuples and
        FrozenDicts with all defaults filled in, all valid values, and no unused
        values. Throws a ValueError if any part of the input dict is invalid.
        """
        if 'namespace' not in config:
            raise ValueError("Namespace config without namespace name")

        namespace = config['namespace']

        try:
            for job in config.get('jobs') or []:
                job['namespace'] = config.get('namespace')

            return kls.create(config)
        except Exception as e:
            raise ValueError(f"Namespace {namespace} {e}").with_traceback(
                e.__traceback__
            )
