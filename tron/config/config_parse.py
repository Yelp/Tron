import itertools
import logging

import six

from tron.config.schema import MASTER_NAMESPACE
from tron.config.tron_config import NamedTronConfig
from tron.config.tron_config import TronConfig

log = logging.getLogger(__name__)


def validate_fragment(name, fragment):
    """Validate a fragment with a partial context."""
    if name == MASTER_NAMESPACE:
        return TronConfig.from_config(fragment)
    return NamedTronConfig.from_config(dict(namespace=name, **fragment))


def get_nodes_from_master_namespace(master):
    return set(itertools.chain(master.nodes, master.node_pools))


def validate_config_mapping(config_mapping):
    if MASTER_NAMESPACE not in config_mapping:
        msg = "A config mapping requires a %s namespace"
        raise ValueError(msg % MASTER_NAMESPACE)

    master = TronConfig.from_config(config_mapping.pop(MASTER_NAMESPACE))
    nodes = get_nodes_from_master_namespace(master)
    yield MASTER_NAMESPACE, master

    for name, content in six.iteritems(config_mapping):
        yield name, NamedTronConfig.from_config(
            dict(
                namespace=name,
                nodes=nodes,
                command_context=master.command_context,
                **content
            )
        )


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
