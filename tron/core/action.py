from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import logging

from pyrsistent import CheckedPMap
from pyrsistent import field
from pyrsistent import PClass
from pyrsistent import PSet
from pyrsistent import pset
from pyrsistent import s

from tron import node
from tron.config import schema
from tron.config.config_utils import IDENTIFIER_RE
from tron.config.config_utils import TIME_INTERVAL_RE
from tron.config.config_utils import TIME_INTERVAL_UNITS
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.utils import maybe_decode

log = logging.getLogger(__name__)


def factory_time_delta(value):
    if isinstance(value, datetime.timedelta):
        return value

    error_msg = "Value at %s is not a valid time delta: %s"
    matches = TIME_INTERVAL_RE.match(value)
    if not matches:
        raise RuntimeError(error_msg % (config_context.path, value))

    units = matches.group('units')
    if units not in TIME_INTERVAL_UNITS:
        raise RuntimeError(error_msg % (config_context.path, value))

    time_spec = {TIME_INTERVAL_UNITS[units]: int(matches.group('value'))}
    return datetime.timedelta(**time_spec)


class Action(PClass):
    """A configurable data object for an Action."""

    name = field(
        type=str,
        factory=maybe_decode,
        mandatory=True,
        invariant=lambda x: (
            bool(IDENTIFIER_RE.match(x)),
            'Invalid action name: %s' % x,
        ),
    )
    command = field(type=str, mandatory=True)
    node_pool = field(type=(str, type(None)), mandatory=True)
    requires = field(type=PSet, initial=s(), factory=pset)
    retries = field(type=(int, type(None)), initial=None)
    executor = field(
        type=(str, type(None)),
        invariant=lambda x: (x in schema.ExecutorTypes, 'Invalid executor'),
        initial=schema.ExecutorTypes.ssh,
    )
    cluster = field(type=(str, type(None)), initial=None)
    pool = field(type=(str, type(None)), initial=None)
    cpus = field(type=(float, type(None)), initial=None, factory=float)
    mem = field(type=(float, type(None)), initial=None, factory=float)
    service = field(type=(str, type(None)), initial=None)
    deploy_group = field(type=(str, type(None)), initial=None)
    expected_runtime = field(
        type=(datetime.timedelta, type(None)),
        initial=datetime.timedelta(hours=24),
        factory=factory_time_delta,
    )

    constraints = field(initial=[])
    docker_image = field()
    docker_parameters = field(initial=[])
    env = field(initial={})
    extra_volumes = field(initial=[])
    mesos_address = field()

    required_actions = field(type=PSet, initial=s(), factory=pset)
    dependent_actions = field(type=PSet, initial=s(), factory=pset)

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @classmethod
    def from_config(cls, config, config_context, **kwargs):
        """Factory method for creating a new Action."""
        if config is None:
            config = {}

        config.update(kwargs)

        if 'node_pool' not in config:
            node_name = config.get('node')
            node_repo = node.NodePoolRepository.get_instance()
            config['node_pool'] = node_repo.get_by_name(node_name)
            if 'node' in config:
                del config['node']

        cluster = config.get('cluster')
        if cluster is not None and \
                not config_context.partial and \
                cluster not in config_context.clusters:
            raise ValueError(
                "Unknown cluster name {} at {}".format(
                    cluster,
                    config_context.path,
                ),
            )

        if config_context.path[-15:] == '.cleanup_action':
            if 'name' in config and config['name'] != CLEANUP_ACTION_NAME:
                raise ValueError(
                    "Cleanup actions cannot have custom names at {}".format(
                        config_context.path,
                    ),
                )
            config['name'] = CLEANUP_ACTION_NAME

        if config['name'] == CLEANUP_ACTION_NAME:
            if config_context.path[-8:] == '.actions':
                raise ValueError(
                    "Action name reserved for cleanup action at {}.{}".format(
                        config_context.path,
                        config['name'],
                    ),
                )

            requires = config.get('requires')
            if requires is not None and len(requires) > 0:
                raise ValueError(
                    "Cleanup action cannot have dependencies, "
                    "has {} at {}".format(
                        requires,
                        config_context.path,
                    ),
                )

        return cls.create(config)


class ActionMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = Action

    @classmethod
    def from_config(cls, items, config_context):
        """Factory method for creating a new ActionMap."""
        if items is None:
            items = []

        all_names = [item['name'] for item in items]
        uniq_names = set(all_names)

        if len(uniq_names) < len(all_names):
            raise ValueError(
                "Duplicate action names found: {} at {}.actions".format(
                    [name for name in uniq_names if all_names.count(name) > 1],
                    config_context.path,
                ),
            )

        return cls.create(
            {
                item['name']: Action.from_config(
                    item,
                    config_context=config_context,
                )
                for item in items
            }
        )
