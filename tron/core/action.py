import datetime
import logging
from enum import Enum

from pyrsistent import CheckedPMap
from pyrsistent import CheckedPVector
from pyrsistent import field
from pyrsistent import PRecord
from pyrsistent import PSet
from pyrsistent import pset
from pyrsistent import s

from tron import node
from tron.config import ConfigRecord
from tron.config.config_utils import IDENTIFIER_RE
from tron.config.config_utils import TIME_INTERVAL_RE
from tron.config.config_utils import TIME_INTERVAL_UNITS
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.utils import maybe_decode

log = logging.getLogger(__name__)


def factory_time_delta(value):
    if isinstance(value, datetime.timedelta):
        return value

    error_msg = "Value is not a valid time delta: %s"
    matches = TIME_INTERVAL_RE.match(value)
    if not matches:
        raise RuntimeError(error_msg % value)

    units = matches.group('units')
    if units not in TIME_INTERVAL_UNITS:
        raise RuntimeError(error_msg % value)

    time_spec = {TIME_INTERVAL_UNITS[units]: int(matches.group('value'))}
    return datetime.timedelta(**time_spec)


class Constraint(PRecord):
    attribute = field(mandatory=True)
    operator = field(mandatory=True)
    value = field(mandatory=True)


class Constraints(CheckedPVector):
    __type__ = Constraint


class VolumeModes(Enum):
    RO = 'RO'
    RW = 'RW'


class Volume(PRecord):
    container_path = field(mandatory=True)
    host_path = field(mandatory=True)
    mode = field(mandatory=True, type=VolumeModes, factory=VolumeModes)


class Volumes(CheckedPVector):
    __type__ = Volume


class DockerParam(PRecord):
    key = field(mandatory=True)
    value = field(mandatory=True)


class DockerParams(CheckedPVector):
    __type__ = DockerParam


class ExecutorTypes(Enum):
    ssh = 'ssh'
    mesos = 'mesos'


class Action(ConfigRecord):
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
        type=ExecutorTypes,
        initial=ExecutorTypes.ssh,
        factory=ExecutorTypes,
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

    constraints = field(type=Constraints, initial=Constraints())
    docker_image = field(initial=None)
    docker_parameters = field(type=DockerParams, initial=DockerParams())
    env = field(initial={}, type=dict)
    extra_volumes = field(type=Volumes, initial=Volumes())
    mesos_address = field(initial=None)

    required_actions = field(type=PSet, initial=s(), factory=pset)
    dependent_actions = field(type=PSet, initial=s(), factory=pset)

    @property
    def is_cleanup(self):
        return self.name == CLEANUP_ACTION_NAME

    @classmethod
    def from_config(cls, config):
        """Factory method for creating a new Action."""
        if config is None or isinstance(config, Action):
            return config

        config = dict(**config)

        if 'node_pool' not in config:
            node_name = config.get('node')
            node_repo = node.NodePoolRepository.get_instance()
            config['node_pool'] = node_repo.get_by_name(node_name)
            if 'node' in config:
                del config['node']

        if config['name'] == CLEANUP_ACTION_NAME:
            requires = config.get('requires')
            if requires is not None and len(requires) > 0:
                raise ValueError(
                    "Cleanup action cannot have dependencies, has {}".
                    format(requires)
                )

        if config.get('executor') == ExecutorTypes.mesos:
            required_keys = {'cpus', 'mem', 'docker_image', 'mesos_address'}
            missing_keys = required_keys - set(config.keys())
            if missing_keys:
                name = config['name']
                raise ValueError(
                    f"Mesos executor for action {name} is missing "
                    f"these required keys: {missing_keys}"
                )

        return cls.create(config)


class ActionMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = Action

    @classmethod
    def from_config(cls, items):
        """Factory method for creating a new ActionMap."""
        if items is None or isinstance(items, ActionMap):
            return items

        all_names = [item['name'] for item in items]
        uniq_names = set(all_names)

        if len(uniq_names) < len(all_names):
            raise ValueError(
                "Duplicate action names found: {}".format([
                    name for name in uniq_names if all_names.count(name) > 1
                ])
            )

        return cls.create({
            item['name']: Action.from_config(item)
            for item in items
        })
