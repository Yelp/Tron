import getpass

from pyrsistent import CheckedPMap
from pyrsistent import field

from tron.config import ConfigRecord
from tron.config.config_utils import IDENTIFIER_RE


class Node(ConfigRecord):
    name = field(
        type=str,
        invariant=lambda n: (
            bool(IDENTIFIER_RE.match(n)),
            "name {} is not a valid identifier".format(n)
        )
    )
    hostname = field(type=str, mandatory=True)
    username = field(type=(str, type(None)), initial=None)
    port = field(type=int, initial=22)

    @classmethod
    def from_config(kls, val, *_):
        if type(val) is str:
            val = dict(name=val, hostname=val)

        # Allow only hostname or only name to be specified
        val.setdefault('name', val.get('hostname'))
        val.setdefault('hostname', val.get('name'))

        if val.get('username') is None:
            val['username'] = getpass.getuser()

        return kls.create(val)


class NodeMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = Node

    @classmethod
    def from_config(kls, val):
        if val is None:
            return NodeMap()

        if isinstance(val, list):
            nval = {}
            for v in val:
                if isinstance(v, str):
                    node = Node.from_config(dict(name=v, hostname=v))
                elif isinstance(v, dict):
                    node = Node.from_config(v)
                else:
                    raise ValueError(
                        "Can't make tron.config.Node out of {}".format(
                            type(v)
                        )
                    )
                nval[node.name] = node
            val = nval

        return kls.create(val)


class NodePool(ConfigRecord):
    name = field(
        type=str,
        invariant=lambda n: (
            bool(IDENTIFIER_RE.match(n)),
            "name {} is not a valid identifier".format(n)
        )
    )
    nodes = field(type=NodeMap, initial=NodeMap())

    @classmethod
    def from_config(kls, val):
        if isinstance(val, list):
            val = dict(
                name='_'.join(val),
                nodes=val,
            )

        if isinstance(val.get('nodes'), list):
            val['nodes'] = NodeMap.from_config(val['nodes'])

        return kls.create(val)


class NodePoolMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = NodePool

    @classmethod
    def from_config(kls, val):
        if isinstance(val, list):
            val = {v['name']: v for v in val}

        for k in val:
            val[k] = NodePool.from_config(val[k])

        return kls.create(val)
