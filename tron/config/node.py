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
            f"name {n} is not a valid identifier"
        )
    )
    hostname = field(type=str, mandatory=True)
    username = field(type=(str, type(None)), initial=None)
    port = field(type=int, initial=22)

    @classmethod
    def from_config(kls, val, _):
        if type(val) is str:
            val = dict(name=val, hostname=val)

        if val.get('username') is None:
            val['username'] = getpass.getuser()

        return kls.create(val)


class NodeMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = Node

    @classmethod
    def from_config(kls, val, ctx):
        if val is None:
            return NodeMap()

        if isinstance(val, list):
            nval = {}
            for v in val:
                if isinstance(v, str):
                    nval[v] = Node.from_config(dict(name=v, hostname=v), ctx)
                else:
                    nval[v['name']] = Node.from_config(v, ctx)
            val = nval

        return kls.create(val)


class NodePool(ConfigRecord):
    name = field(
        type=str,
        invariant=lambda n: (
            bool(IDENTIFIER_RE.match(n)),
            f"name {n} is not a valid identifier"
        )
    )
    nodes = field(type=NodeMap, initial=NodeMap())

    @classmethod
    def from_config(kls, val, ctx):
        if isinstance(val, list):
            val = dict(
                name='_'.join(val),
                nodes=val,
            )

        if isinstance(val.get('nodes'), list):
            val['nodes'] = NodeMap.from_config(val['nodes'], ctx)

        return kls.create(val)


class NodePoolMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = NodePool

    @classmethod
    def from_config(kls, val, ctx):
        if isinstance(val, list):
            val = {v['name']: v for v in val}

        for k in val:
            val[k] = NodePool.from_config(val[k], ctx)

        return kls.create(val)
