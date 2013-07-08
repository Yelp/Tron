import simplejson as json
import cPickle as pickle

try:
    import msgpack
    no_msgpack = False
except ImportError:
    no_msgpack = True

try:
    import yaml
    no_yaml = False
except ImportError:
    no_yaml = True


class TransportModuleError(Exception):
    """Raised if a transport module is used without it being installed."""
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return repr(self.code)


class JSONTransport(object):
    @classmethod
    def serialize(cls, data):
        return json.dumps(data)

    @classmethod
    def deserialize(cls, data_str):
        return json.loads(data_str)


class cPickleTransport(object):
    @classmethod
    def serialize(cls, data):
        return pickle.dumps(data)

    @classmethod
    def deserialize(cls, data_str):
        return pickle.loads(data_str)


class MsgPackTransport(object):
    @classmethod
    def serialize(cls, data):
        if no_msgpack:
            raise TransportModuleError('MessagePack not installed.')
        return msgpack.packb(data, use_list=False)

    @classmethod
    def deserialize(cls, data_str):
        if no_msgpack:
            raise TransportModuleError('MessagePack not installed.')
        return msgpack.unpackb(data_str, use_list=False)


class YamlTransport(object):
    @classmethod
    def serialize(cls, data):
        if no_yaml:
            raise TransportModuleError('PyYaml not installed.')
        return yaml.dump(data)

    @classmethod
    def deserialize(cls, data_str):
        if no_yaml:
            raise TransportModuleError('PyYaml not installed.')
        return yaml.load(data_str)
