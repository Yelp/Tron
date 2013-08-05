"""Message serialization modules for tronstore. This allows for simple writing
of stdin/out with strings that can then be put back into tuples of data
for rebuilding messages.

This is also used by the SQLAlchemy store object, an option for saving state
with tronstore, by serializing the state data into a string that's saved in
a SQL database, or by deserializing strings that are saved into state data."""
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


class SerializerModuleError(Exception):
    """Raised if a serialization module is used without it being installed."""
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return repr(self.code)


class JSONSerializer(object):
    @classmethod
    def serialize(cls, data):
        return json.dumps(data)

    @classmethod
    def deserialize(cls, data_str):
        return json.loads(data_str)


class cPickleSerializer(object):
    @classmethod
    def serialize(cls, data):
        return pickle.dumps(data)

    @classmethod
    def deserialize(cls, data_str):
        return pickle.loads(data_str)


class MsgPackSerializer(object):
    @classmethod
    def serialize(cls, data):
        if no_msgpack:
            raise SerializerModuleError('MessagePack not installed.')
        return msgpack.packb(data)

    @classmethod
    def deserialize(cls, data_str):
        if no_msgpack:
            raise SerializerModuleError('MessagePack not installed.')
        return msgpack.unpackb(data_str)


class YamlSerializer(object):
    @classmethod
    def serialize(cls, data):
        if no_yaml:
            raise SerializerModuleError('PyYaml not installed.')
        return yaml.dump(data)

    @classmethod
    def deserialize(cls, data_str):
        if no_yaml:
            raise SerializerModuleError('PyYaml not installed.')
        return yaml.load(data_str)


serialize_class_map = {
    'json': JSONSerializer,
    'pickle': cPickleSerializer,
    'msgpack': MsgPackSerializer,
    'yaml': YamlSerializer
}
