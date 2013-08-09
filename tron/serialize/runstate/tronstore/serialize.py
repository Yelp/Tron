"""Message serialization modules for tronstore. This allows for simple writing
of stdin/out with strings that can then be put back into tuples of data
for rebuilding messages.

This is also used by the SQLAlchemy store object, an option for saving state
with tronstore, by serializing the state data into a string that's saved in
a SQL database, or by deserializing strings that are saved into state data."""
import datetime
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


def custom_decode(obj):
    """A custom decoder for datetime and tuple objects.
    The tuple part only works for JSON, as MsgPack handles tuples and lists
    itself no matter what.
    """
    try:
        if b'__tuple__' in obj:
            return tuple(custom_decode(o) for o in obj['items'])
        elif b'__datetime__' in obj:
            obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
        return obj
    except:
        return obj


def custom_encode(obj):
    """A custom encoder for datetime and tuple objects."""
    if isinstance(obj, tuple):
        return {'__tuple__': True, 'items': [custom_encode(e) for e in obj]}
    elif isinstance(obj, datetime.datetime):
        return {'__datetime__': True, 'as_str': obj.strftime("%Y%m%dT%H:%M:%S.%f")}
    return obj


class SerializerModuleError(Exception):
    """Raised if a serialization module is used without it being installed."""
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return repr(self.code)


class JSONSerializer(object):
    name = 'json'

    @classmethod
    def serialize(cls, data):
        return json.dumps(data, default=custom_encode, tuple_as_array=False)

    @classmethod
    def deserialize(cls, data_str):
        return json.loads(data_str, object_hook=custom_decode)


class cPickleSerializer(object):
    name = 'pickle'

    @classmethod
    def serialize(cls, data):
        return pickle.dumps(data)

    @classmethod
    def deserialize(cls, data_str):
        return pickle.loads(data_str)


class MsgPackSerializer(object):
    name = 'msgpack'

    @classmethod
    def serialize(cls, data):
        if no_msgpack:
            raise SerializerModuleError('MessagePack not installed.')
        return msgpack.packb(data, default=custom_encode)

    @classmethod
    def deserialize(cls, data_str):
        if no_msgpack:
            raise SerializerModuleError('MessagePack not installed.')
        return msgpack.unpackb(data_str, object_hook=custom_decode, use_list=0)


class YamlSerializer(object):
    name = 'yaml'

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
