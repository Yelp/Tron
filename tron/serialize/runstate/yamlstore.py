from collections import namedtuple
import itertools
from tron.serialize import runstate
yaml = None

YamlKey = namedtuple('ShelveKey', ['type', 'iden'])

class YamlStateStore(object):

    def __init__(self, filename, buffer_size):
        self.filename           = filename
        self.buffer             = {}
        self.buffer_size        = buffer_size
        self.counter            = itertools.cycle(xrange(self.buffer_size))
        self.buffer.setdefault(runstate.JOB_STATE, {})
        self.buffer.setdefault(runstate.SERVICE_STATE, {})
        self.buffer.setdefault(runstate.MCP_STATE, {})

    def check_missing_imports(self):
        try:
            import yaml
            global yaml
            return None
        except ImportError:
            return 'yaml'

    def build_key(self, type, iden):
        return YamlKey(type, iden)

    def save(self, key, state_data):
        self.buffer[key.type][key.iden] = state_data
        if not self.counter.next():
            self.write_buffer()

    def write_buffer(self):
        with open(self.filename, 'w') as fh:
            yaml.dump(self.buffer, fh)

    def restore(self, keys):
        with open(self.filename) as fh:
            self.buffer = yaml.load(fh)
        return (self.buffer[key.type].get(key.iden) for key in keys)

    def cleanup(self):
        pass
