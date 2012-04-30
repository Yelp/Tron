"""Store state in a local YAML file.

WARNING: Using this store is NOT recommended.  It will be far too slow for
anything but the most trivial setups.  It should only be used with a high
buffer size (10+), and a low run_limit (< 10).
"""
from collections import namedtuple
import itertools
import operator
import os
yaml = None # For pyflakes

YamlKey = namedtuple('YamlKey', ['type', 'iden'])

class YamlStateStore(object):

    def __init__(self, filename):
        # Differ import of yaml until class is instantiated
        import yaml
        global yaml
        assert yaml
        self.filename           = filename
        self.buffer             = {}

    def build_key(self, type, iden):
        return YamlKey(type, iden)

    def restore(self, keys):
        if not os.path.exists(self.filename):
            return {}

        with open(self.filename, 'r') as fh:
            self.buffer = yaml.load(fh)

        items = (self.buffer.get(key.type, {}).get(key.iden) for key in keys)
        key_item_pairs = itertools.izip(keys, items)
        return dict(itertools.ifilter(operator.itemgetter(1), key_item_pairs))

    def save(self, key, state_data):
        self.buffer.setdefault(key.type, {})[key.iden] = state_data
        self._write_buffer()

    def _write_buffer(self):
        with open(self.filename, 'w') as fh:
            yaml.dump(self.buffer, fh)

    def cleanup(self):
        pass
