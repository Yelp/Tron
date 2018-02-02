"""Store state in a local YAML file.

WARNING: Using this store is NOT recommended.  It will be far too slow for
anything but the most trivial setups.  It should only be used with a high
buffer size (10+), and a low run_limit (< 10).
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import operator
import os
from collections import namedtuple

from tron.serialize import runstate
import yaml

try:
    from yaml.cyaml import CSafeLoader as Loader
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Loader = yaml.SafeLoader

yaml = None  # For pyflakes

YamlKey = namedtuple('YamlKey', ['type', 'iden'])

TYPE_MAPPING = {
    runstate.JOB_STATE:     'jobs',
    runstate.SERVICE_STATE: 'services',
    runstate.MCP_STATE:     runstate.MCP_STATE,
}


class YamlStateStore(object):

    def __init__(self, filename):
        # Differ import of yaml until class is instantiated
        import yaml
        global yaml
        assert yaml
        self.filename = filename
        self.buffer = {}

    def build_key(self, type, iden):
        return YamlKey(TYPE_MAPPING[type], iden)

    def restore(self, keys):
        if not os.path.exists(self.filename):
            return {}

        with open(self.filename, 'r') as fh:
            self.buffer = yaml.load(fh, Loader=Loader)

        items = (self.buffer.get(key.type, {}).get(key.iden) for key in keys)
        key_item_pairs = itertools.izip(keys, items)
        return dict(itertools.ifilter(operator.itemgetter(1), key_item_pairs))

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            self.buffer.setdefault(key.type, {})[key.iden] = state_data
        self._write_buffer()

    def _write_buffer(self):
        with open(self.filename, 'w') as fh:
            yaml.dump(self.buffer, fh)

    def cleanup(self):
        pass

    def __repr__(self):
        return "YamlStateStore('%s')" % self.filename
