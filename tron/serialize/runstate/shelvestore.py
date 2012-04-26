from collections import namedtuple
import logging
import shelve
from tron.serialize import runstate

log = logging.getLogger(__name__)

ShelveKey = namedtuple('ShelveKey', ['type', 'iden'])

class ShelveStateStore(object):
    """Persist state using `shelve`."""

    def __init__(self, filename):
        self.filename = filename
        self.shelve = shelve.open(self.filename)
        self.shelve.setdefault(runstate.JOB_STATE, {})
        self.shelve.setdefault(runstate.SERVICE_STATE, {})
        self.shelve.setdefault(runstate.MCP_STATE, {})

    def check_missing_imports(self):
        return None

    def build_key(self, type, iden):
        return ShelveKey(type, iden)

    def save(self, key, state_data):
        self.shelve[key.type][key.iden] = state_data

    def restore(self, keys):
        return (self.shelve[key.type].get(key.iden) for key in keys)

    def cleanup(self):
        self.shelve.close()
