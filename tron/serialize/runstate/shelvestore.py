import logging
import shelve
import operator
import itertools

log = logging.getLogger(__name__)

class ShelveKey(object):

    def __init__(self, type, iden):
        self.type               = type
        self.iden               = iden

    @property
    def key(self):
        return "%s___%s" % (self.type, self.iden)

    def __str__(self):
        return "%s %s" % (self.type, self.iden)

class ShelveStateStore(object):
    """Persist state using `shelve`."""

    def __init__(self, filename):
        self.filename = filename
        self.shelve = shelve.open(self.filename)

    def build_key(self, type, iden):
        return ShelveKey(type, iden)

    def save(self, key, state_data):
        self.shelve[key.key] = state_data
        self.shelve.sync()

    def restore(self, keys):
        items = itertools.izip(keys, (self.shelve.get(key.key) for key in keys))
        return filter(operator.itemgetter(1), items)

    def cleanup(self):
        self.shelve.close()