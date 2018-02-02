from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import logging
import operator
import shelve

log = logging.getLogger(__name__)


class ShelveKey(object):
    __slots__ = ['type', 'iden']

    def __init__(self, type, iden):
        self.type = type
        self.iden = iden

    @property
    def key(self):
        return "%s___%s" % (self.type, self.iden)

    def __str__(self):
        return "%s %s" % (self.type, self.iden)

    def __eq__(self, other):
        return self.type == other.type and self.iden == other.iden

    def __hash__(self):
        return hash(self.key)


class ShelveStateStore(object):
    """Persist state using `shelve`."""

    def __init__(self, filename):
        self.filename = filename
        self.shelve = shelve.open(self.filename)

    def build_key(self, type, iden):
        return ShelveKey(type, iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            self.shelve[key.key] = state_data
        self.shelve.sync()

    def restore(self, keys):
        items = itertools.izip(
            keys, (
                self.shelve.get(str(key.key))
                for key in keys
            ),
        )
        return dict(itertools.ifilter(operator.itemgetter(1), items))

    def cleanup(self):
        self.shelve.close()

    def __repr__(self):
        return "ShelveStateStore('%s')" % self.filename
