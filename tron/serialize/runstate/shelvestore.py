from __future__ import absolute_import
from __future__ import unicode_literals

import dbm
import logging
import operator
import pickle
import shelve
import sys
from io import BytesIO

from six.moves import filter
from six.moves import zip


log = logging.getLogger(__name__)


class Py2Shelf(shelve.Shelf):
    def __init__(self, filename, flag='c', protocol=2, writeback=False, keyencoding='ascii'):
        self.keyencoding = keyencoding

        if sys.version_info[0] == 3:
            shelve.Shelf.__init__(
                self, dbm.open(
                    filename, flag,
                ), protocol, writeback, keyencoding,
            )
        else:
            shelve.Shelf.__init__(
                self, dbm.open(filename, flag), protocol, writeback,
            )

    def __getitem__(self, key):
        try:
            value = self.cache[key]
        except KeyError:
            f = BytesIO(self.dict[key.encode(self.keyencoding)])
            if sys.version_info[0] == 3:
                value = pickle.load(f, encoding='bytes')
            else:
                value = pickle.load(f)
            if self.writeback:
                self.cache[key] = value
        return value

    def __setitem__(self, key, value):
        if self.writeback:
            self.cache[key] = value
        f = BytesIO()
        pickle.dump(obj=value, file=f, protocol=self._protocol)
        self.dict[key.encode(self.keyencoding)] = f.getvalue()


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
        self.shelve = Py2Shelf(
            self.filename,
            keyencoding='utf8',
        )

    def build_key(self, type, iden):
        return ShelveKey(type, iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            self.shelve[str(key.key)] = state_data
        self.shelve.sync()

    def restore(self, keys):
        items = zip(
            keys, (
                self.shelve.get(str(key.key))
                for key in keys
            ),
        )
        return dict(filter(operator.itemgetter(1), items))

    def cleanup(self):
        self.shelve.close()

    def __repr__(self):
        return "ShelveStateStore('%s')" % self.filename
