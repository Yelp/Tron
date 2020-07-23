import logging
import operator
import pickle
import shelve
import sys
from io import BytesIO

import bsddb3

from tron.utils import maybe_decode

log = logging.getLogger(__name__)


class Py2Shelf(shelve.Shelf):
    def __init__(self, filename, flag='c', protocol=2, writeback=False):
        db = bsddb3.hashopen(filename, flag)
        args = [self, db, protocol, writeback]
        if sys.version_info[0] == 3:
            args.append('utf8')
        shelve.Shelf.__init__(*args)

    def __getitem__(self, key):
        try:
            value = self.cache[key]
        except KeyError:
            f = BytesIO(self.dict[key.encode('utf8')])
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
        self.dict[key.encode('utf8')] = f.getvalue()

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
        encoded_key = key.encode('utf8')
        if encoded_key in self.dict:
            del self.dict[encoded_key]


class ShelveKey(object):
    __slots__ = ['type', 'iden']

    def __init__(self, type, iden):
        self.type = maybe_decode(type)
        self.iden = maybe_decode(iden)

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
        self.shelve = Py2Shelf(self.filename)

    def build_key(self, type, iden):
        return ShelveKey(type, iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            shelve_key = str(key.key)
            if state_data is None:
                self.shelve.delete(shelve_key)
            else:
                self.shelve[shelve_key] = state_data
        self.shelve.sync()

    def restore(self, keys):
        items = zip(
            keys,
            (self.shelve.get(str(key.key)) for key in keys),
        )
        return dict(filter(operator.itemgetter(1), items))

    def cleanup(self):
        self.shelve.close()

    def __repr__(self):
        return "ShelveStateStore('%s')" % self.filename
