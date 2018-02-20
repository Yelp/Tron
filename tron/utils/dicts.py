"""Data structures used in tron."""
from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
from collections import Mapping

import six


def invert_dict_list(dictionary):
    """Invert a dictionary of lists. All values in the lists should be unique.
    """
    def invert(key, seq):
        for item in seq:
            yield item, key

    seq = (invert(k, v) for k, v in six.iteritems(dictionary))
    return dict(itertools.chain.from_iterable(seq))


class FrozenDict(Mapping):
    """Simple implementation of an immutable dictionary so we can freeze the
    command context, set of jobs/services, actions, etc.

    from http://stackoverflow.com/questions/2703599/what-would-be-a-frozen-dict
    """

    def __init__(self, *args, **kwargs):
        if hasattr(self, '_d'):
            raise Exception("Can't call __init__ twice")
        self._d = dict(*args, **kwargs)
        self._hash = None

    def __repr__(self):
        return 'FrozenDict(%r)' % self._d

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]

    def __hash__(self):
        # It would have been simpler and maybe more obvious to
        # use hash(tuple(sorted(self._d.iteritems()))) from this discussion
        # so far, but this solution is O(n). I don't know what kind of
        # n we are going to run into, but sometimes it's hard to resist the
        # urge to optimize when it will gain improved algorithmic performance.
        if self._hash is None:
            self._hash = 0
            for key, value in six.iteritems(self):
                self._hash ^= hash(key)
                self._hash ^= hash(value)
        return self._hash
