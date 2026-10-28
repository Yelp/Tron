import logging
import operator
import shelve
from typing import Any

import bsddb3  # type: ignore

log = logging.getLogger(__name__)


class BsdDbShelf(shelve.Shelf):
    """A shelve.Shelf backed by a bsddb3 hash database."""

    def __init__(self, filename, flag="c", writeback=False):
        super().__init__(bsddb3.hashopen(filename, flag), writeback=writeback, keyencoding="utf8")


class ShelveKey:
    __slots__ = ["type", "iden"]

    def __init__(self, type, iden):
        self.type = type
        self.iden = iden

    @property
    def key(self):
        return f"{self.type}___{self.iden}"

    def __str__(self):
        return f"{self.type} {self.iden}"

    def __eq__(self, other):
        return self.type == other.type and self.iden == other.iden

    def __hash__(self):
        return hash(self.key)


class ShelveStateStore:
    """Persist state using `shelve`."""

    def __init__(self, filename):
        self.filename = filename
        self.shelve = BsdDbShelf(self.filename)

    def build_key(self, type: str, iden: str) -> ShelveKey:
        return ShelveKey(type, iden)

    def save(self, key_value_pairs):
        for key, state_data in key_value_pairs:
            shelve_key = str(key.key)
            if state_data is None:
                self.shelve.pop(shelve_key, None)
            else:
                self.shelve[shelve_key] = state_data
        self.shelve.sync()

    def restore(self, keys: list[ShelveKey]) -> dict[ShelveKey, Any]:
        items = zip(
            keys,
            (self.shelve.get(str(key.key)) for key in keys),
        )
        return dict(filter(operator.itemgetter(1), items))

    def cleanup(self):
        self.shelve.close()

    def __repr__(self):
        return "ShelveStateStore('%s')" % self.filename
