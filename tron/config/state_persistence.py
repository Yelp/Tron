from enum import Enum

from pyrsistent import field
from pyrsistent import PRecord


class StatePersistenceTypes(Enum):
    shelve = 'shelve'
    sql = 'sql'
    yaml = 'yaml'


class StatePersistence(PRecord):
    name = field(type=str)
    store_type = field(
        type=StatePersistenceTypes,
        factory=StatePersistenceTypes,
        initial=StatePersistenceTypes.shelve,
    )
    connection_details = field(type=(str, type(None)), initial=None)
    buffer_size = field(
        type=int,
        initial=1,
        factory=int,
        invariant=lambda b: (b >= 1, "Buffer must be >= 1")
    )

    @staticmethod
    def from_config(val, _):
        return StatePersistence.create(val)
