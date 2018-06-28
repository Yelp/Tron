from enum import Enum

from pyrsistent import field

from tron.config import ConfigRecord


class StatePersistenceTypes(Enum):
    shelve = 'shelve'
    sql = 'sql'
    yaml = 'yaml'


class StatePersistence(ConfigRecord):
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
