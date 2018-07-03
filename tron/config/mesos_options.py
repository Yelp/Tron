from pyrsistent import field
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v

from tron.config import ConfigRecord


class MesosOptions(ConfigRecord):
    enabled = field(type=bool, initial=False)
    default_volumes = field(type=PVector, initial=v(), factory=pvector)
    dockercfg_location = field(type=(str, type(None)), initial=None)
    offer_timeout = field(type=int, initial=300)
