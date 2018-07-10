from pyrsistent import field

from tron.config import ConfigRecord
from tron.core.action import Volumes


class MesosOptions(ConfigRecord):
    enabled = field(type=bool, initial=False)
    default_volumes = field(
        type=Volumes, initial=Volumes(), factory=Volumes.from_config
    )
    dockercfg_location = field(type=(str, type(None)), initial=None)
    offer_timeout = field(type=int, initial=300)
