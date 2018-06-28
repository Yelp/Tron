from pyrsistent import field

from tron.config import ConfigRecord


class MesosOptions(ConfigRecord):
    enabled = field(type=bool, initial=False)
