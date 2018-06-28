from pyrsistent import field
from pyrsistent import PRecord


class NotificationOptions(PRecord):
    smtp_host = field(type=(str, type(None)), initial=None)
    notification_addr = field(type=(str, type(None)), initial=None)

    @staticmethod
    def from_config(val, _):
        return NotificationOptions.create(val)
