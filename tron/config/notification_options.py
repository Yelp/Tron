from pyrsistent import field

from tron.config import ConfigRecord


class NotificationOptions(ConfigRecord):
    smtp_host = field(type=(str, type(None)), initial=None)
    notification_addr = field(type=(str, type(None)), initial=None)
