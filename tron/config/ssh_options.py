import os

from pyrsistent import field
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v


class SSHOptions(PRecord):
    agent = field(
        type=bool,
        initial=False,
        invariant=lambda a: (
            not a or 'SSH_AUTH_SOCK' in os.environ,
            'No SSH Agent available ($SSH_AUTH_SOCK)'
        )
    )
    identities = field(type=PVector, initial=v(), factory=pvector)
    known_hosts_file = field(type=(str, None.__class__), initial=None)
    connect_timeout = field(type=int, initial=30)
    idle_connection_timeout = field(type=int, initial=3600)
    jitter_min_load = field(type=int, initial=4)
    jitter_max_delay = field(type=int, initial=20)
    jitter_load_factor = field(type=int, initial=1)

    @staticmethod
    def from_config(val, _):
        return SSHOptions.create(val)
