from enum import Enum

from pyrsistent import field
from pyrsistent import PRecord


class ActionRunnerTypes(Enum):
    none = 'none'
    subprocess = 'subprocess'


class ActionRunner(PRecord):
    runner_type = field(factory=ActionRunnerTypes)
    remote_exec_path = field(type=(str, None.__class__), initial='')
    remote_status_path = field(type=(str, None.__class__), initial='/tmp')

    @staticmethod
    def from_config(val, _):
        return ActionRunner.create(val)
