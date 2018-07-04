from enum import Enum

from pyrsistent import field
from pyrsistent import PRecord


class ActionRunnerTypes(Enum):
    none = 'none'
    subprocess = 'subprocess'


class ActionRunner(PRecord):
    runner_type = field(
        type=ActionRunnerTypes,
        initial=ActionRunnerTypes.subprocess,
        factory=lambda rt: ActionRunnerTypes(rt or 'subprocess')
    )
    remote_exec_path = field(type=(str, type(None)), initial='')
    remote_status_path = field(type=(str, type(None)), initial='/tmp')

    @staticmethod
    def from_config(val, *_):
        return ActionRunner.create(val or {})
