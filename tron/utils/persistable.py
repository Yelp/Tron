from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict


class Persistable(ABC):
    @staticmethod
    @abstractmethod
    def to_json(state_data: Dict[Any, Any]) -> str:
        pass
