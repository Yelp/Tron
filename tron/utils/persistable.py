from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import Optional


class Persistable(ABC):
    @staticmethod
    @abstractmethod
    def to_json(state_data: Dict[Any, Any]) -> Optional[str]:
        pass

    @staticmethod
    @abstractmethod
    def from_json(state_data: str) -> Dict[Any, Any]:
        pass
