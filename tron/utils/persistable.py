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
