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
    def from_json(state_data: str) -> Dict[str, Any]:
        # This method is called on because it is intended to handle the deserialization of JSON data into a
        # dictionary representation of the state. This allows the method to be used in a more flexible and generic way,
        # enabling different classes to implement their own specific logic for converting the dictionary into an instance of the
        # class. By returning a dictionary, the method provides a common interface for deserialization, while allowing subclasses
        # to define how the dictionary should be used to restore the state of the object.
        pass
