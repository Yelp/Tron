"""Utilities for working with collections."""
import logging
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Protocol
from typing import TypeVar

log = logging.getLogger(__name__)


# Protocol definition for items managed by MappingCollection
class Item(Protocol):
    def get_name(self) -> str:
        ...

    def restore_state(self, state_data: Any) -> None:
        ...

    def disable(self) -> None:
        ...

    def __eq__(self, other: object) -> bool:
        ...


I = TypeVar("I", bound=Item)  # noqa: E741


class MappingCollection(Dict[str, I]):
    """Dictionary like object for managing collections of items. Item is
    expected to support the following interface, and should be hashable.

    class Item(object):

        def get_name(self): ...

        def restore_state(self, state_data): ...

        def disable(self): ...

        def __eq__(self, other): ...

    """

    def __init__(self, item_name: str) -> None:
        dict.__init__(self)
        self.item_name = item_name

    def filter_by_name(self, names: Iterable[str]) -> None:  # noqa: E741
        for name in set(self) - set(names):
            self.remove(name)

    def remove(self, name: str) -> None:
        if name not in self:
            raise ValueError(f"{self.item_name} {name} unknown")

        log.info("Removing %s %s", self.item_name, name)
        self.pop(name).disable()

    # TODO: pretty sure there's a bug here since update_func will in reality return True/False/None
    # ...which would seem to do the wrong thing in add()
    def contains_item(self, item: I, handle_update_func: Callable[[I], Any]) -> Optional[bool]:  # noqa: E741

        if item == self.get(item.get_name()):
            return True

        return handle_update_func(item) if item.get_name() in self else False

    def add(self, item: I, update_func: Callable[[I], Any]) -> bool:  # noqa: E741
        if self.contains_item(item, update_func):
            return False

        log.info("Adding new %s", item)
        self[item.get_name()] = item
        return True

    def replace(self, item: I) -> bool:  # noqa: E741
        return self.add(item, self.remove_item)

    def remove_item(self, item: I) -> None:  # noqa: E741
        return self.remove(item.get_name())
