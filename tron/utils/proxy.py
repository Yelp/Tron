"""Utilities for creating classes that proxy function calls."""
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import ValuesView


# luisp tried his hardest (sorta) to type hint this before giving up and sprinkling Any everywhere.
# feel free to improve this if you dare
class CollectionProxy:
    """Proxy attribute lookups to a sequence of objects."""

    def __init__(
        self,
        obj_list_getter: Callable[[], ValuesView[Any]],
        definition_list: Optional[List[Tuple[str, Callable[[Iterable[Any]], Any], bool]]] = None,
    ) -> None:
        """See add() for a description of proxy definitions."""
        self.obj_list_getter: Callable[[], ValuesView[Any]] = obj_list_getter
        self._defs: Dict[str, Tuple[Callable[[Iterable[Any]], Any], bool]] = {}
        for definition in definition_list or []:
            self.add(*definition)

    def add(self, attribute_name: str, aggregate_func: Callable[[Iterable[Any]], Any], is_callable: bool) -> None:
        """Add attributes to proxy, the aggregate function to use on the
        sequence of returned values, and a boolean identifying if this
        attribute is a callable or not.

            attribute_name - the name of the attribute to proxy
            aggregate_func - a function that takes a sequence as its only argument
            callable       - if this attribute is a callable on every object in
                             the obj_list (boolean)
        """
        self._defs[attribute_name] = (aggregate_func, is_callable)

    def perform(self, name: str) -> Any:
        """Attempt to perform the proxied lookup.  Raises AttributeError if
        the name is not defined.
        """
        if name not in self._defs:
            raise AttributeError(name)

        obj_list = self.obj_list_getter
        aggregate_func: Callable[[Iterable[Any]], Any]
        is_callable: bool
        aggregate_func, is_callable = self._defs[name]

        if not is_callable:
            return aggregate_func(getattr(i, name) for i in obj_list())

        def func(*args: Any, **kwargs: Any) -> Any:
            return aggregate_func(getattr(item, name)(*args, **kwargs) for item in obj_list())

        return func


def func_proxy(
    name: str, func: Callable[[Iterable[Any]], Any]
) -> Tuple[str, Callable[[Iterable[Any]], Any], Literal[True]]:
    return name, func, True


def attr_proxy(
    name: str, func: Callable[[Iterable[Any]], Any]
) -> Tuple[str, Callable[[Iterable[Any]], Any], Literal[False]]:
    return name, func, False


class AttributeProxy:
    """Proxy attribute lookups to another object."""

    def __init__(self, dest_obj: Any, attribute_list: Optional[List[str]] = None) -> None:
        self._attributes = set(attribute_list or [])
        self.dest_obj = dest_obj

    def add(self, attribute_name: str) -> None:
        self._attributes.add(attribute_name)

    def perform(self, attribute_name: str) -> Any:
        if attribute_name not in self._attributes:
            raise AttributeError(attribute_name)

        return getattr(self.dest_obj, attribute_name)
