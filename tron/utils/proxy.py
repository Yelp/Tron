"""
 Utilities for creating classes that proxy function calls.
"""
import functools


class CollectionProxy(object):
    """Proxy attribute lookups to a sequence of objects."""

    def __init__(self, obj_list, definition_list=None):
        """See add() for a description of proxy definitions."""
        self.obj_list = obj_list
        self._defs = {}
        for definition in definition_list or []:
            self.add(*definition)

    def add(self, attribute_name, aggregate_func, callable):
        """Add calls and their destination.

            attribute_name - the name of the attribute to proxy
            aggregate_func - a function that takes a sequence as its only argument
            obj_list       - the list of objects to pass to aggregate_func
            callable       - if this attribute is a callable on every object in
                             the obj_list
        """
        self._defs[attribute_name] = (aggregate_func, callable)

    def perform(self, attribute_name, *args, **kwargs):
        """Attempt to perform the proxied lookup.  Raises AttributeError if
        the attribute_name is not defined.
        """
        if attribute_name not in self._defs:
            raise AttributeError(attribute_name)

        aggregate_func, callable = self._defs[attribute_name]
        if callable:
            return functools.partial(
                aggregate_func,
                (getattr(i, attribute_name)(*args, **kwargs) for i in self.obj_list)
            )

        return aggregate_func(getattr(i, attribute_name) for i in self.obj_list)


class AttributeProxy(object):
    """Proxy attribute lookups to another object."""

    def __init__(self, dest_obj, attribute_list=None):
        self._attributes = set(attribute_list or [])
        self.dest_obj = dest_obj

    def add(self, attribute_name):
        self._attributes.add(attribute_name)

    def perform(self, attribute_name):
        if attribute_name not in self._attributes:
            raise AttributeError(attribute_name)

        return getattr(self.dest_obj, attribute_name)