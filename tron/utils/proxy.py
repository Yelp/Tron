"""Utilities for creating classes that proxy function calls."""
from __future__ import absolute_import
from __future__ import unicode_literals


class CollectionProxy(object):
    """Proxy attribute lookups to a sequence of objects."""

    def __init__(self, obj_list_getter, definition_list=None):
        """See add() for a description of proxy definitions."""
        self.obj_list_getter = obj_list_getter
        self._defs = {}
        for definition in definition_list or []:
            self.add(*definition)

    def add(self, attribute_name, aggregate_func, is_callable):
        """Add attributes to proxy, the aggregate function to use on the
        sequence of returned values, and a boolean identifying if this
        attribute is a callable or not.

            attribute_name - the name of the attribute to proxy
            aggregate_func - a function that takes a sequence as its only argument
            callable       - if this attribute is a callable on every object in
                             the obj_list (boolean)
        """
        self._defs[attribute_name] = (aggregate_func, is_callable)

    def perform(self, name):
        """Attempt to perform the proxied lookup.  Raises AttributeError if
        the name is not defined.
        """
        if name not in self._defs:
            raise AttributeError(name)

        obj_list = self.obj_list_getter
        aggregate_func, is_callable = self._defs[name]

        if not is_callable:
            return aggregate_func(getattr(i, name) for i in obj_list())

        def func(*args, **kwargs):
            return aggregate_func(
                getattr(item, name)(*args, **kwargs) for item in obj_list()
            )

        return func


def func_proxy(name, func):
    return name, func, True


def attr_proxy(name, func):
    return name, func, False


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
