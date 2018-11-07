"""Utilities for working with collections."""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

log = logging.getLogger(__name__)


class MappingCollection(dict):
    """Dictionary like object for managing collections of items. Item is
    expected to support the following interface, and should be hashable.

    class Item(object):

        def get_name(self): ...

        def restore_state(self, state_data): ...

        def disable(self): ...

        def __eq__(self, other): ...

    """

    def __init__(self, item_name):
        dict.__init__(self)
        self.item_name = item_name

    def filter_by_name(self, names):
        for name in set(self) - set(names):
            self.remove(name)

    def remove(self, name):
        if name not in self:
            raise ValueError("%s %s unknown" % (self.item_name, name))

        log.info("Removing %s %s", self.item_name, name)
        self.pop(name).disable()

    def contains_item(self, item, handle_update_func):
        if item == self.get(item.get_name()):
            return True

        return handle_update_func(item) if item.get_name() in self else False

    def add(self, item, update_func):
        if self.contains_item(item, update_func):
            return False

        log.info("Adding new %s" % item)
        self[item.get_name()] = item
        return True

    def replace(self, item):
        return self.add(item, self.remove_item)

    def remove_item(self, item):
        return self.remove(item.get_name())
