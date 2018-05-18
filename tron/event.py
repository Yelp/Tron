from __future__ import absolute_import
from __future__ import unicode_literals

import itertools
import logging
import operator
from collections import deque

import six

from tron.utils import timeutils

log = logging.getLogger(__name__)

# Special character used to split an entity name into hierarchy levels
NAME_CHARACTER = '.'


class EventLevel(object):
    """An event level that supports ordering."""
    __slots__ = ('order', 'label')

    def __init__(self, order, label):
        self.order = order
        self.label = label

    def __eq__(self, other):
        return self.order == other.order

    def __cmp__(self, other):
        # https://docs.python.org/3.0/whatsnew/3.0.html#ordering-comparisons
        # TODO: drop this
        return (self.order > other.order) - (self.order < other.order)

    def __lt__(self, other):
        return self.order < other.order

    def __le__(self, other):
        return self.order <= other.order

    def __gt__(self, other):
        return self.order > other.order

    def __ge__(self, other):
        return self.order >= other.order

    def __hash__(self):
        return hash(self.order)


LEVEL_INFO = EventLevel(0, "INFO")  # Troubleshooting information
LEVEL_OK = EventLevel(1, "OK")  # Expected behaviour
LEVEL_NOTICE = EventLevel(2, "NOTICE")  # Troubling behaviour
LEVEL_CRITICAL = EventLevel(3, "CRITICAL")  # Major Failure


class EventStore(object):
    """An index of event level to a circular buffer of events. Supports
    retrieving events which with a minimal level.
    """
    DEFAULT_LIMIT = 10

    def __init__(self, limits=None):
        self.limits = limits or dict()
        self.events = {}

    def _build_deque(self, category):
        limit = self.limits.get(category, self.DEFAULT_LIMIT)
        return deque(maxlen=limit)

    def append(self, event):
        level = event.level
        if level not in self.events:
            self.events[level] = self._build_deque(level)
        self.events[level].append(event)

    def get_events(self, min_level=LEVEL_INFO):
        min_level = min_level or LEVEL_INFO
        event_iterable = six.iteritems(self.events)
        groups = (e for key, e in event_iterable if key >= min_level)
        return itertools.chain.from_iterable(groups)

    __iter__ = get_events


class Event(object):
    """Data object for storing details of an event."""
    __slots__ = ('entity', 'time', 'level', 'name', 'data')

    def __init__(self, entity, level, name, **data):
        self.entity = entity
        self.time = timeutils.current_time()
        self.level = level
        self.name = name
        self.data = data


class EventRecorder(object):
    """A node in a tree which stores EventRecorders, links to children,
    and adds missing children on get_child().
    """
    __slots__ = ('name', 'children', 'events')

    def __init__(self, name):
        self.name = name
        self.children = {}
        self.events = EventStore()

    def get_child(self, child_key):
        if child_key in self.children:
            return self.children[child_key]

        split_char = NAME_CHARACTER
        name_parts = [self.name, child_key] if self.name else [child_key]
        child_name = split_char.join(name_parts)
        child = EventRecorder(child_name)
        return self.children.setdefault(child_key, child)

    def remove_child(self, child_key):
        if child_key in self.children:
            del self.children[child_key]

    def _record(self, level, name, **data):
        self.events.append(Event(self.name, level, name, **data))

    def list(self, min_level=None, child_events=True):
        if child_events:
            events = self._events_with_child_events(min_level)
        else:
            events = self.events.get_events(min_level)
        return sorted(events, key=operator.attrgetter('time'), reverse=True)

    def _events_with_child_events(self, min_level):
        """Yield all events and all child events which were recorded with a
        level greater than or equal to min_level.
        """
        for event in self.events.get_events(min_level):
            yield event
        for child in six.itervalues(self.children):
            for event in child._events_with_child_events(min_level):
                yield event

    def info(self, name, **data):
        return self._record(LEVEL_INFO, name, **data)

    def ok(self, name, **data):
        return self._record(LEVEL_OK, name, **data)

    def notice(self, name, **data):
        return self._record(LEVEL_NOTICE, name, **data)

    def critical(self, name, **data):
        return self._record(LEVEL_CRITICAL, name, **data)


def get_recorder(entity_name=''):
    """Return an EventRecorder object which stores events for the entity
    identified by `entity_name`. Returns the root recorder if not name is
    given.
    """
    return EventManager.get_instance().get(entity_name)


class EventManager(object):
    """Create and store EventRecorder objects in a hierarchy based on
    the name of the entity name.
    """

    _instance = None

    def __init__(self):
        if self._instance is not None:
            raise ValueError("Use EventManger.get_instance()")
        self.root_recorder = EventRecorder('')

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _get_name_parts(self, entity_name):
        return entity_name.split(NAME_CHARACTER) if entity_name else []

    def get(self, entity_name):
        """Search for and return the event recorder in the tree."""
        recorder = self.root_recorder
        for child_key in self._get_name_parts(entity_name):
            recorder = recorder.get_child(child_key)

        return recorder

    @classmethod
    def reset(cls):
        cls.get_instance().recorders = EventRecorder('')

    def remove(self, entity_name):
        """Remove an event recorder."""
        recorder = self.root_recorder
        name_parts = self._get_name_parts(entity_name)
        for child_key in name_parts[:-1]:
            recorder = recorder.get_child(child_key)

        recorder.remove_child(name_parts[-1])
