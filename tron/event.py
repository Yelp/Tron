import heapq
from collections import deque, namedtuple
import itertools
import sys
import weakref

from tron.utils import timeutils
from tron.utils import observer

# Event Levels INFO is for troubleshooting information. This may be verbose but
# shouldn't cause any monitors to make any decisions.
LEVEL_INFO = "INFO"

# OK indicates the entity is doing great and any monitors that considered the
# entity to be in non-ok state can reset itself.
LEVEL_OK = "OK"

# NOTICE indicates some troubling behavior, but not yet a complete failure. It
# would be appropriate to highlight this event, but don't go waking up the
# president just yet.
LEVEL_NOTICE = "NOTICE"

# CRITICAL indicates the entity has had a major failure. Call in the troops.
LEVEL_CRITICAL = "CRITICAL"

# To allow our levels to be ordered, we provide this list. Use .index(level) to
# be able to compare levels
ORDERED_LEVELS = [
    LEVEL_INFO,
    LEVEL_OK,
    LEVEL_NOTICE,
    LEVEL_CRITICAL,
]

EventType = namedtuple('EventType', ['level', 'name'])


class FixedLimitStore(object):
    """Simple data store that keeps a fixed number of elements based on their
    'category'. Also known as a circular buffer or ring buffer. After
    sys.maxint events an internal counter will wrap back to 0. Some events
    may be out during that period.
    """
    DEFAULT_LIMIT = 10

    def __init__(self, limits):
        self._limits = limits or dict()
        self._values = {}
        self.counter = itertools.cycle(xrange(sys.maxint))

    def _build_deque(self, category):
        limit = self._limits.get(category, self.DEFAULT_LIMIT)
        return deque(maxlen=limit)

    def append(self, category, item):
        if category not in self._values:
            self._values[category] = self._build_deque(category)
        self._values[category].append((self.counter.next(), item))

    def __iter__(self):
        events = heapq.merge(*self._values.itervalues())
        return (val for _, val in events)


class Event(object):
    """Data object for storing details of an event."""
    __slots__ = ('_src', 'time', 'level', 'name', 'data')

    def __init__(self, src, level, name, **data):
        self._src = weakref.ref(src)
        self.time = timeutils.current_time()
        self.level = level
        self.name = name
        self.data = data

    @property
    def entity(self):
        src = self._src()
        return src.entity if src else None


class EventRecorder(observer.Observer):
    """Record events in a tree of listeners. Tron uses this class by having
    one process-wide event recorder (in the MCP) with each job and service
    having a child recorder, and each job run having a child recorder of the
    job.

    Events are propagated up the chain if they are of high enough severity.
    """

    def __init__(self, entity, parent=None, limits=None):
        self._store = FixedLimitStore(limits)
        self._parent = None
        self._entity = weakref.ref(entity)
        self.watch(entity)

        if parent:
            self.set_parent(parent)

    def set_parent(self, parent):
        self._parent = weakref.ref(parent)

    def _get_entity(self):
        return self._entity()

    def _set_entity(self, entity):
        self._entity = weakref.ref(entity)

    entity = property(_get_entity, _set_entity)

    def record(self, event):
        self._store.append(event.level, event)

        # Propagate if we have a parent set (and the level is high enough to
        # care)
        if (self._parent and
            ORDERED_LEVELS.index(event.level) >
                ORDERED_LEVELS.index(LEVEL_INFO)):
            self._parent().record(event)

    def emit_info(self, name, **data):
        self.record(Event(self, LEVEL_INFO, name, **data))

    def emit_ok(self, name, **data):
        self.record(Event(self, LEVEL_OK, name, **data))

    def emit_notice(self, name, **data):
        self.record(Event(self, LEVEL_NOTICE, name, **data))

    def emit_critical(self, name, **data):
        self.record(Event(self, LEVEL_CRITICAL, name, **data))

    def list(self, min_level=None):
        # Levels are actually descriptive strings, but we provide a way to get
        # the order via ORDERED_LEVELS constant
        min_level_ndx = None
        if min_level is not None:
            min_level_ndx = ORDERED_LEVELS.index(min_level)

        # TODO: this should go into FixedLimitStore
        return [event for event in self
                if (min_level is None or
                    ORDERED_LEVELS.index(event.level) >= min_level_ndx)]

    def __iter__(self):
        return self._store.__iter__()

    def watcher(self, observable, event):
        """Watch for events and create and store Event objects."""
        if not isinstance(event, EventType):
            return

        self.record(Event(self, event.level, event.name))



class EventManager(object):
    """Create and store EventRecorder objects for observable objects.
    This class is a singleton.
    """

    _instance = None

    def __init__(self):
        if self._instance is not None:
            raise ValueError(
                    "EventManager already instantiated. Use get_instance().")

        self.recorders = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _build_key(self, observable):
        """Create a unique key for this observable object.
        EventManager makes the assumption that objects str values will be
        unique.
        """
        return str(observable)

    def add(self, observable, parent=None):
        """Create an EventRecorder for the observable and store it."""
        key = self._build_key(observable)
        if key in self.recorders:
            raise ValueError("%s is already being managed." % observable)

        parent_recorder = None
        if parent:
            parent_recorder = self.get(parent)
            if not parent_recorder:
                raise ValueError("Parent %s is not being managed." % parent)

        event_recorder = EventRecorder(observable, parent_recorder)
        self.recorders[key] = event_recorder
        return event_recorder

    def get(self, observable):
        """Return an EventRecorder given an observable."""
        key = self._build_key(observable)
        return self.recorders.get(key, None)

    def clear(self):
        self.recorders.clear()
