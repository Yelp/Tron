import weakref

from tron.utils import timeutils

# Event Levels INFO is for troubleshooting information. This may be verbose but
# shouldn't cause any monitors to make any decisions.

LEVEL_INFO = "INFO"

# OK inidicates the entity is doing great and any monitors that considered the
# entity to be in non-ok state can reset itself.
LEVEL_OK = "OK"

# NOTICE inidicates some troubling behavior, but not yet a complete failure. It
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


class FixedLimitStore(object):
    """Simple data store that keeps a fixed number of elements based on their
    'category'. Acts like a list, but see our special version of 'append'.
    """

    DEFAULT_LIMIT = 10

    def __init__(self, limits):
        self._limits = limits or dict()
        self._values = []

    def append(self, item, category):
        self._values.append((category, item))

        # Do we need to remove a value?
        current_count = sum((1 for cat, val in self._values
                             if cat == category))
        if current_count > self._limits.get(category, self.DEFAULT_LIMIT):
            for ndx, val in enumerate(self._values):
                if val[0] == category:
                    break
            else:
                assert False, "Didn't find category %r in %r" % (category,
                                                                 self._values)

            del self._values[ndx]

    def __iter__(self):
        return (val for cat, val in self._values)


class Event(object):
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
        if src:
            return src.entity
        else:
            return None


class EventRecorder(object):

    def __init__(self, entity, parent=None, limits=None):
        self._store = FixedLimitStore(limits)
        self._parent = None
        self._entity = weakref.ref(entity)

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
        self._store.append(event, event.level)

        # Propogate if we have a parent set (and the level is high enough to
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
        # Level's are actually descriptive strings, but we provide a way to get
        # the order via ORDERED_LEVELS constant
        min_level_ndx = None
        if min_level is not None:
            min_level_ndx = ORDERED_LEVELS.index(min_level)

        return [event for event in self
                if (min_level is None or
                    ORDERED_LEVELS.index(event.level) >= min_level_ndx)]

    def __iter__(self):
        return self._store.__iter__()
