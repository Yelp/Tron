import weakref

from tron.utils import timeutils

# Event Levels
INFO = "INFO"
NOTICE = "NOTICE"
ERROR = "ERROR"

# To allow our levels to be ordered, we provide this list
# Use .index(level) to be able to compare levels
ORDERED_LEVELS = [
    INFO,
    NOTICE,
    ERROR,
]

# Potential Event Names
# "failure"
# "delay"
# "start"
# "stop"
# "schedule"
# "config"

class FixedLimitStore(object):
    """Simple data store that keeps a fixed number of elements based on their 'category'

    Acts like a list, but see our special version of 'append'
    """

    DEFAULT_LIMIT = 10
    def __init__(self, limits):
        self._limits = limits or dict()
        self._values = []
    
    def append(self, item, category):
        self._values.append((category, item))
        
        # Do we need to remove a value?
        current_count = sum((1 for cat, val in self._values if cat == category))
        if current_count > self._limits.get(category, self.DEFAULT_LIMIT):
            for ndx, val in enumerate(self._values):
                if val[0] == category:
                    break
            else:
                assert False, "Didn't find category %r in %r" % (category, self._values)
            
            del self._values[ndx]

    def __iter__(self):
        return (val for cat, val in self._values)


class Event(object):
    def __init__(self, entity, level, name, **data):
        self._entity = weakref.ref(entity)
        self.time = timeutils.current_time()
        self.level = level
        self.name = name
        self.data = data

    @property
    def entity(self):
        return self._entity()


class EventRecorder(object):
    def __init__(self, entity, parent=None, limits=None):
        self._store = FixedLimitStore(limits)
        self._parent = None
        self._entity = weakref.ref(entity)
        
        if parent:
            self._parent = weakref.ref(parent)
    
    def record(self, event):
        self._store.append(event, event.level)
        if self._parent:
            self._parent().record(event)

    def emit_info(self, name, **data):
        self.record(Event(self._entity(), INFO, name, **data))

    def emit_notice(self, name, **data):
        self.record(Event(self._entity(), NOTICE, name, **data))

    def emit_error(self, name, **data):
        self.record(Event(self._entity(), ERROR, name, **data))

    def list(self, min_level=None):
        # Level's are actually descriptive strings, but we provide a way to get the
        # order via ORDERED_LEVELS constant
        min_level_ndx = None
        if min_level is not None:
            min_level_ndx = ORDERED_LEVELS.index(min_level)
        
        return [event for event in self if min_level is None or ORDERED_LEVELS.index(event.level) >= min_level_ndx]
    
    def __iter__(self):
        return self._store.__iter__()
