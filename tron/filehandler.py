"""
Tools for managing and properly closing file handles.
"""
import logging
import time

from tron.utils.dicts import OrderedDict

log = logging.getLogger('tron.filehandler')


class FileHandleWrapper(object):
    """Acts as a proxy to file handles.  Wrap a file handle and stores
    access time and metadata.  These objects should only be created
    by FileHandleManager. Do not instantiate them on their own."""

    def __init__(self, manager, name):
        self.manager = manager
        self.name = name
        self.last_accessed = time.time()
        self._fh = None

    def close(self):
        self.close_wrapped()
        self.manager.remove(self)

    def close_wrapped(self):
        """Close only the underlying file handle."""
        if self._fh and not self._fh.closed:
            self._fh.close()
        self._fh = None

    def write(self, content):
        """Write content to the fh. Re-open if necessary."""
        if not self._fh or self._fh.closed:
            try:
                self._fh = open(self.name, 'a')
            except IOError, e:
                log.error("Failed to open %s: %s", (self.name, e))
                return

        self.last_accessed = time.time()
        self._fh.write(content)
        self.manager.update(self)


class FileHandleManager(object):
    """Creates FileHandleWrappers, closes handles when they have
    been inactive for a period of time, and transparently re-open then next
    time they are needed. All files are opened in append mode.

    This class is singleton.  An already configured instance can be
    retrieving by using get_instance() (and will be created if None),
    max_idle_time can be set by calling the classmethod set_max_idle_time()
    """

    _instance = None

    def __init__(self, max_idle_time=60):
        """
            Create a new instance.
            max_idle_time           - max idle time in seconds
        """
        if self.__class__._instance:
            raise ValueError(
                "FileHandleManager is a singleton. Call get_instance()")
        self.max_idle_time = max_idle_time
        self.cache = OrderedDict()
        self.__class__._instance = self

    @classmethod
    def set_max_idle_time(cls, max_idle_time):
        inst = cls.get_instance()
        inst.max_idle_time = max_idle_time

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        """Empty the cache and reset the instance to it's original state."""
        inst = cls.get_instance()
        for fh_wrapper in inst.cache.values():
            inst.remove(fh_wrapper)

    def open(self, filename):
        """Retrieve a file handle from the cache based on name.  Returns a
        FileHandleWrapper. If the handle is not in the cache, create a new
        instance.
        """
        if filename in self.cache:
            return self.cache[filename]
        fhw = FileHandleWrapper(self, filename)
        self.cache[filename] = fhw
        return fhw

    def cleanup(self, time_func=time.time):
        """Close any file handles that have been idle for longer than
        max_idle_time. time_func is primary used for testing.
        """
        if not self.cache:
            return

        cur_time = time_func()
        for name, fh_wrapper in self.cache.items():
            if cur_time - fh_wrapper.last_accessed > self.max_idle_time:
                fh_wrapper.close()
            else:
                break

    def remove(self, fh_wrapper):
        """Remove the fh_wrapper from the cache and access_order."""
        if fh_wrapper.name in self.cache:
            del self.cache[fh_wrapper.name]

    def update(self, fh_wrapper):
        """Remove and re-add the file handle to the cache so that it's keys
        are still ordered by last access. Calls cleanup() to remove any file
        handles that have been idle for too long.
        """
        self.remove(fh_wrapper)
        self.cache[fh_wrapper.name] = fh_wrapper
        self.cleanup()