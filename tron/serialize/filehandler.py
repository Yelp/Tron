"""
Tools for managing and properly closing file handles.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import os.path
import shutil
import time
from collections import OrderedDict
from subprocess import PIPE
from subprocess import Popen

import six

from tron.utils import maybe_encode

log = logging.getLogger(__name__)


class NullFileHandle(object):
    """A No-Op object that supports a File interface."""
    closed = True

    @classmethod
    def write(cls, _):
        pass

    @classmethod
    def close(cls):
        pass


class FileHandleWrapper(object):
    """Acts as a proxy to file handles.  Wrap a file handle and stores
    access time and metadata.  These objects should only be created
    by FileHandleManager. Do not instantiate them on their own.
    """
    __slots__ = ['manager', 'name', 'last_accessed', '_fh']

    def __init__(self, manager, name):
        self.manager = manager
        self.name = name
        self.last_accessed = time.time()
        self._fh = NullFileHandle

    def close(self):
        self.close_wrapped()
        self.manager.remove(self)

    def close_wrapped(self):
        """Close only the underlying file handle."""
        self._fh.close()
        self._fh = NullFileHandle

    def write(self, content):
        """Write content to the fh. Re-open if necessary."""
        if self._fh == NullFileHandle:
            try:
                self._fh = open(self.name, 'ab')
            except IOError as e:
                log.error("Failed to open %s: %s", self.name, e)
                return

        self.last_accessed = time.time()
        self._fh.write(maybe_encode(content))
        self.manager.update(self)

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()


class FileHandleManager(object):
    """Creates FileHandleWrappers, closes handles when they have
    been inactive for a period of time, and transparently re-open the next
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
            msg = "FileHandleManager is a singleton. Call get_instance()"
            raise ValueError(msg)
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
        for fh_wrapper in list(inst.cache.values()):
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
        for name, fh_wrapper in list(self.cache.items()):
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


class OutputStreamSerializer(object):
    """Manage writing to and reading from files in a directory hierarchy."""

    def __init__(self, base_path):
        self.base_path = os.path.join(*base_path)
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def full_path(self, filename):
        return os.path.join(self.base_path, filename)

    # TODO: do not use subprocess
    def tail(self, filename, num_lines=None):
        """Tail a file using `tail`."""
        path = self.full_path(filename)
        if not path or not os.path.exists(path):
            return []
        if not num_lines:
            num_lines = six.MAXSIZE

        try:
            cmd = ('tail', '-n', str(num_lines), path)
            tail_sub = Popen(cmd, stdout=PIPE)
            return list(line.rstrip() for line in tail_sub.stdout)
        except OSError as e:
            log.error("Could not tail %s: %s" % (path, e))
            return []

    def open(self, filename):
        """Return a FileHandleManager for the output path."""
        path = self.full_path(filename)
        return FileHandleManager.get_instance().open(path)


class OutputPath(object):
    """A list like object used to construct a file path for output. The
    file path is constructed by joining the base path with any additional
    path elements.
    """
    __slots__ = ['base', 'parts']

    def __init__(self, base='.', *path_parts):
        self.base = base
        self.parts = list(path_parts or [])

    def append(self, part):
        self.parts.append(part)

    def __iter__(self):
        yield self.base
        for p in self.parts:
            yield p

    def __str__(self):
        return os.path.join(*self)

    def clone(self, *parts):
        """Return a new OutputPath object which has a base of the str value
        of this object.
        """
        return type(self)(str(self), *parts)

    def delete(self):
        """Remove the directory and its contents."""
        try:
            shutil.rmtree(str(self))
        except OSError as e:
            log.warning("Failed to delete %s: %s" % (self, e))

    def __eq__(self, other):
        return self.base == other.base and self.parts == other.parts

    def __ne__(self, other):
        return not self == other
