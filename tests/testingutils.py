from __future__ import absolute_import
from __future__ import unicode_literals

import functools
import logging
import time

import mock

from testifycompat import class_setup
from testifycompat import class_teardown
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tron.utils import timeutils

log = logging.getLogger(__name__)

# TODO: remove when replaced with tron.eventloop


class MockReactorTestCase(TestCase):
    """Patch the reactor to a MockReactor."""

    # Override this in subclasses
    module_to_mock = None

    @class_setup
    def class_setup_patched_reactor(self):
        msg = "%s must set a module_to_mock field" % self.__class__
        assert self.module_to_mock, msg
        self.old_reactor = getattr(self.module_to_mock, 'reactor')

    @class_teardown
    def teardown_patched_reactor(self):
        setattr(self.module_to_mock, 'reactor', self.old_reactor)

    @setup
    def setup_mock_reactor(self):
        self.reactor = mock.MagicMock()
        setattr(self.module_to_mock, 'reactor', self.reactor)


# TODO: remove
class MockTimeTestCase(TestCase):

    now = None

    @setup
    def setup_current_time(self):
        assert self.now, "%s must set a now field" % self.__class__
        self.old_current_time = timeutils.current_time
        timeutils.current_time = lambda tz=None: self.now

    @teardown
    def teardown_current_time(self):
        timeutils.current_time = self.old_current_time
        # Reset 'now' back to what was set on the class because some test may
        # have changed it
        self.now = self.__class__.now


def retry(max_tries=3, delay=0.1, exceptions=(KeyError, IndexError)):
    """A function decorator for re-trying an operation. Useful for MongoDB
    which is only eventually consistent.
    """

    def wrapper(f):
        @functools.wraps(f)
        def wrap(*args, **kwargs):
            for _ in range(max_tries):
                try:
                    return f(*args, **kwargs)
                except exceptions:
                    time.sleep(delay)
            raise

        return wrap

    return wrapper


def autospec_method(method, *args, **kwargs):
    """create an autospec for an instance method."""
    mocked_method = mock.create_autospec(method, *args, **kwargs)
    setattr(method.__self__, method.__name__, mocked_method)
