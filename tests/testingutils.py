from contextlib import contextmanager
import logging

from testify import  TestCase, setup
from testify import class_setup, class_teardown


log = logging.getLogger(__name__)

# this exists as logging.NullHandler as of Python 2.7
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


@contextmanager
def no_handlers_for_logger(name=None):
    """Temporarily remove handlers all handlers from a logger.

    Use this in a `with` block. For example::

        with no_handlers_for_logger('tron.mcp'):
            # do stuff with mcp

    Any handlers you add inside the `with` block will be removed at the end.
    """
    log = logging.getLogger(name)
    old_handlers = log.handlers

    # add null handler so logging doesn't yell about there being no handlers
    log.handlers = [NullHandler()]

    yield

    log.handlers = old_handlers


class MockReactorTestCase(TestCase):
    """Patch the reactor to a MockReactor."""

    # Override this in subclasses
    module_to_mock = None

    @class_setup
    def setup_patched_reactor(self):
        msg = "subclassses of %s must set reactor_to_mock" % self.__class__
        assert self.module_to_mock, msg

        self.old_module = self.module_to_mock

    @class_teardown
    def teardown_patched_reactor(self):
        setattr(self.module_to_mock, 'reactor', self.old_module)

    @setup
    def teardown_mock_reactor_calls(self):
        self.reactor = Turtle()
        setattr(self.module_to_mock, 'reactor', self.reactor)


class Turtle(object):
    """A more complete Mock implementation."""
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
        self.calls = []
        self.returns = []

    def __getattr__(self, name):
        self.__dict__[name] = type(self)()
        return self.__dict__[name]

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        new_turtle = type(self)()
        self.returns.append(new_turtle)
        return new_turtle
