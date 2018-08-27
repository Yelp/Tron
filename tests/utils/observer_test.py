from __future__ import absolute_import
from __future__ import unicode_literals

from unittest import mock

from testifycompat import assert_equal
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_length
from tron.utils.observer import Observable
from tron.utils.observer import Observer


class TestObservable(TestCase):
    @setup
    def setup_observer(self):
        self.obs = Observable()

    def test_attach(self):
        def func():
            return 1

        self.obs.attach('a', func)
        assert_equal(len(self.obs._observers), 1)
        assert_equal(self.obs._observers['a'], [func])

    def test_listen_seq(self):
        def func():
            return 1

        self.obs.attach(['a', 'b'], func)
        assert_equal(len(self.obs._observers), 2)
        assert_equal(self.obs._observers['a'], [func])
        assert_equal(self.obs._observers['b'], [func])

    def test_notify(self):
        handler = mock.MagicMock()
        self.obs.attach(['a', 'b'], handler)
        self.obs.notify('a')
        assert_equal(len(handler.handler.mock_calls), 1)
        self.obs.notify('b')
        assert_equal(len(handler.handler.mock_calls), 2)


class TestObserverClear(TestCase):
    @setup
    def setup_observer(self):
        self.obs = Observable()

        def func():
            return 1

        self.obs.attach('a', func)
        self.obs.attach('b', func)
        self.obs.attach(True, func)
        self.obs.attach(['a', 'b'], func)

    def test_clear_listeners_all(self):
        self.obs.clear_observers()
        assert_equal(len(self.obs._observers), 0)

    def test_clear_listeners_some(self):
        self.obs.clear_observers('a')
        assert_equal(len(self.obs._observers), 2)
        assert_equal(set(self.obs._observers.keys()), {True, 'b'})

    def test_remove_observer_none(self):
        def observer():
            return 2

        self.obs.remove_observer(observer)
        assert_equal(set(self.obs._observers.keys()), {True, 'a', 'b'})
        assert_length(self.obs._observers['a'], 2)
        assert_length(self.obs._observers['b'], 2)
        assert_length(self.obs._observers[True], 1)

    def test_remove_observer(self):
        def observer():
            return 2

        self.obs.attach('a', observer)
        self.obs.attach('c', observer)
        self.obs.remove_observer(observer)
        assert_length(self.obs._observers['a'], 2)
        assert_length(self.obs._observers['b'], 2)
        assert_length(self.obs._observers[True], 1)
        assert_length(self.obs._observers['c'], 0)


class MockObserver(Observer):
    def __init__(self, obs, event):
        self.obs = obs
        self.event = event
        self.watch(obs, event)
        self.has_watched = 0

    def handler(self, obs, event):
        assert_equal(obs, self.obs)
        assert_equal(event, self.event)
        self.has_watched += 1


class TestObserver(TestCase):
    @setup
    def setup_observer(self):
        self.obs = Observable()

    def test_watch(self):
        event = "FIVE"
        handler = MockObserver(self.obs, event)

        self.obs.notify(event)
        assert_equal(handler.has_watched, 1)
        self.obs.notify("other event")
        assert_equal(handler.has_watched, 1)
        self.obs.notify(event)
        assert_equal(handler.has_watched, 2)


if __name__ == "__main__":
    run()
