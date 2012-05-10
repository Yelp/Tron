from testify import run, setup, assert_equal, TestCase, turtle
from tron.utils.observer import Observable, Observer


class ObservableTestCase(TestCase):

    @setup
    def setup_observer(self):
        self.obs = Observable()

    def test_attach(self):
        func = lambda: 1
        self.obs.attach('a', func)
        assert_equal(len(self.obs._observers), 1)
        assert_equal(self.obs._observers['a'], [func])

    def test_listen_seq(self):
        func = lambda: 1
        self.obs.attach(['a', 'b'], func)
        assert_equal(len(self.obs._observers), 2)
        assert_equal(self.obs._observers['a'], [func])
        assert_equal(self.obs._observers['b'], [func])

    def test_notify(self):
        handler = turtle.Turtle()
        self.obs.attach(['a', 'b'], handler)
        self.obs.notify('a')
        assert_equal(len(handler.handler.calls), 1)
        self.obs.notify('b')
        assert_equal(len(handler.handler.calls), 2)

class ObserverClearTestCase(TestCase):

    @setup
    def setup_observer(self):
        self.obs = Observable()
        func = lambda: 1
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
        assert_equal(set(self.obs._observers.keys()), set([True, 'b']))


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


class ObserverTestCase(TestCase):

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