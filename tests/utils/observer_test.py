from testify import run, setup, assert_equal, TestCase, turtle
from tron.utils.observer import Observable


class ObserverTestCase(TestCase):

    @setup
    def setup_observer(self):
        self.obs = Observable()

    def test_listen(self):
        func = lambda: 1
        self.obs.listen('a', func)
        assert_equal(len(self.obs._listeners), 1)
        assert_equal(self.obs._listeners['a'], [func])

    def test_listen_seq(self):
        func = lambda: 1
        self.obs.listen(['a', 'b'], func)
        assert_equal(len(self.obs._listeners), 2)
        assert_equal(self.obs._listeners['a'], [func])
        assert_equal(self.obs._listeners['b'], [func])

    def test_notify(self):
        func = turtle.Turtle()
        self.obs.listen(['a', 'b'], func)
        self.obs.notify('a')
        assert_equal(len(func.calls), 1)
        self.obs.notify('b')
        assert_equal(len(func.calls), 2)

class ObserverClearTestCase(TestCase):

    @setup
    def setup_observer(self):
        self.obs = Observable()
        func = lambda: 1
        self.obs.listen('a', func)
        self.obs.listen('b', func)
        self.obs.listen(True, func)
        self.obs.listen(['a', 'b'], func)

    def test_clear_listeners_all(self):
        self.obs.clear_listeners()
        assert_equal(len(self.obs._listeners), 0)

    def test_clear_listeners_some(self):
        self.obs.clear_listeners('a')
        assert_equal(len(self.obs._listeners), 2)
        assert_equal(set(self.obs._listeners.keys()), set([True, 'b']))





if __name__ == "__main__":
    run()