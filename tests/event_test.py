from testify import setup, TestCase, assert_equal, teardown, assert_raises
from testify import turtle, assert_in

from tron import event

class FixedLimitStoreTestCase(TestCase):

    @setup
    def build_store(self):
        self.limits = {
            event.LEVEL_INFO:       2,
            event.LEVEL_CRITICAL:   3
        }
        self.store = event.FixedLimitStore(self.limits)

    @setup
    def add_data(self):
        for i in xrange(1,5):
            self.store.append(event.LEVEL_INFO, "test%s" % i)

        for i in xrange(5,10):
            self.store.append(event.LEVEL_CRITICAL, "test%s" % i)

        self.store.append(event.LEVEL_OK, "alpha")

    def test_build_deque(self):
        deq = self.store._build_deque('stars')
        deq.extend(range(12))
        assert_equal(len(deq), event.FixedLimitStore.DEFAULT_LIMIT)

    def test_append(self):
        assert_equal(len(self.store._values), 3)
        for level, limit in self.limits.iteritems():
            assert_equal(len(self.store._values[level]), limit)

    def test__iter__(self):
        values = list(self.store)
        expected = ['test3', 'test4', 'test7', 'test8', 'test9', 'alpha']
        assert_equal(values, expected)


class EventRecorderTestCase(TestCase):

    @setup
    def build_recorders(self):
        self.observable = turtle.Turtle()
        self.parent_recorder = event.EventRecorder(self.observable)
        self.recorder = event.EventRecorder(
                self.observable, parent=self.parent_recorder)

    def test_record_with_parent(self):
        self.recorder.record(event.Event(
                self.observable, event.LEVEL_INFO, "hello"))
        self.recorder.emit_notice("hello again")

        assert_equal(len(self.recorder.list()), 2)
        assert_equal(len(self.parent_recorder.list()), 1)

        assert_equal(len(self.recorder.list(min_level=event.LEVEL_CRITICAL)), 0)
        assert_equal(len(self.recorder.list(min_level=event.LEVEL_NOTICE)), 1)

    def test_handler(self):
        cat_event = event.EventType(event.LEVEL_OK, "a cat happened")
        self.recorder.handler(self.observable, cat_event)
        assert_equal(len(self.recorder.list()), 1)
        first_event = self.recorder.list()[0]
        assert_equal(first_event.level, cat_event.level)
        assert_equal(first_event.name,  cat_event.name)

    def test_handler_non_event_type_event(self):
        non_event = ('bogus', 'thing')
        self.recorder.handler(self.observable, non_event)
        assert_equal(len(self.recorder.list()), 0)


class EntitySwapTestCase(TestCase):
    """EventRecorder should be able to swap out the underlying entity, and all
    the associated events should be updated
    """

    @setup
    def build_recorder(self):
        self.entity = turtle.Turtle()
        self.orig_entity = turtle.Turtle()
        self.recorder = event.EventRecorder(self.orig_entity)

    @setup
    def create_event(self):
        self.recorder.emit_notice("hello")

    def test(self):
        assert self.entity != self.orig_entity
        self.recorder.entity = self.entity

        evt = self.recorder.list()[0]
        assert_equal(evt.entity, self.entity)


class EventManagerTestCase(TestCase):

    class MockObservable(turtle.Turtle):
        def __str__(self):
            return 'thisid'

    @setup
    def setup_manager(self):
        self.manager = event.EventManager.get_instance()
        self.observable = self.MockObservable()

    @teardown
    def teardown_manager(self):
        self.manager.clear()

    def test_get_instance(self):
        assert_equal(self.manager, event.EventManager.get_instance())
        assert_raises(ValueError, event.EventManager)

    def test_build_key(self):
        assert_equal(self.manager._build_key(self.observable), "thisid")

    def test_add(self):
        recorder = self.manager.add(self.observable)
        assert_equal(recorder.entity, self.observable)
        assert_in(recorder, self.manager.recorders.values())

    def test_add_duplicate(self):
        self.manager.add(self.observable)
        assert_raises(ValueError, self.manager.add, self.observable)

    def test_add_parent(self):
        parent = turtle.Turtle()
        parent_recorder = self.manager.add(parent)
        recorder = self.manager.add(self.observable, parent)
        assert_equal(recorder._parent(), parent_recorder)

    def test_add_missing_parent(self):
        parent = turtle.Turtle()
        recorder = self.manager.add(self.observable, parent)
        assert_equal(recorder._parent, None)

    def test_get(self):
        recorder = self.manager.add(self.observable)
        assert_equal(self.manager.get(self.observable), recorder)
        assert_equal(recorder.entity, self.observable)

    def test_get_missing(self):
        assert self.manager.get(self.observable) is None
