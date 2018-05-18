from __future__ import absolute_import
from __future__ import unicode_literals

import six
from testify import assert_equal
from testify import assert_raises
from testify import setup
from testify import teardown
from testify import TestCase

from tests.assertions import assert_length
from tron import event


class EventStoreTestCase(TestCase):
    @setup
    def build_store(self):
        self.limits = {
            event.LEVEL_INFO: 2,
            event.LEVEL_CRITICAL: 3,
        }
        self.store = event.EventStore(self.limits)

    def _build_event(self, level, name):
        return event.Event('entity', level, name)

    @setup
    def add_data(self):
        for i in range(1, 5):
            self.store.append(
                self._build_event(
                    event.LEVEL_INFO,
                    "test%s" % i,
                ),
            )

        for i in range(5, 10):
            e = self._build_event(event.LEVEL_CRITICAL, "test%s" % i)
            self.store.append(e)

        self.store.append(self._build_event(event.LEVEL_OK, "alpha"))

    def test_build_deque(self):
        deq = self.store._build_deque('stars')
        deq.extend(range(12))
        assert_equal(len(deq), event.EventStore.DEFAULT_LIMIT)

    def test_append(self):
        assert_equal(len(self.store.events), 3)
        for level, limit in six.iteritems(self.limits):
            assert_equal(len(self.store.events[level]), limit)

    def test_get_events(self):
        values = {e.name for e in self.store.get_events()}
        expected = {'test3', 'test4', 'test7', 'test8', 'test9', 'alpha'}
        assert_equal(values, expected)

    def test_get_events_with_min_level(self):
        values = {e.name for e in self.store.get_events(event.LEVEL_OK)}
        expected = {'test7', 'test8', 'test9', 'alpha'}
        assert_equal(values, expected)


class EventRecorderTestCase(TestCase):
    @setup
    def build_recorders(self):
        self.entity_name = 'the_name'
        self.recorder = event.EventRecorder(self.entity_name)

    def test_get_child(self):
        child_rec = self.recorder.get_child('start')
        assert_equal(self.recorder.get_child('start'), child_rec)
        assert_equal(child_rec.name, 'the_name.start')

    def test_get_child_missing(self):
        child_rec = self.recorder.get_child('next')
        assert_equal(child_rec.name, 'the_name.next')
        assert_equal(self.recorder.children['next'], child_rec)

    def test_remove_child(self):
        self.recorder.get_child('next')
        self.recorder.remove_child('next')
        assert 'next' not in self.recorder.children

    def test_remove_child_missing(self):
        self.recorder.remove_child('bogus')
        assert 'bogus' not in self.recorder.children

    def test_record(self):
        self.recorder._record(event.LEVEL_CRITICAL, 'this thing')
        recorded_event = self.recorder.events.events[event.LEVEL_CRITICAL][0]
        assert_equal(recorded_event.level, event.LEVEL_CRITICAL)
        assert_equal(recorded_event.name, 'this thing')
        assert_equal(recorded_event.entity, self.entity_name)

    def test_list_with_children(self):
        self.recorder.ok('one')
        self.recorder.notice('two')
        child_rec = self.recorder.get_child('stars')
        child_rec.critical('three')
        child_rec.ok('four')
        self.recorder.info('five')

        events = self.recorder.list()
        expected = reversed(['one', 'two', 'three', 'four', 'five'])
        assert_equal([e.name for e in events], list(expected))

    def test_list_without_children(self):
        self.recorder.ok('one')
        self.recorder.notice('two')
        child_rec = self.recorder.get_child('stars')
        child_rec.critical('three')
        child_rec.ok('four')
        self.recorder.info('five')

        events = self.recorder.list(child_events=False)
        expected = ['five', 'two', 'one']
        assert_equal([e.name for e in events], expected)

    def test_list_no_events(self):
        assert_length(self.recorder.list(), 0)
        assert_length(self.recorder.list(child_events=False), 0)


class EventManagerTestCase(TestCase):
    @setup
    def setup_manager(self):
        self.manager = event.EventManager.get_instance()
        self.root = self.manager.root_recorder

    @teardown
    def teardown_manager(self):
        self.manager.reset()

    def test_get_instance(self):
        assert_equal(self.manager, event.EventManager.get_instance())
        assert_raises(ValueError, event.EventManager)

    def test_get_root(self):
        recorder = self.manager.get('')
        assert_equal(recorder, self.root)

    def test_get_nested(self):
        name = 'one.two'
        recorder = self.manager.get(name)
        assert_equal(self.manager.get(name), recorder)
        assert_equal(recorder.name, name)

    def test_get_missing(self):
        name = 'name.second.third'
        recorder = self.manager.get(name)
        assert_equal(
            self.root.children['name'].children['second'].children['third'],
            recorder,
        )
        assert_equal(recorder.name, name)

    def test_remove(self):
        self.manager.get('one')
        self.manager.remove('one')
        assert not self.manager.root_recorder.children.get('one')

    def test_remove_nested(self):
        self.manager.get('one.two')
        self.manager.remove('one.two')
        assert not self.root.children['one'].children.get('two')
        assert self.root.children['one']

    def test_remove_missing_nested(self):
        self.manager.get('one')
        self.manager.remove('one.two')
        assert not self.root.children['one'].children.get('two')
        assert self.root.children['one']

    def test_remove_missing(self):
        self.manager.remove('bogus')
        assert not self.manager.root_recorder.children.get('bogus')
