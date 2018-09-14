import os
import tempfile

import mock

from testifycompat import assert_equal
from testifycompat import setup
from testifycompat import teardown
from testifycompat import TestCase
from tron import eventbus


class MakeEventBusTestCase(TestCase):
    @setup
    def setup(self):
        self.logdir = tempfile.TemporaryDirectory()

    @teardown
    def teardown(self):
        self.logdir.cleanup()

    @mock.patch('tron.eventbus.time', autospec=True)
    def test_setup_eventbus_dir(self, time):
        os.rmdir(self.logdir.name)

        time.time = mock.Mock(return_value=1.0)
        eb = eventbus.make_eventbus(self.logdir.name)
        assert os.path.exists(self.logdir.name)
        assert os.path.exists(os.path.join(self.logdir.name, "current"))

        time.time = mock.Mock(return_value=2.0)
        eb.event_log = {'foo': 'bar'}
        eb.sync_save_log("test")

        new_eb = eventbus.make_eventbus(self.logdir.name)
        new_eb.sync_load_log()
        assert new_eb.event_log == eb.event_log


class EventBusTestCase(TestCase):
    @setup
    def setup(self):
        self.log_dir = tempfile.TemporaryDirectory(prefix="tron_eventbus_test")
        self.eventbus = eventbus.make_eventbus(self.log_dir.name)
        self.eventbus.enabled = True

    @teardown
    def teardown(self):
        self.log_dir.cleanup()

    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_start(self, reactor):
        self.eventbus.sync_load_log = mock.Mock()
        reactor.callLater = mock.Mock()
        self.eventbus.start()
        assert self.eventbus.sync_load_log.call_count is 1
        assert reactor.callLater.call_count is 1

    def test_shutdown(self):
        assert self.eventbus.enabled
        self.eventbus.sync_save_log = mock.Mock()
        self.eventbus.shutdown()
        assert not self.eventbus.enabled
        assert self.eventbus.sync_save_log.call_count == 1

    def test_publish(self):
        evt = {'id': 'foo'}
        self.eventbus.publish(evt)
        assert self.eventbus.publish_queue.pop() is evt

    def test_subscribe(self):
        ps = ('foo', 'bar', 'cb')
        self.eventbus.subscribe(*ps)
        assert self.eventbus.subscribe_queue.pop() == ps

    def test_has_event(self):
        assert not self.eventbus.has_event('foo')
        self.eventbus.event_log['foo'] = 'bar'
        assert self.eventbus.has_event('foo')

    @mock.patch('tron.eventbus.time', autospec=True)
    def test_sync_load_log(self, time):
        time.time = mock.Mock(return_value=1.0)
        self.eventbus.event_log = {'foo': 'bar'}
        self.eventbus.sync_save_log("test")
        self.eventbus.event_log = {}
        self.eventbus.sync_load_log()
        assert self.eventbus.event_log == {'foo': 'bar'}

    @mock.patch('tron.eventbus.time', autospec=True)
    def test_sync_save_log_time(self, time):
        time.time = mock.Mock(return_value=1.0)
        self.eventbus.sync_save_log("test")
        current_link = os.readlink(self.eventbus.log_current)
        assert_equal(current_link, os.path.join(self.log_dir.name, "1.pickle"))

        time.time = mock.Mock(return_value=2.0)
        self.eventbus.sync_save_log("test")
        new_link = os.readlink(self.eventbus.log_current)
        assert_equal(new_link, os.path.join(self.log_dir.name, "2.pickle"))
        assert os.path.exists(current_link)
        assert os.path.exists(new_link)

    @mock.patch('tron.eventbus.time', autospec=True)
    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_sync_loop(self, reactor, time):
        time.time = mock.Mock(return_value=0)
        reactor.callLater = mock.Mock()
        self.eventbus.enabled = True
        self.eventbus.sync_shutdown = mock.Mock()
        self.eventbus.sync_loop()
        assert reactor.callLater.call_count is 1
        assert self.eventbus.sync_shutdown.call_count is 0

    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_sync_loop_shutdown(self, reactor):
        reactor.callLater = mock.Mock()
        self.eventbus.enabled = False
        self.eventbus.sync_save_log = mock.Mock()
        self.eventbus.sync_loop()
        assert reactor.callLater.call_count is 0

    @mock.patch('tron.eventbus.time', autospec=True)
    def test_sync_process_save_log(self, time):
        time.time = mock.Mock(return_value=10)
        self.eventbus.log_updates = 1
        self.eventbus.log_last_save = 0
        self.eventbus.log_save_interval = 20
        self.eventbus.sync_save_log = mock.Mock()
        self.eventbus.sync_process()
        assert self.eventbus.sync_save_log.call_count is 0

        time.time = mock.Mock(return_value=21)
        self.eventbus.sync_process()
        assert self.eventbus.sync_save_log.call_count is 1

        time.time = mock.Mock(return_value=0)
        self.eventbus.log_updates = 0
        self.eventbus.log_save_updates = 20
        self.eventbus.sync_save_log = mock.Mock()
        self.eventbus.sync_process()
        assert self.eventbus.sync_save_log.call_count is 0

        self.eventbus.log_updates = 21
        self.eventbus.sync_process()
        assert self.eventbus.sync_save_log.call_count is 1
        assert self.eventbus.log_updates is 0

    @mock.patch('tron.eventbus.time', autospec=True)
    def test_sync_process_flush_queues(self, time):
        time.time = mock.Mock(return_value=10)
        self.eventbus.sync_subscribe = mock.Mock()
        self.eventbus.sync_publish = mock.Mock()

        for _ in range(5):
            self.eventbus.publish_queue.append(mock.Mock())
            self.eventbus.subscribe_queue.append(mock.Mock())

        self.eventbus.sync_process()

        assert_equal(self.eventbus.sync_subscribe.call_count, 5)
        assert_equal(self.eventbus.sync_publish.call_count, 5)

    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_sync_publish(self, reactor):
        reactor.callLater = mock.Mock()
        evt = {'id': 'foo', 'bar': 'baz'}
        self.eventbus.event_log = {}
        self.eventbus.log_save_updates = 0
        self.eventbus.sync_publish(evt)
        assert self.eventbus.log_updates is 1
        assert reactor.callLater.call_count is 1

    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_sync_publish_replace(self, reactor):
        evt1 = {'id': 'foo', 'bar': 'baz'}
        evt2 = {'id': 'foo', 'bar': 'quux'}
        self.eventbus.event_log = {}
        self.eventbus.log_save_updates = 0
        self.eventbus.sync_publish(evt1)
        self.eventbus.sync_publish(evt2)
        assert self.eventbus.log_updates is 2
        assert reactor.callLater.call_count is 2

    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_sync_publish_duplicate(self, reactor):
        evt = {'id': 'foo', 'bar': 'baz'}
        self.eventbus.event_log = {'foo': {'bar': 'baz'}}
        self.eventbus.log_save_updates = 0
        self.eventbus.sync_publish(evt)
        assert self.eventbus.log_updates is 0
        assert reactor.callLater.call_count is 0

    def test_sync_subscribe(self):
        self.eventbus.event_subscribers = {}
        self.eventbus.sync_subscribe(('pre', 'sub', 'cb'))
        assert self.eventbus.event_subscribers == {'pre': [('sub', 'cb')]}

        self.eventbus.sync_subscribe(('pre', 'sub2', 'cb2'))
        assert self.eventbus.event_subscribers == {
            'pre': [('sub', 'cb'), ('sub2', 'cb2')]
        }

    def test_sync_unsubscribe(self):
        self.eventbus.event_subscribers = {}
        self.eventbus.sync_subscribe(('pre', 'sub', 'cb'))
        self.eventbus.sync_subscribe(('pre', 'sub2', 'cb2'))
        assert self.eventbus.event_subscribers == {
            'pre': [('sub', 'cb'), ('sub2', 'cb2')]
        }

        self.eventbus.sync_unsubscribe(('pre', 'sub'))
        assert self.eventbus.event_subscribers == {'pre': [('sub2', 'cb2')]}
        self.eventbus.sync_unsubscribe(('pre', 'sub2'))
        assert self.eventbus.event_subscribers == {}

    @mock.patch('tron.eventbus.reactor', autospec=True)
    def test_sync_notify(self, reactor):
        reactor.callLater = mock.Mock()
        self.eventbus.event_log = {'p': {}, 'pre': {}, 'prefix': {}}
        self.eventbus.event_subscribers = {
            'pre': [('sub', 'm1')],
            'prefix': [('sub', 'm2'), ('sub2', 'm3')]
        }

        self.eventbus.sync_notify('p')
        assert reactor.callLater.call_count is 0

        self.eventbus.sync_notify('pre')
        assert reactor.callLater.call_count is 1

        self.eventbus.sync_notify('prefix')
        assert reactor.callLater.call_count is 4
