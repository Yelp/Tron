import logging
import os
import pickle
import signal
import time
from collections import defaultdict
from collections import deque

from twisted.internet import reactor

log = logging.getLogger(__name__)


def consume_dequeue(queue, func):
    queue_length = len(queue)
    for _ in range(queue_length):
        func(queue.popleft())


class EventBus:
    instance = None

    @staticmethod
    def create(log_dir):
        """Create log directory and link to current log if those don't
        already exist"""
        EventBus.shutdown()
        eb = EventBus(log_dir)

        if not os.path.exists(eb.log_dir):
            log.warning(f"creating {eb.log_dir}")
            os.mkdir(eb.log_dir)

        if not os.path.exists(eb.log_current) or not os.path.exists(
            os.readlink(eb.log_current)
        ):
            log.warning(f"creating {eb.log_current}")
            eb.sync_save_log("initial save")

        EventBus.instance = eb
        return eb

    @staticmethod
    def start():
        if not EventBus.instance:
            return
        return EventBus.instance._start()

    @staticmethod
    def shutdown():
        if not EventBus.instance:
            return
        EventBus.instance._shutdown()
        EventBus.instance = None

    @staticmethod
    def publish(event):
        if not EventBus.instance:
            return
        return EventBus.instance._publish(event)

    @staticmethod
    def subscribe(prefix, subscriber, callback):
        if not EventBus.instance:
            return
        return EventBus.instance._subscribe(prefix, subscriber, callback)

    @staticmethod
    def clear_subscriptions(subscriber):
        if not EventBus.instance:
            return
        return EventBus.instance._clear_subscriptions(subscriber)

    @staticmethod
    def has_event(event):
        if not EventBus.instance:
            return
        return EventBus.instance._has_event(event)

    @staticmethod
    def discard(event):
        if not EventBus.instance:
            return
        return EventBus.instance._discard(event)

    def __init__(self, log_dir):
        self.enabled = False
        self.event_log = {}
        self.event_subscribers = defaultdict(list)
        self.publish_queue = deque()
        self.subscribe_queue = deque()
        self.clear_subscription_queue = deque()
        self.log_dir = log_dir
        self.log_current = os.path.join(self.log_dir, "current")
        self.log_updates = 0
        self.log_last_save = 0
        self.log_save_interval = 60  # save every minute
        self.log_save_updates = 100  # save every 100 updates

    def _start(self):
        self.enabled = True
        log.info("starting")
        self.sync_load_log()
        reactor.callLater(0, self.sync_loop)

    def _shutdown(self):
        if self.enabled:
            self.enabled = False
            self.sync_save_log("shutdown")
            log.info("shutdown completed")

    def _publish(self, event):
        if isinstance(event, str):
            event = {'id': event}
        if isinstance(event, dict):
            self.publish_queue.append(event)
            log.debug(f"publish of {event['id']} enqueued")
            return True
        else:
            log.error(f"can't publish {event!r}, must be dict")
            return False

    def _discard(self, event) -> bool:
        if not self._has_event(event):
            return False
        del self.event_log[event]
        return True

    def _subscribe(self, prefix, subscriber, callback):
        self.subscribe_queue.append((prefix, subscriber, callback))
        log.debug(f"subscription ({prefix}, {subscriber}) enqueued")

    def _clear_subscriptions(self, subscriber):
        self.clear_subscription_queue.append(subscriber)
        log.debug(f"clearing subscriptions for {subscriber}")

    def _has_event(self, event_id):
        return event_id in self.event_log

    def sync_load_log(self):
        started = time.time()
        with open(self.log_current, 'rb') as f:
            self.event_log = pickle.load(f)
        duration = time.time() - started
        log.info(f"log read from disk, took {duration:.4}s")

    def sync_save_log(self, reason):
        started = time.time()
        new_file = os.path.join(self.log_dir, f"{int(started)}.pickle")
        try:
            with open(new_file, 'xb') as f:
                pickle.dump(self.event_log, f)
        except FileExistsError:
            log.exception(
                f"unable to dump the log, file {new_file} already exists, "
                f"too many updates/sec? current: {self.log_updates}, "
                f"threshold: {self.log_save_updates}"
            )
            return False

        # atomically replace `current` symlink
        tmplink = os.path.join(self.log_dir, "tmp")
        try:
            os.remove(tmplink)
        except FileNotFoundError:
            pass
        os.symlink(new_file, tmplink)
        os.replace(tmplink, self.log_current)

        duration = time.time() - started
        log.info(f"log dumped to disk because {reason}, took {duration:.4}s")
        return True

    def sync_loop(self):
        if not self.enabled:
            return

        try:
            self.sync_process()
        except Exception:
            log.error("eventbus exception:", exc_info=1)
            os.kill(os.getpid(), signal.SIGTERM)

        reactor.callLater(1, self.sync_loop)

    def sync_process(self):
        save_reason = None
        if time.time() > self.log_last_save + self.log_save_interval:
            if self.log_updates > 0:
                save_reason = (
                    f"{self.log_save_interval}s passed, "
                    f"{self.log_updates} updates"
                )
            else:
                self.log_last_save = time.time()
                log.debug("skipping save, no updates")
        elif self.log_updates > self.log_save_updates:
            save_reason = f"{self.log_save_updates} updates"

        if save_reason and self.sync_save_log(save_reason):
            self.log_last_save = time.time()
            self.log_updates = 0

        consume_dequeue(self.subscribe_queue, self.sync_subscribe)
        consume_dequeue(
            self.clear_subscription_queue, self.sync_clear_subscriptions
        )
        consume_dequeue(self.publish_queue, self.sync_publish)

    def sync_publish(self, event):
        event = pickle.loads(pickle.dumps(event))
        event_id = event['id']
        del event['id']
        if event_id in self.event_log:
            if self.event_log[event_id] != event:
                log.info(f"replacing event: {event_id}")
            else:
                log.debug(f"duplicate event: {event}")
                return

        self.event_log[event_id] = event
        self.log_updates += 1
        log.debug(f"event stored: {event_id} {event}")

        reactor.callLater(0, self.sync_notify, event_id)

    def sync_subscribe(self, prefix_subscriber_cb):
        prefix, subscriber, cb = prefix_subscriber_cb
        self.event_subscribers[prefix].append((subscriber, cb))
        log.debug(f"subscriber registered: {prefix_subscriber_cb}")

    def sync_unsubscribe(self, prefix_sub):
        prefix, sub = prefix_sub

        if prefix not in self.event_subscribers:
            log.debug(f"can't unsubscribe, not found for prefix {prefix}")
            return

        new_subs = [
            sub_cb for sub_cb in self.event_subscribers[prefix]
            if sub_cb[0] != sub
        ]
        if new_subs:
            self.event_subscribers[prefix] = new_subs
        else:
            del self.event_subscribers[prefix]
        log.debug(f"subscription removed: {prefix} / {sub}")

    def sync_clear_subscriptions(self, subscriber):
        new_subscriptions = defaultdict(list)
        removed = 0
        for prefix, subs in self.event_subscribers.items():
            for (sub, cb) in subs:
                if sub == subscriber:
                    removed += 1
                    continue
                new_subscriptions[prefix].append((sub, cb))
        self.event_subscribers = new_subscriptions

        if removed > 0:
            log.debug(f"subscriptions of {subscriber} removed: {removed}")

    def sync_notify(self, event_id):
        event = self.event_log[event_id]
        log.debug(f"notifying subscribers about {event_id}")
        for prefix, subscribers in self.event_subscribers.items():
            log.debug(f"check {prefix}: {event_id.startswith(prefix)}")
            if event_id.startswith(prefix):
                for (sub, cb) in subscribers:
                    log.debug(f"notifying {sub} about {event_id}")
                    reactor.callLater(0, cb, dict(id=event_id, **event))
