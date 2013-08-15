#!/usr/bin/env python
import time
import signal
import os
import traceback
from threading import Thread, Lock
from Queue import Queue, Empty

from tron.serialize.runstate.tronstore.messages import StoreRequestFactory, StoreResponseFactory
from tron.serialize.runstate.tronstore import store
from tron.serialize.runstate.tronstore import msg_enums


def _discard_signal(signum, frame):
    pass


def _register_null_handlers():
    signal.signal(signal.SIGINT, _discard_signal)
    signal.signal(signal.SIGHUP, _discard_signal)
    signal.signal(signal.SIGTERM, _discard_signal)


def handle_requests(request_queue, resp_factory, pipe, store_class, do_work):
    """Handle requests by acting on store_class with the appropriate action.
    Requests are taken from request_queue until do_work.val (it should be a
    PoolBool) is False.
    This is run in a separate thread.
    """

    # This should probably be lower rather than higher
    WORK_TIMEOUT = 1.0

    while do_work.val or not request_queue.empty():
        try:
            request = request_queue.get(block=True, timeout=WORK_TIMEOUT)

            if request.req_type == msg_enums.REQUEST_SAVE:
                store_class.save(request.data[0], request.data[1], request.data_type)

            elif request.req_type == msg_enums.REQUEST_RESTORE:
                success, data = store_class.restore(request.data, request.data_type)
                pipe.send_bytes(resp_factory.build(success, request.id, data).serialized)

            else:
                pipe.send_bytes(resp_factory.build(False, request.id, '').serialized)

        except Empty:
            continue


class SyncPipe(object):
    """An object to handle synchronization over pipe operations. In particular,
    the send and recv functions have a mutex as they are subject to
    race conditions.
    """

    def __init__(self, pipe):
        self.lock = Lock()
        self.pipe = pipe

    # None is actually a valid timeout (blocks forever), so we need to use
    # something different for checking for a non-supplied kwarg
    def poll(self, *args, **kwargs):
        return self.pipe.poll(*args, **kwargs)

    def send_bytes(self, *args, **kwargs):
        with self.lock:
            return self.pipe.send_bytes(*args, **kwargs)

    def recv_bytes(self, *args, **kwargs):
        with self.lock:
            return self.pipe.recv_bytes(*args, **kwargs)


class PoolBool(object):
    """The PoolBool(TM) is a mutable boolean wrapper used for signaling."""

    def __init__(self, value=True):
        if not value in (True, False):
            raise TypeError('expected boolean, got %r' % value)
        self._val = value

    @property
    def value(self):
        return self._val
    val = value

    def set(self, value):
        if not value in (True, False):
            raise TypeError('expected boolean, got %r' % value)
        self._val = value


class TronstorePool(object):
    """A thread pool with POOL_SIZE workers for handling requests. Enqueues
    save and restore requests into a queue that is then consumed by the
    workers, which send an appropriate response.
    """

    POOL_SIZE = 16

    def __init__(self, resp_fact, pipe, store):
        """Initialize the thread pool. Please make a new pool if any of the
        objects passed to __init__ change.
        """
        self.request_queue    = Queue()
        self.response_factory = resp_fact
        self.pipe             = pipe
        self.store_class      = store
        self.keep_working     = PoolBool(True)
        self.thread_pool      = [Thread(target=handle_requests,
                                args=(
                                    self.request_queue,
                                    self.response_factory,
                                    self.pipe,
                                    self.store_class,
                                    self.keep_working
                                )) for i in range(self.POOL_SIZE)]

    def start(self):
        """Start the thread pool."""
        self.keep_working.set(True)
        for thread in self.thread_pool:
            thread.daemon = False
            thread.start()

    def stop(self):
        """Stop the thread pool."""
        self.keep_working.set(False)
        while self.has_work() \
        or any([thread.is_alive() for thread in self.thread_pool]):
            time.sleep(0.5)

    def enqueue_work(self, work):
        """Enqueue a request for the workers to consume and process."""
        self.request_queue.put(work)

    def has_work(self):
        """Returns whether there is still work to be consumed by workers."""
        return not self.request_queue.empty()

    def work_size(self):
        """Returns the amount of work left to be consumed."""
        return self.request_queue.qsize()


class TronstoreMain(object):
    """The main Tronstore class. Initializes a bunch of stuff and then has a
    main_loop function that loops and handles requests from trond.
    """

    # this can be rather long- it's only real use it to clean up tronstore
    # in case it's orphaned... however, it should be SHORTER than
    # SHUTDOWN_TIMEOUT in process.py. in addition, making this longer
    # can cause trond to take longer to fully shutdown.
    POLL_TIMEOUT = 2.0

    def __init__(self, config, pipe, logger):
        """Sets up the needed objects for Tronstore, including message
        factories, a synchronized pipe and store object, a thread pool for
        handling requests, and some internal invariants.
        """
        self.log              = logger
        self.pipe             = SyncPipe(pipe)
        self.request_factory  = StoreRequestFactory()
        self.response_factory = StoreResponseFactory()
        self.store_class      = store.SyncStore(config, logger)
        self.thread_pool      = TronstorePool(self.response_factory, self.pipe,
                                    self.store_class)
        self.is_shutdown      = False
        self.shutdown_req_id  = None
        self.config           = config

    def _get_all_from_pipe(self):
        """Gets all of the requests from the pipe, returning an array of serialized
        requests (they still need to be decoded).
        """
        requests = []
        while self.pipe.poll():
            requests.append(self.pipe.recv_bytes())
        return requests

    def _reconfigure(self, request):
        """Reconfigures Tronstore by attempting to make a new store object
        from the recieved configuration. If anything goes wrong, we revert
        back to the old configuration.
        """
        self.log.warn('Loading new configuration...')
        self.thread_pool.stop()
        self.store_class.cleanup()
        self.log.info('Cleaned up old store objects.')
        try:
            self.log.debug('Attempting to create new store object...')
            self.store_class = store.SyncStore(request.data, self.log)
            self.log.debug('Created %r.' % self.store_class)

            self.log.debug('Creating new thread pool...')
            self.thread_pool = TronstorePool(self.response_factory, self.pipe,
                                    self.store_class)
            self.thread_pool.start()
            self.log.debug('Thread pool is running.')

            self.log.debug('Sending response that configuration was successful...')
            self.config = request.data
            self.pipe.send_bytes(self.response_factory.build(True, request.id, '').serialized)
            self.log.info('Configuration loaded successfully.')
        except:
            self.log.exception('Error encountered when loading config')

            self.log.debug('Recreating old store object...')
            self.store_class = store.SyncStore(self.config, self.log)
            self.log.debug('Created %r.' % self.store_class)

            self.log.debug('Recreating old thread pool...')
            self.thread_pool = TronstorePool(self.response_factory, self.pipe,
                                    self.store_class)
            self.thread_pool.start()
            self.log.debug('Thread pool is running.')

            self.log.debug('Sending response that configuration failed...')
            self.pipe.send_bytes(self.response_factory.build(False, request.id, '').serialized)
            self.log.error('Failed to load configuration, reverted to old config.')

    def _handle_request(self, request):
        """Handle a request by either doing something with it ourselves
        (in the case of shutdown/config), or passing it to a worker in the
        thread pool (for save/restore).
        """
        if request.req_type == msg_enums.REQUEST_SHUTDOWN:
            self.log.warn('Got a shutdown request, shutting down...')
            self.is_shutdown = True
            self.shutdown_req_id = request.id

        elif request.req_type == msg_enums.REQUEST_CONFIG:
            self._reconfigure(request)

        else:
            self.thread_pool.enqueue_work(request)

    def _shutdown(self):
        """Shutdown Tronstore. Calls os._exit, and should only be called
        once all work has been completed.
        """
        self.log.info('Shutting down. There are still %s requests to handle.'
            % self.thread_pool.work_size())
        self.thread_pool.stop()
        self.store_class.cleanup()

        self.log.info('Tronstore is exiting, notifying trond.')
        if self.shutdown_req_id:
            shutdown_resp = self.response_factory.build(True, self.shutdown_req_id, '')
            self.pipe.send_bytes(shutdown_resp.serialized)
        os._exit(0)  # Hard exit- should kill everything.

    def main_loop(self):
        """The main Tronstore event loop. Starts the thread pool and then
        simply polls for requests until a shutdown request is recieved, after
        which it cleans up and exits.
        """
        self.log.info('Tronstore is starting.')
        self.thread_pool.start()

        while True:
            try:
                if self.pipe.poll(self.POLL_TIMEOUT):
                    requests = self._get_all_from_pipe()
                    requests = map(self.request_factory.from_msg, requests)
                    self.log.debug('Received %s requests.' % len(requests))
                    for request in requests:
                        self._handle_request(request)

                elif self.is_shutdown:
                    self._shutdown()

                else:
                    # Did tron die?
                    try:
                        os.kill(os.getppid(), 0)
                    except:
                        self.log.error('trond appears to have died. Shutting down...')
                        self.is_shutdown = True

                if self.thread_pool.work_size() > 100:
                    self.log.warn('Tronstore is falling behind with a queue of %s requests!'
                        % self.thread_pool.work_size())

            except IOError, e:
                # Error #4 is a system interrupt, caused by ^C
                if e.errno != 4:
                    raise


def main(config, pipe, logger):
    """The main method to start Tronstore with. Simply takes the configuration
    and pipe objects, and then registers some null signal handlers before
    passing everything off to TronstoreMain.

    This process is spawned by trond in order to offload state save/load
    operations such that trond can focus on the more important things without
    blocking for chunks of time.

    Messages are sent via Pipes (also part of python's multiprocessing module).
    This allows for easy polling and no need to handle chunking of messages.

    The process intercepts the two shutdown signals (SIGINT and SIGTERM) in order
    to prevent the process from exiting early when trond wants to do some final
    shutdown things (realistically, trond should be handling all shutdown
    operations, as this is a child process.)
    """
    _register_null_handlers()
    tronstore = TronstoreMain(config, pipe, logger)
    tronstore.main_loop()
