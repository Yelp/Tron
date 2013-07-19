#!/usr/bin/env python
"""This process is spawned by trond in order to offload state save/load
operations such that trond can focus on the more important things without
blocking for large chunks of time. It takes arguments in the main method
passed by python's multiprocessing module in order to configure itself and use
the correct methods for state saving and message transport with trond.

Messages are sent via Pipes (also part of python's multiprocessing module).
This allows for easy polling and no need to handle chunking of messages.

The process intercepts the two shutdown signals (SIGINT and SIGTERM) in order
to prevent the process from exiting early when trond wants to do some final
shutdown things (realistically, it should be handling all shutdown operations
as this is a child process.)
"""
import time
import signal
from threading import Thread, Lock
from Queue import Queue, Empty

from tron.serialize.runstate.tronstore.messages import StoreRequestFactory, StoreResponseFactory
from tron.serialize.runstate.tronstore import store
from tron.serialize.runstate.tronstore import msg_enums

# This timeout MUST BE SHORTER than the one in process.py!
# Seriously, if this is longer, everything will break!
SHUTDOWN_TIMEOUT = 3.0
POOL_SIZE = 35


def shutdown_handler(signum, frame):
    """This is just here to stop tronstore from exiting early. The process
    will be terminated from the main tron daemon when all requests have been
    finished. This is needed because Python propogates signals to
    spawned processes, and tronstore is going to get a TON of requests whenever
    a SIGINT is sent to the main daemon, as it has to save everything before
    it can gracefully shut down."""
    pass


def parse_config(config):
    """Parse the configuration file and set up the store class."""
    name = config.name
    transport_method = config.transport_method
    store_type = config.store_type
    connection_details = config.connection_details
    db_store_method = config.db_store_method

    return (store.build_store(name, store_type, connection_details, db_store_method), transport_method)

def get_all_from_pipe(pipe):
    """Gets all of the requests from the pipe, returning an array of serialized
    requests (they still need to be decoded).
    """
    requests = []
    while pipe.poll():
        requests.append(pipe.recv_bytes())
    return requests

def handle_request(request, store_class, pipe, factory, save_lock, restore_lock):
    """Handle a request by acting on store_class with the appropriate action.

    This is run in a separate thread. As such, there's two mutexes here- one
    for the save requests, and one for the restore requests."""

    if request.req_type == msg_enums.REQUEST_SAVE:
        with save_lock:
            success = store_class.save(request.data[0], request.data[1], request.data_type)
        pipe.send_bytes(factory.build(success, request.id, '').serialized)

    elif request.req_type == msg_enums.REQUEST_RESTORE:
        with restore_lock:
            success, data = store_class.restore(request.data, request.data_type)
        pipe.send_bytes(factory.build(success, request.id, data).serialized)

    else:
        pipe.send_bytes(factory.build(False, request.id, '').serialized)


def _remove_finished_threads(running_threads):
    """A small helper function to clean out the running_threads array.
    Doesn't actually create a new instance of a list; it modifies
    the existing list as a side effect, and returns the number
    of running threads that it cleaned up."""
    counter = 0
    for i in range(len(running_threads) - 1, -1, -1):
        if not running_threads[i].is_alive():
            running_threads.pop(i)
            counter += 1
    return counter


def thread_starter(queue, running_threads):
    """A method to start threads that have been queued up in queue. Also takes
    a reference to a list (running_threads) that this function will store any
    threads it has started in, so the main method knows if there's still
    currently executing requests.

    Keep in mind because running_threads is a reference to a single instance
    of a list object, it CANNOT be reassigned to another instance in order to
    allow the main thread to know what's running. As such, all operations on
    running_threads must be method calls to modify the list instance given
    to this thread."""
    global is_shutdown, POOL_SIZE

    pool_counter = POOL_SIZE

    while not is_shutdown or not queue.empty():
        pool_counter += _remove_finished_threads(running_threads)

        if pool_counter <= 0:
            time.sleep(0.5)
            continue

        try:
            thread = queue.get(timeout=0.5)
            thread.start()
            pool_counter -= 1
            running_threads.append(thread)
        except Empty:
            continue

    while len(running_threads) != 0:
        _remove_finished_threads(running_threads)


def main(config, pipe):
    """The main run loop for tronstore. This loop sets up everything
    based on the configuration tronstore got, and then simply
    waits for requests to handle from pipe. It spawns threads for
    save and restore requests, which will send responses back over
    the pipe once completed."""
    global is_shutdown, SHUTDOWN_TIMEOUT
    is_shutdown = False

    store_class, transport_method = parse_config(config)

    request_factory = StoreRequestFactory(transport_method)
    response_factory = StoreResponseFactory(transport_method)
    save_lock = Lock()
    restore_lock = Lock()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    running_threads = []
    thread_queue = Queue()
    thread_pool = Thread(target=thread_starter, args=(thread_queue, running_threads))
    thread_pool.daemon = True
    thread_pool.start()

    while True:
        timeout = SHUTDOWN_TIMEOUT if is_shutdown else None
        try:
            if pipe.poll(timeout):
                requests = get_all_from_pipe(pipe)
                requests = map(request_factory.rebuild, requests)
                for request in requests:
                    if request.req_type == msg_enums.REQUEST_SHUTDOWN:
                        is_shutdown = True
                        shutdown_req_id = request.id

                    elif request.req_type == msg_enums.REQUEST_CONFIG:
                        while len(running_threads) != 0:
                            time.sleep(0.5)
                        store_class.cleanup()
                        store_class, transport_method = parse_config(request.data)
                        request_factory.update_method(transport_method)
                        response_factory.update_method(transport_method)

                    else:
                        request_thread = Thread(target=handle_request,
                            args=(
                                request,
                                store_class,
                                pipe,
                                response_factory,
                                save_lock,
                                restore_lock))
                        request_thread.daemon = True
                        thread_queue.put(request_thread)
            else:
                # We have to wait for all requests to clean up first.
                while len(running_threads) != 0:
                    time.sleep(0.5)
                store_class.cleanup()
                pipe.send_bytes(response_factory.build(True, shutdown_req_id, '').serialized)
                return
        # Signals cause pipe.poll to throw IOErrors...
        except IOError:
            continue
