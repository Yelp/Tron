import signal
import logging
import os
from multiprocessing import Process, Pipe

from tron.serialize.runstate.tronstore import tronstore
from tron.serialize.runstate.tronstore.messages import StoreResponseFactory

log = logging.getLogger(__name__)


class TronStoreError(Exception):
    """Raised whenever tronstore exits for an unknown reason."""
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return repr(self.code)


class StoreProcessProtocol(object):
    """The class that actually communicates with tronstore. This is a subclass
    of the twisted ProcessProtocol class, which has a set of internals that can
    communicate with a child proccess via stdin/stdout via interrupts.

    Because of this I/O structure imposed by twisted, there are two types of
    messages: requests and responses. Responses are always of the same form,
    while requests have an enumerator (see msg_enums.py) to identify the
    type of request.
    """
    # This timeout MUST be longer than the POLL_TIMEOUT in tronstore!
    SHUTDOWN_TIMEOUT = 100.0
    POLL_TIMEOUT = 10.0

    def __init__(self):
        self.config = None
        self.response_factory = StoreResponseFactory()
        self.orphaned_responses = {}
        self.is_shutdown = False
        self._start_process()

    def _start_process(self):
        """Spawn the tronstore process. The arguments given to tronstore must
        match the signature for tronstore.main.
        """
        self.pipe, child_pipe = Pipe()
        store_args = (self.config, child_pipe)

        self.process = Process(target=tronstore.main, args=store_args)
        self.process.daemon = True
        self.process.start()

    def _verify_is_alive(self):
        """A check to verify that tronstore is alive. Attempts to restart
        tronstore if it finds that it exited for some reason."""
        if not self.process.is_alive():
            code = self.process.exitcode
            log.warn("tronstore exited prematurely with status code %d. Attempting to restart." % code)
            self._start_process()
            if not self.process.is_alive():
                raise TronStoreError("tronstore crashed with status code %d and failed to restart" % code)

    def send_request(self, request):
        """Send a StoreRequest to tronstore and immediately return without
        waiting for tronstore's response.
        """
        if self.is_shutdown:
            return
        self._verify_is_alive()

        self.pipe.send_bytes(request.serialized)

    def _poll_for_response(self, id, timeout):
        """Polls for a response to the request with identifier id. Throws
        any responses that it isn't looking for into a dict, and tries to
        retrieve a matching response from this dict before pulling new
        responses.

        If Tron is extended into a synchronous program, simply just add a
        lock around this function ( with mutex.lock(): ) and everything'll
        be fine.
        """
        if id in self.orphaned_responses:
            # response = self.orphaned_responses[id]
            # del self.orphaned_responses[id]
            return self.orphaned_responses.pop(id)

        while self.pipe.poll(timeout):
            response = self.response_factory.rebuild(self.pipe.recv_bytes())
            if response.id == id:
                return response
            else:
                self.orphaned_responses[response.id] = response
        return None

    def send_request_get_response(self, request):
        """Send a StoreRequest to tronstore, and block until tronstore responds
        with the appropriate data. The StoreResponse is returned as is, with no
        modifications. Blocks for POLL_TIMEOUT seconds until returning None.
        """

        if self.is_shutdown:
            return self.response_factory.build(False, request.id, '')
        self._verify_is_alive()

        self.pipe.send_bytes(request.serialized)
        response = self._poll_for_response(request.id, self.POLL_TIMEOUT)
        if not response:
            log.warn(("tronstore took longer than %d seconds to respond to a"
                     "request, and it was dropped.") % self.POLL_TIMEOUT)
            return self.response_factory.build(False, request.id, '')
        else:
            return response

    def send_request_shutdown(self, request):
        """Shut down the process protocol. Waits for SHUTDOWN_TIMEOUT seconds
        for tronstore to send a response, after which it kills both pipes
        and the process itself.

        Calling this prevents ANY further requests from being made to tronstore
        as the process will be killed.
        """
        if self.is_shutdown or not self.process.is_alive():
            self.pipe.close()
            self.is_shutdown = True
            return
        self.is_shutdown = True

        self.pipe.send_bytes(request.serialized)
        response = self._poll_for_response(request.id, self.SHUTDOWN_TIMEOUT)

        if not response or not response.success:
            log.error("tronstore failed to shut down cleanly.")

        self.pipe.close()
        # We can't actually use process.terminate(), as that sends a SIGTERM
        # to the process, which unfortunately is registered to do nothing
        # (as the process depends on trond to shut itself down, and shuts
        # itself down if trond is dead anyway.)
        # We want a hard kill regardless.
        try:
            os.kill(self.process.pid, signal.SIGKILL)
        except:
            pass

    def update_config(self, new_config):
        """Update the configuration. Needed to make sure that tronstore
        is restarted with the correct configuration upon exiting
        prematurely."""
        self.config = new_config
