import signal
import logging
import os
from multiprocessing import Process, Pipe

from tron.serialize.runstate.tronstore import tronstore
from tron.serialize.runstate.tronstore.messages import StoreResponseFactory

log = logging.getLogger(__name__)


class TronstoreError(Exception):
    """Raised whenever tronstore exits for an unknown reason."""
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return repr(self.code)


class StoreProcessProtocol(object):
    """The class that actually spawns and handles the tronstore process.

    This class uses the python multiprocessing module. Upon creation, it
    starts tronstore with a null configuration. A reconfiguration request
    must be sent to tronstore via one of the supplied object methods before
    it will be able to actually perform saves and restores. Calling
    update_config on this object will simply update the saved configuration
    object- it won't actually update the configuration that the tronstore
    process itself is using unless a _verify_is_alive fails and tronstore is
    restarted.

    Communication with the process is handled by a Pipe object, which can
    simply pass entire Python objects via Pickle. Despite this, we still
    serialize all requests with cPickle before sending them, as cPickle
    is much faster and effectively the same as cPickle.
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
        """Spawn the tronstore process with the saved configuration."""
        self.pipe, child_pipe = Pipe()
        store_args = (self.config, child_pipe, logging.getLogger('tronstore'))

        self.process = Process(target=tronstore.main, args=store_args)
        self.process.daemon = True
        self.process.start()
        log.info('Tronstore is starting.')

    def _verify_is_alive(self):
        """A check to verify that tronstore is alive. Attempts to restart
        tronstore if it finds that it exited for some reason.
        """
        if not self.process.is_alive():
            code = self.process.exitcode
            log.warn(("Tronstore exited prematurely with status code %d. "
                "Attempting to restart.") % code)
            self._start_process()
            if not self.process.is_alive():
                raise TronstoreError(("Tronstore crashed with status code %d "
                    "and failed to restart.") % code)

    def send_request(self, request):
        """Send a StoreRequest to tronstore and immediately return without
        waiting for tronstore's response.
        """
        if self.is_shutdown:
            log.warn('Attempted to send a store request of type %s while shut down!'
                % request.req_type)
            return
        self._verify_is_alive()

        self.pipe.send_bytes(request.serialized)

    def _poll_for_response(self, id, timeout):
        """Polls for a response to the request with identifier id. Throws
        any responses that it isn't looking for into a dict, and tries to
        retrieve a matching response from this dict before pulling new
        responses.
        """
        if id in self.orphaned_responses:
            return self.orphaned_responses.pop(id)

        while self.pipe.poll(timeout):
            response = self.response_factory.from_msg(self.pipe.recv_bytes())
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
            log.warn('Attempted to send a request of type %s while shut down!'
                % request.req_type)
            return self.response_factory.build(False, request.id, '')
        self._verify_is_alive()

        self.pipe.send_bytes(request.serialized)
        response = self._poll_for_response(request.id, self.POLL_TIMEOUT)
        if not response:
            log.warn(("Tronstore took longer than %d seconds to respond to a "
                     "request, and it was dropped.") % self.POLL_TIMEOUT)
            return self.response_factory.build(False, request.id, '')
        else:
            return response

    def send_request_shutdown(self, request):
        """Shut down the process protocol. Waits for SHUTDOWN_TIMEOUT seconds
        for tronstore to send a shutdown response, killing both pipes and the
        process itself if no shutdown response was returned.

        Calling this prevents ANY further requests from being made to tronstore
        as the process will be killed.
        """
        log.info('Shutting down tronstore...')
        if self.is_shutdown or not self.process.is_alive():
            log.warn("Tried to shutdown Tronstore while it was already shut down!")
            self.pipe.close()
            self.is_shutdown = True
            return
        self.is_shutdown = True

        self.pipe.send_bytes(request.serialized)
        response = self._poll_for_response(request.id, self.SHUTDOWN_TIMEOUT)

        if not response or not response.success:
            log.error("Tronstore failed to shut down cleanly.")
        else:
            log.info('Tronstore is shut down.')

        self.pipe.close()
        # We can't actually use process.terminate(), as that sends a SIGTERM
        # to the process, which unfortunately is registered to do nothing
        # (as the process depends on trond to shut itself down, and shuts
        # itself down if trond is dead anyway.)
        # We want a hard kill regardless. The only way we should get to
        # this code is if tronstore is about to call os._exit(0) itself.
        try:
            os.kill(self.process.pid, signal.SIGKILL)
        except:
            pass

    def update_config(self, new_config):
        """Update the configuration. Needed to make sure that tronstore
        is restarted with the correct configuration upon exiting
        prematurely."""
        self.config = new_config
