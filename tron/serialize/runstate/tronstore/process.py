import time
import logging
from threading import Semaphore

from twisted.internet.protocol import ProcessProtocol
from twisted.internet import reactor
from tron.serialize.runstate.tronstore.chunking import StoreChunkHandler

log = logging.getLogger(__name__)

class TronStoreError(Exception):
    """Raised whenever tronstore exits for an unknown reason."""
    def __init__(self, code):
        self.code = code
    def __str__(self):
        return repr(self.code)

class StoreProcessProtocol(ProcessProtocol):
    """The class that actually communicates with tronstore. This is a subclass
    of the twisted ProcessProtocol class, which can run and asynchronously
    communicate with a child proccess. The requests and responses are matched
    together by the unique integer ids assigned to each request (which are also
    present in responses).
    """

    SHUTDOWN_TIMEOUT = 5.0
    SHUTDOWN_SLEEP = 0.5

    def __init__(self, response_factory):
        self.response_factory = response_factory
        self.chunker = StoreChunkHandler()
        self.requests = {}
        self.responses = {}
        self.semaphores = {}  # semaphores used for synchronization
        self.is_shutdown = False

    def outRecieved(self, data):
        responses = self.chunker.handle(data)
        for response_str in responses:
            response = self.response_factory.rebuild(response_str)
            if response.id in self.requests:
                if not response.success:
                    log.warn("tronstore request #%d failed. Request type was %d." % (response.id, self.requests[response.id].req_type))
                if response.id in self.monitors:
                    self.responses[response.id] = response
                    self.semaphores[response.id].release()
                del self.requests[response.id]

    def processExited(self, reason):
        if not self.is_shutdown:
            raise TronStoreError(reason.getErrorMessage())

    def processEnded(self, reason):
        if not self.is_shutdown:
            self.transport.loseConnection()
            reactor.stop()

    def send_request(self, request):
        """Send a request to tronstore and immediately return without
        waiting for tronstore's response.
        """
        if self.is_shutdown:
            return
        self.requests[request.id] = request
        self.transport.write(self.chunker.sign(request.serialized))

    def send_request_get_response(self, request):
        """Send a request to tronstore, and block until tronstore responds
        with the appropriate data. If the request was successful, we return
        whatever data tronstore sent us, otherwise, None is returned.
        """
        if self.is_shutdown:
            return None
        self.requests[request.id] = request
        self.semaphores[request.id] = Semaphore(0)
        self.transport.write(self.chunker.sign(request.serialized))
        self.semaphores[request.id].acquire()
        del self.semaphores[request.id]
        response = self.responses[request.id]
        del self.responses[requests.id]
        return response.data if response.success else None

    def shutdown(self):
        """Shut down the process protocol. Waits for SHUTDOWN_TIMEOUT seconds
        for all pending requests to get responses from tronstore, after which
        it cuts the connection. It checks if all requests have been completed
        every SHUTDOWN_SLEEP seconds.

        Calling this prevents ANY further requests being made to tronstore.
        """
        self.is_shutdown = True
        time_waited = 0
        while (not len(self.requests.items()) == 0) and time_waited < self.SHUTDOWN_TIMEOUT:
            time.sleep(SHUTDOWN_SLEEP)  # wait for all pending requests to finish
            time_waited += SHUTDOWN_SLEEP
        self.transport.signalProcess("INT")
        self.transport.loseConnection()
        self.reactor.stop()
