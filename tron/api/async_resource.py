import threading
import time
from typing import Any
from typing import Callable
from typing import Tuple
from typing import TypeVar

from twisted.internet import defer
from twisted.internet import threads
from twisted.web import server

from tron.metrics import timer


# R is the type of the resource instance (e.g., self in a render_METHOD)
R = TypeVar("R")
# U is the return type for the function wrapped by 'exclusive'
U = TypeVar("U")


def report_resource_request(resource: R, request: server.Request, duration_ms: float) -> None:
    timer(
        name=f"tron.api.{resource.__class__.__name__}",
        delta=duration_ms,
        dimensions={"method": request.method.decode()},
    )


class AsyncResource:
    capacity: int = 10
    semaphore: threading.Semaphore = threading.Semaphore(value=capacity)
    lock: threading.Lock = threading.Lock()

    @staticmethod
    def finish(
        result_tuple: Tuple[bytes, float],  # Result from AsyncResource.process
        request: server.Request,  # Argument passed to addCallback
        resource: R,  # Argument passed to addCallback
    ) -> None:
        actual_result: bytes
        duration_ms: float
        actual_result, duration_ms = result_tuple
        request.write(actual_result)
        request.finish()
        report_resource_request(resource, request, duration_ms)

    @staticmethod
    def process(
        fn: Callable[[R, server.Request], bytes],
        resource: R,
        request: server.Request,
    ) -> Tuple[bytes, float]:
        start_time = time.time()
        with AsyncResource.semaphore:
            result: bytes = fn(resource, request)
        duration_ms: float = 1000 * (time.time() - start_time)
        return result, duration_ms

    @staticmethod
    def bounded(
        fn: Callable[[R, server.Request], int],
    ) -> Callable[[R, server.Request], int]:
        def wrapper(resource: R, request: server.Request) -> int:
            deferred: defer.Deferred[
                Any
            ] = threads.deferToThread(  # i don't love the Any, but I don't want to actually figure out the type right now
                AsyncResource.process,
                fn,
                resource,
                request,
            )
            deferred.addCallback(AsyncResource.finish, request, resource)
            deferred.addErrback(request.processingFailed)
            return server.NOT_DONE_YET

        return wrapper

    @staticmethod
    def exclusive(
        fn: Callable[[R, server.Request], U],
    ) -> Callable[[R, server.Request], U]:
        def wrapper(resource: R, request: server.Request) -> U:
            start_time = time.time()
            with AsyncResource.lock:
                # this will wait until all bounded requests finished processing
                for _ in range(AsyncResource.capacity):
                    AsyncResource.semaphore.acquire()
                try:
                    result: U = fn(resource, request)
                    return result
                finally:
                    for _ in range(AsyncResource.capacity):
                        AsyncResource.semaphore.release()
                    duration_ms: float = 1000 * (time.time() - start_time)
                    report_resource_request(resource, request, duration_ms)

        return wrapper
