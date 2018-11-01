import threading
import time

from twisted.internet import threads
from twisted.web import server

from tron.metrics import timer


def report_resource_request(resource, request, duration_ms):
    timer(
        name=f'tron.api.{resource.__class__.__name__}',
        delta=duration_ms,
        dimensions={'method': request.method.decode()}
    )


class AsyncResource():
    capacity = 10
    semaphore = threading.Semaphore(value=capacity)
    lock = threading.Lock()

    @staticmethod
    def finish(result, request, resource):
        result, duration_ms = result
        request.write(result)
        request.finish()
        report_resource_request(resource, request, duration_ms)

    @staticmethod
    def process(fn, resource, request):
        start = time.time()
        with AsyncResource.semaphore:
            result = fn(resource, request)
        duration_ms = 1000 * (time.time() - start)
        return result, duration_ms

    @staticmethod
    def bounded(fn):
        def wrapper(resource, request):
            d = threads.deferToThread(
                AsyncResource.process, fn, resource, request
            )
            d.addCallback(AsyncResource.finish, request, resource)
            d.addErrback(request.processingFailed)
            return server.NOT_DONE_YET

        return wrapper

    @staticmethod
    def exclusive(fn):
        def wrapper(resource, request):
            # ensures only one exclusive request starts consuming the semaphore
            start = time.time()
            with AsyncResource.lock:
                # this will wait until all bounded requests finished processing
                for _ in range(AsyncResource.capacity):
                    AsyncResource.semaphore.acquire()
                try:
                    return fn(resource, request)
                finally:
                    for _ in range(AsyncResource.capacity):
                        AsyncResource.semaphore.release()
                    duration_ms = 1000 * (time.time() - start)
                    report_resource_request(resource, request, duration_ms)

        return wrapper
