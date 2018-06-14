import threading

from twisted.internet import threads
from twisted.web import server


class AsyncResource():
    capacity = 10
    semaphore = threading.Semaphore(value=capacity)
    lock = threading.Lock()

    @staticmethod
    def finish(result, request):
        request.write(result)
        request.finish()

    @staticmethod
    def process(fn, resource, request):
        with AsyncResource.semaphore:
            return fn(resource, request)

    @staticmethod
    def bounded(fn):
        def wrapper(resource, request):
            d = threads.deferToThread(
                AsyncResource.process, fn, resource, request
            )
            d.addCallback(AsyncResource.finish, request)
            d.addErrback(lambda f: f)
            return server.NOT_DONE_YET

        return wrapper

    @staticmethod
    def exclusive(fn):
        def wrapper(*args, **kwargs):
            # ensures only one exclusive request starts consuming the semaphore
            with AsyncResource.lock:
                # this will wait until all bounded requests finished processing
                for _ in range(AsyncResource.capacity):
                    AsyncResource.semaphore.acquire()
                try:
                    return fn(*args, **kwargs)
                finally:
                    for _ in range(AsyncResource.capacity):
                        AsyncResource.semaphore.release()

        return wrapper
