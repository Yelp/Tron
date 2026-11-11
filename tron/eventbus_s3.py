"""Write-only mirror of emitted EventBus triggers into S3.

Tron keeps its own trigger state in memory and in the local pickle; nothing
here is ever read back by Tron. The mirror exists so another product can
consume triggers from S3 without touching Tron's internal state.

Objects are written once and never rewritten: the conditional put
(``IfNoneMatch="*"``) makes a re-emission a no-op and preserves the original
``published_at``. There is deliberately no delete path, because Tron treats
emitted triggers as append-only (see the note in ``EventBus._discard``).
"""
import json
import logging
import queue
import threading
import time
from typing import Any

import boto3
import botocore.exceptions
from botocore.config import Config

import tron.prom_metrics as prom_metrics

log = logging.getLogger(__name__)

SCHEMA_VERSION = 1
PUT_ATTEMPTS = 4
INITIAL_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 30.0
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 10.0

_STOP = object()


class S3EventWriter:
    """Background thread that mirrors triggers to S3, best effort.

    Failures never propagate to Tron: the event is retried with backoff and
    then dropped, because a missed object only means the downstream consumer
    misses a notification -- Tron's own state is unaffected.
    """

    def __init__(
        self,
        bucket: str,
        region: str,
        cluster_name: str,
        client: Any = None,
    ) -> None:
        retry_config = Config(retries={"max_attempts": 5, "mode": "standard"})
        self.client = client or boto3.client("s3", region_name=region, config=retry_config)
        self.bucket = bucket
        self.region = region
        self.cluster_name = cluster_name
        self.prefix = f"v1/{cluster_name}/"
        self.queue: queue.Queue[Any] = queue.Queue()
        self.thread = threading.Thread(
            target=self._write_loop,
            name="tron-eventbus-s3",
            daemon=True,
        )

    @property
    def config_key(self) -> tuple[str, str, str]:
        return (self.bucket, self.region, self.cluster_name)

    def start(self) -> "S3EventWriter":
        self.thread.start()
        return self

    def enqueue(self, event_id: str, body: dict[str, Any]) -> None:
        self.queue.put((event_id, body))
        prom_metrics.tron_eventbus_s3_queue_depth_gauge.set(self.queue.qsize())

    def stop(self, timeout: float = SHUTDOWN_DRAIN_TIMEOUT_SECONDS) -> None:
        """Drain queued writes and stop the writer thread.

        Used on shutdown and when the config changes. Anything still queued
        after ``timeout`` is abandoned: the thread is a daemon, so trond can
        exit without waiting for a wedged S3 call.
        """
        if not self.thread.is_alive():
            return

        self.queue.put(_STOP)
        self.thread.join(timeout)
        if self.thread.is_alive():
            log.warning(
                f"eventbus S3 writer did not drain within {timeout}s, " f"abandoning {self.queue.qsize()} queued writes"
            )

    def _write_loop(self) -> None:
        while True:
            item = self.queue.get()
            try:
                if item is _STOP:
                    return
                event_id, body = item
                self._write_with_retries(event_id, body)
            except Exception:
                # Never let the writer thread die: that would silently stop
                # mirroring for the rest of the process's lifetime.
                log.exception("unexpected error in eventbus S3 writer")
            finally:
                self.queue.task_done()
                prom_metrics.tron_eventbus_s3_queue_depth_gauge.set(self.queue.qsize())

    def _write_with_retries(self, event_id: str, body: dict[str, Any]) -> bool:
        for attempt in range(1, PUT_ATTEMPTS + 1):
            started = time.time()
            try:
                self._put(event_id, body)
                return True
            except Exception:
                prom_metrics.tron_eventbus_s3_write_errors_counter.inc()
                if attempt == PUT_ATTEMPTS:
                    log.exception(f"giving up on writing trigger {event_id} to s3://{self.bucket}/{self.prefix}")
                    return False

                delay = min(
                    INITIAL_BACKOFF_SECONDS * 2 ** (attempt - 1),
                    MAX_BACKOFF_SECONDS,
                )
                log.warning(
                    f"failed to write trigger {event_id} to S3 "
                    f"(attempt {attempt}/{PUT_ATTEMPTS}), retrying in {delay}s",
                    exc_info=True,
                )
                time.sleep(delay)
            finally:
                prom_metrics.tron_eventbus_s3_write_seconds_histogram.observe(time.time() - started)

        return False

    def _put(self, event_id: str, body: dict[str, Any]) -> None:
        payload = {**body, "schema": SCHEMA_VERSION, "published_at": time.time()}
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self.prefix + event_id,
                Body=json.dumps(payload).encode(),
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "PreconditionFailed":
                raise
            # Already mirrored by an earlier emission; first write wins.
            log.debug(f"trigger {event_id} already present in s3://{self.bucket}/{self.prefix}")
