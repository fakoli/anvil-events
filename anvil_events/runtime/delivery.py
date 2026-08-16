"""Asynchronous local-outbox to JetStream delivery pump."""

from __future__ import annotations

import time

from ..domain import validate_event
from ..transport import NATSClient


class DeliveryPump:
    def __init__(self, store, url, stop_event, stats, client_factory=NATSClient):
        self.store = store
        self.url = url
        self.stop = stop_event
        self.stats = stats
        self.client_factory = client_factory
        self.retry = {}
        self.seen = set()
        self.position = None
        self.sleep_until = None
        self.sleep_signature = None

    def pending_batch(self, max_events=16):
        now = time.monotonic()

        def eligible(event):
            state = self.retry.get(event.get("event_id"))
            return state is None or now >= state["next_at"]

        batch, eof, repaired, position, _, signature = (
            self.store.select_pending_batch(
                max_events,
                self.seen,
                validate_event,
                eligible,
                start_after=self.position,
                max_scan=max_events * 4,
                return_meta=True,
            )
        )
        self.position = None if repaired or eof else position
        if eof:
            self.seen.clear()
        if not batch and eof and self.retry:
            self.sleep_until = min(item["next_at"] for item in self.retry.values())
            self.sleep_signature = signature
        return batch

    def deliver(self, client, batch):
        attempted = failed = 0
        now = time.monotonic()
        for event in batch:
            if self.stop.is_set():
                break
            event_id = event["event_id"]
            state = self.retry.get(event_id)
            if state and now < state["next_at"]:
                continue
            attempted += 1
            self.stats.increment("retried")
            try:
                puback = client.publish_js(
                    event["subject"],
                    event,
                    msg_id=event_id,
                    wait_ack=True,
                    timeout=5,
                )
                self.store.record_puback(event, puback)
                self.stats.increment("acked")
                self.retry.pop(event_id, None)
            except Exception as exc:
                failed += 1
                self.seen.discard(event_id)
                failures = (state or {}).get("failures", 0) + 1
                delay = min(60, 2 ** min(failures, 6))
                self.retry[event_id] = {
                    "failures": failures,
                    "next_at": now + delay,
                }
                self.stats.increment("delivery_errors")
                self.stats.update(last_delivery_error=str(exc)[:300])
                self.store.note_delivery_failure(
                    event_id, exc, retry_after=time.time() + delay,
                )
        if attempted:
            self.position = None
        if self.retry:
            self.sleep_until = min(item["next_at"] for item in self.retry.values())
        else:
            self.sleep_until = self.sleep_signature = None
        return attempted, failed

    def run(self):
        while not self.stop.is_set():
            if self._wait_for_retry_or_new_work():
                continue
            client = None
            batch = []
            try:
                batch = self.pending_batch()
                if batch:
                    client = self.client_factory(self.url).connect(timeout=5)
                    self.stats.update(producer_connected=True)
                    attempted, failed = self.deliver(client, batch)
                    if attempted and not failed:
                        self.stats.update(last_delivery_error=None)
            except Exception as exc:
                for event in batch:
                    self.seen.discard(event.get("event_id"))
                self.position = None
                self.stats.update(producer_connected=False)
                self.stats.increment("delivery_errors")
                self.stats.update(last_delivery_error=str(exc)[:300])
            finally:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass
            self.stop.wait(1)

    def _wait_for_retry_or_new_work(self):
        if self.sleep_until is None:
            return False
        delay = self.sleep_until - time.monotonic()
        if delay <= 0:
            self.sleep_until = self.sleep_signature = None
            return False
        if self.seen or self.sleep_signature is None:
            return False
        self.stop.wait(min(delay, 1))
        if self.stop.is_set():
            return True
        if self.store.pending_signature() == self.sleep_signature:
            return True
        self.sleep_until = self.sleep_signature = None
        return False
