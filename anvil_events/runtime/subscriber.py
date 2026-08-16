"""Durable JetStream subscriber and local processing boundary."""

from __future__ import annotations

import json
import threading

from ..domain import validate_event
from ..transport import NATSClient


class Subscriber:
    def __init__(self, store, url, stream, durable, subject, allowed_producers,
                 stop_event, stats, processor=None, client_factory=NATSClient):
        self.store = store
        self.url = url
        self.stream = stream
        self.durable = durable
        self.subject = subject
        self.allowed_producers = allowed_producers
        self.stop = stop_event
        self.stats = stats
        self.processor = processor
        self.client_factory = client_factory
        self._client = None
        self._client_lock = threading.Lock()

    def handle(self, body, broker_subject=None):
        self.stats.increment("received")
        try:
            event = json.loads(body)
        except Exception:
            self.stats.increment("dropped")
            return True, False
        valid, _ = validate_event(
            event, allowed_producers=self.allowed_producers,
        )
        if not valid:
            self.stats.increment("dropped")
            return True, False
        if broker_subject is not None and broker_subject != event["subject"]:
            self.stats.increment("dropped")
            return True, False
        try:
            journaled = self.store.append_journal(event)
        except Exception as exc:
            self.stats.update(last_error=str(exc)[:300])
            return False, False
        if journaled:
            self.stats.increment("journaled")
        outcome = None
        try:
            if self.processor is not None:
                outcome = self.processor.process(event)
        except Exception as exc:
            self.stats.update(last_error=str(exc)[:300])
            return False, journaled
        self.stats.update(last_error=None)
        if outcome is not None and outcome.state == "awaiting-approval":
            return False, journaled
        return True, journaled

    def run(self):
        while not self.stop.is_set():
            client = None
            try:
                client = self.client_factory(self.url).connect(timeout=5)
                with self._client_lock:
                    self._client = client
                delivery_subject = client.bind_durable_consumer(
                    self.stream, self.durable, self.subject, timeout=5,
                )
                self.stats.update(broker_connected=True, last_error=None)
                while not self.stop.is_set():
                    for message in client.receive(
                            count=1, timeout=1, subscription=delivery_subject):
                        processed, _ = self.handle(
                            message["body"], message["subject"],
                        )
                        if processed and message.get("reply"):
                            client.ack(message["reply"])
            except Exception as exc:
                self.stats.update(
                    broker_connected=False, last_error=str(exc)[:300],
                )
                self.stop.wait(1)
            finally:
                with self._client_lock:
                    self._client = None
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass

    def abort(self):
        with self._client_lock:
            client = self._client
        if client is not None:
            client.abort()
