"""Composition root for subscriber, delivery, reconciliation, and health."""

from __future__ import annotations

import hashlib
import os
import re
import socket
import threading

from ..domain import parse_allowed_producers
from ..reconciliation import load_node_runtime
from ..store import open_event_store
from ..transport.security import parse_endpoint
from .delivery import DeliveryPump
from .health import HealthServer
from .stats import RuntimeStats
from .subscriber import Subscriber


class EventsService:
    def __init__(self, root, url, subject, stream, health, *, durable=None,
                 store_backend="auto", node_config=None):
        self.root = root
        self.url = url
        self.subject = subject
        self.stream = stream
        self.store = open_event_store(root, backend=store_backend)
        self.stop_event = threading.Event()
        self.stats = RuntimeStats()
        runtime = load_node_runtime(node_config, self.store) if node_config else None
        allowed = (
            runtime.allowed_producers if runtime else parse_allowed_producers()
        )
        raw_host = socket.gethostname()
        host_token = re.sub(r"[^A-Za-z0-9_-]+", "-", raw_host).strip("-")
        identity = "\0".join(("subscriber", raw_host, stream, subject))
        suffix = hashlib.sha256(identity.encode()).hexdigest()[:12]
        self.durable = durable or f"anvil-events-{host_token or 'node'}-{suffix}"
        self.subscriber = Subscriber(
            self.store,
            url,
            stream,
            self.durable,
            subject,
            allowed,
            self.stop_event,
            self.stats,
            processor=runtime.processor if runtime else None,
        )
        self.delivery = DeliveryPump(
            self.store, url, self.stop_event, self.stats, stream=stream,
        )
        self.reconciliation_enabled = runtime is not None
        self.health = HealthServer(health, self.health_snapshot, self.stop_event)
        self.health_address = health
        self.threads = ()
        self._logged_url = None

    def log_banner(self):
        if self._logged_url == self.url:
            return
        scheme, _, _ = parse_endpoint(self.url)
        mode = os.environ.get("ANVIL_EVENTS_TRANSPORT_MODE", "development")
        print(
            f"anvil-events serve: subject={self.subject} root={self.root} "
            f"transport={mode}/{scheme} "
            f"health={self.health_address[0]}:{self.health_address[1]} "
            f"reconcile={str(self.reconciliation_enabled).lower()}",
            flush=True,
        )
        self._logged_url = self.url

    def start(self):
        if self.threads:
            raise RuntimeError("service already started")
        health_thread = self.health.start()
        self.health_address = self.health.address
        subscriber = threading.Thread(
            target=self.subscriber.run, name="event-subscriber", daemon=False,
        )
        delivery = threading.Thread(
            target=self.delivery.run, name="event-delivery", daemon=False,
        )
        subscriber.start()
        delivery.start()
        self.threads = (subscriber, delivery, health_thread)
        return self.threads

    def health_snapshot(self):
        result = self.stats.snapshot()
        try:
            result.update(self.store.status())
            result["store_error"] = None
        except Exception as exc:
            result["pending"] = -1
            result["store_error"] = str(exc)[:300]
        workers = self.threads[:2]
        workers_alive = bool(workers) and all(thread.is_alive() for thread in workers)
        result["workers_alive"] = workers_alive
        result["local_accepting"] = result["pending"] >= 0 and workers_alive
        delivery_healthy = not (
            result["pending"] > 0 and result["last_delivery_error"]
        )
        result["fleet_delivery_ready"] = bool(
            result["broker_connected"] and workers_alive and delivery_healthy
        )
        result["delivery_healthy"] = delivery_healthy
        result["delivery_backoff_events"] = len(self.delivery.retry)
        result["reconciliation_enabled"] = self.reconciliation_enabled
        return result

    def failed_workers(self):
        return [thread.name for thread in self.threads if not thread.is_alive()]

    def stop(self, timeout=5):
        self.stop_event.set()
        self.subscriber.abort()
        self.health.close(timeout)
        for thread in self.threads[:2]:
            if thread is not threading.current_thread():
                thread.join(timeout)
        alive = [thread.name for thread in self.threads[:2] if thread.is_alive()]
        if alive:
            raise RuntimeError(f"service workers did not stop: {alive}")
