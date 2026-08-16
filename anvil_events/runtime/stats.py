"""Thread-safe runtime counters and state."""

from __future__ import annotations

import threading
import time


class RuntimeStats:
    def __init__(self):
        self._lock = threading.Lock()
        self._values = {
            "received": 0,
            "journaled": 0,
            "dropped": 0,
            "retried": 0,
            "acked": 0,
            "broker_connected": False,
            "last_error": None,
            "producer_connected": False,
            "delivery_errors": 0,
            "last_delivery_error": None,
            "started": time.time(),
        }

    def update(self, **values):
        with self._lock:
            self._values.update(values)

    def increment(self, key, amount=1):
        with self._lock:
            self._values[key] += amount

    def snapshot(self):
        with self._lock:
            return dict(self._values)
