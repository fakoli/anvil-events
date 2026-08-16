"""Logical archive retention with an atomic audit event."""

from __future__ import annotations

import os
import time


class Retention:
    def __init__(self, database, events):
        self.database = database
        self.events = events

    def collect(self, archive_days=90, max_bytes=500 * 1024 * 1024):
        if isinstance(archive_days, bool) or archive_days < 0:
            raise ValueError("archive_days must be non-negative")
        if isinstance(max_bytes, bool) or max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        cutoff = time.time() - archive_days * 86400
        degraded = None
        with self.database.transaction() as connection:
            removed = connection.execute(
                """
                UPDATE events SET producer_state = NULL
                 WHERE producer_state = 'acked' AND acked_at < ?
                """,
                (cutoff,),
            ).rowcount
            if removed:
                degraded = self.events.journal_v1_in(
                    connection, "local:gc", "event.degraded", "local",
                    {"cause": "retention sweep deleted expired archives",
                     "records": removed},
                )["event_id"]
        with self.database.connect() as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        size = sum(
            os.path.getsize(path)
            for path in (self.database.path, self.database.path + "-wal")
            if os.path.exists(path)
        )
        return {
            "removed": removed,
            "rotated": False,
            "evicted": 0,
            "size": size,
            "unresolved_oversize": size > max_bytes,
            "degraded": degraded,
        }
