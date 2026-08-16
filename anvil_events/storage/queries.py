"""Verified event reads and small indexed storage projections."""

from __future__ import annotations

import hashlib
import json

from .events import canonical


class EventQueries:
    def __init__(self, database):
        self.database = database

    def read(self, where, params=()):
        query = (
            "SELECT event_id, envelope_json, canonical_sha256 FROM events WHERE "
            + where
            + " ORDER BY observed_at, producer_seq, row_id"
        )
        with self.database.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        result = []
        for row in rows:
            try:
                event = json.loads(row["envelope_json"])
                encoded = canonical(event)
                digest = hashlib.sha256(encoded.encode()).hexdigest()
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"stored event failed integrity check: {row['event_id']}"
                ) from exc
            if (encoded != row["envelope_json"]
                    or digest != row["canonical_sha256"]):
                raise ValueError(
                    f"stored event failed integrity check: {row['event_id']}"
                )
            result.append(event)
        return result

    def count_pending(self):
        with self.database.connect() as connection:
            return connection.execute(
                "SELECT COUNT(*) FROM events WHERE producer_state = 'pending'"
            ).fetchone()[0]

    def pending_signature(self):
        with self.database.connect() as connection:
            counter = int(connection.execute(
                "SELECT value FROM metadata WHERE key = 'pending_change_counter'"
            ).fetchone()[0])
            count = connection.execute(
                "SELECT COUNT(*) FROM events WHERE producer_state = 'pending'"
            ).fetchone()[0]
        return counter, count

    def load_sequences(self):
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT producer, last_sequence FROM producer_sequences"
            ).fetchall()
        return {row["producer"]: row["last_sequence"] for row in rows}

    def load_cursors(self):
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT subject, producer, event_id, producer_seq FROM cursors"
            ).fetchall()
        cursors = {}
        for row in rows:
            cursors.setdefault(row["subject"], {})[row["producer"]] = {
                "last_event_id": row["event_id"],
                "producer_seq": row["producer_seq"],
            }
        return cursors
