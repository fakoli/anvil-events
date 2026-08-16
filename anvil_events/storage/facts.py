"""Transactional, idempotent fact projection."""

from __future__ import annotations

import hashlib
import json
import time

from ..domain import validate_event
from ..ingest import event_to_fact


class FactRepository:
    def __init__(self, database):
        self.database = database

    def add(self, event, allowed_producers=None):
        ok, _ = validate_event(event, allowed_producers=allowed_producers)
        if not ok:
            return None
        fact = event_to_fact(event)
        encoded = json.dumps(
            fact, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT fact_json, fact_sha256 FROM facts WHERE event_id = ?",
                (fact["event_id"],),
            ).fetchone()
            if existing is not None:
                if (existing["fact_json"] != encoded
                        or existing["fact_sha256"] != digest):
                    raise ValueError(
                        f"fact identity collision for {fact['event_id']!r}"
                    )
                return None
            connection.execute(
                """
                INSERT INTO facts(event_id, fact_json, fact_sha256, stored_at)
                VALUES (?, ?, ?, ?)
                """,
                (fact["event_id"], encoded, digest, time.time()),
            )
        return fact

    def read(self):
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT fact_json FROM facts ORDER BY stored_at, event_id"
            ).fetchall()
        return [json.loads(row["fact_json"]) for row in rows]
