"""Indexed pending delivery selection and corruption quarantine."""

from __future__ import annotations

import hashlib
import json
import time

from .events import canonical


class PendingDelivery:
    def __init__(self, database, events, queries):
        self.database = database
        self.events = events
        self.queries = queries

    def repair(self, validator):
        repaired = 0
        with self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT event_id, envelope_json, canonical_sha256 FROM events "
                "WHERE producer_state = 'pending'"
            ).fetchall()
            for row in rows:
                try:
                    event = json.loads(row["envelope_json"])
                    ok, reason = validator(event)
                    encoded = canonical(event)
                    if (encoded != row["envelope_json"]
                            or hashlib.sha256(encoded.encode()).hexdigest()
                            != row["canonical_sha256"]):
                        ok, reason = False, "canonical event integrity mismatch"
                except Exception as exc:
                    ok, reason = False, str(exc)
                if ok:
                    continue
                connection.execute(
                    """
                    INSERT INTO quarantine(
                        event_id, raw_json, reason, quarantined_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (row["event_id"], row["envelope_json"], reason, time.time()),
                )
                connection.execute(
                    "UPDATE events SET producer_state = NULL WHERE event_id = ?",
                    (row["event_id"],),
                )
                connection.execute(
                    """
                    UPDATE operations
                       SET state = 'INDETERMINATE',
                           error = 'event record quarantined'
                     WHERE event_id = ? AND state = 'RECORDED'
                    """,
                    (row["event_id"],),
                )
                repaired += 1
            if repaired:
                self.events._bump_pending(connection)
                self.events.journal_v1_in(
                    connection, "local:recovery", "event.degraded", "local",
                    {"cause": "invalid pending records quarantined",
                     "records": repaired},
                )
        return repaired

    def select(self, max_events, seen, validator, eligible=None,
               start_after=None, max_scan=None, return_meta=False):
        if max_events < 1:
            raise ValueError("max_events must be positive")
        scan_limit = max_scan or max_events * 4
        start = int(start_after or 0)
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT row_id, envelope_json, canonical_sha256 FROM events
                 WHERE producer_state = 'pending' AND row_id > ?
                 ORDER BY row_id LIMIT ?
                """,
                (start, scan_limit + 1),
            ).fetchall()
        reached_eof = len(rows) <= scan_limit
        rows = rows[:scan_limit]
        selected = []
        invalid = False
        last_position = start_after
        for row in rows:
            last_position = row["row_id"]
            try:
                event = json.loads(row["envelope_json"])
                ok, _ = validator(event)
                encoded = canonical(event)
                if (encoded != row["envelope_json"]
                        or hashlib.sha256(encoded.encode()).hexdigest()
                        != row["canonical_sha256"]):
                    ok = False
            except Exception:
                ok = False
            if not ok:
                invalid = True
                continue
            if eligible is not None and not eligible(event):
                continue
            event_id = event["event_id"]
            if event_id in seen:
                continue
            seen.add(event_id)
            selected.append(event)
            if len(selected) >= max_events:
                reached_eof = False
                break
        repaired = self.repair(validator) if invalid else 0
        if repaired:
            last_position = None
        signature = self.queries.pending_signature() if reached_eof else None
        result = (
            selected, reached_eof, repaired, last_position, len(rows), signature,
        )
        return result if return_meta else result[:3]
