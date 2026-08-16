"""Immutable event identities and transactional producer/journal roles."""

from __future__ import annotations

import hashlib
import json
import re
import time

from ..domain import make_event, validate_event, validate_payload
from ..domain_v2 import make_event_v2
from ..nats_mini import encode_js_publish


def canonical(event):
    return json.dumps(
        event, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )


class EventRepository:
    def __init__(self, database):
        self.database = database

    @staticmethod
    def _bump_pending(connection):
        connection.execute(
            """
            UPDATE metadata
               SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
             WHERE key = 'pending_change_counter'
            """
        )

    @staticmethod
    def _update_sequence(connection, producer, sequence):
        connection.execute(
            """
            INSERT INTO producer_sequences(producer, last_sequence)
            VALUES (?, ?)
            ON CONFLICT(producer) DO UPDATE SET
                last_sequence = MAX(last_sequence, excluded.last_sequence)
            """,
            (producer, sequence),
        )

    @staticmethod
    def _next_sequence(connection, producer):
        row = connection.execute(
            "SELECT last_sequence FROM producer_sequences WHERE producer = ?",
            (producer,),
        ).fetchone()
        return (row[0] if row else 0) + 1

    def put(self, connection, event, producer_state=None, journaled=False):
        ok, reason = validate_event(event)
        if not ok:
            raise ValueError(f"invalid event: {reason}")
        encoded = canonical(event)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        encode_js_publish(encoded, event["event_id"])
        existing = connection.execute(
            """
            SELECT envelope_json, canonical_sha256, producer_state, journaled,
                   EXISTS(
                       SELECT 1 FROM quarantine
                        WHERE quarantine.event_id = events.event_id
                   ) AS quarantined
              FROM events WHERE event_id = ?
            """,
            (event["event_id"],),
        ).fetchone()
        if existing is not None:
            if existing["canonical_sha256"] != digest:
                raise ValueError(
                    f"event identity collision for {event['event_id']!r}"
                )
            if existing["envelope_json"] != encoded:
                if not existing["quarantined"]:
                    raise ValueError(
                        f"event identity collision for {event['event_id']!r}"
                    )
                connection.execute(
                    """
                    UPDATE events SET envelope_json = ?, canonical_size = ?
                     WHERE event_id = ?
                    """,
                    (encoded, len(encoded.encode()), event["event_id"]),
                )
            old_state = existing["producer_state"]
            new_state = old_state
            if producer_state == "acked" or old_state is None:
                new_state = producer_state or old_state
            elif producer_state == "pending" and old_state != "acked":
                new_state = "pending"
            connection.execute(
                """
                UPDATE events
                   SET producer_state = ?, journaled = MAX(journaled, ?),
                       acked_at = CASE
                           WHEN ? = 'acked' THEN COALESCE(acked_at, ?)
                           ELSE acked_at
                       END
                 WHERE event_id = ?
                """,
                (new_state, int(journaled), new_state, time.time(),
                 event["event_id"]),
            )
            self._update_sequence(
                connection, event["producer"], event["producer_seq"],
            )
            return False, old_state != new_state
        owner = connection.execute(
            "SELECT event_id FROM events WHERE producer = ? AND producer_seq = ?",
            (event["producer"], event["producer_seq"]),
        ).fetchone()
        if owner is not None:
            raise ValueError(
                f"producer sequence collision with {owner['event_id']!r}"
            )
        connection.execute(
            """
            INSERT INTO events(
                event_id, producer, producer_seq, observed_at, subject, kind,
                envelope_json, canonical_sha256, canonical_size,
                producer_state, journaled, acked_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"], event["producer"], event["producer_seq"],
                event["observed_at"], event["subject"], event["kind"], encoded,
                digest, len(encoded.encode()), producer_state, int(journaled),
                time.time() if producer_state == "acked" else None, time.time(),
            ),
        )
        self._update_sequence(
            connection, event["producer"], event["producer_seq"],
        )
        return True, producer_state is not None

    def emit_v1_in(self, connection, producer, kind, host, payload,
                   correlation_id=None):
        ok, reason = validate_payload(kind, payload)
        if not ok:
            raise ValueError(reason)
        event = make_event(
            producer, kind, host, payload,
            correlation_id=correlation_id,
            producer_seq=self._next_sequence(connection, producer),
        )
        _, changed = self.put(connection, event, producer_state="pending")
        if changed:
            self._bump_pending(connection)
        return event

    def journal_v1_in(self, connection, producer, kind, host, payload,
                      correlation_id=None):
        """Record a local audit event that is never broker delivery work."""
        ok, reason = validate_payload(kind, payload)
        if not ok:
            raise ValueError(reason)
        event = make_event(
            producer, kind, host, payload,
            correlation_id=correlation_id,
            producer_seq=self._next_sequence(connection, producer),
        )
        self.put(connection, event, journaled=True)
        return event

    def emit_v2_in(self, connection, producer, kind, node, payload,
                   correlation_id=None, causes=None):
        event = make_event_v2(
            producer, kind, node, payload,
            producer_seq=self._next_sequence(connection, producer),
            correlation_id=correlation_id,
            causes=causes,
        )
        _, changed = self.put(connection, event, producer_state="pending")
        if changed:
            self._bump_pending(connection)
        return event

    def emit_v1(self, producer, kind, host, payload, correlation_id=None):
        with self.database.transaction() as connection:
            return self.emit_v1_in(
                connection, producer, kind, host, payload, correlation_id,
            )

    def emit_v2(self, producer, kind, node, payload, correlation_id=None,
                causes=None):
        with self.database.transaction() as connection:
            return self.emit_v2_in(
                connection, producer, kind, node, payload,
                correlation_id=correlation_id, causes=causes,
            )

    def append(self, event):
        with self.database.transaction() as connection:
            _, changed = self.put(connection, event, producer_state="pending")
            if changed:
                self._bump_pending(connection)

    def append_journal(self, event):
        with self.database.transaction() as connection:
            prior = connection.execute(
                "SELECT journaled FROM events WHERE event_id = ?",
                (event.get("event_id"),),
            ).fetchone()
            self.put(connection, event, journaled=True)
            return prior is None or not bool(prior["journaled"])

    def record_puback(self, event, puback):
        if not isinstance(puback, dict):
            raise ValueError("PubAck evidence must be an object")
        stream = puback.get("stream")
        sequence = puback.get("seq")
        if (not isinstance(sequence, int) or isinstance(sequence, bool)
                or sequence < 1):
            raise ValueError("PubAck sequence must be a positive integer")
        if not isinstance(stream, str) or not stream:
            raise ValueError("PubAck must contain stream and positive seq")
        duplicate = puback.get("duplicate", False)
        if not isinstance(duplicate, bool):
            raise ValueError("PubAck duplicate must be a boolean")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", stream):
            raise ValueError("PubAck stream must be one safe token")
        with self.database.transaction() as connection:
            prior = connection.execute(
                """
                SELECT producer_state, puback_stream, puback_sequence
                  FROM events WHERE event_id = ?
                """,
                (event["event_id"],),
            ).fetchone()
            if prior is None:
                raise ValueError(
                    f"cannot acknowledge unknown event {event['event_id']!r}"
                )
            if prior["producer_state"] == "acked" and (
                    prior["puback_stream"] != stream
                    or prior["puback_sequence"] != sequence):
                raise ValueError(
                    f"conflicting PubAck evidence for {event['event_id']!r}"
                )
            _, changed = self.put(connection, event, producer_state="acked")
            connection.execute(
                """
                UPDATE events
                   SET puback_stream = ?, puback_sequence = ?,
                       puback_duplicate = ?, acked_at = COALESCE(acked_at, ?)
                 WHERE event_id = ?
                """,
                (stream, sequence, int(duplicate), time.time(), event["event_id"]),
            )
            connection.execute(
                """
                INSERT INTO cursors(subject, producer, event_id, producer_seq)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(subject, producer) DO UPDATE SET
                    event_id = excluded.event_id,
                    producer_seq = excluded.producer_seq
                WHERE excluded.producer_seq > cursors.producer_seq
                """,
                (
                    event["subject"], event["producer"], event["event_id"],
                    event["producer_seq"],
                ),
            )
            if changed:
                self._bump_pending(connection)

    def note_delivery_failure(self, event_id, error, retry_after=None):
        with self.database.connect() as connection:
            connection.execute(
                """
                UPDATE events
                   SET delivery_attempts = delivery_attempts + 1,
                       last_delivery_error = ?, retry_after = ?
                 WHERE event_id = ? AND producer_state = 'pending'
                """,
                (str(error)[:300], retry_after, event_id),
            )
