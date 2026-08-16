"""Durable lifecycle-operation intent and resolution ledger."""

from __future__ import annotations

import json
import time


class OperationLedger:
    def __init__(self, database, events):
        self.database = database
        self.events = events

    @staticmethod
    def _check(name, value):
        if not isinstance(value, str) or not value or len(value) > 256:
            raise ValueError(f"{name} must be a non-empty string <= 256 characters")

    def prepare(self, operation_id, idempotency_key, producer, kind, intent,
                correlation_id=None):
        for name, value in (
            ("operation_id", operation_id),
            ("idempotency_key", idempotency_key),
            ("producer", producer),
            ("kind", kind),
        ):
            self._check(name, value)
        encoded = json.dumps(
            intent, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                same = (
                    existing["operation_id"] == operation_id
                    and existing["producer"] == producer
                    and existing["kind"] == kind
                    and existing["intent_json"] == encoded
                    and existing["correlation_id"] == correlation_id
                )
                if not same:
                    raise ValueError("operation idempotency-key collision")
                return dict(existing), True
            connection.execute(
                """
                INSERT INTO operations(
                    operation_id, idempotency_key, producer, kind,
                    correlation_id, state, intent_json, prepared_at
                ) VALUES (?, ?, ?, ?, ?, 'PREPARED', ?, ?)
                """,
                (operation_id, idempotency_key, producer, kind,
                 correlation_id, encoded, time.time()),
            )
            row = connection.execute(
                "SELECT * FROM operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            return dict(row), False

    def record(self, operation_id, idempotency_key, producer, kind, node, payload,
               correlation_id=None, causes=None):
        """Atomically accept one idempotent local event and its operation row."""
        for name, value in (
            ("operation_id", operation_id),
            ("idempotency_key", idempotency_key),
            ("producer", producer),
            ("kind", kind),
            ("node", node),
        ):
            self._check(name, value)
        intent = {"node": node, "payload": payload, "causes": list(causes or [])}
        encoded = json.dumps(
            intent, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                same = (
                    existing["operation_id"] == operation_id
                    and existing["producer"] == producer
                    and existing["kind"] == kind
                    and existing["intent_json"] == encoded
                    and existing["correlation_id"] == correlation_id
                    and existing["state"] == "RECORDED"
                    and existing["event_id"] is not None
                )
                if not same:
                    raise ValueError("operation idempotency-key collision")
                row = connection.execute(
                    "SELECT envelope_json FROM events WHERE event_id = ?",
                    (existing["event_id"],),
                ).fetchone()
                if row is None:
                    raise RuntimeError("recorded operation is missing its event")
                return json.loads(row["envelope_json"]), True
            if connection.execute(
                "SELECT 1 FROM operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone():
                raise ValueError("operation-id collision")
            event = self.events.emit_v2_in(
                connection, producer, kind, node, payload, correlation_id,
                causes=causes,
            )
            now = time.time()
            connection.execute(
                """
                INSERT INTO operations(
                    operation_id, idempotency_key, producer, kind,
                    correlation_id, state, intent_json, prepared_at,
                    resolved_at, event_id
                ) VALUES (?, ?, ?, ?, ?, 'RECORDED', ?, ?, ?, ?)
                """,
                (
                    operation_id, idempotency_key, producer, kind,
                    correlation_id, encoded, now, now, event["event_id"],
                ),
            )
            return event, False

    def resolve(self, operation_id, node, payload, *, succeeded=True, error=None):
        self._check("operation_id", operation_id)
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
        expected_state = "APPLIED" if succeeded else "FAILED"
        expected_error = str(error)[:300] if error is not None else None
        with self.database.transaction() as connection:
            operation = connection.execute(
                "SELECT * FROM operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if operation is None:
                raise ValueError(f"unknown operation {operation_id!r}")
            if operation["state"] == "INDETERMINATE":
                raise ValueError(
                    "indeterminate operation requires explicit operator recovery"
                )
            if operation["event_id"]:
                if (operation["state"] != expected_state
                        or operation["error"] != expected_error):
                    raise ValueError("operation resolution conflicts with durable result")
                row = connection.execute(
                    "SELECT envelope_json FROM events WHERE event_id = ?",
                    (operation["event_id"],),
                ).fetchone()
                if row is None:
                    raise RuntimeError("resolved operation is missing its event")
                event = json.loads(row[0])
                if (event["node"] != node or encoded != operation["intent_json"]
                        or event["payload"] != payload):
                    raise ValueError("operation resolution conflicts with durable result")
                return event, True
            if operation["state"] != "PREPARED":
                raise ValueError("operation is not prepared for resolution")
            if encoded != operation["intent_json"]:
                raise ValueError("operation resolution differs from durable intent")
            durable_payload = json.loads(operation["intent_json"])
            event = self.events.emit_v2_in(
                connection, operation["producer"], operation["kind"], node,
                durable_payload, operation["correlation_id"],
            )
            changed = connection.execute(
                """
                UPDATE operations
                   SET state = ?, resolved_at = ?, event_id = ?, error = ?
                 WHERE operation_id = ? AND state = 'PREPARED'
                """,
                (expected_state, time.time(), event["event_id"], expected_error,
                 operation_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("operation state changed during resolution")
            return event, False

    def mark_indeterminate(self, operation_id, error):
        with self.database.connect() as connection:
            changed = connection.execute(
                """
                UPDATE operations SET state = 'INDETERMINATE', error = ?
                 WHERE operation_id = ? AND state = 'PREPARED'
                """,
                (str(error)[:300], operation_id),
            ).rowcount
        if not changed:
            raise ValueError(f"operation {operation_id!r} is not prepared")

    def unresolved(self):
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT operation_id, idempotency_key, producer, kind,
                       correlation_id, state, prepared_at, error
                  FROM operations
                 WHERE state IN ('PREPARED', 'INDETERMINATE')
                 ORDER BY prepared_at
                """
            ).fetchall()
        return [dict(row) for row in rows]
