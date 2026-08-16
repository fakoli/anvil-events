"""Transactional local state for reconciliation attempts and generations."""

from __future__ import annotations

import hashlib
import json
import time


class ReconcileState:
    def __init__(self, event_store):
        self.store = event_store

    @staticmethod
    def operation_id(node, event_id):
        return "reconcile:" + hashlib.sha256(
            f"{node}\0{event_id}".encode(),
        ).hexdigest()

    def claim(self, node, desired):
        payload = desired["payload"]
        operation_id = self.operation_id(node, desired["event_id"])
        with self.store.transaction(immediate=True) as conn:
            try:
                current = conn.execute(
                    "SELECT * FROM reconcile_resources WHERE node = ? AND resource = ?",
                    (node, payload["resource"]),
                ).fetchone()
                if current is not None:
                    if payload["generation"] < current["generation"]:
                        return "superseded", operation_id
                    if payload["generation"] == current["generation"]:
                        same = (
                            payload["revision"] == current["revision"]
                            and payload["content_sha256"] == current["content_sha256"]
                            and payload["adapter"] == current["adapter"]
                        )
                        if not same:
                            raise ValueError(
                                "conflicting desired state for an applied generation"
                            )
                        return "applied", operation_id
                attempt = conn.execute(
                    "SELECT state FROM reconcile_attempts WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if attempt is not None:
                    return attempt["state"].lower(), operation_id
                generation_owner = conn.execute(
                    """
                    SELECT event_id, generation, state
                      FROM reconcile_attempts
                     WHERE node = ? AND resource = ?
                     ORDER BY generation DESC LIMIT 1
                    """,
                    (node, payload["resource"]),
                ).fetchone()
                if generation_owner is not None:
                    if payload["generation"] < generation_owner["generation"]:
                        return "superseded", operation_id
                    if payload["generation"] == generation_owner["generation"]:
                        raise ValueError(
                            "conflicting desired events reuse one resource generation"
                        )
                conn.execute(
                    """
                    INSERT INTO reconcile_attempts(
                        operation_id, event_id, node, resource, generation,
                        state, started_at
                    ) VALUES (?, ?, ?, ?, ?, 'PREPARED', ?)
                    """,
                    (
                        operation_id, desired["event_id"], node,
                        payload["resource"], payload["generation"], time.time(),
                    ),
                )
                return "prepared", operation_id
            except Exception:
                raise

    def set_preview(self, operation_id, preview, awaiting_approval=False):
        encoded = json.dumps({
            "summary": preview.summary,
            "changes": list(preview.changes),
        }, sort_keys=True)
        state = "AWAITING_APPROVAL" if awaiting_approval else "PREPARED"
        with self.store.transaction() as conn:
            changed = conn.execute(
                """
                UPDATE reconcile_attempts SET preview_json = ?, state = ?
                 WHERE operation_id = ? AND state IN ('PREPARED', 'AWAITING_APPROVAL')
                """,
                (encoded, state, operation_id),
            ).rowcount
        if not changed:
            raise ValueError("reconciliation attempt is not previewable")

    def complete(self, operation_id, desired, node):
        payload = desired["payload"]
        with self.store.transaction(immediate=True) as conn:
            try:
                current = conn.execute(
                    "SELECT generation, revision, content_sha256 FROM reconcile_resources "
                    "WHERE node = ? AND resource = ?",
                    (node, payload["resource"]),
                ).fetchone()
                if current is not None and payload["generation"] < current["generation"]:
                    raise ValueError("a newer resource generation is already applied")
                if current is not None and payload["generation"] == current["generation"]:
                    if (current["revision"] != payload["revision"]
                            or current["content_sha256"] != payload["content_sha256"]):
                        raise ValueError("generation content conflict")
                conn.execute(
                    """
                    INSERT INTO reconcile_resources(
                        node, resource, generation, revision, content_sha256,
                        adapter, event_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(node, resource) DO UPDATE SET
                        generation = excluded.generation,
                        revision = excluded.revision,
                        content_sha256 = excluded.content_sha256,
                        adapter = excluded.adapter,
                        event_id = excluded.event_id,
                        updated_at = excluded.updated_at
                    WHERE excluded.generation >= reconcile_resources.generation
                    """,
                    (
                        node, payload["resource"], payload["generation"],
                        payload["revision"], payload["content_sha256"],
                        payload["adapter"], desired["event_id"], time.time(),
                    ),
                )
                conn.execute(
                    """
                    UPDATE reconcile_attempts
                       SET state = 'APPLIED', completed_at = ?
                     WHERE operation_id = ?
                    """,
                    (time.time(), operation_id),
                )
            except Exception:
                raise

    def fail(self, operation_id, error, indeterminate=False):
        state = "INDETERMINATE" if indeterminate else "FAILED"
        with self.store.transaction() as conn:
            changed = conn.execute(
                """
                UPDATE reconcile_attempts
                   SET state = ?, error = ?, completed_at = ?
                 WHERE operation_id = ?
                """,
                (state, str(error)[:300], time.time(), operation_id),
            ).rowcount
        if not changed:
            raise ValueError("unknown reconciliation attempt")
