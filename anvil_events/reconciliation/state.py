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
                abandoned = conn.execute(
                    """
                    SELECT operation_id FROM reconcile_applications
                     WHERE node = ? AND resource = ?
                    """,
                    (node, payload["resource"]),
                ).fetchone()
                if abandoned is not None:
                    conn.execute(
                        """
                        UPDATE reconcile_attempts
                           SET state = 'INDETERMINATE',
                               error = 'process ended during external apply',
                               completed_at = ?
                         WHERE operation_id = ?
                        """,
                        (time.time(), abandoned["operation_id"]),
                    )
                    conn.execute(
                        "DELETE FROM reconcile_applications WHERE operation_id = ?",
                        (abandoned["operation_id"],),
                    )
                    if abandoned["operation_id"] == operation_id:
                        return "indeterminate", operation_id
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
                        owner = conn.execute(
                            """
                            SELECT operation_id FROM reconcile_attempts
                             WHERE node = ? AND resource = ? AND generation = ?
                            """,
                            (node, payload["resource"], payload["generation"]),
                        ).fetchone()
                        if owner is not None:
                            operation_id = owner["operation_id"]
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

    def reopen_applied(self, operation_id, desired, node):
        """Re-open one applied generation after its adapter detects drift."""
        payload = desired["payload"]
        with self.store.transaction(immediate=True) as conn:
            current = conn.execute(
                """
                SELECT generation, revision, content_sha256, adapter
                  FROM reconcile_resources
                 WHERE node = ? AND resource = ?
                """,
                (node, payload["resource"]),
            ).fetchone()
            if current is None or any(
                current[field] != payload[field]
                for field in (
                    "generation", "revision", "content_sha256", "adapter",
                )
            ):
                raise ValueError("applied resource changed before drift repair")
            changed = conn.execute(
                """
                UPDATE reconcile_attempts
                   SET state = 'PREPARED', preview_json = NULL,
                       error = NULL, started_at = ?, completed_at = NULL
                 WHERE operation_id = ?
                """,
                (time.time(), operation_id),
            ).rowcount
            if not changed:
                raise ValueError("applied reconciliation attempt is missing")

    def applied_event_ids(self, node):
        with self.store.transaction(immediate=False) as conn:
            rows = conn.execute(
                """
                SELECT event_id FROM reconcile_resources
                 WHERE node = ? ORDER BY resource
                """,
                (node,),
            ).fetchall()
        return tuple(row["event_id"] for row in rows)

    def begin_apply(self, operation_id):
        with self.store.transaction(immediate=True) as conn:
            attempt = conn.execute(
                """
                SELECT node, resource FROM reconcile_attempts
                 WHERE operation_id = ? AND state = 'PREPARED'
                """,
                (operation_id,),
            ).fetchone()
            if attempt is None:
                raise ValueError("reconciliation attempt is not ready to apply")
            conn.execute(
                """
                INSERT INTO reconcile_applications(
                    operation_id, node, resource, started_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    operation_id, attempt["node"], attempt["resource"],
                    time.time(),
                ),
            )

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
                applying = conn.execute(
                    "SELECT 1 FROM reconcile_applications WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if applying is None:
                    raise ValueError("reconciliation attempt is not applying")
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
                conn.execute(
                    "DELETE FROM reconcile_applications WHERE operation_id = ?",
                    (operation_id,),
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
            conn.execute(
                "DELETE FROM reconcile_applications WHERE operation_id = ?",
                (operation_id,),
            )
        if not changed:
            raise ValueError("unknown reconciliation attempt")
