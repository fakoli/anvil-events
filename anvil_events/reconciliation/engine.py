"""Desired-state reconciliation orchestration with explicit policy gates."""

from __future__ import annotations

from dataclasses import dataclass

from ..domain_v2 import content_sha256, validate_event_v2
from .contracts import DenyByDefaultPolicy
from .resource_lock import resource_lock
from .state import ReconcileState


@dataclass(frozen=True)
class ReconcileResult:
    state: str
    operation_id: str
    outcome_kind: str | None
    payload: dict | None


class ReconcileEngine:
    def __init__(self, node, event_store, adapters, artifact_resolver,
                 policy=None):
        self.node = node
        self.store = event_store
        self.adapters = adapters
        self.artifacts = artifact_resolver
        self.policy = policy or DenyByDefaultPolicy()
        self.state = ReconcileState(event_store)

    def process(self, desired):
        ok, reason = validate_event_v2(desired)
        if not ok:
            raise ValueError(f"invalid desired event: {reason}")
        if desired["kind"] != "state.desired":
            raise ValueError("reconciler accepts only state.desired events")
        payload = desired["payload"]
        targets = payload.get("targets")
        if targets is not None and self.node not in targets:
            return ReconcileResult("not-targeted", "", None, None)
        with resource_lock(self.store.root, self.node, payload["resource"]):
            return self._process_locked(desired, payload)

    def _process_locked(self, desired, payload):
        claim, operation_id = self.state.claim(self.node, desired)
        if claim == "superseded":
            return ReconcileResult(claim, operation_id, None, None)
        if claim == "applied":
            return ReconcileResult(
                claim, operation_id, "reconcile.applied",
                self._outcome_payload(payload, operation_id),
            )
        if claim in ("failed", "indeterminate"):
            return ReconcileResult(
                claim, operation_id, "reconcile.failed",
                {**self._outcome_payload(payload, operation_id),
                 "error": f"reconciliation is {claim}"},
            )
        try:
            adapter = self.adapters.get(payload["adapter"])
            artifact = self.artifacts.resolve(
                payload["artifact"], payload["revision"],
            )
        except ValueError as exc:
            self.state.fail(operation_id, exc)
            return ReconcileResult(
                "failed", operation_id, "reconcile.failed",
                {**self._outcome_payload(payload, operation_id),
                 "error": "artifact or adapter resolution failed"},
            )
        if artifact.revision != payload["revision"]:
            self.state.fail(operation_id, "artifact revision mismatch")
            return ReconcileResult(
                "failed", operation_id, "reconcile.failed",
                {**self._outcome_payload(payload, operation_id),
                 "error": "artifact revision mismatch"},
            )
        if content_sha256(artifact.data) != payload["content_sha256"]:
            self.state.fail(operation_id, "artifact digest mismatch")
            return ReconcileResult(
                "failed", operation_id, "reconcile.failed",
                {**self._outcome_payload(payload, operation_id),
                 "error": "artifact digest mismatch"},
            )
        try:
            preview = adapter.preview(desired, artifact)
        except Exception as exc:
            self.state.fail(operation_id, exc)
            return ReconcileResult(
                "failed", operation_id, "reconcile.failed",
                {**self._outcome_payload(payload, operation_id),
                 "error": "adapter preview failed"},
            )
        if not self.policy.allows(desired, preview):
            self.state.set_preview(operation_id, preview, awaiting_approval=True)
            outcome = self._outcome_payload(payload, operation_id)
            return ReconcileResult(
                "awaiting-approval", operation_id,
                "reconcile.awaiting_approval", outcome,
            )
        self.state.set_preview(operation_id, preview)
        self.state.begin_apply(operation_id)
        try:
            adapter.apply(desired, artifact)
        except Exception as exc:
            self.state.fail(operation_id, exc, indeterminate=True)
            return ReconcileResult(
                "indeterminate", operation_id, "reconcile.failed",
                {**self._outcome_payload(payload, operation_id),
                 "error": "adapter apply result is indeterminate"},
            )
        try:
            verified = adapter.verify(desired, artifact)
        except Exception as exc:
            verified = False
            verify_error = exc
        else:
            verify_error = RuntimeError("adapter verification returned false")
        if not verified:
            try:
                adapter.rollback(desired)
            except Exception as rollback_error:
                verify_error = RuntimeError(
                    f"verification failed; rollback failed: {rollback_error}"
                )
                public_error = "adapter verification and rollback failed"
            else:
                public_error = "adapter verification failed"
            self.state.fail(operation_id, verify_error)
            return ReconcileResult(
                "failed", operation_id, "reconcile.failed",
                {
                    **self._outcome_payload(payload, operation_id),
                    "error": public_error,
                },
            )
        self.state.complete(operation_id, desired, self.node)
        return ReconcileResult(
            "applied", operation_id, "reconcile.applied",
            self._outcome_payload(payload, operation_id),
        )

    def _outcome_payload(self, desired, operation_id):
        return {
            "resource": desired["resource"],
            "generation": desired["generation"],
            "revision": desired["revision"],
            "content_sha256": desired["content_sha256"],
            "adapter": desired["adapter"],
            "operation_id": operation_id,
            "node": self.node,
        }
