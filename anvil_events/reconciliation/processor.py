"""Subscriber-side desired-state processing and outcome recording."""

from __future__ import annotations

from .engine import ReconcileResult
from .state import ReconcileState


class DesiredStateProcessor:
    def __init__(self, engine, store, *, producer, node):
        self.engine = engine
        self.store = store
        self.producer = producer
        self.node = node

    def process(self, event):
        if event["kind"] != "state.desired":
            return None
        try:
            result = self.engine.process(event)
        except ValueError:
            payload = event["payload"]
            operation_id = ReconcileState.operation_id(self.node, event["event_id"])
            result = ReconcileResult(
                "rejected",
                operation_id,
                "reconcile.failed",
                {
                    "resource": payload["resource"],
                    "generation": payload["generation"],
                    "revision": payload["revision"],
                    "adapter": payload["adapter"],
                    "operation_id": operation_id,
                    "node": self.node,
                    "error": "desired state rejected",
                },
            )
        if result.outcome_kind is None:
            return result
        operation_key = f"outcome:{result.operation_id}:{result.outcome_kind}"
        self.store.record_v2(
            operation_key,
            operation_key,
            self.producer,
            result.outcome_kind,
            self.node,
            result.payload,
            correlation_id=event.get("correlation_id"),
            causes=[event["event_id"]],
        )
        return result
