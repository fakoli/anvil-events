"""Event-domain types and validation with no storage or transport coupling."""

from __future__ import annotations

import os
import re
import time
from datetime import datetime

KINDS = frozenset([
    "serve.up", "serve.down", "profile.enter", "profile.leave",
    "promote.applied", "promote.rolled_back", "config.adopted",
    "repo.synced", "host.status", "divergence", "event.degraded",
])
PAYLOAD_REQUIRED = {
    "serve.up": ("serve", "model", "port"),
    "serve.down": ("serve",),
    "profile.enter": ("mode", "profile"),
    "profile.leave": ("mode", "profile"),
    "promote.applied": ("tier", "model"),
    "promote.rolled_back": ("tier", "restored_model"),
    "config.adopted": ("file",),
    "repo.synced": ("repo", "ok"),
    "host.status": ("host", "reachable"),
    "divergence": ("issue",),
    "event.degraded": ("cause",),
}
PAYLOAD_ALLOWED = {
    "serve.up": frozenset(["serve", "model", "port", "gpu_roles", "residency"]),
    "serve.down": frozenset(["serve", "graceful"]),
    "profile.enter": frozenset([
        "profile", "mode", "exclusive_target", "restore_group",
    ]),
    "profile.leave": frozenset([
        "profile", "mode", "exclusive_target", "restore_group",
    ]),
    "promote.applied": frozenset([
        "promotion", "tier", "model", "context", "rollback",
    ]),
    "promote.rolled_back": frozenset([
        "promotion", "tier", "restored_model",
    ]),
    "config.adopted": frozenset(["file", "files", "state", "repo", "rev"]),
    "repo.synced": frozenset(["repo", "ok", "committed", "pushed", "error"]),
    "host.status": frozenset(["host", "reachable", "gpu_used", "gpu_free"]),
    "divergence": frozenset(["issue", "declared", "live", "delta"]),
    "event.degraded": frozenset([
        "cause", "event_id", "file", "bytes", "records", "pending",
    ]),
}
_STRING_FIELDS = frozenset([
    "serve", "model", "residency", "profile", "mode", "exclusive_target",
    "restore_group", "promotion", "tier", "rollback", "restored_model", "file",
    "state", "repo", "rev", "error", "host", "issue", "cause", "event_id",
])
_BOOL_FIELDS = frozenset(["graceful", "ok", "committed", "pushed", "reachable"])
_INTEGER_FIELDS = frozenset(["port", "context", "bytes", "records", "pending"])
_NUMBER_FIELDS = frozenset(["gpu_used", "gpu_free"])
SCHEMA = "https://anvil.dev/schemas/events/v1.json"
_HOST_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_PRODUCER = re.compile(r"^[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*$")
_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$",
    re.IGNORECASE,
)
_ENVELOPE_REQUIRED = (
    "version", "event_id", "producer", "producer_seq", "observed_at", "emitted_at",
    "correlation_id", "schema", "host", "kind", "subject", "payload",
)
_OPTIONAL_ENVELOPE = frozenset(["causes"])


def validate_payload(kind, payload):
    """Validate the v1 per-kind payload allowlist and scalar types."""
    if not isinstance(payload, dict):
        return False, "payload must be a dict"
    missing = [
        field for field in PAYLOAD_REQUIRED.get(kind, ())
        if payload.get(field) is None
    ]
    if missing:
        return False, f"payload missing required fields: {missing}"
    unknown = set(payload) - PAYLOAD_ALLOWED.get(kind, frozenset())
    if unknown:
        return False, f"payload has unknown fields: {sorted(unknown)}"
    for key, value in payload.items():
        if value is None and key not in PAYLOAD_REQUIRED.get(kind, ()):
            continue
        if key in _STRING_FIELDS and not isinstance(value, str):
            return False, f"payload field {key!r} must be a string"
        if key in _BOOL_FIELDS and not isinstance(value, bool):
            return False, f"payload field {key!r} must be a boolean"
        if key in _INTEGER_FIELDS:
            if (isinstance(value, bool)
                    or not isinstance(value, int | float)
                    or (isinstance(value, float) and not value.is_integer())):
                return False, f"payload field {key!r} must be an integer"
        if key in _NUMBER_FIELDS and (isinstance(value, bool)
                                      or not isinstance(value, int | float)):
            return False, f"payload field {key!r} must be a number"
        if key == "gpu_roles" and (
                not isinstance(value, list)
                or not all(isinstance(item, str) for item in value)):
            return False, "payload field 'gpu_roles' must be a string array"
        if key == "files" and (
                not isinstance(value, list)
                or not all(isinstance(item, str) for item in value)):
            return False, "payload field 'files' must be a string array"
    return True, ""


def utcnow_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def make_event(producer, kind, host, payload, correlation_id=None,
               producer_seq=1, observed_at=None, causes=None, version=1):
    """Build a v1 event envelope."""
    if kind not in KINDS:
        raise ValueError(
            "unknown kind {!r}; frozen kinds: {}".format(
                kind, ",".join(sorted(KINDS)),
            )
        )
    if not isinstance(producer, str) or not _PRODUCER.fullmatch(producer):
        raise ValueError("producer must be colon-separated safe tokens")
    if not isinstance(host, str) or not _HOST_TOKEN.fullmatch(host):
        raise ValueError("host must be one safe NATS token")
    if correlation_id is not None and not isinstance(correlation_id, str):
        raise ValueError("correlation_id must be a string or null")
    seq = int(producer_seq)
    if seq < 1:
        raise ValueError("producer_seq must be positive")
    return {
        "version": version,
        "event_id": f"{producer}:{seq:06d}",
        "producer": producer,
        "producer_seq": seq,
        "observed_at": observed_at or utcnow_iso(),
        "emitted_at": utcnow_iso(),
        "correlation_id": correlation_id,
        "schema": SCHEMA,
        "host": host,
        "kind": kind,
        "subject": f"anvil.fleet.{host}.{kind}",
        "payload": payload or {},
        "causes": list(causes or []),
    }


def is_rfc3339(value):
    if not isinstance(value, str) or not _RFC3339.fullmatch(value):
        return False
    try:
        normalized = value[:10] + "T" + value[11:]
        if normalized.endswith(("Z", "z")):
            normalized = normalized[:-1] + "+00:00"
        datetime.fromisoformat(normalized)
        return True
    except ValueError:
        return False


def validate_event(event, allowed_producers=None):
    """Validate a complete v1 event envelope."""
    if not isinstance(event, dict):
        return False, "not a dict"
    if event.get("version") == 2:
        from .domain_v2 import validate_event_v2

        return validate_event_v2(event, allowed_producers=allowed_producers)
    version = event.get("version")
    if (isinstance(version, bool) or not isinstance(version, int | float)
            or version != 1):
        return False, f"unsupported version {version!r}"
    missing = [field for field in _ENVELOPE_REQUIRED if field not in event]
    if missing:
        return False, f"envelope missing required fields: {missing}"
    extra = set(event) - set(_ENVELOPE_REQUIRED) - _OPTIONAL_ENVELOPE
    if extra:
        return False, f"envelope has unknown fields: {sorted(extra)}"
    if event.get("schema") != SCHEMA:
        return False, "unsupported schema URI"
    if (not isinstance(event.get("producer"), str)
            or not _PRODUCER.fullmatch(event["producer"])):
        return False, "producer must be colon-separated safe tokens"
    if (not isinstance(event.get("host"), str)
            or not _HOST_TOKEN.fullmatch(event["host"])):
        return False, "host must be one safe NATS token"
    if not is_rfc3339(event.get("observed_at")) or not is_rfc3339(
            event.get("emitted_at")):
        return False, "observed_at/emitted_at must be ISO date-times"
    if (event.get("correlation_id") is not None
            and not isinstance(event.get("correlation_id"), str)):
        return False, "correlation_id must be a string or null"
    causes = event.get("causes", [])
    if not isinstance(causes, list) or not all(
            isinstance(cause, str) and cause for cause in causes):
        return False, "causes must be a list of non-empty event IDs"
    kind = event.get("kind")
    if kind not in KINDS:
        return False, f"forged/unknown kind {kind!r}"
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return False, "payload must be a dict"
    if event.get("subject") != f"anvil.fleet.{event['host']}.{kind}":
        return False, "subject does not match host/kind"
    try:
        sequence = int(event["producer_seq"])
    except (TypeError, ValueError):
        return False, "producer_seq must be an integer"
    if (isinstance(event["producer_seq"], bool) or sequence < 1
            or sequence != event["producer_seq"]):
        return False, "producer_seq must be a positive integer"
    if event.get("event_id") != f"{event['producer']}:{sequence:06d}":
        return False, "event_id does not match producer/producer_seq"
    if allowed_producers is not None and event["producer"] not in allowed_producers:
        return False, "producer is not authorized"
    return validate_payload(kind, payload)


def parse_allowed_producers(value=None):
    """Parse configured producer identities; empty configuration denies all."""
    if value is None:
        value = os.environ.get("ANVIL_EVENTS_ALLOWED_PRODUCERS", "")
    return frozenset(item.strip() for item in value.split(",") if item.strip())
