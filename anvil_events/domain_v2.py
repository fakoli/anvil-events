"""Generic v2 event envelope for desired-state convergence."""

from __future__ import annotations

import hashlib
import json
import re

from .domain import is_rfc3339, utcnow_iso

SCHEMA_V2 = "https://anvil.dev/schemas/events/v2.json"
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_KIND = re.compile(r"^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)+$")
_RESOURCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:token|password|passwd|secret|credential|authorization|cookie|"
    r"api_key|private_key|ssh_private_key|access_key(?:_id)?|secret_access_key|"
    r"client_secret|client_key|signing_key|bearer|session_key)(?:$|_)",
    re.IGNORECASE,
)
_REQUIRED = frozenset([
    "version", "event_id", "producer", "producer_seq", "observed_at",
    "emitted_at", "correlation_id", "schema", "node", "kind", "subject",
    "payload", "causes",
])

DESIRED_KINDS = frozenset([
    "state.desired",
    "reconcile.applied",
    "reconcile.failed",
    "reconcile.awaiting_approval",
    "operation.indeterminate",
    "delivery.degraded",
])


def _valid_resource(value):
    return bool(
        isinstance(value, str)
        and _RESOURCE.fullmatch(value)
        and all(
            segment and segment not in (".", "..")
            and segment[0].isalnum()
            for segment in value.split("/")
        )
    )


def valid_resource_identifier(value):
    """Return whether value is a portable logical resource/artifact identifier."""
    return _valid_resource(value)


def valid_revision_identifier(value):
    """Return whether value is one safe immutable revision identifier."""
    return bool(isinstance(value, str) and _REVISION.fullmatch(value))


def canonical_json(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )


def content_sha256(value):
    raw = value if isinstance(value, bytes) else canonical_json(value).encode()
    return hashlib.sha256(raw).hexdigest()


def _contains_sensitive_key(value):
    pending = [(value, 0)]
    visited = 0
    while pending:
        current, depth = pending.pop()
        visited += 1
        if visited > 100_000 or depth > 64:
            raise ValueError("payload nesting is too complex")
        if isinstance(current, dict):
            for key, child in current.items():
                camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
                normalized = re.sub(
                    r"[^A-Za-z0-9]+", "_", camel_split,
                ).strip("_").lower()
                if _SENSITIVE_KEY.search(normalized):
                    return True
                pending.append((child, depth + 1))
        elif isinstance(current, list):
            pending.extend((child, depth + 1) for child in current)
        elif isinstance(current, str):
            if re.match(r"^(?:https?|nats|tls)://", current, re.IGNORECASE):
                return True
            if re.search(
                    r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", current,
                    re.IGNORECASE):
                return True
    return False


def validate_payload(kind, payload):
    if not isinstance(payload, dict):
        return False, "payload must be an object"
    try:
        if _contains_sensitive_key(payload):
            return False, "payload contains a credential-shaped key or URL"
        encoded = canonical_json(payload).encode()
    except (RecursionError, TypeError, ValueError) as exc:
        if "nesting is too complex" in str(exc):
            return False, str(exc)
        return False, "payload must contain only JSON values"
    if len(encoded) > 1024 * 1024 - 4096:
        return False, "payload exceeds the v2 envelope limit"
    if kind in DESIRED_KINDS:
        required = {
            "state.desired": {
                "resource", "generation", "revision", "content_sha256", "adapter",
                "artifact",
            },
            "reconcile.applied": {
                "resource", "generation", "revision", "content_sha256", "adapter",
            },
            "reconcile.failed": {
                "resource", "generation", "revision", "adapter", "error",
            },
            "reconcile.awaiting_approval": {
                "resource", "generation", "revision", "adapter",
            },
            "operation.indeterminate": {"operation_id", "error"},
            "delivery.degraded": {"event_id", "error"},
        }[kind]
        missing = sorted(required - set(payload))
        if missing:
            return False, f"payload missing required fields: {missing}"
    if "resource" in payload and not _valid_resource(payload["resource"]):
        return False, "resource must be a safe logical identifier"
    if "generation" in payload and (
            isinstance(payload["generation"], bool)
            or not isinstance(payload["generation"], int)
            or payload["generation"] < 1):
        return False, "generation must be a positive integer"
    if "content_sha256" in payload and (
            not isinstance(payload["content_sha256"], str)
            or not _SHA256.fullmatch(payload["content_sha256"])):
        return False, "content_sha256 must be a lowercase SHA-256 digest"
    if "targets" in payload and (
            not isinstance(payload["targets"], list)
            or not payload["targets"]
            or len(set(payload["targets"])) != len(payload["targets"])
            or not all(
                isinstance(target, str) and _SAFE_TOKEN.fullmatch(target)
                for target in payload["targets"]
            )):
        return False, "targets must be a non-empty array of unique node tokens"
    for field in ("revision", "adapter", "artifact", "operation_id", "error"):
        if field in payload and (
                not isinstance(payload[field], str) or not payload[field]
                or len(payload[field]) > (300 if field == "error" else 256)
                or "\r" in payload[field] or "\n" in payload[field]):
            return False, f"{field} must be a bounded non-empty string"
    if "adapter" in payload and not _SAFE_TOKEN.fullmatch(payload["adapter"]):
        return False, "adapter must be one safe token"
    if "revision" in payload and not valid_revision_identifier(payload["revision"]):
        return False, "revision must be one safe immutable identifier"
    if "artifact" in payload and not valid_resource_identifier(payload["artifact"]):
        return False, "artifact must be a safe logical identifier"
    return True, ""


def make_event_v2(producer, kind, node, payload, *, producer_seq,
                  correlation_id=None, causes=None, observed_at=None):
    if not isinstance(producer, str) or len(producer) > 256 or not re.fullmatch(
            r"[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*", producer):
        raise ValueError("producer must be colon-separated safe tokens")
    if (not isinstance(node, str) or len(node) > 64
            or not _SAFE_TOKEN.fullmatch(node)):
        raise ValueError("node must be one safe token")
    if not isinstance(kind, str) or len(kind) > 128 or not _KIND.fullmatch(kind):
        raise ValueError("kind must be a dotted lowercase identifier")
    if producer.split(":", 1)[0] != node:
        raise ValueError("producer identity must belong to the event node")
    if isinstance(producer_seq, bool) or not isinstance(producer_seq, int) \
            or producer_seq < 1:
        raise ValueError("producer_seq must be a positive integer")
    if correlation_id is not None and (
            not isinstance(correlation_id, str) or len(correlation_id) > 256):
        raise ValueError("correlation_id must be a string or null")
    ok, reason = validate_payload(kind, payload)
    if not ok:
        raise ValueError(reason)
    now = utcnow_iso()
    return {
        "version": 2,
        "event_id": f"{producer}:{producer_seq:06d}",
        "producer": producer,
        "producer_seq": producer_seq,
        "observed_at": observed_at or now,
        "emitted_at": now,
        "correlation_id": correlation_id,
        "schema": SCHEMA_V2,
        "node": node,
        "kind": kind,
        "subject": f"anvil.events.v2.{node}.{kind}",
        "payload": payload,
        "causes": list(causes or []),
    }


def validate_event_v2(event, allowed_producers=None):
    if not isinstance(event, dict):
        return False, "not an object"
    if set(event) != _REQUIRED:
        missing = sorted(_REQUIRED - set(event))
        extra = sorted(set(event) - _REQUIRED)
        return False, f"envelope fields mismatch: missing={missing} extra={extra}"
    if event.get("version") != 2 or isinstance(event.get("version"), bool):
        return False, "unsupported v2 version"
    if event.get("schema") != SCHEMA_V2:
        return False, "unsupported v2 schema URI"
    producer = event.get("producer")
    sequence = event.get("producer_seq")
    if (not isinstance(producer, str) or len(producer) > 256
            or not re.fullmatch(r"[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*", producer)):
        return False, "producer must be colon-separated safe tokens"
    if (isinstance(sequence, bool) or not isinstance(sequence, int)
            or sequence < 1):
        return False, "producer_seq must be a positive integer"
    if event.get("event_id") != f"{producer}:{sequence:06d}":
        return False, "event_id does not match producer sequence"
    node = event.get("node")
    kind = event.get("kind")
    if (not isinstance(node, str) or len(node) > 64
            or not _SAFE_TOKEN.fullmatch(node)):
        return False, "node must be one safe token"
    if not isinstance(kind, str) or len(kind) > 128 or not _KIND.fullmatch(kind):
        return False, "kind must be a dotted lowercase identifier"
    if producer.split(":", 1)[0] != node:
        return False, "producer identity must belong to the event node"
    if event.get("subject") != f"anvil.events.v2.{node}.{kind}":
        return False, "subject does not match node and kind"
    if event.get("correlation_id") is not None and (
            not isinstance(event["correlation_id"], str)
            or len(event["correlation_id"]) > 256):
        return False, "correlation_id must be a string or null"
    if not all(is_rfc3339(value) for value in (
            event.get("observed_at"), event.get("emitted_at"))):
        return False, "event timestamps must be RFC 3339 date-times"
    causes = event.get("causes")
    if not isinstance(causes, list) or not all(
            isinstance(cause, str) and cause and len(cause) <= 256
            for cause in causes):
        return False, "causes must be non-empty event IDs"
    if len(set(causes)) != len(causes):
        return False, "causes must not contain duplicates"
    if allowed_producers is not None and producer not in allowed_producers:
        return False, "producer is not authorized"
    return validate_payload(kind, event.get("payload"))
