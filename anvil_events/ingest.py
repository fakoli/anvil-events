"""Validated event-to-fact projection.

The active fact store is SQLite on every platform. Legacy JSONL is read-only
input for migration and verification; the event product performs no Git or
repository mutation.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from .domain import parse_allowed_producers, validate_event
from .legacy_jsonl import ARCHIVE_JSONL, DAILY_JSONL, iter_managed_jsonl

_DROP_FACT_FIELDS = frozenset([
    "token", "password", "secret", "api_key", "apikey", "credential",
    "authorization", "cookie",
])
_SENSITIVE_PARTS = re.compile(
    r"(?:^|_)(?:token|password|passwd|secret|credential|authorization|cookie)(?:$|_)"
)


def _sensitive_key(key, drop_fields):
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key))
    normalized = re.sub(
        r"[^A-Za-z0-9]+", "_", camel_split,
    ).strip("_").lower()
    compact = normalized.replace("_", "")
    suffixes = (
        "token", "password", "passwd", "secret", "credential",
        "authorization", "cookie", "apikey",
    )
    return (
        normalized in drop_fields
        or normalized in {"api_key", "apikey"}
        or bool(_SENSITIVE_PARTS.search(normalized))
        or compact.endswith(suffixes)
    )


def _redact(value, drop_fields):
    if isinstance(value, dict):
        return {
            key: _redact(child, drop_fields)
            for key, child in value.items()
            if not _sensitive_key(key, drop_fields)
        }
    if isinstance(value, list):
        return [_redact(child, drop_fields) for child in value]
    return value


def event_to_fact(event, drop_fields=_DROP_FACT_FIELDS):
    """Project a previously validated event to deterministic redacted facts."""
    return {
        "event_id": event.get("event_id"),
        "kind": event.get("kind"),
        "node": event.get("node", event.get("host")),
        "observed_at": event.get("observed_at"),
        "correlation_id": event.get("correlation_id"),
        "payload": _redact(event.get("payload") or {}, drop_fields),
    }


def fact_store_add(store_path, event, drop_fields=_DROP_FACT_FIELDS,
                   allowed_producers=None):
    """Compatibility helper backed by SQLite, never mutable JSONL."""
    from .storage import SQLiteStore

    path = Path(store_path)
    root = path if not path.suffix else path.parent / "facts-store"
    store = SQLiteStore(root)
    ok, _ = validate_event(event, allowed_producers=allowed_producers)
    if not ok:
        return None
    # FactRepository uses the standard projection; preserve an explicit custom
    # drop set for compatibility by refusing a divergent projection contract.
    if drop_fields != _DROP_FACT_FIELDS:
        raise ValueError("custom fact redaction is not supported by the v2 store")
    return store.add_fact(event, allowed_producers=allowed_producers)


class EventSource:
    def __init__(self, root):
        self.root = os.path.abspath(os.fspath(root))

    def read_all(self):
        from .storage import DATABASE_NAME, SQLiteStore

        if os.path.isfile(os.path.join(self.root, DATABASE_NAME)):
            return list(SQLiteStore(self.root).read_all())
        events = []
        for directory, pattern in (
            ("outbox", DAILY_JSONL),
            ("archive", ARCHIVE_JSONL),
            ("journal", DAILY_JSONL),
        ):
            events.extend(iter_managed_jsonl(
                os.path.join(self.root, directory), pattern,
                malformed="raise",
            ))
        return events


# V1 compatibility name for callers that used the old private helper.
_OutboxForIngest = EventSource


def cmd_ingest(args, fact_store=None):
    if isinstance(args, dict):
        root = args.get("root")
        count = args.get("count")
        store_root = args.get("store")
    elif not isinstance(args, str):
        root = args.root
        count = args.count
        store_root = args.store
    else:
        root = args
        count = None
        store_root = None
    allowed = parse_allowed_producers()
    if fact_store is None:
        from .storage import DATABASE_NAME, SQLiteStore

        if store_root:
            sink = SQLiteStore(store_root)
        elif os.path.isfile(os.path.join(root, DATABASE_NAME)):
            sink = SQLiteStore(root)
        else:
            sink = SQLiteStore(os.path.join(root, "facts-store"))
        def store_fact(event):
            return sink.add_fact(event, allowed_producers=allowed)

        fact_store = store_fact
    valid = []
    seen = set()
    dropped = 0
    reasons = {}
    for event in EventSource(root).read_all():
        ok, reason = validate_event(event, allowed_producers=allowed)
        if not ok:
            dropped += 1
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        event_id = event["event_id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        valid.append(event)
    if count is not None:
        valid = valid[:count]
    stored = sum(fact_store(event) is not None for event in valid)
    print(f"ingest: stored={stored} dropped={dropped}")
    for reason, total in sorted(reasons.items()):
        print(f"  drop: {total} x {reason}")
    return 0 if dropped == 0 else 1


def cli_cmd_emit(kind, payload, correlation, host, root):
    """Compatibility local-only emitter; never invokes Git or the broker."""
    from .store import open_event_store

    return open_event_store(root).emit(
        host, kind, host, payload, correlation_id=correlation,
    )


def register(subparsers):
    ingest = subparsers.add_parser(
        "ingest", help="project validated events into transactional facts",
    )
    ingest.add_argument("--count", type=int, default=None)
    ingest.add_argument(
        "--store", default=None,
        help="SQLite fact-store root (default: source store or root/facts-store)",
    )
    ingest.set_defaults(func=cmd_ingest)
