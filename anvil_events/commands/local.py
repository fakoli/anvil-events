"""Local event-store commands; none of these contact the broker."""

from __future__ import annotations

import hashlib
import json
import os
import sys

from ..dependency_graph import DependencyGraphChecker
from ..legacy_jsonl import ARCHIVE_JSONL, DAILY_JSONL, iter_managed_jsonl
from ..storage import DATABASE_NAME, SQLiteStore
from ..store import backend_name, open_event_store


def _store(args):
    return open_event_store(args.root, backend=args.backend)


def init(args):
    store = _store(args)
    print(f"backend: {backend_name(store)}")
    print(f"database: {store.database_path}")
    return 0


def record(args):
    """Atomically accept an idempotent v2 event without broker I/O."""
    store = _store(args)
    if not isinstance(store, SQLiteStore):
        raise RuntimeError("record requires the SQLite backend")
    raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("record requires one JSON payload object on stdin")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("record payload must be a JSON object")
    operation_id = "record:" + hashlib.sha256(
        args.operation_key.encode(),
    ).hexdigest()
    event, repeated = store.record_v2(
        operation_id,
        args.operation_key,
        args.producer,
        args.kind,
        args.node,
        payload,
        correlation_id=args.correlation,
    )
    print(json.dumps({
        "accepted": True,
        "already_recorded": repeated,
        "event_id": event["event_id"],
    }, sort_keys=True))
    return 0


def status(args):
    store = _store(args)
    result = store.status()
    result["degraded"] = bool(
        result["pending"] or result["quarantined"]
        or result["unresolved_operations"]
    )
    if args.json:
        print(json.dumps(result, sort_keys=True))
        return 0
    for key in (
        "backend", "schema_version", "pending", "archived", "journaled",
        "cursors", "facts", "quarantined", "unresolved_operations",
        "degraded",
    ):
        print(f"{key}: {result[key]}")
    return 0


def replay(args):
    events = list(_store(args).read_all())
    events.sort(key=lambda event: (
        event.get("observed_at", ""), event.get("producer_seq", 0),
    ))
    for event in events[-args.lines:]:
        print("{} {} {} corr={}".format(
            event.get("observed_at", "?")[:19],
            event.get("event_id"),
            event.get("kind"),
            event.get("correlation_id"),
        ))
    return 0


def _read_verify_input(path):
    if os.path.isdir(path):
        database = os.path.join(path, DATABASE_NAME)
        if os.path.isfile(database):
            return list(SQLiteStore(path).read_all())
        events = []
        for subdirectory, pattern in (
            ("outbox", DAILY_JSONL),
            ("archive", ARCHIVE_JSONL),
            ("journal", DAILY_JSONL),
        ):
            events.extend(iter_managed_jsonl(
                os.path.join(path, subdirectory), pattern, malformed="raise",
            ))
        return events
    parent, name = os.path.split(os.path.abspath(path))
    if ARCHIVE_JSONL.fullmatch(name):
        pattern = ARCHIVE_JSONL
    elif DAILY_JSONL.fullmatch(name):
        pattern = DAILY_JSONL
    else:
        raise ValueError("verify input is not a managed JSONL filename")
    return list(iter_managed_jsonl(
        parent, pattern, malformed="raise", managed_name=name,
    ))


def verify(args):
    events = _read_verify_input(args.path)
    events.sort(key=lambda event: (
        event.get("observed_at", ""), event.get("producer_seq", 0),
    ))
    ok, error = DependencyGraphChecker.check(events)
    print(f"dependency graph: {'OK' if ok else 'VIOLATED'} ({len(events)} events)")
    if error:
        print(f"  {error}")
        return 1
    return 0


def gc(args):
    result = _store(args).gc(archive_days=args.archive_days)
    print(f"gc: expired {result['removed']} archived event(s)")
    if result["degraded"]:
        print(f"gc: audit event {result['degraded']}")
    if result["unresolved_oversize"]:
        print("gc: database remains above the configured size guard")
        return 1
    return 0


def migrate_legacy(args):
    target = SQLiteStore(args.root)
    result = target.import_legacy(
        args.legacy_root, offline=args.offline_source,
    )
    state = "already imported" if result["already_imported"] else "imported"
    print(
        f"migration: {state}; pending={result['pending']} "
        f"archive={result['acked']} journal={result['journal']}"
    )
    print(f"database: {target.database_path}")
    print("source retained: yes")
    return 0
