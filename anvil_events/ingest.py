"""M4: private operator adapter — validated ingestion + repo-sync.

Core plumbing for:
- `sync-repo`: after a successful lifecycle change, commit the operator
  repo's pending state and (optionally) push, then emit correlation-linked
  `config.adopted` + `repo.synced` events.
- `ingest`: consume events from a journal/outbox and store validated ones
  as facts, dropping forged/invalid events and never storing unknowns.

Keeps `dependencies = []` (stdlib only); the Hermes fact-store hook is
injected/patched in the operator environment. All I/O is through injectable
seams (`_run`) so the suite stays hermetic.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .outbox import (
    _ARCHIVE_JSONL,
    _DAILY_JSONL,
    KINDS,
    SCHEMA,
    iter_managed_jsonl,
    open_pinned_directory,
    open_regular_at,
    validate_payload,
)

# Frozen kinds + the payload fields we will store as facts.
_FROZEN_KINDS = KINDS

# Fields ALWAYS dropped from stored facts (they leak implementation/host
# details or are high-cardinality noise; the operator adapter never stores
# credentials, tokens, or secrets).
_DROP_FACT_FIELDS = frozenset([
    "token", "password", "secret", "api_key", "apikey", "credential",
    "authorization", "cookie",
])

_ENVELOPE_REQUIRED = (
    "version", "event_id", "producer", "producer_seq", "observed_at", "emitted_at",
    "correlation_id", "schema", "host", "kind", "subject", "payload",
)
_SCHEMA = SCHEMA
_HOST_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_PRODUCER = re.compile(r"^[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*$")
_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$",
    re.IGNORECASE,
)
_OPTIONAL_ENVELOPE = frozenset(["causes"])

def _is_datetime(value):
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


def parse_allowed_producers(value=None):
    """Parse configured producer identities; empty configuration denies all."""
    if value is None:
        value = os.environ.get("ANVIL_EVENTS_ALLOWED_PRODUCERS", "")
    return frozenset(item.strip() for item in value.split(",") if item.strip())


def validate_event(ev, allowed_producers=None) -> tuple[bool, str]:
    """Return (ok, reason). An event is valid if:
    - it is a dict with version==1
    - kind is in the frozen vocabulary
    - required payload fields are PRESENT (None-check, so valid `False`
      booleans like `ok=False` / `reachable=False` are accepted)
    """
    if not isinstance(ev, dict):
        return False, "not a dict"
    version = ev.get("version")
    if (isinstance(version, bool) or not isinstance(version, int | float)
            or version != 1):
        return False, f"unsupported version {ev.get('version')!r}"
    missing_envelope = [field for field in _ENVELOPE_REQUIRED if field not in ev]
    if missing_envelope:
        return False, f"envelope missing required fields: {missing_envelope}"
    extra_envelope = set(ev) - set(_ENVELOPE_REQUIRED) - _OPTIONAL_ENVELOPE
    if extra_envelope:
        return False, f"envelope has unknown fields: {sorted(extra_envelope)}"
    if ev.get("schema") != _SCHEMA:
        return False, "unsupported schema URI"
    if not isinstance(ev.get("producer"), str) or not _PRODUCER.fullmatch(ev["producer"]):
        return False, "producer must be colon-separated safe tokens"
    if not isinstance(ev.get("host"), str) or not _HOST_TOKEN.fullmatch(ev["host"]):
        return False, "host must be one safe NATS token"
    if not _is_datetime(ev.get("observed_at")) or not _is_datetime(ev.get("emitted_at")):
        return False, "observed_at/emitted_at must be ISO date-times"
    if ev.get("correlation_id") is not None and not isinstance(ev.get("correlation_id"), str):
        return False, "correlation_id must be a string or null"
    causes = ev.get("causes", [])
    if not isinstance(causes, list) or not all(isinstance(c, str) and c for c in causes):
        return False, "causes must be a list of non-empty event IDs"
    kind = ev.get("kind")
    if kind not in _FROZEN_KINDS:
        return False, f"forged/unknown kind {kind!r}"
    payload = ev.get("payload")
    if not isinstance(payload, dict):
        return False, "payload must be a dict"
    expected_subject = f"anvil.fleet.{ev['host']}.{kind}"
    if ev.get("subject") != expected_subject:
        return False, "subject does not match host/kind"
    try:
        seq = int(ev["producer_seq"])
    except (TypeError, ValueError):
        return False, "producer_seq must be an integer"
    if isinstance(ev["producer_seq"], bool) or seq < 1 or seq != ev["producer_seq"]:
        return False, "producer_seq must be a positive integer"
    expected_event_id = f"{ev['producer']}:{seq:06d}"
    if ev.get("event_id") != expected_event_id:
        return False, "event_id does not match producer/producer_seq"
    if allowed_producers is not None and ev.get("producer") not in allowed_producers:
        return False, "producer is not authorized"
    ok, reason = validate_payload(kind, payload)
    if not ok:
        return False, reason
    return True, ""


def fact_store_add(store_path, ev, drop_fields=_DROP_FACT_FIELDS,
                   allowed_producers=None):
    """Append a validated event as a JSONL fact line to a stdlib fact store.

    Drops sensitive/implementation fields from the fact payload. Returns the
    fact record, or None if the event is invalid (never stores unknowns).
    """
    ok, reason = validate_event(ev, allowed_producers=allowed_producers)
    if not ok:
        return None
    sensitive_parts = re.compile(
        r"(?:^|_)(?:token|password|passwd|secret|credential|authorization|cookie)(?:$|_)"
    )

    def sensitive_key(key):
        camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
        normalized = re.sub(r"[^A-Za-z0-9]+", "_", camel_split).strip("_").lower()
        compact = normalized.replace("_", "")
        semantic_suffixes = (
            "token", "password", "passwd", "secret", "credential",
            "authorization", "cookie", "apikey",
        )
        return (normalized in drop_fields or normalized in {"api_key", "apikey"}
                or bool(sensitive_parts.search(normalized))
                or compact.endswith(semantic_suffixes))

    def redact(value):
        if isinstance(value, dict):
            return {k: redact(v) for k, v in value.items()
                    if not sensitive_key(k)}
        if isinstance(value, list):
            return [redact(v) for v in value]
        return value

    payload = redact(ev.get("payload") or {})
    fact = {
        "event_id": ev.get("event_id"),
        "kind": ev.get("kind"),
        "host": ev.get("host"),
        "observed_at": ev.get("observed_at"),
        "correlation_id": ev.get("correlation_id"),
        "payload": payload,
    }
    path = Path(store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    import fcntl
    directory_fd = open_pinned_directory(path.parent)
    try:
        lock_fd = open_regular_at(
            directory_fd, path.name + ".lock",
            os.O_RDWR | os.O_APPEND | os.O_CREAT,
        )
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            data_fd = open_regular_at(
                directory_fd, path.name, os.O_RDWR | os.O_CREAT,
            )
            try:
                data = b""
                while True:
                    chunk = os.read(data_fd, 65536)
                    if not chunk:
                        break
                    data += chunk
                if data and not data.endswith(b"\n"):
                    boundary = data.rfind(b"\n") + 1
                    torn = data[boundary:]
                    quarantine_name = (
                        f"{path.name}.{time.time_ns()}.{os.getpid()}.torn"
                    )
                    quarantine_fd = open_regular_at(
                        directory_fd, quarantine_name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    )
                    try:
                        os.write(quarantine_fd, torn)
                        os.fsync(quarantine_fd)
                    finally:
                        os.close(quarantine_fd)
                    os.ftruncate(data_fd, boundary)
                    os.fsync(data_fd)
                    os.fsync(directory_fd)
                    data = data[:boundary]
                for line in data.splitlines():
                    try:
                        if json.loads(line).get("event_id") == fact["event_id"]:
                            return None
                    except (json.JSONDecodeError, AttributeError):
                        continue
                encoded = (json.dumps(fact, sort_keys=True) + "\n").encode()
                os.lseek(data_fd, 0, os.SEEK_END)
                view = memoryview(encoded)
                while view:
                    written = os.write(data_fd, view)
                    if written <= 0:
                        raise OSError("short fact-store append")
                    view = view[written:]
                os.fsync(data_fd)
                os.fsync(directory_fd)
            finally:
                os.close(data_fd)
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
    finally:
        os.close(directory_fd)
    return fact


def _default_store(root, store_path, allowed_producers):
    """Standard fact_store_add bound to a JSONL path."""
    return lambda ev: fact_store_add(
        store_path or os.path.join(root, "facts.jsonl"), ev,
        allowed_producers=allowed_producers,
    )


def cmd_ingest(args, fact_store=None):
    """Consume root's outbox/journal and store VALIDATED events as facts.

    Accepts either a Namespace (CLI) or explicit (root, count, store_path).
    The effective fact store is `fact_store` if given (e.g. the Hermes hook),
    else the stdlib JSONL store at `store_path` (default: root/facts.jsonl).
    Invalid events are counted and dropped (never stored).
    """
    if isinstance(args, dict):
        root = args.get("root")
        count = args.get("count")
        store_path = args.get("store")
    elif not isinstance(args, str):  # Namespace
        root = args.root
        count = args.count
        store_path = args.store
    else:  # legacy positional
        root = args
        count = None
        store_path = None
    allowed_producers = parse_allowed_producers()
    store = fact_store or _default_store(root, store_path, allowed_producers)
    o = _OutboxForIngest(root)
    events = o.read_all()
    deduped = []
    seen = set()
    dropped = 0
    reasons = {}
    for event in events:
        ok, reason = validate_event(event, allowed_producers=allowed_producers)
        if not ok:
            dropped += 1
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        event_id = event.get("event_id") if isinstance(event, dict) else None
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        deduped.append(event)
    events = deduped
    if count is not None:
        events = events[:count]
    stored = 0
    for ev in events:
        fact = store(ev)
        if fact is not None:
            stored += 1
    print(f"ingest: stored={stored} dropped={dropped}")
    for reason, n in sorted(reasons.items()):
        print(f"  drop: {n} x {reason}")
    return 0 if dropped == 0 else 1


class _OutboxForIngest:
    """Read-only view over producer history + subscriber journal JSONL."""

    def __init__(self, root):
        self.outbox_dir = os.path.join(root, "outbox")
        self.archive_dir = os.path.join(root, "archive")
        self.journal_dir = os.path.join(root, "journal")

    def read_all(self):
        events = []
        for d, managed_pattern in (
            (self.outbox_dir, _DAILY_JSONL),
            (self.archive_dir, _ARCHIVE_JSONL),
            (self.journal_dir, _DAILY_JSONL),
        ):
            events.extend(iter_managed_jsonl(d, managed_pattern))
        return events


def git_sync(repo_dir, message="ops: adopt recorded state", push=False,
             _run=subprocess.run):
    """Commit pending changes in `repo_dir` and optionally push.

    Returns `(rc, details)` where details is ALWAYS a dict (never a bare
    string — cmd_sync_repo does `**details`). A failed `git status` is an
    error (rc non-zero), not a clean-tree no-op. A clean tree (empty
    status, rc 0) is a no-op success with `committed=False`.
    """
    def run(argv, **kwargs):
        return _run(argv, capture_output=True, text=True, **kwargs)

    changed = run(["git", "status", "--porcelain"], cwd=repo_dir)
    if changed.returncode != 0:
        return changed.returncode, {
            "committed": False, "pushed": False,
            "error": (changed.stderr or changed.stdout or "git status failed").strip(),
        }
    if changed.stdout.strip():
        add = run(["git", "add", "-A"], cwd=repo_dir)
        if add.returncode != 0:
            return add.returncode, {
                "committed": False, "pushed": False,
                "error": (add.stderr or "git add failed").strip(),
            }
        commit = run(["git", "commit", "-m", message], cwd=repo_dir)
        if commit.returncode != 0:
            return commit.returncode, {
                "committed": False, "pushed": False,
                "error": (commit.stderr or "git commit failed").strip(),
            }
        committed = True
    else:
        committed = False
    pushed = False
    if push:
        push_result = run(["git", "push"], cwd=repo_dir)
        if push_result.returncode != 0:
            return push_result.returncode, {
                "committed": committed, "pushed": False,
                "error": (push_result.stderr or "git push failed").strip(),
            }
        pushed = True
    return 0, {"committed": committed, "pushed": pushed}


def cmd_sync_repo(args, _git=git_sync, _emit=None):
    """Commit+push the operator repo and emit correlation-linked events.

    Emits `config.adopted` (the repo's adopted config state) then
    `repo.synced` (repo + ok) — both with args.correlation.
    """
    repo_dir = args.dir
    if not repo_dir or not os.path.isdir(repo_dir):
        raise ValueError(f"not a directory: {repo_dir!r}")
    if not args.correlation:
        raise ValueError("sync-repo requires --correlation (links to promote)")
    rc, details = _git(repo_dir, push=args.push)
    emit = _emit or (lambda kind, payload, correlation, host, root:
                     (cli_cmd_emit(kind, payload, correlation, host, root)))
    # config.adopted: the operator adopted this config state
    emit("config.adopted", {"file": "operator", "state": "adopted"},
         args.correlation, args.host, args.root)
    # repo.synced: ok = commit+push succeeded
    emit("repo.synced", {"repo": repo_dir, "ok": rc == 0, **details},
         args.correlation, args.host, args.root)
    return rc


def cli_cmd_emit(kind, payload, correlation, host, root):
    """Thin wrapper: emit with the standard correlation/host/root flow."""
    from .cli import _outbox
    o = _outbox(root)
    o.emit(host, kind, host, payload, correlation_id=correlation)


def register(subparsers):
    """Register the M4 subcommands on `subparsers` (argparse)."""
    sync = subparsers.add_parser("sync-repo", help="commit+push the operator repo, emit config.adopted/repo.synced")
    sync.add_argument("--dir", required=True, help="operator repo directory")
    sync.add_argument("--correlation", required=True, help="correlation id linking to promote")
    sync.add_argument("--push", action="store_true", help="git push after commit")
    sync.add_argument("--host", default="node-a", help="host identity")
    sync.set_defaults(func=cmd_sync_repo)

    ing = subparsers.add_parser("ingest", help="validated ingestion of events as facts")
    # NOTE: --root is the TOP-LEVEL flag (global); do NOT shadow it here.
    ing.add_argument("--count", type=int, default=None, help="max events to ingest")
    ing.add_argument("--store", default=None, help="fact store path (default root/facts.jsonl)")
    ing.set_defaults(func=cmd_ingest)
