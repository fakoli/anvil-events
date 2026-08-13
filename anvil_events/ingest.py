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
import subprocess
from pathlib import Path

# Frozen kinds + the payload fields we will store as facts. `serve.up` needs
# serve/model/port; anything else is a forged/unknown event -> dropped.
_FROZEN_KINDS = frozenset([
    "serve.up", "serve.down", "profile.enter", "profile.leave",
    "promote.applied", "promote.rolled_back", "config.adopted",
    "repo.synced", "host.status", "divergence", "event.degraded",
])

# payload allowlist: the fact-store columns we accept per kind. Extra payload
# keys are tolerated (forward-compat); REQUIRED keys must be present.
_PAYLOAD_REQUIRED = {
    "serve.up": ("serve", "model", "port"),
    "serve.down": ("serve",),
    "profile.enter": ("mode", "profile"),
    "profile.leave": ("mode", "profile"),
    "promote.applied": ("tier", "model"),
    "promote.rolled_back": ("tier", "model"),
    "config.adopted": ("file",),
    "repo.synced": ("repo", "ok"),
    "host.status": ("host", "reachable"),
    "divergence": ("issue",),
    "event.degraded": ("cause",),
}

# Fields ALWAYS dropped from stored facts (they leak implementation/host
# details or are high-cardinality noise; the operator adapter never stores
# credentials, tokens, or secrets).
_DROP_FACT_FIELDS = frozenset([
    "token", "password", "secret", "api_key", "apikey", "credential",
    "authorization", "cookie",
])


def validate_event(ev) -> tuple[bool, str]:
    """Return (ok, reason). An event is valid if:
    - it is a dict with version==1
    - kind is in the frozen vocabulary
    - required payload fields are present and non-empty
    """
    if not isinstance(ev, dict):
        return False, "not a dict"
    if ev.get("version") != 1:
        return False, f"unsupported version {ev.get('version')!r}"
    kind = ev.get("kind")
    if kind not in _FROZEN_KINDS:
        return False, f"forged/unknown kind {kind!r}"
    payload = ev.get("payload")
    if not isinstance(payload, dict):
        return False, "payload must be a dict"
    required = _PAYLOAD_REQUIRED.get(kind, ())
    missing = [f for f in required if not payload.get(f)]
    if missing:
        return False, f"payload missing required fields: {missing}"
    return True, ""


def fact_store_add(store_path, ev, drop_fields=_DROP_FACT_FIELDS):
    """Append a validated event as a JSONL fact line to a stdlib fact store.

    Drops sensitive/implementation fields from the fact payload. Returns the
    fact record, or None if the event is invalid (never stores unknowns).
    """
    ok, reason = validate_event(ev)
    if not ok:
        return None
    payload = {
        k: v for k, v in (ev.get("payload") or {}).items()
        if k.lower() not in drop_fields
    }
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
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(fact, sort_keys=True) + "\n")
    return fact


def _default_store(root, store_path):
    """Standard fact_store_add bound to a JSONL path."""
    return lambda ev: fact_store_add(store_path or os.path.join(root, "facts.jsonl"), ev)


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
    store = fact_store or _default_store(root, store_path)
    o = _OutboxForIngest(root)
    events = o.read_all()
    if count is not None:
        events = events[:count]
    stored = 0
    dropped = 0
    reasons = {}
    for ev in events:
        ok, reason = validate_event(ev)
        if not ok:
            dropped += 1
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        fact = store(ev)
        if fact is not None:
            stored += 1
    print(f"ingest: stored={stored} dropped={dropped}")
    for reason, n in sorted(reasons.items()):
        print(f"  drop: {n} x {reason}")
    return 0 if dropped == 0 else 1


class _OutboxForIngest:
    """Read-only view over an Outbox root (pending + archive JSONL)."""

    def __init__(self, root):
        self.outbox_dir = os.path.join(root, "outbox")
        self.archive_dir = os.path.join(root, "archive")

    def read_all(self):
        events = []
        for d in (self.outbox_dir, self.archive_dir):
            if not os.path.isdir(d):
                continue
            for fn in sorted(os.listdir(d)):
                if not fn.endswith(".jsonl"):
                    continue
                with open(os.path.join(d, fn), encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        return events


def git_sync(repo_dir, message="ops: adopt recorded state", push=False,
             _run=subprocess.run):
    """Commit pending changes in `repo_dir` and optionally push.

    Returns (rc, details). Uses `_run` for hermeticity. Commits only when
    there are staged/untracked changes; a clean tree is a no-op success.
    """
    def run(argv, **kwargs):
        return _run(argv, capture_output=True, text=True, **kwargs)

    changed = run(["git", "status", "--porcelain"], cwd=repo_dir)
    if changed.returncode == 0 and changed.stdout.strip():
        add = run(["git", "add", "-A"], cwd=repo_dir)
        if add.returncode != 0:
            return add.returncode, add.stderr.strip()
        commit = run(["git", "commit", "-m", message], cwd=repo_dir)
        if commit.returncode != 0:
            return commit.returncode, commit.stderr.strip()
        committed = True
    else:
        committed = False
    pushed = False
    if push:
        push_result = run(["git", "push"], cwd=repo_dir)
        if push_result.returncode != 0:
            return push_result.returncode, push_result.stderr.strip()
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
