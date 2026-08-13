"""Outbox, LogPlayer-style target queues, and causal-consistency checker.

Stdlib-only. Implements the durability model from PRD "Reliability contract"
and research/2026-08-12-theory-map.md (LogPlayer arXiv:1911.11286; causal
consistency arXiv:2011.09753).
"""
import json
import os
import threading
import time

KINDS = frozenset([
    "serve.up", "serve.down", "profile.enter", "profile.leave",
    "promote.applied", "promote.rolled_back", "config.adopted",
    "repo.synced", "host.status", "divergence", "event.degraded",
])
SCHEMA = "https://anvil.dev/schemas/events/v1.json"


def utcnow_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def make_event(producer, kind, host, payload, correlation_id=None,
               producer_seq=1, observed_at=None, version=1):
    """Build a v1 envelope. Raises ValueError on unknown kind."""
    if kind not in KINDS:
        raise ValueError("unknown kind %r; frozen kinds: %s"
                         % (kind, ",".join(sorted(KINDS))))
    seq = int(producer_seq)
    return {
        "version": version,
        "event_id": "%s:%06d" % (producer, seq),
        "producer": producer,
        "producer_seq": seq,
        "observed_at": observed_at or utcnow_iso(),
        "emitted_at": utcnow_iso(),
        "correlation_id": correlation_id,
        "schema": SCHEMA,
        "host": host,
        "kind": kind,
        "subject": "anvil.fleet.%s.%s" % (host, kind),
        "payload": payload or {},
    }


class Outbox:
    """Append-only JSONL outbox with fsync + acked archive + per-target cursors.

    outbox/<YYYY-MM-DD>.jsonl  = PENDING (durable record, written BEFORE publish)
    archive/<YYYY-MM-DD>.jsonl = ACKED (delivered past the cursor)
    cursors.json               = {target: {"last_event_id", "producer_seq"}}
    """

    def __init__(self, root):
        self.root = root
        self.outbox_dir = os.path.join(root, "outbox")
        self.archive_dir = os.path.join(root, "archive")
        self.cursor_file = os.path.join(root, "cursors.json")
        for d in (self.outbox_dir, self.archive_dir):
            os.makedirs(d, exist_ok=True)
        self._lock = threading.RLock()

    # -- append (outbox-first; fsync) -------------------------------------
    def append(self, event):
        with self._lock:
            day = event.get("observed_at", utcnow_iso())[:10]
            path = os.path.join(self.outbox_dir, day + ".jsonl")
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, sort_keys=True) + "\n")
                f.flush()
                os.fsync(f.fileno())
            return path

    def read_pending(self):
        """Yield all pending events (all outbox files), oldest first."""
        for fn in sorted(os.listdir(self.outbox_dir)):
            if not fn.endswith(".jsonl"):
                continue
            with open(os.path.join(self.outbox_dir, fn), encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)

    def count_pending(self):
        return sum(1 for _ in self.read_pending())

    def ack(self, event):
        """Move a delivered event to the archive + advance the target cursor."""
        with self._lock:
            day = event.get("observed_at", utcnow_iso())[:10]
            src = os.path.join(self.outbox_dir, day + ".jsonl")
            dst = os.path.join(self.archive_dir, day + ".jsonl")
            key = json.dumps(event, sort_keys=True)
            if os.path.exists(src):
                with open(src, encoding="utf-8") as f:
                    lines = [l for l in f if l.strip() != key]
                with open(src, "w", encoding="utf-8") as f:
                    f.writelines(lines)
            with open(dst, "a", encoding="utf-8") as f:
                f.write(key + "\n")
            self._set_cursor(event)

    def _set_cursor(self, event):
        curs = self.load_cursors()
        target = event.get("subject", "")
        curs[target] = {"last_event_id": event.get("event_id"),
                        "producer_seq": event.get("producer_seq")}
        tmp = self.cursor_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(curs, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.cursor_file)

    def load_cursors(self):
        if os.path.exists(self.cursor_file):
            with open(self.cursor_file, encoding="utf-8") as f:
                return json.load(f)
        return {}

    # -- retention (gc) ------------------------------------------------------
    def gc(self, archive_days=90):
        cutoff = time.time() - archive_days * 86400
        removed = 0
        for fn in os.listdir(self.archive_dir):
            p = os.path.join(self.archive_dir, fn)
            if os.path.getmtime(p) < cutoff:
                os.remove(p)
                removed += 1
        return removed


class TargetQueue:
    """LogPlayer-style per-target queue (arXiv:1911.11286 §2.4-2.5).

    States: NORMAL / SUSPENDED / RECOVERY_FETCHING / FETCHING_COMPLETED.
    A term increments on every reconnect; entries pushed under an expired
    term are dropped -> duplicate prevention after reconnect.
    """

    NORMAL, SUSPENDED, RECOVERY_FETCHING, FETCHING_COMPLETED = range(4)
    _NAME = {0: "NORMAL", 1: "SUSPENDED", 2: "RECOVERY_FETCHING", 3: "FETCHING_COMPLETED"}

    def __init__(self):
        self.state = self.NORMAL
        self.term = 1
        self.normal = []
        self.catchup = []

    def __repr__(self):
        return "<TargetQueue %s term=%d normal=%d catchup=%d>" % (
            self._NAME[self.state], self.term, len(self.normal), len(self.catchup))

    def push(self, entry, is_normal=True, term=1):
        if self.state == self.SUSPENDED:
            return False
        if term != self.term:  # stale term -> drop (duplicate prevention)
            return False
        (self.normal if is_normal else self.catchup).append(entry)
        return True

    def front(self):
        if self.state in (self.RECOVERY_FETCHING, self.FETCHING_COMPLETED) \
                and self.catchup:
            return self.catchup[0]
        if self.state == self.FETCHING_COMPLETED and not self.catchup:
            self.state = self.NORMAL
            if self.normal:
                return self.normal[0]
        if self.state == self.NORMAL and self.normal:
            return self.normal[0]
        return None

    def pop(self):
        e = self.front()
        if e is None:
            return None
        if self.state in (self.RECOVERY_FETCHING, self.FETCHING_COMPLETED) \
                and self.catchup:
            self.catchup.pop(0)
            if self.state == self.FETCHING_COMPLETED and not self.catchup:
                self.state = self.NORMAL
        elif self.state == self.NORMAL and self.normal:
            self.normal.pop(0)
        return e

    def suspend(self):
        self.state = self.SUSPENDED
        self.normal.clear()
        self.catchup.clear()

    def reconnect(self):
        self.term += 1
        self.state = self.RECOVERY_FETCHING

    def fetching_completed(self):
        if self.catchup:
            self.state = self.FETCHING_COMPLETED
        else:
            self.state = self.NORMAL


class CausalChecker:
    """Cycle-check journal replay for causal consistency (arXiv:2011.09753).

    Builds the happens-before graph: per-producer producer_seq chains and
    per-correlation_id observed_at chains. A cycle => not causally consistent
    (e.g. forged/mis-ordered events).
    """

    @staticmethod
    def check(events):
        n = len(events)
        adj = [[] for _ in range(n)]
        by_producer = {}
        by_corr = {}
        for i, e in enumerate(events):
            by_producer.setdefault(e.get("producer"), []).append(
                (e.get("producer_seq", 0), i))
            corr = e.get("correlation_id")
            if corr:
                by_corr.setdefault(corr, []).append(
                    (e.get("observed_at", ""), i))
        for lst in by_producer.values():          # program order: seq asc
            lst.sort()
            for (_, i), (_, j) in zip(lst, lst[1:]):
                adj[i].append(j)
        for lst in by_corr.values():              # correlation order: observed asc
            lst.sort(key=lambda t: t[0])
            for (_, i), (_, j) in zip(lst, lst[1:]):
                if i != j:
                    adj[i].append(j)
        # DFS cycle detection
        WHITE, GREY, BLACK = 0, 1, 2
        color = [WHITE] * n

        def dfs(u):
            color[u] = GREY
            for v in adj[u]:
                if color[v] == GREY:
                    return v
                if color[v] == WHITE:
                    cyc = dfs(v)
                    if cyc is not None:
                        return cyc
            color[u] = BLACK
            return None

        for u in range(n):
            if color[u] == WHITE:
                cyc = dfs(u)
                if cyc is not None:
                    return False, ("cycle at event %d: %s"
                                   % (cyc, events[cyc].get("event_id")))
        return True, None
