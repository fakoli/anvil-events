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
               producer_seq=1, observed_at=None, causes=None, version=1):
    """Build a v1 envelope. Raises ValueError on unknown kind."""
    if kind not in KINDS:
        raise ValueError("unknown kind %r; frozen kinds: %s"
                         % (kind, ",".join(sorted(KINDS))))
    seq = int(producer_seq)
    ev = {
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
    if causes:
        ev["causes"] = causes          # explicit causal edges (event_ids)
    return ev


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

    def emit(self, producer, kind, host, payload, correlation_id=None):
        """Append with an inter-process lock so sequences never collide.

        Uses a flock on the outbox lock file: read/compute/append is one
        critical section, so two concurrent emitters get distinct seqs.
        """
        import fcntl
        lock_path = os.path.join(self.root, ".lock")
        with open(lock_path, "a") as lockf:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
            try:
                pending_seqs = [e.get("producer_seq", 0)
                                for e in self.read_pending()]
                curs = self.load_cursors()
                acked_max = max([c.get("producer_seq", 0)
                                 for c in curs.values()] or [0])
                seq = max([1] + pending_seqs + [acked_max]) + 1
                event = make_event(producer, kind, host, payload,
                                   correlation_id=correlation_id,
                                   producer_seq=seq)
                self.append(event)
                return event
            finally:
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)

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
        """Yield all pending events (all outbox files), oldest first.

        A torn final line (no trailing newline — the signature of a crash
        mid-append) is dropped, not yielded: the event was never durably
        completed.
        """
        for fn in sorted(os.listdir(self.outbox_dir)):
            if not fn.endswith(".jsonl"):
                continue
            with open(os.path.join(self.outbox_dir, fn), encoding="utf-8") as f:
                data = f.read()
            lines = data.splitlines()
            # if the file doesn't end with a newline, the last line is torn
            if data and not data.endswith("\n") and lines:
                lines = lines[:-1]
            for line_ in lines:
                line_ = line_.strip()
                if line_:
                    yield json.loads(line_)

    def count_pending(self):
        return sum(1 for _ in self.read_pending())

    def ack(self, event):
        """Confirm delivery: record into archive, THEN remove from pending.

        Order matters for crash-safety: we never remove from pending before
        the archive has the record. A crash between the two leaves a
        duplicate archive entry (harmless: consumers dedup by `event_id`);
        it NEVER loses the event.
        """
        with self._lock:
            day = event.get("observed_at", utcnow_iso())[:10]
            src = os.path.join(self.outbox_dir, day + ".jsonl")
            dst = os.path.join(self.archive_dir, day + ".jsonl")
            key = json.dumps(event, sort_keys=True)
            # 1) archive first (durable record of delivery)
            with open(dst, "a", encoding="utf-8") as f:
                f.write(key + "\n")
                f.flush()
                os.fsync(f.fileno())
            # 2) then remove from pending (atomic rewrite)
            if os.path.exists(src):
                with open(src, encoding="utf-8") as f:
                    lines = [line_ for line_ in f if line_.strip() != key]
                tmp = src + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.writelines(lines)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, src)
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
    def gc(self, archive_days=90, max_bytes=500 * 1024 * 1024):
        """Delete archive files older than `archive_days`; guard archive size.

        If the archive directory exceeds `max_bytes`, rotate to a new file and
        emit an `event.degraded` record (no silent growth). Returns a dict of
        what happened.
        """
        cutoff = time.time() - archive_days * 86400
        removed = 0
        for fn in os.listdir(self.archive_dir):
            p = os.path.join(self.archive_dir, fn)
            if os.path.getmtime(p) < cutoff:
                os.remove(p)
                removed += 1
        # size guard
        total = sum(os.path.getsize(os.path.join(self.archive_dir, f))
                    for f in os.listdir(self.archive_dir)
                    if f.endswith(".jsonl"))
        rotated = False
        degraded = None
        if total > max_bytes:
            # rotate: rename current day archive to a timestamped overflow
            day = utcnow_iso()[:10]
            src = os.path.join(self.archive_dir, day + ".jsonl")
            if os.path.exists(src):
                dst = os.path.join(self.archive_dir,
                                   "%s.%d.jsonl" % (day, int(time.time())))
                os.replace(src, dst)
                rotated = True
            degraded = make_event("local:gc", "event.degraded", "local",
                                  {"cause": "archive size %d > %d"
                                   % (total, max_bytes)})
            self.append(degraded)
        return {"removed": removed, "rotated": rotated,
                "size": total, "degraded": (degraded or {}).get("event_id")}


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

    Builds the happens-before graph from EXPLICIT edges only:
      - per-producer `producer_seq` chains (program order),
      - `causes` lists (explicit causal edges: `causes` names upstream
        event_ids, so the edge runs cause -> effect).
    Duplicate `event_id`s are deduplicated first. Uses an iterative
    topological sort (Kahn) so large journals cannot overflow the stack.
    Ordering is never inferred from timestamps.
    """

    @staticmethod
    def check(events):
        # Dedup by event_id (keep first occurrence — replay of a journal
        # may legally repeat a record, e.g. from an archive + outbox scan).
        seen = set()
        uniq = []
        by_id = {}
        for e in events:
            eid = e.get("event_id")
            if eid in seen:
                continue
            seen.add(eid)
            uniq.append(e)
            by_id[eid] = len(uniq) - 1
        events = uniq
        n = len(events)
        # topological sort via Kahn's algorithm (no recursion)
        adj = [set() for _ in range(n)]
        indeg = [0] * n
        by_producer = {}
        for i, e in enumerate(events):
            by_producer.setdefault(e.get("producer"), []).append(
                (e.get("producer_seq", 0), i))
            # explicit causal edges ONLY: `causes` = upstream event_ids.
            # cause -> effect: edge j -> i (j happened before i).
            for cev in (e.get("causes") or []):
                j = by_id.get(cev)
                if j is not None and i not in adj[j]:
                    adj[j].add(i)
                    indeg[i] += 1
        for lst in by_producer.values():
            lst.sort()
            for (_, i), (_, j) in zip(lst, lst[1:]):
                if j not in adj[i]:
                    adj[i].add(j)
                    indeg[j] += 1
        # Kahn: if we can't drain all nodes, there's a cycle
        import collections
        q = collections.deque([i for i in range(n) if indeg[i] == 0])
        order = []
        while q:
            u = q.popleft()
            order.append(u)
            for v in adj[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        if len(order) != n:
            # find a node still on a cycle for the message
            remaining = [i for i in range(n) if i not in set(order)]
            cyc = remaining[0] if remaining else 0
            return False, ("cycle involving event %d: %s"
                           % (cyc, events[cyc].get("event_id")))
        return True, None
