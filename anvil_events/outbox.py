"""Outbox, LogPlayer-style target queues, and causal-consistency checker.

Stdlib-only. Implements the durability model from PRD "Reliability contract"
and research/2026-08-12-theory-map.md (LogPlayer arXiv:1911.11286; causal
consistency arXiv:2011.09753).
"""
import json
import os
import re
import stat
import threading
import time
from datetime import datetime

from .nats_mini import encode_js_publish

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
    "profile.enter": frozenset(["profile", "mode", "exclusive_target", "restore_group"]),
    "profile.leave": frozenset(["profile", "mode", "exclusive_target", "restore_group"]),
    "promote.applied": frozenset(["promotion", "tier", "model", "context", "rollback"]),
    "promote.rolled_back": frozenset(["promotion", "tier", "restored_model"]),
    "config.adopted": frozenset(["file", "files", "state", "repo", "rev"]),
    "repo.synced": frozenset(["repo", "ok", "committed", "pushed", "error"]),
    "host.status": frozenset(["host", "reachable", "gpu_used", "gpu_free"]),
    "divergence": frozenset(["issue", "declared", "live", "delta"]),
    "event.degraded": frozenset(["cause", "event_id", "file", "bytes", "records",
                                  "pending"]),
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
_DAILY_JSONL = re.compile(r"^\d{4}-\d{2}-\d{2}\.jsonl$")
_ARCHIVE_JSONL = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:\.\d{9,11})?\.jsonl$"
)


def iter_managed_jsonl(directory, managed_pattern=_DAILY_JSONL,
                       malformed="skip", managed_name=None):
    """Yield JSON objects from exact managed regular files without symlinks.

    A crash-torn final record is ignored consistently. Complete malformed
    records are skipped by default; callers may request `malformed="raise"`.
    When `managed_name` is supplied, only that exact managed file is read.
    """
    flags = (os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
             | getattr(os, "O_NOFOLLOW", 0))
    try:
        directory_fd = os.open(directory, flags)
    except (FileNotFoundError, NotADirectoryError, OSError):
        return
    try:
        names = ([managed_name] if managed_name is not None
                 else sorted(os.listdir(directory_fd)))
        for fn in names:
            if not managed_pattern.fullmatch(fn):
                continue
            file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(fn, file_flags, dir_fd=directory_fd)
            except (FileNotFoundError, OSError):
                continue
            with os.fdopen(fd, "rb") as f:
                current = os.fstat(f.fileno())
                if not stat.S_ISREG(current.st_mode):
                    continue
                data = f.read()
            lines = data.splitlines()
            if data and not data.endswith(b"\n") and lines:
                lines = lines[:-1]
            for line in lines:
                if not line.strip():
                    continue
                try:
                    yield json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    if malformed == "raise":
                        raise
    finally:
        os.close(directory_fd)


def open_pinned_directory(directory):
    """Open an existing directory without following a final symlink."""
    flags = (os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
             | getattr(os, "O_NOFOLLOW", 0))
    fd = os.open(directory, flags)
    if not stat.S_ISDIR(os.fstat(fd).st_mode):
        os.close(fd)
        raise OSError("managed parent is not a directory")
    return fd


def open_regular_at(directory_fd, name, flags, mode=0o600):
    """Open/create a regular file relative to a pinned directory, no symlinks."""
    fd = os.open(name, flags | getattr(os, "O_NOFOLLOW", 0), mode,
                 dir_fd=directory_fd)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise OSError("managed target is not a regular file")
    return fd


def read_regular_fd(directory_fd, name):
    """Read one managed regular file relative to a pinned directory."""
    fd = open_regular_at(directory_fd, name, os.O_RDONLY)
    try:
        chunks = []
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    finally:
        os.close(fd)


def replace_regular_fd(directory_fd, name, data):
    """Atomically replace a managed regular file relative to a pinned directory."""
    tmp_name = f".{name}.{time.time_ns()}.{os.getpid()}.tmp"
    try:
        existing_fd = open_regular_at(directory_fd, name, os.O_RDONLY)
        os.close(existing_fd)
        tmp_fd = open_regular_at(
            directory_fd, tmp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        )
        try:
            view = memoryview(data)
            while view:
                written = os.write(tmp_fd, view)
                if written <= 0:
                    raise OSError("short atomic rewrite")
                view = view[written:]
            os.fsync(tmp_fd)
        finally:
            os.close(tmp_fd)
        try:
            os.replace(
                tmp_name, name,
                src_dir_fd=directory_fd, dst_dir_fd=directory_fd,
            )
        except Exception:
            try:
                os.unlink(tmp_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            raise
        os.fsync(directory_fd)
    finally:
        pass


def read_regular_nofollow(path):
    """Read one regular file through a pinned parent directory."""
    directory, name = os.path.split(os.fspath(path))
    directory_fd = open_pinned_directory(directory or ".")
    try:
        return read_regular_fd(directory_fd, name)
    finally:
        os.close(directory_fd)


def replace_regular_nofollow(path, data):
    """Atomically replace an existing regular file, pinned and no-follow."""
    directory, name = os.path.split(os.fspath(path))
    directory_fd = open_pinned_directory(directory or ".")
    try:
        replace_regular_fd(directory_fd, name, data)
    finally:
        os.close(directory_fd)


def write_new_regular_nofollow(path, data):
    """Create one new regular file exclusively under a pinned parent."""
    directory, name = os.path.split(os.fspath(path))
    directory_fd = open_pinned_directory(directory or ".")
    try:
        fd = open_regular_at(
            directory_fd, name, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        )
        try:
            view = memoryview(data)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short exclusive write")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def validate_payload(kind, payload):
    """Validate the frozen per-kind payload allowlist and scalar types."""
    if not isinstance(payload, dict):
        return False, "payload must be a dict"
    missing = [field for field in PAYLOAD_REQUIRED.get(kind, ())
               if payload.get(field) is None]
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
        if key == "gpu_roles" and (not isinstance(value, list)
                                    or not all(isinstance(v, str) for v in value)):
            return False, "payload field 'gpu_roles' must be a string array"
        if key == "files" and (not isinstance(value, list)
                                or not all(isinstance(v, str) for v in value)):
            return False, "payload field 'files' must be a string array"
    return True, ""


def utcnow_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def make_event(producer, kind, host, payload, correlation_id=None,
               producer_seq=1, observed_at=None, causes=None, version=1):
    """Build a v1 envelope. Raises ValueError on unknown kind."""
    if kind not in KINDS:
        raise ValueError("unknown kind {!r}; frozen kinds: {}".format(kind, ",".join(sorted(KINDS))))
    if not isinstance(producer, str) or not _PRODUCER.fullmatch(producer):
        raise ValueError("producer must be colon-separated safe tokens")
    if not isinstance(host, str) or not _HOST_TOKEN.fullmatch(host):
        raise ValueError("host must be one safe NATS token")
    if correlation_id is not None and not isinstance(correlation_id, str):
        raise ValueError("correlation_id must be a string or null")
    seq = int(producer_seq)
    if seq < 1:
        raise ValueError("producer_seq must be positive")
    ev = {
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
    return ev


class Outbox:
    """Append-only JSONL outbox with fsync + acked archive + per-target cursors.

    outbox/<YYYY-MM-DD>.jsonl  = PENDING (durable record, written BEFORE publish)
    archive/<YYYY-MM-DD>.jsonl = ACKED (delivered past the cursor)
    cursors.json               = {target: {"last_event_id", "producer_seq"}}
    """

    def _with_flock(self, fn, *args, **kwargs):
        """Run `fn` under the inter-process outbox lock (fcntl flock).

        The same lock that `emit()` uses, so ack()/gc()/append() cannot race
        across processes (the threading.RLock only guards same-process).
        """
        import fcntl
        directory_fd = open_pinned_directory(self.root)
        try:
            lock_fd = open_regular_at(
                directory_fd, ".lock", os.O_RDWR | os.O_APPEND | os.O_CREAT,
            )
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                return fn(*args, **kwargs)
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
        finally:
            os.close(directory_fd)

    def __init__(self, root):
        self.root = root
        self.outbox_dir = os.path.join(root, "outbox")
        self.archive_dir = os.path.join(root, "archive")
        self.journal_dir = os.path.join(root, "journal")
        self.quarantine_dir = os.path.join(root, "quarantine")
        self.cursor_file = os.path.join(root, "cursors.json")
        self.producer_seq_file = os.path.join(root, "producer-seqs.json")
        for d in (self.outbox_dir, self.archive_dir, self.journal_dir,
                  self.quarantine_dir):
            os.makedirs(d, exist_ok=True)
        self._lock = threading.RLock()
        self._with_flock(self._repair_torn_locked)
        self._with_flock(self._migrate_legacy_cursors_locked)

    def emit(self, producer, kind, host, payload, correlation_id=None):
        """Append with an inter-process lock so sequences never collide.

        Uses `_with_flock` (fcntl on the outbox lock file): read/compute/
        append is one critical section, so two concurrent emitters get
        distinct seqs.
        """
        return self._with_flock(self._emit_locked, producer, kind, host,
                                payload, correlation_id)

    def _emit_locked(self, producer, kind, host, payload, correlation_id=None):
        ok, reason = validate_payload(kind, payload)
        if not ok:
            raise ValueError(reason)
        seqs = self.load_producer_seqs()
        # Scan retained producer history as crash recovery for the window
        # between fsync'ing an event and advancing producer-seqs.json.
        retained_max = 0
        for event in self.read_producer_history():
            if event.get("producer") == producer:
                retained_max = max(retained_max, int(event.get("producer_seq", 0)))
        cursor_max = 0
        for by_producer in self.load_cursors().values():
            if isinstance(by_producer, dict):
                cursor_max = max(
                    cursor_max,
                    int((by_producer.get(producer) or {}).get("producer_seq", 0)),
                )
        seq = max(int(seqs.get(producer, 0)), retained_max, cursor_max) + 1
        event = make_event(producer, kind, host, payload,
                           correlation_id=correlation_id, producer_seq=seq)
        self._append_locked(event)
        seqs[producer] = seq
        self._write_json_atomic(self.producer_seq_file, seqs)
        return event

    def load_producer_seqs(self):
        try:
            data = read_regular_nofollow(self.producer_seq_file)
        except FileNotFoundError:
            return {}
        return json.loads(data)

    @staticmethod
    def _write_json_atomic(path, value):
        data = json.dumps(value, indent=2, sort_keys=True).encode()
        directory, name = os.path.split(os.fspath(path))
        directory_fd = open_pinned_directory(directory or ".")
        tmp_name = f".{name}.{time.time_ns()}.{os.getpid()}.tmp"
        try:
            tmp_fd = open_regular_at(
                directory_fd, tmp_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            )
            try:
                view = memoryview(data)
                while view:
                    written = os.write(tmp_fd, view)
                    if written <= 0:
                        raise OSError("short atomic JSON write")
                    view = view[written:]
                os.fsync(tmp_fd)
            finally:
                os.close(tmp_fd)
            try:
                os.replace(
                    tmp_name, name,
                    src_dir_fd=directory_fd, dst_dir_fd=directory_fd,
                )
            except Exception:
                try:
                    os.unlink(tmp_name, dir_fd=directory_fd)
                except FileNotFoundError:
                    pass
                raise
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    # -- append (outbox-first; fsync) -------------------------------------
    def append(self, event):
        """Durably append under the same process lock used by ack()/gc()."""
        return self._with_flock(self._append_locked, event)

    @staticmethod
    def _event_day(event):
        observed = event.get("observed_at")
        if not isinstance(observed, str):
            raise ValueError("observed_at must be an ISO-8601 string")
        try:
            parsed = datetime.fromisoformat(observed.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("observed_at must be valid ISO-8601") from exc
        day = parsed.date().isoformat()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise ValueError("observed_at produced an unsafe journal day")
        return day

    def _append_locked(self, event):
        with self._lock:
            day = self._event_day(event)
            encoded = json.dumps(event, sort_keys=True)
            encode_js_publish(encoded, event.get("event_id"))
            outbox_fd = open_pinned_directory(self.outbox_dir)
            try:
                self._repair_and_append_line_fd(
                    outbox_fd, day + ".jsonl", "outbox", encoded,
                )
            finally:
                os.close(outbox_fd)
            return os.path.join(self.outbox_dir, day + ".jsonl")

    def read_pending(self):
        """Yield all pending events (all outbox files), oldest first.

        A torn final line (no trailing newline — the signature of a crash
        mid-append) is dropped, not yielded: the event was never durably
        completed.
        """
        yield from iter_managed_jsonl(self.outbox_dir)

    @staticmethod
    def _read_jsonl_dir(directory, managed_pattern=_DAILY_JSONL):
        yield from iter_managed_jsonl(directory, managed_pattern)

    def read_archive(self):
        yield from self._read_jsonl_dir(self.archive_dir, _ARCHIVE_JSONL)

    def read_journal(self):
        yield from self._read_jsonl_dir(self.journal_dir)

    def read_producer_history(self):
        yield from self.read_pending()
        yield from self.read_archive()

    def append_journal(self, event):
        """Durably append a received event once, separate from producer pending."""
        return self._with_flock(self._append_journal_locked, event)

    def _append_journal_locked(self, event):
        with self._lock:
            event_id = event.get("event_id")
            if event_id and any(e.get("event_id") == event_id
                                for e in self.read_journal()):
                return False
            day = self._event_day(event)
            journal_fd = open_pinned_directory(self.journal_dir)
            try:
                self._repair_and_append_line_fd(
                    journal_fd, day + ".jsonl", "journal",
                    json.dumps(event, sort_keys=True),
                )
            finally:
                os.close(journal_fd)
            return True

    def _repair_torn_locked(self):
        """Quarantine and truncate crash-torn outbox tails, then flag degraded."""
        repairs = []
        outbox_fd = open_pinned_directory(self.outbox_dir)
        try:
            for fn in sorted(os.listdir(outbox_fd)):
                if not _DAILY_JSONL.fullmatch(fn):
                    continue
                size = self._repair_append_tail(outbox_fd, fn, "outbox")
                if size:
                    repairs.append((fn, size))
        finally:
            os.close(outbox_fd)
        for fn, size in repairs:
            try:
                self._emit_locked(
                    "local:recovery", "event.degraded", "local",
                    {"cause": "torn outbox tail quarantined",
                     "file": fn, "bytes": size},
                )
            except FileNotFoundError:
                # The managed outbox directory was replaced/removed beneath
                # us mid-repair (attacker or external cleanup). The repair
                # itself is durable; the alert is best-effort and must not
                # crash startup.
                pass

    def repair_invalid_pending(self, validator=None):
        """Quarantine malformed complete JSONL records and retain valid work."""
        return self._with_flock(self._repair_invalid_pending_locked, validator)

    def _quarantine_relative(self, directory_fd, stem, suffix, torn_bytes):
        """Write a quarantined record relative to a pinned managed directory.

        Using the pinned dirfd (instead of reconstructing the quarantine
        pathname after a possible parent swap) makes quarantine atomic with
        the repair/rewrite it accompanies: an attacker replacing the managed
        directory cannot redirect the quarantine record either. Records are
        named `<stem>.<suffix>.<ns>.<pid>.torn` when `suffix` is a label such
        as `outbox`/`journal`/`archive`, and `<stem>.<ns>.<pid>.<suffix>`
        (e.g. `<stem>.<ns>.<pid>.invalid`) for invalid-record quarantines.
        """
        is_torn = suffix in ("outbox", "journal", "archive")
        quarantine_fd = open_pinned_directory(self.quarantine_dir)
        try:
            if is_torn:
                qname = f"{stem}.{suffix}.{time.time_ns()}.{os.getpid()}.torn"
            else:
                qname = f"{stem}.{time.time_ns()}.{os.getpid()}.{suffix}"
            fd = open_regular_at(
                quarantine_fd, qname,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            )
            try:
                view = memoryview(torn_bytes)
                while view:
                    written = os.write(fd, view)
                    if written <= 0:
                        raise OSError("short quarantine write")
                    view = view[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.fsync(quarantine_fd)
        finally:
            os.close(quarantine_fd)

    def _repair_invalid_pending_locked(self, validator=None):
        repaired = 0
        outbox_fd = open_pinned_directory(self.outbox_dir)
        try:
            for fn in sorted(os.listdir(outbox_fd)):
                if not _DAILY_JSONL.fullmatch(fn):
                    continue
                valid = []
                invalid = []
                lines = read_regular_fd(outbox_fd, fn).splitlines(keepends=True)
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        if not line.endswith(b"\n"):
                            raise ValueError("pending record has torn tail")
                        value = json.loads(line)
                        if not isinstance(value, dict):
                            raise ValueError("pending record is not an object")
                        if validator is not None and not validator(value)[0]:
                            raise ValueError("pending record failed envelope validation")
                        valid.append(line)
                    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                        invalid.append(line)
                if not invalid:
                    continue
                self._quarantine_relative(
                    outbox_fd, fn, "invalid", b"".join(invalid),
                )
                replace_regular_fd(outbox_fd, fn, b"".join(valid))
                repaired += len(invalid)
            if repaired:
                os.fsync(outbox_fd)
        finally:
            os.close(outbox_fd)
        if repaired:
            try:
                self._emit_locked(
                    "local:recovery", "event.degraded", "local",
                    {"cause": "malformed pending records quarantined",
                     "records": repaired},
                )
            except FileNotFoundError:
                # managed outbox dir replaced/removed beneath us; the
                # quarantine+rewrite is already durable — alert is best-effort
                pass
        return repaired

    def select_pending_batch(self, max_events, seen, validator, eligible=None,
                             start_after=None, max_scan=None, return_meta=False):
        """Select at most N unseen valid events, repairing corruption en route.

        Validation stops as soon as the batch is full. `seen` provides a fair
        logical round without relying on mutable file/list offsets.
        Returns `(events, reached_eof, repaired_count, last_position,
        scanned_count, eof_signature)` where the signature is captured under
        the same flock as an all-backoff EOF decision.
        """
        result = self._with_flock(
            self._select_pending_batch_locked,
            max_events, seen, validator, eligible, start_after,
            max_scan or max_events * 4,
        )
        return result if return_meta else result[:3]

    def _select_pending_batch_locked(self, max_events, seen, validator, eligible,
                                     start_after, max_scan):
        selected = []
        repaired = 0
        reached_eof = True
        scanned = 0
        last_position = start_after
        outbox_fd = open_pinned_directory(self.outbox_dir)
        try:
            for fn in sorted(os.listdir(outbox_fd)):
                if not _DAILY_JSONL.fullmatch(fn):
                    continue
                invalid = []
                invalid_indexes = set()
                data = read_regular_fd(outbox_fd, fn)
                lines = data.splitlines(keepends=True)
                for index, line in enumerate(lines):
                    position = (fn, index)
                    if start_after is not None and position <= start_after:
                        continue
                    if scanned >= max_scan:
                        reached_eof = False
                        break
                    scanned += 1
                    last_position = position
                    if not line.strip():
                        continue
                    try:
                        if not line.endswith(b"\n"):
                            raise ValueError("pending record has torn tail")
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                        invalid.append(line)
                        invalid_indexes.add(index)
                        repaired += 1
                        continue
                    if not isinstance(event, dict):
                        invalid.append(line)
                        invalid_indexes.add(index)
                        repaired += 1
                        continue
                    event_id = event.get("event_id")
                    if eligible is not None and not eligible(event):
                        continue
                    if not validator(event)[0]:
                        invalid.append(line)
                        invalid_indexes.add(index)
                        repaired += 1
                        continue
                    if event_id in seen:
                        continue
                    seen.add(event_id)
                    selected.append(event)
                    if len(selected) >= max_events:
                        reached_eof = False
                        break
                if invalid:
                    self._quarantine_relative(
                        outbox_fd, fn, "invalid", b"".join(invalid),
                    )
                    kept = [line for index, line in enumerate(lines)
                            if index not in invalid_indexes]
                    replace_regular_fd(outbox_fd, fn, b"".join(kept))
                if len(selected) >= max_events or scanned >= max_scan:
                    break
            if repaired:
                os.fsync(outbox_fd)
        finally:
            os.close(outbox_fd)
        if repaired:
            try:
                self._emit_locked(
                    "local:recovery", "event.degraded", "local",
                    {"cause": "invalid pending records quarantined",
                     "records": repaired},
                )
            except FileNotFoundError:
                # managed outbox dir replaced/removed beneath us; the
                # quarantine+rewrite is already durable — alert is best-effort
                pass
        eof_signature = None
        if reached_eof:
            try:
                eof_signature = self.pending_signature()
            except FileNotFoundError:
                eof_signature = None
        return (selected, reached_eof, repaired, last_position, scanned,
                eof_signature)

    def count_pending(self):
        return sum(1 for _ in self.read_pending())

    def pending_signature(self):
        """Cheap change token for wakeups; never parses pending JSON rows."""
        signature = []
        outbox_fd = open_pinned_directory(self.outbox_dir)
        try:
            for fn in sorted(os.listdir(outbox_fd)):
                if not _DAILY_JSONL.fullmatch(fn):
                    continue
                st = os.stat(fn, dir_fd=outbox_fd)
                signature.append((fn, st.st_size, st.st_mtime_ns))
        finally:
            os.close(outbox_fd)
        return tuple(signature)

    def ack(self, event):
        """Confirm delivery: record into archive, THEN remove from pending.

        Order matters for crash-safety: we never remove from pending before
        the archive has the record. A crash between the two leaves a
        duplicate archive entry (harmless: consumers dedup by `event_id`);
        it NEVER loses the event.

        Runs under the inter-process flock so it cannot race a concurrent
        `gc()`/`emit()` in another process.
        """
        self._with_flock(self._ack_locked, event)

    def _ack_locked(self, event):
        with self._lock:
            day = self._event_day(event)
            dst = os.path.join(self.archive_dir, day + ".jsonl")
            key = json.dumps(event, sort_keys=True)
            # 1) archive first (durable record of delivery)
            self._repair_and_append_line(dst, "archive", key)
            # 2) then remove from pending (atomic rewrite through the pinned dir)
            outbox_fd = open_pinned_directory(self.outbox_dir)
            try:
                try:
                    data = read_regular_fd(outbox_fd, day + ".jsonl")
                except FileNotFoundError:
                    data = None
                if data is not None:
                    lines = []
                    for line_ in data.splitlines(keepends=True):
                        try:
                            pending = json.loads(line_)
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            lines.append(line_)
                            continue
                        if pending.get("event_id") != event.get("event_id"):
                            lines.append(line_)
                    replace_regular_fd(outbox_fd, day + ".jsonl", b"".join(lines))
            finally:
                os.close(outbox_fd)
            self._set_cursor(event)

    def _repair_and_append_line_fd(self, directory_fd, name, label, encoded):
        """Repair and append through one pinned regular-file descriptor."""
        fd = open_regular_at(
            directory_fd, name,
            os.O_RDWR | os.O_APPEND | os.O_CREAT,
        )
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            chunks = []
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            data = b"".join(chunks)
            if data and not data.endswith(b"\n"):
                boundary = data.rfind(b"\n") + 1
                torn_bytes = data[boundary:]
                self._quarantine_relative(
                    directory_fd, name, label, torn_bytes,
                )
                os.ftruncate(fd, boundary)
            row = encoded.encode("utf-8") if isinstance(encoded, str) else encoded
            if not row.endswith(b"\n"):
                row += b"\n"
            os.lseek(fd, 0, os.SEEK_END)
            view = memoryview(row)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError("short same-fd append")
                view = view[written:]
            os.fsync(fd)
            os.fsync(directory_fd)
        finally:
            os.close(fd)

    def _repair_and_append_line(self, path, label, encoded):
        """Repair and append through one pinned regular-file descriptor."""
        directory, name = os.path.split(os.fspath(path))
        directory_fd = open_pinned_directory(directory or ".")
        try:
            self._repair_and_append_line_fd(
                directory_fd, name, label, encoded,
            )
        finally:
            os.close(directory_fd)

    def _archive_day_exists(self, archive_fd, name):
        try:
            st = os.stat(name, dir_fd=archive_fd)
        except FileNotFoundError:
            return False
        return stat.S_ISREG(st.st_mode)

    def _repair_append_tail(self, directory_fd, name, label):
        """Quarantine/truncate a crash-torn regular file relative to a pinned dir."""
        try:
            fd = open_regular_at(directory_fd, name, os.O_RDWR)
        except FileNotFoundError:
            return False
        try:
            chunks = []
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            data = b"".join(chunks)
            if not data or data.endswith(b"\n"):
                return False
            boundary = data.rfind(b"\n") + 1
            torn_bytes = data[boundary:]
            self._quarantine_relative(
                directory_fd, name, label, torn_bytes,
            )
            os.ftruncate(fd, boundary)
            os.fsync(fd)
            os.fsync(directory_fd)
            return len(torn_bytes)
        finally:
            os.close(fd)

    def _set_cursor(self, event):
        curs = self.load_cursors()
        target = event.get("subject", "")
        producer = event.get("producer", "")
        by_producer = curs.setdefault(target, {})
        # Migrate legacy subject-only state; producer-local sequences from
        # different producers are never comparable.
        if "producer_seq" in by_producer:
            legacy = dict(by_producer)
            by_producer.clear()
            legacy_producer = str(legacy.get("last_event_id", "")).rsplit(":", 1)[0]
            if legacy_producer:
                by_producer[legacy_producer] = legacy
        current = by_producer.get(producer, {})
        if int(current.get("producer_seq", 0)) >= int(event.get("producer_seq", 0)):
            return
        by_producer[producer] = {"last_event_id": event.get("event_id"),
                                 "producer_seq": event.get("producer_seq")}
        self._write_json_atomic(self.cursor_file, curs)

    @staticmethod
    def _normalize_cursors(cursors):
        normalized = {}
        changed = False
        for target, value in cursors.items():
            if isinstance(value, dict) and "producer_seq" in value:
                producer = str(value.get("last_event_id", "")).rsplit(":", 1)[0]
                normalized[target] = {producer: value} if producer else {}
                changed = True
            else:
                normalized[target] = value
        return normalized, changed

    def _migrate_legacy_cursors_locked(self):
        try:
            data = read_regular_nofollow(self.cursor_file)
        except FileNotFoundError:
            return
        cursors = json.loads(data)
        normalized, changed = self._normalize_cursors(cursors)
        if changed:
            self._write_json_atomic(self.cursor_file, normalized)

    def load_cursors(self):
        try:
            data = read_regular_nofollow(self.cursor_file)
        except FileNotFoundError:
            return {}
        cursors, _ = self._normalize_cursors(json.loads(data))
        return cursors
    # -- retention (gc) ------------------------------------------------------
    def gc(self, archive_days=90, max_bytes=500 * 1024 * 1024):
        """Delete old archives; rotate + enforce cap; alert (degraded) on oversize.

        `archive_days`: delete archive files older than N days.
        `max_bytes`: when the archive directory exceeds this, rotate the
        current-day file to a timestamped sibling, emit an `event.degraded`
        record, then ENFORCE the hard cap by evicting the OLDEST rotated
        overflow files until the archive is under `max_bytes` again (true
        retention enforcement — M5; M2 only rotated + alerted).

        Runs under the inter-process flock so it cannot race ack()/emit().
        Returns a dict of what happened, including `evicted` count.
        """
        return self._with_flock(self._gc_locked, archive_days, max_bytes)

    def _gc_locked(self, archive_days, max_bytes):
        with self._lock:
            cutoff = time.time() - archive_days * 86400
            removed = 0
            deleted = False
            archive_fd = open_pinned_directory(self.archive_dir)
            try:
                for fn in os.listdir(archive_fd):
                    managed = re.fullmatch(
                        r"\d{4}-\d{2}-\d{2}(?:\.\d{9,11})?\.jsonl", fn,
                    )
                    try:
                        st = os.stat(fn, dir_fd=archive_fd)
                    except FileNotFoundError:
                        continue
                    if (managed and stat.S_ISREG(st.st_mode)
                            and st.st_mtime < cutoff):
                        os.unlink(fn, dir_fd=archive_fd)
                        removed += 1
                        deleted = True
                # size guard (rotation + alert + HARD CAP enforcement)
                total = 0
                for f in os.listdir(archive_fd):
                    if not _ARCHIVE_JSONL.fullmatch(f):
                        continue
                    try:
                        st = os.stat(f, dir_fd=archive_fd)
                    except FileNotFoundError:
                        continue
                    if stat.S_ISREG(st.st_mode):
                        total += st.st_size
                rotated = False
                evicted = 0
                degraded = None
                if total > max_bytes:
                    # rotate: rename current day archive to a timestamped overflow
                    day = utcnow_iso()[:10]
                    if self._archive_day_exists(archive_fd, day + ".jsonl"):
                        suffix = int(time.time())
                        dst = f"{day}.{suffix}.jsonl"
                        while self._archive_day_exists(archive_fd, dst):
                            suffix += 1
                            dst = f"{day}.{suffix}.jsonl"
                        os.replace(
                            f"{day}.jsonl", dst,
                            src_dir_fd=archive_fd, dst_dir_fd=archive_fd,
                        )
                        rotated = True
                        # fsync the DIRECTORY so the rename is crash-durable
                        os.fsync(archive_fd)
                    # unique degraded identity from the outbox's own sequence
                    # (call _emit_locked directly — we already hold the flock)
                    degraded = self._emit_locked("local:gc", "event.degraded",
                                                 "local",
                                                 {"cause": f"archive size {total} > {max_bytes}"})
                    # HARD CAP enforcement: evict OLDEST rotated overflow files
                    # (timestamped siblings of the day rotation) until under cap.
                    # Strict pattern: `YYYY-MM-DD.<epoch>.jsonl` where <epoch> is
                    # the int(time.time()) suffix the rotation produces (9-11
                    # digits across the representable range, 10 in our era).
                    # Never the bare current-day file, ordinary archives with
                    # dots elsewhere, or odd single/multi-digit suffixes that are
                    # not rotation timestamps.
                    _ROTATED = re.compile(r"\d{4}-\d{2}-\d{2}\.\d{9,11}\.jsonl$")
                    overflow = []
                    for f in os.listdir(archive_fd):
                        if not _ROTATED.match(f):
                            continue
                        try:
                            st = os.stat(f, dir_fd=archive_fd)
                        except FileNotFoundError:
                            continue
                        if stat.S_ISREG(st.st_mode):
                            overflow.append((st.st_mtime, st.st_size, f))
                    overflow.sort()
                    for _, size, f in overflow:
                        if total <= max_bytes:
                            break
                        if size == 0:
                            continue
                        # never evict rotations still inside the retention
                        # window — only aged out content is eligible
                        try:
                            st = os.stat(f, dir_fd=archive_fd)
                        except FileNotFoundError:
                            continue
                        if st.st_mtime >= cutoff:
                            continue
                        os.unlink(f, dir_fd=archive_fd)
                        total -= size
                        evicted += 1
                        deleted = True
                if deleted:
                    # fsync the PINNED archive dirfd (not a pathname reopen):
                    # a directory swap between unlink and fsync must not let
                    # us fsync the replacement instead of the inode we changed
                    os.fsync(archive_fd)
            finally:
                os.close(archive_fd)
            unresolved_oversize = total > max_bytes
            if unresolved_oversize and degraded is None:
                degraded = self._emit_locked(
                    "local:gc", "event.degraded", "local",
                    {"cause": f"archive remains over cap: {total} > {max_bytes}"},
                )
            return {"removed": removed, "rotated": rotated, "evicted": evicted,
                    "size": total,
                    "unresolved_oversize": unresolved_oversize,
                    "degraded": (degraded or {}).get("event_id")}


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
        return f"<TargetQueue {self._NAME[self.state]} term={self.term} normal={len(self.normal)} catchup={len(self.catchup)}>"

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
            for (_, i), (_, j) in zip(lst, lst[1:], strict=False):
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
            return False, f"cycle involving event {cyc}: {events[cyc].get('event_id')}"
        return True, None
