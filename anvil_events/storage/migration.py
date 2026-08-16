"""Fail-closed, provenance-recorded legacy JSONL migration."""

from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from ..domain import validate_event
from ..legacy_jsonl import ARCHIVE_JSONL, DAILY_JSONL


@dataclass(frozen=True)
class LegacyItem:
    role: str
    file: str
    line: int
    raw_sha256: str
    event: dict


@dataclass(frozen=True)
class LegacySnapshot:
    root: Path
    fingerprint: str
    items: tuple[LegacyItem, ...]
    producer_sequences: dict
    cursors: dict


@contextmanager
def legacy_source_lock(root, offline=False):
    if os.name == "nt":
        if not offline:
            raise RuntimeError(
                "Windows legacy migration requires --offline-source"
            )
        yield
        return
    import fcntl

    lock_path = os.path.join(root, ".lock")
    descriptor = os.open(lock_path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class LegacyMigrator:
    def __init__(self, database, events):
        self.database = database
        self.events = events

    @staticmethod
    def _metadata(root, name, digest, default):
        path = root / name
        if not os.path.lexists(path):
            return default
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"legacy metadata is not a regular file: {path}")
        raw = path.read_bytes()
        digest.update(f"{name}\0".encode())
        digest.update(raw)
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"invalid legacy metadata: {path}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"legacy metadata must be an object: {path}")
        return value

    @classmethod
    def snapshot(cls, root):
        supplied_root = Path(root)
        if supplied_root.is_symlink() or not supplied_root.is_dir():
            raise ValueError("legacy root must be a real directory")
        root_path = supplied_root.resolve()
        digest = hashlib.sha256()
        items = []
        locations = (
            ("outbox", DAILY_JSONL, "pending"),
            ("archive", ARCHIVE_JSONL, "acked"),
            ("journal", DAILY_JSONL, "journal"),
        )
        for directory_name, pattern, role in locations:
            directory = root_path / directory_name
            if not directory.exists():
                continue
            if directory.is_symlink() or not directory.is_dir():
                raise ValueError(f"legacy {directory_name} is not a real directory")
            for path in sorted(directory.iterdir(), key=lambda item: item.name):
                if not pattern.fullmatch(path.name):
                    continue
                if path.is_symlink() or not path.is_file():
                    raise ValueError(
                        f"legacy managed path is not a regular file: {path}"
                    )
                raw_file = path.read_bytes()
                relative = f"{directory_name}/{path.name}"
                digest.update(f"{relative}\0".encode())
                digest.update(raw_file)
                if raw_file and not raw_file.endswith(b"\n"):
                    raise ValueError(f"legacy managed file has a torn tail: {path}")
                for line_number, raw_line in enumerate(raw_file.splitlines(), 1):
                    if not raw_line.strip():
                        continue
                    try:
                        event = json.loads(raw_line)
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        raise ValueError(
                            f"invalid JSON in {path}:{line_number}"
                        ) from exc
                    ok, reason = validate_event(event)
                    if not ok:
                        raise ValueError(
                            f"invalid event in {path}:{line_number}: {reason}"
                        )
                    items.append(LegacyItem(
                        role, relative, line_number,
                        hashlib.sha256(raw_line).hexdigest(), event,
                    ))
        producer_roles = {}
        for item in items:
            if item.role in ("pending", "acked"):
                producer_roles.setdefault(item.event["event_id"], set()).add(item.role)
        conflict = next((
            event_id for event_id, roles in producer_roles.items()
            if roles == {"pending", "acked"}
        ), None)
        if conflict is not None:
            raise ValueError(
                f"legacy event has conflicting pending/acked roles: {conflict}"
            )
        sequences = cls._metadata(
            root_path, "producer-seqs.json", digest, {},
        )
        for producer, sequence in sequences.items():
            if (not isinstance(producer, str) or isinstance(sequence, bool)
                    or not isinstance(sequence, int) or sequence < 0):
                raise ValueError("invalid producer sequence metadata")
        cursors = cls._metadata(root_path, "cursors.json", digest, {})
        return LegacySnapshot(
            root_path, digest.hexdigest(), tuple(items), sequences, cursors,
        )

    @staticmethod
    def _cursor_rows(cursors):
        for subject, by_producer in cursors.items():
            if not isinstance(subject, str) or not isinstance(by_producer, dict):
                raise ValueError("invalid cursor metadata")
            if "producer_seq" in by_producer:
                event_id = by_producer.get("last_event_id", "")
                producer = str(event_id).rsplit(":", 1)[0]
                by_producer = {producer: by_producer} if producer else {}
            for producer, cursor in by_producer.items():
                if not isinstance(cursor, dict):
                    raise ValueError("invalid cursor metadata")
                sequence = cursor.get("producer_seq")
                event_id = cursor.get("last_event_id")
                if (not isinstance(sequence, int) or isinstance(sequence, bool)
                        or sequence < 1
                        or event_id != f"{producer}:{sequence:06d}"):
                    raise ValueError("invalid cursor identity metadata")
                yield subject, producer, event_id, sequence

    def import_source(self, root, offline=False):
        supplied_root = Path(root)
        if supplied_root.is_symlink() or not supplied_root.is_dir():
            raise ValueError("legacy root must be a real directory")
        with legacy_source_lock(root, offline=offline):
            snapshot = self.snapshot(root)
            counts = {
                role: sum(item.role == role for item in snapshot.items)
                for role in ("pending", "acked", "journal")
            }
            with self.database.transaction() as connection:
                completed = connection.execute(
                    "SELECT source_fingerprint FROM migration_runs "
                    "WHERE source_root = ?",
                    (str(snapshot.root),),
                ).fetchone()
                if completed is not None:
                    if completed["source_fingerprint"] != snapshot.fingerprint:
                        raise ValueError(
                            "legacy source changed after a completed migration"
                        )
                    return {**counts, "already_imported": True,
                            "fingerprint": snapshot.fingerprint}
                connection.execute(
                    """
                    INSERT INTO migration_runs(
                        source_root, source_fingerprint, imported_at,
                        pending_count, archive_count, journal_count
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(snapshot.root), snapshot.fingerprint, time.time(),
                        counts["pending"], counts["acked"], counts["journal"],
                    ),
                )
                before = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE producer_state = 'pending'"
                ).fetchone()[0]
                for item in snapshot.items:
                    self.events.put(
                        connection, item.event,
                        producer_state=item.role if item.role in ("pending", "acked")
                        else None,
                        journaled=item.role == "journal",
                    )
                    connection.execute(
                        """
                        INSERT INTO migration_items(
                            source_root, source_role, source_file,
                            line_number, raw_sha256, event_id
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(snapshot.root), item.role, item.file, item.line,
                            item.raw_sha256, item.event["event_id"],
                        ),
                    )
                for producer, sequence in snapshot.producer_sequences.items():
                    self.events._update_sequence(connection, producer, sequence)
                for subject, producer, event_id, sequence in self._cursor_rows(
                        snapshot.cursors):
                    connection.execute(
                        """
                        INSERT INTO cursors(
                            subject, producer, event_id, producer_seq
                        ) VALUES (?, ?, ?, ?)
                        ON CONFLICT(subject, producer) DO UPDATE SET
                            event_id = excluded.event_id,
                            producer_seq = MAX(
                                cursors.producer_seq, excluded.producer_seq
                            )
                        """,
                        (subject, producer, event_id, sequence),
                    )
                    self.events._update_sequence(connection, producer, sequence)
                after = connection.execute(
                    "SELECT COUNT(*) FROM events WHERE producer_state = 'pending'"
                ).fetchone()[0]
                if after != before:
                    self.events._bump_pending(connection)
                if connection.execute("PRAGMA foreign_key_check").fetchall():
                    raise ValueError("foreign-key check failed after migration")
                if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("SQLite integrity check failed after migration")
                if self.snapshot(root).fingerprint != snapshot.fingerprint:
                    raise ValueError("legacy source changed during migration")
            return {**counts, "already_imported": False,
                    "fingerprint": snapshot.fingerprint}


def legacy_state_exists(root):
    root = Path(root)
    return any(
        (root / name).exists()
        for name in (
            "outbox", "archive", "journal", "cursors.json",
            "producer-seqs.json",
        )
    )
