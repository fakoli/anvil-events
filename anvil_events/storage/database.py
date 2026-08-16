"""SQLite connection and schema ownership."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

DATABASE_NAME = "events.db"
SCHEMA_VERSION = 1
APPLICATION_ID = 0x414E5645  # "ANVE"


class Database:
    def __init__(self, root):
        self.root = os.path.abspath(os.fspath(root))
        if os.path.lexists(self.root) and os.path.islink(self.root):
            raise OSError("events root must not be a symlink")
        os.makedirs(self.root, exist_ok=True)
        self.path = os.path.join(self.root, DATABASE_NAME)
        if os.path.lexists(self.path) and os.path.islink(self.path):
            raise OSError("events database must not be a symlink")
        self._initialize()

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(
            self.path, timeout=30, isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self, immediate=True):
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _initialize(self):
        with self.connect() as connection:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise OSError(f"SQLite WAL mode unavailable: {mode!r}")
            application_id = connection.execute(
                "PRAGMA application_id",
            ).fetchone()[0]
            user_tables = connection.execute(
                """
                SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
            if application_id not in (0, APPLICATION_ID):
                raise RuntimeError("database belongs to another application")
            if application_id == 0 and user_tables:
                raise RuntimeError(
                    "refusing to initialize over an unowned SQLite database"
                )
            current = connection.execute("PRAGMA user_version").fetchone()[0]
            if current not in (0, SCHEMA_VERSION):
                raise RuntimeError(
                    f"unsupported event-store schema {current}; "
                    f"expected {SCHEMA_VERSION}"
                )
            connection.executescript(_SCHEMA)
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")
            connection.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES (?, ?)",
                ("pending_change_counter", "0"),
            )


_SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS producer_sequences (
    producer TEXT PRIMARY KEY,
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0)
);

CREATE TABLE IF NOT EXISTS events (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    producer TEXT NOT NULL,
    producer_seq INTEGER NOT NULL CHECK (producer_seq > 0),
    observed_at TEXT NOT NULL,
    subject TEXT NOT NULL,
    kind TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    canonical_sha256 TEXT NOT NULL,
    canonical_size INTEGER NOT NULL CHECK (canonical_size >= 0),
    producer_state TEXT CHECK (
        producer_state IS NULL OR producer_state IN ('pending', 'acked')
    ),
    journaled INTEGER NOT NULL DEFAULT 0 CHECK (journaled IN (0, 1)),
    acked_at REAL,
    puback_stream TEXT,
    puback_sequence INTEGER CHECK (
        puback_sequence IS NULL OR puback_sequence > 0
    ),
    puback_duplicate INTEGER CHECK (
        puback_duplicate IS NULL OR puback_duplicate IN (0, 1)
    ),
    delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    last_delivery_error TEXT,
    retry_after REAL,
    created_at REAL NOT NULL,
    UNIQUE (producer, producer_seq)
);

CREATE INDEX IF NOT EXISTS idx_events_pending
    ON events (producer_state, row_id);
CREATE INDEX IF NOT EXISTS idx_events_archive
    ON events (producer_state, observed_at, producer_seq);
CREATE INDEX IF NOT EXISTS idx_events_journal
    ON events (journaled, observed_at, producer_seq);

CREATE TABLE IF NOT EXISTS cursors (
    subject TEXT NOT NULL,
    producer TEXT NOT NULL,
    event_id TEXT NOT NULL,
    producer_seq INTEGER NOT NULL CHECK (producer_seq > 0),
    PRIMARY KEY (subject, producer)
);

CREATE TABLE IF NOT EXISTS facts (
    event_id TEXT PRIMARY KEY,
    fact_json TEXT NOT NULL,
    fact_sha256 TEXT NOT NULL,
    stored_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    raw_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    quarantined_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS operations (
    operation_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    producer TEXT NOT NULL,
    kind TEXT NOT NULL,
    correlation_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN ('PREPARED', 'RECORDED', 'APPLIED', 'FAILED', 'INDETERMINATE')
    ),
    intent_json TEXT NOT NULL,
    prepared_at REAL NOT NULL,
    resolved_at REAL,
    event_id TEXT UNIQUE,
    error TEXT,
    FOREIGN KEY(event_id) REFERENCES events(event_id)
);

CREATE TABLE IF NOT EXISTS reconcile_resources (
    node TEXT NOT NULL,
    resource TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    revision TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    adapter TEXT NOT NULL,
    event_id TEXT NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (node, resource),
    FOREIGN KEY(event_id) REFERENCES events(event_id)
);

CREATE TABLE IF NOT EXISTS reconcile_attempts (
    operation_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    node TEXT NOT NULL,
    resource TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    state TEXT NOT NULL CHECK (
        state IN ('PREPARED', 'AWAITING_APPROVAL', 'APPLIED',
                  'FAILED', 'INDETERMINATE')
    ),
    preview_json TEXT,
    error TEXT,
    started_at REAL NOT NULL,
    completed_at REAL,
    UNIQUE (event_id, node),
    FOREIGN KEY(event_id) REFERENCES events(event_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_reconcile_generation
    ON reconcile_attempts (node, resource, generation);

CREATE TABLE IF NOT EXISTS migration_runs (
    source_root TEXT PRIMARY KEY,
    source_fingerprint TEXT NOT NULL,
    imported_at REAL NOT NULL,
    pending_count INTEGER NOT NULL,
    archive_count INTEGER NOT NULL,
    journal_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS migration_items (
    source_root TEXT NOT NULL,
    source_role TEXT NOT NULL,
    source_file TEXT NOT NULL,
    line_number INTEGER NOT NULL CHECK (line_number > 0),
    raw_sha256 TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (source_root, source_role, source_file, line_number),
    FOREIGN KEY(source_root)
        REFERENCES migration_runs(source_root) ON DELETE CASCADE
);
"""
