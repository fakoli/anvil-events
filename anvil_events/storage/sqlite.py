"""Small facade composing the SQLite storage repositories."""

from __future__ import annotations

from ..domain import validate_event
from .database import SCHEMA_VERSION, Database
from .delivery import PendingDelivery
from .events import EventRepository
from .facts import FactRepository
from .migration import LegacyMigrator
from .operations import OperationLedger
from .queries import EventQueries
from .retention import Retention


class SQLiteStore:
    backend = "sqlite"

    def __init__(self, root):
        self.database = Database(root)
        self.root = self.database.root
        self.database_path = self.database.path
        self.events = EventRepository(self.database)
        self.queries = EventQueries(self.database)
        self.delivery = PendingDelivery(
            self.database, self.events, self.queries,
        )
        self.operations = OperationLedger(self.database, self.events)
        self.facts = FactRepository(self.database)
        self.retention = Retention(self.database, self.events)
        self.migration = LegacyMigrator(self.database, self.events)
        # Display-only compatibility attributes. No new code treats these as
        # directories containing mutable JSONL state.
        self.outbox_dir = self.root
        self.archive_dir = self.root
        self.journal_dir = self.root
        self.quarantine_dir = self.root

    def transaction(self, immediate=True):
        return self.database.transaction(immediate=immediate)

    def emit(self, producer, kind, host, payload, correlation_id=None):
        return self.events.emit_v1(
            producer, kind, host, payload, correlation_id,
        )

    def emit_v2(self, producer, kind, node, payload, correlation_id=None,
                causes=None):
        return self.events.emit_v2(
            producer, kind, node, payload,
            correlation_id=correlation_id, causes=causes,
        )

    def append(self, event):
        self.events.append(event)
        return self.database_path

    def append_journal(self, event):
        return self.events.append_journal(event)

    def record_puback(self, event, puback):
        return self.events.record_puback(event, puback)

    def ack(self, event, puback=None):
        evidence = puback or {
            "stream": "legacy-unknown",
            "seq": event["producer_seq"],
            "duplicate": False,
        }
        return self.events.record_puback(event, evidence)

    def note_delivery_failure(self, event_id, error, retry_after=None):
        return self.events.note_delivery_failure(event_id, error, retry_after)

    def read_pending(self):
        yield from self.queries.read("producer_state = 'pending'")

    def read_archive(self):
        yield from self.queries.read("producer_state = 'acked'")

    def read_journal(self):
        yield from self.queries.read("journaled = 1")

    def read_producer_history(self):
        yield from self.queries.read("producer_state IS NOT NULL")

    def read_all(self):
        yield from self.queries.read(
            "producer_state IS NOT NULL OR journaled = 1",
        )

    def count_pending(self):
        return self.queries.count_pending()

    def pending_signature(self):
        return self.queries.pending_signature()

    def load_producer_seqs(self):
        return self.queries.load_sequences()

    def load_cursors(self):
        return self.queries.load_cursors()

    def repair_invalid_pending(self, validator=None):
        return self.delivery.repair(validator or validate_event)

    def select_pending_batch(self, max_events, seen, validator, eligible=None,
                             start_after=None, max_scan=None, return_meta=False):
        return self.delivery.select(
            max_events, seen, validator, eligible=eligible,
            start_after=start_after, max_scan=max_scan,
            return_meta=return_meta,
        )

    def add_fact(self, event, allowed_producers=None):
        return self.facts.add(event, allowed_producers=allowed_producers)

    def read_facts(self):
        yield from self.facts.read()

    def gc(self, archive_days=90, max_bytes=500 * 1024 * 1024):
        return self.retention.collect(archive_days, max_bytes)

    def prepare_operation(self, operation_id, idempotency_key, producer, kind,
                          intent, correlation_id=None):
        return self.operations.prepare(
            operation_id, idempotency_key, producer, kind, intent,
            correlation_id=correlation_id,
        )

    def record_v2(self, operation_id, idempotency_key, producer, kind, node,
                  payload, correlation_id=None, causes=None):
        return self.operations.record(
            operation_id, idempotency_key, producer, kind, node, payload,
            correlation_id=correlation_id, causes=causes,
        )

    def resolve_operation(self, operation_id, node, payload, *, succeeded=True,
                          error=None):
        return self.operations.resolve(
            operation_id, node, payload, succeeded=succeeded, error=error,
        )

    def mark_operation_indeterminate(self, operation_id, error):
        return self.operations.mark_indeterminate(operation_id, error)

    def list_unresolved_operations(self):
        return self.operations.unresolved()

    def import_legacy(self, legacy_root, offline=False):
        return self.migration.import_source(legacy_root, offline=offline)

    def status(self):
        with self.database.connect() as connection:
            counts = connection.execute(
                """
                SELECT
                    SUM(producer_state = 'pending') AS pending,
                    SUM(producer_state = 'acked') AS archived,
                    SUM(journaled = 1) AS journaled
                FROM events
                """
            ).fetchone()
            cursors = connection.execute(
                "SELECT COUNT(*) FROM cursors"
            ).fetchone()[0]
            facts = connection.execute(
                "SELECT COUNT(*) FROM facts"
            ).fetchone()[0]
            unresolved = connection.execute(
                """
                SELECT COUNT(*) FROM operations
                 WHERE state IN ('PREPARED', 'INDETERMINATE')
                """
            ).fetchone()[0]
            quarantined = connection.execute(
                "SELECT COUNT(*) FROM quarantine"
            ).fetchone()[0]
        return {
            "backend": self.backend,
            "schema_version": SCHEMA_VERSION,
            "database": self.database_path,
            "pending": int(counts["pending"] or 0),
            "archived": int(counts["archived"] or 0),
            "journaled": int(counts["journaled"] or 0),
            "cursors": cursors,
            "facts": facts,
            "unresolved_operations": unresolved,
            "quarantined": quarantined,
        }
