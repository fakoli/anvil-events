from __future__ import annotations

import copy
import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from helpers import desired_event, desired_payload

from anvil_events.domain import make_event, validate_event
from anvil_events.storage import DATABASE_NAME, SQLiteStore
from anvil_events.storage import database as database_module


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = self.temporary.name
        self.store = SQLiteStore(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_fresh_store_reopens_without_drift(self):
        self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        reopened = SQLiteStore(self.root)
        self.assertEqual(1, reopened.status()["pending"])
        self.assertEqual(1, reopened.status()["schema_version"])

    def test_concurrent_fresh_initializers_share_one_owned_schema(self):
        root = Path(self.root) / "concurrent-initialize"
        with ThreadPoolExecutor(max_workers=8) as pool:
            stores = list(pool.map(lambda _: SQLiteStore(root), range(16)))
        self.assertTrue(all(
            store.status()["schema_version"] == 1 for store in stores
        ))

    def test_concurrent_producer_sequences_are_unique(self):
        def emit(index):
            return self.store.emit_v2(
                "node-a:router", "state.desired", "node-a",
                desired_payload(generation=index + 1, revision=f"rev-{index + 1}"),
            )["producer_seq"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            sequences = list(pool.map(emit, range(24)))
        self.assertEqual(list(range(1, 25)), sorted(sequences))

    def test_event_identity_collision_fails_closed(self):
        event = desired_event()
        self.store.append(event)
        conflict = copy.deepcopy(event)
        conflict["payload"]["artifact"] = "routing/other"
        with self.assertRaisesRegex(ValueError, "identity collision"):
            self.store.append(conflict)

    def test_atomic_record_is_idempotent(self):
        first, repeated = self.store.record_v2(
            "operation-1", "key-1", "node-a:router", "state.desired",
            "node-a", desired_payload(),
        )
        second, repeated_again = self.store.record_v2(
            "operation-1", "key-1", "node-a:router", "state.desired",
            "node-a", desired_payload(),
        )
        self.assertFalse(repeated)
        self.assertTrue(repeated_again)
        self.assertEqual(first, second)
        self.assertEqual(1, self.store.count_pending())

    def test_idempotency_key_cannot_change_intent(self):
        self.store.record_v2(
            "operation-1", "key-1", "node-a:router", "state.desired",
            "node-a", desired_payload(),
        )
        with self.assertRaisesRegex(ValueError, "collision"):
            self.store.record_v2(
                "operation-1", "key-1", "node-a:router", "state.desired",
                "node-a", desired_payload(generation=2, revision="rev-2"),
            )

    def test_puback_and_cursor_commit_together(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        self.store.record_puback(event, {
            "stream": "ANVIL_EVENTS", "seq": 7, "duplicate": False,
        })
        self.assertEqual(0, self.store.count_pending())
        self.assertEqual(1, len(list(self.store.read_archive())))
        cursor = self.store.load_cursors()[event["subject"]][event["producer"]]
        self.assertEqual(event["event_id"], cursor["last_event_id"])

    def test_unknown_event_cannot_be_acknowledged(self):
        with self.assertRaisesRegex(ValueError, "unknown event"):
            self.store.record_puback(desired_event(), {
                "stream": "ANVIL_EVENTS", "seq": 1,
            })

    def test_conflicting_puback_evidence_fails_closed(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        self.store.record_puback(event, {"stream": "ANVIL_EVENTS", "seq": 1})
        with self.assertRaisesRegex(ValueError, "conflicting PubAck"):
            self.store.record_puback(event, {"stream": "ANVIL_EVENTS", "seq": 2})

    def test_puback_fields_are_strict(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        for ack in (
            {"stream": "bad.stream", "seq": 1},
            {"stream": "ANVIL", "seq": 1, "duplicate": "false"},
            {"stream": "ANVIL", "seq": 0},
            {"stream": "ANVIL", "seq": 1.5},
            {"stream": "ANVIL", "seq": True},
        ):
            with self.assertRaises(ValueError):
                self.store.record_puback(event, ack)

    def test_journal_deduplicates_identical_delivery(self):
        event = desired_event()
        self.assertTrue(self.store.append_journal(event))
        self.assertFalse(self.store.append_journal(copy.deepcopy(event)))
        self.assertEqual(1, len(list(self.store.read_journal())))

    def test_corrupt_pending_row_is_quarantined(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        with self.store.database.connect() as connection:
            connection.execute(
                "UPDATE events SET envelope_json = 'not json' WHERE event_id = ?",
                (event["event_id"],),
            )
        selected, _, repaired = self.store.select_pending_batch(
            10, set(), validate_event,
        )
        self.assertEqual([], selected)
        self.assertEqual(1, repaired)
        self.assertEqual(0, self.store.count_pending())
        self.assertEqual(1, self.store.status()["quarantined"])
        self.assertEqual("event.degraded", next(self.store.read_journal())["kind"])

    def test_valid_but_tampered_pending_row_is_quarantined(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        altered = copy.deepcopy(event)
        altered["payload"]["artifact"] = "routing/other"
        with self.store.database.connect() as connection:
            connection.execute(
                "UPDATE events SET envelope_json = ? WHERE event_id = ?",
                (json.dumps(altered, sort_keys=True, separators=(",", ":")),
                 event["event_id"]),
            )
        selected, _, repaired = self.store.select_pending_batch(
            10, set(), validate_event,
        )
        self.assertEqual([], selected)
        self.assertEqual(1, repaired)

    def test_quarantined_envelope_can_be_healed_by_canonical_redelivery(self):
        event = desired_event()
        self.store.append(event)
        with self.store.database.connect() as connection:
            connection.execute(
                "UPDATE events SET envelope_json = 'not json' WHERE event_id = ?",
                (event["event_id"],),
            )
        self.store.select_pending_batch(10, set(), validate_event)
        self.store.append_journal(event)
        healed = [
            item for item in self.store.read_journal()
            if item["event_id"] == event["event_id"]
        ]
        self.assertEqual([event], healed)

    def test_journal_only_corruption_heals_on_canonical_redelivery(self):
        event = desired_event()
        self.store.append_journal(event)
        with self.store.database.connect() as connection:
            connection.execute(
                "UPDATE events SET envelope_json = 'not json' WHERE event_id = ?",
                (event["event_id"],),
            )
        self.store.append_journal(event)
        self.assertEqual([event], list(self.store.read_journal()))
        self.assertEqual(1, self.store.status()["quarantined"])

    def test_quarantine_marks_record_operation_indeterminate(self):
        event, _ = self.store.record_v2(
            "operation-1", "key-1", "node-a:router", "state.desired",
            "node-a", desired_payload(),
        )
        with self.store.database.connect() as connection:
            connection.execute(
                "UPDATE events SET envelope_json = 'not json' WHERE event_id = ?",
                (event["event_id"],),
            )
        self.store.select_pending_batch(10, set(), validate_event)
        unresolved = self.store.list_unresolved_operations()
        self.assertEqual("INDETERMINATE", unresolved[0]["state"])

    def test_negative_retention_is_rejected_without_writes(self):
        self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        before = self.store.status()
        with self.assertRaises(ValueError):
            self.store.gc(archive_days=-1)
        self.assertEqual(before, self.store.status())

    def test_retention_expires_archives_and_records_audit(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        self.store.record_puback(event, {"stream": "ANVIL_EVENTS", "seq": 1})
        result = self.store.gc(archive_days=0)
        self.assertEqual(1, result["removed"])
        self.assertIsNotNone(result["degraded"])
        self.assertEqual(0, self.store.count_pending())
        self.assertEqual("event.degraded", next(self.store.read_journal())["kind"])

    def test_retention_preserves_operation_identity_rows(self):
        event, _ = self.store.record_v2(
            "operation-1", "key-1", "node-a:router", "state.desired",
            "node-a", desired_payload(),
        )
        self.store.record_puback(event, {"stream": "ANVIL_EVENTS", "seq": 1})
        self.store.gc(archive_days=0)
        repeated, already_recorded = self.store.record_v2(
            "operation-1", "key-1", "node-a:router", "state.desired",
            "node-a", desired_payload(),
        )
        self.assertTrue(already_recorded)
        self.assertEqual(event, repeated)

    def test_fact_projection_is_idempotent_and_redacted(self):
        event = desired_event()
        event["payload"]["ordinary"] = {"value": 3}
        first = self.store.add_fact(
            event, allowed_producers={"node-a:router"},
        )
        second = self.store.add_fact(
            event, allowed_producers={"node-a:router"},
        )
        self.assertEqual(3, first["payload"]["ordinary"]["value"])
        self.assertIsNone(second)

    def test_foreign_sqlite_database_is_not_adopted(self):
        other = Path(self.root) / "foreign"
        other.mkdir()
        connection = sqlite3.connect(other / DATABASE_NAME)
        connection.execute("CREATE TABLE unrelated(value TEXT)")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "unowned"):
            SQLiteStore(other)

    def test_interrupted_initialization_rolls_back_and_retries(self):
        interrupted = Path(self.root) / "interrupted"

        def fail_after_first_statement(connection):
            connection.execute(next(database_module._schema_statements()))
            raise OSError("synthetic process death")

        with patch.object(
                database_module, "_apply_schema",
                side_effect=fail_after_first_statement):
            with self.assertRaisesRegex(OSError, "synthetic"):
                SQLiteStore(interrupted)
        recovered = SQLiteStore(interrupted)
        self.assertEqual(0, recovered.status()["pending"])

    def test_operation_indeterminate_is_visible(self):
        self.store.prepare_operation(
            "external-1", "external-key", "node-a:controller",
            "plugin.changed", {"action": "reload"},
        )
        self.store.mark_operation_indeterminate("external-1", "connection lost")
        unresolved = self.store.list_unresolved_operations()
        self.assertEqual("INDETERMINATE", unresolved[0]["state"])

    def test_operation_resolution_must_match_durable_intent(self):
        self.store.prepare_operation(
            "external-1", "external-key", "node-a:controller",
            "plugin.changed", {"action": "reload"},
        )
        with self.assertRaisesRegex(ValueError, "durable intent"):
            self.store.resolve_operation(
                "external-1", "node-a", {"action": "replace"},
            )
        event, repeated = self.store.resolve_operation(
            "external-1", "node-a", {"action": "reload"},
        )
        self.assertFalse(repeated)
        self.assertEqual({"action": "reload"}, event["payload"])
        same, repeated = self.store.resolve_operation(
            "external-1", "node-a", {"action": "reload"},
        )
        self.assertTrue(repeated)
        self.assertEqual(event, same)
        with self.assertRaisesRegex(ValueError, "durable result"):
            self.store.resolve_operation(
                "external-1", "node-b", {"action": "reload"},
            )

    def test_indeterminate_operation_cannot_be_silently_resolved(self):
        self.store.prepare_operation(
            "external-1", "external-key", "node-a:controller",
            "plugin.changed", {"action": "reload"},
        )
        self.store.mark_operation_indeterminate("external-1", "connection lost")
        with self.assertRaisesRegex(ValueError, "explicit operator recovery"):
            self.store.resolve_operation(
                "external-1", "node-a", {"action": "reload"},
            )


class LegacyMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.legacy = base / "legacy"
        self.target = base / "target"
        (self.legacy / "outbox").mkdir(parents=True)
        (self.legacy / "archive").mkdir()
        (self.legacy / "journal").mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def _legacy_event(sequence=1):
        return make_event(
            "node-a:serve", "serve.down", "node-a", {"serve": "primary"},
            producer_seq=sequence,
        )

    def _write(self, subdirectory, event, *, newline=True):
        path = self.legacy / subdirectory / "2026-08-16.jsonl"
        encoded = json.dumps(event) + ("\n" if newline else "")
        path.write_text(encoded, encoding="utf-8")
        return path

    def test_migration_is_transactional_idempotent_and_retains_source(self):
        source = self._write("outbox", self._legacy_event())
        store = SQLiteStore(self.target)
        first = store.import_legacy(self.legacy, offline=True)
        second = store.import_legacy(self.legacy, offline=True)
        self.assertFalse(first["already_imported"])
        self.assertTrue(second["already_imported"])
        self.assertEqual(1, store.count_pending())
        self.assertTrue(source.exists())

    def test_changed_completed_source_is_rejected(self):
        path = self._write("outbox", self._legacy_event())
        store = SQLiteStore(self.target)
        store.import_legacy(self.legacy, offline=True)
        path.write_text(
            json.dumps(self._legacy_event(2)) + "\n", encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "changed"):
            store.import_legacy(self.legacy, offline=True)

    def test_torn_tail_is_rejected_before_target_changes(self):
        self._write("outbox", self._legacy_event(), newline=False)
        store = SQLiteStore(self.target)
        with self.assertRaisesRegex(ValueError, "torn tail"):
            store.import_legacy(self.legacy, offline=True)
        self.assertEqual(0, store.status()["pending"])

    def test_pending_and_acked_role_conflict_is_rejected(self):
        event = self._legacy_event()
        self._write("outbox", event)
        self._write("archive", event)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            SQLiteStore(self.target).import_legacy(self.legacy, offline=True)

    @unittest.skipUnless(os.name == "nt", "Windows-only offline assertion")
    def test_windows_requires_explicit_offline_source(self):
        self._write("outbox", self._legacy_event())
        with self.assertRaisesRegex(RuntimeError, "offline"):
            SQLiteStore(self.target).import_legacy(self.legacy)


if __name__ == "__main__":
    unittest.main()
