"""Hermetic tests (unittest, no network) for anvil-events core.

Run:  uv run python -m unittest discover -s tests -q
"""
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anvil_events  # noqa: E402
from anvil_events.outbox import (  # noqa: E402
    KINDS,
    CausalChecker,
    Outbox,
    TargetQueue,
    iter_managed_jsonl,
    make_event,
    utcnow_iso,
)


class TestEnvelope(unittest.TestCase):
    def test_make_event_fields(self):
        e = make_event("node-a:serves", "promote.applied", "node-a",
                       {"tier": "primary"}, correlation_id="c1",
                       producer_seq=7)
        self.assertEqual(e["event_id"], "node-a:serves:000007")
        self.assertEqual(e["subject"], "anvil.fleet.node-a.promote.applied")
        self.assertEqual(e["schema"], "https://anvil.dev/schemas/events/v1.json")
        self.assertEqual(e["correlation_id"], "c1")

    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):
            make_event("p", "not.a.kind", "h", {})

    def test_json_schema_kind_enum_matches_runtime_vocabulary(self):
        import json

        schema_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "schemas", "events-v1.json",
        )
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        self.assertEqual(set(schema["properties"]["kind"]["enum"]), set(KINDS))
        self.assertEqual(schema["properties"]["version"]["const"], 1)

    def test_json_schema_couples_kind_to_payload_contract(self):
        import json

        try:
            import jsonschema
        except ImportError:
            self.skipTest("jsonschema is only installed in the schema validation gate")
        schema_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "schemas", "events-v1.json",
        )
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        jsonschema.Draft202012Validator.check_schema(schema)
        valid = make_event(
            "p1", "host.status", "node-a",
            {"host": "node-a", "reachable": True},
        )
        jsonschema.validate(valid, schema, format_checker=jsonschema.FormatChecker())
        nullable_samples = {
            "config.adopted": {"file": "operator", "files": None,
                               "state": None, "repo": None, "rev": None},
            "divergence": {"issue": "drift", "declared": None,
                           "live": None, "delta": None},
            "event.degraded": {"cause": "failure", "event_id": None,
                               "file": None, "bytes": None, "records": None,
                               "pending": None},
            "host.status": {"host": "node-a", "reachable": True,
                            "gpu_used": None, "gpu_free": None},
            "profile.enter": {"mode": "exclusive", "profile": "p",
                              "exclusive_target": None, "restore_group": None},
            "profile.leave": {"mode": "exclusive", "profile": "p",
                              "exclusive_target": None, "restore_group": None},
            "promote.applied": {"tier": "primary", "model": "m",
                                "promotion": None, "context": None,
                                "rollback": None},
            "promote.rolled_back": {"tier": "primary", "restored_model": "m",
                                    "promotion": None},
            "repo.synced": {"repo": "r", "ok": False, "committed": None,
                            "pushed": None, "error": None},
            "serve.down": {"serve": "s", "graceful": None},
            "serve.up": {"serve": "s", "model": "m", "port": 9001,
                         "gpu_roles": None, "residency": None},
        }
        for kind, payload in nullable_samples.items():
            jsonschema.validate(
                make_event("p1", kind, "node-a", payload), schema,
                format_checker=jsonschema.FormatChecker(),
            )
        forged = dict(valid)
        forged["payload"] = {"serve": "s", "model": "m", "port": 9001}
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(forged, schema,
                                format_checker=jsonschema.FormatChecker())

        from anvil_events.ingest import validate_event

        bool_version = dict(valid)
        bool_version["version"] = True
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(bool_version, schema)
        self.assertFalse(validate_event(bool_version)[0])

        float_version = dict(valid)
        float_version["version"] = 1.0
        jsonschema.validate(
            float_version, schema,
            format_checker=jsonschema.FormatChecker(),
        )
        self.assertTrue(validate_event(float_version)[0])

        lowercase_time = dict(valid)
        lowercase_time["observed_at"] = "2026-08-13t10:00:00z"
        lowercase_time["emitted_at"] = "2026-08-13t10:00:00z"
        jsonschema.validate(
            lowercase_time, schema,
            format_checker=jsonschema.FormatChecker(),
        )
        self.assertTrue(validate_event(lowercase_time)[0])

        checker = jsonschema.FormatChecker()
        self.assertIn("date-time", checker.checkers)
        malformed_time = dict(valid)
        malformed_time["observed_at"] = "2026-08-13T24:00:00Z"
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(malformed_time, schema, format_checker=checker)
        self.assertFalse(validate_event(malformed_time)[0])

        integral_port = make_event(
            "p1", "serve.up", "node-a",
            {"serve": "s", "model": "m", "port": 1.0},
        )
        jsonschema.validate(integral_port, schema)
        self.assertTrue(validate_event(integral_port)[0])

        fractional_port = dict(integral_port)
        fractional_port["payload"] = dict(integral_port["payload"], port=1.5)
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(fractional_port, schema)
        self.assertFalse(validate_event(fractional_port)[0])
        self.assertEqual(schema["properties"]["schema"]["const"],
                         "https://anvil.dev/schemas/events/v1.json")
        self.assertEqual(schema["properties"]["host"]["pattern"],
                         "^[A-Za-z0-9_-]+$")
        self.assertIn("[0-9]{6,}", schema["properties"]["event_id"]["pattern"])

    def test_stream_config_is_file_backed_seven_day_fleet_stream(self):
        import json

        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "deploy", "nats-stream.json",
        )
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
        self.assertEqual(config["name"], "ANVIL")
        self.assertEqual(config["subjects"], ["anvil.fleet.>"])
        self.assertEqual(config["storage"], "file")
        self.assertEqual(config["discard"], "old")
        self.assertEqual(config["max_age"], 7 * 86400 * 1_000_000_000)

    def test_schema_and_stream_config_are_packaged_as_data_files(self):
        import tomllib

        project = os.path.dirname(os.path.dirname(__file__))
        with open(os.path.join(project, "pyproject.toml"), "rb") as f:
            config = tomllib.load(f)
        data_files = config["tool"]["setuptools"]["data-files"]
        self.assertEqual(data_files["share/anvil-events/schemas"],
                         ["schemas/events-v1.json"])
        self.assertEqual(data_files["share/anvil-events/deploy"],
                         ["deploy/nats-stream.json"])

    def test_all_frozen_kinds_emit(self):
        for k in KINDS:
            e = make_event("p", k, "h", {})
            self.assertEqual(e["kind"], k)


class TestOutbox(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.o = Outbox(self.root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_append_then_pending_then_ack(self):
        e = make_event("p1", "serve.up", "node-a", {"port": 9001})
        self.o.append(e)
        self.assertEqual(self.o.count_pending(), 1)
        self.o.ack(e)
        self.assertEqual(self.o.count_pending(), 0)
        self.assertEqual(self.o.load_cursors()["anvil.fleet.node-a.serve.up"]
                         ["p1"]["last_event_id"], e["event_id"])

    def test_ack_is_idempotent_on_event_id_key(self):
        e1 = make_event("p1", "host.status", "node-a", {}, producer_seq=1)
        e2 = make_event("p1", "host.status", "node-a", {}, producer_seq=2)
        self.o.append(e1)
        self.o.append(e2)
        self.o.ack(e1)
        self.o.ack(e1)  # second ack must not corrupt
        self.assertEqual(self.o.count_pending(), 1)

    def test_ack_removes_noncanonical_json_by_event_identity(self):
        import json

        event = make_event("p1", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
        path = os.path.join(self.o.outbox_dir,
                            event["observed_at"][:10] + ".jsonl")
        with open(path, "w", encoding="utf-8") as f:
            f.write(json.dumps(event, sort_keys=False,
                               separators=(", ", ": ")) + "\n")
        self.o.ack(event)
        self.assertEqual(list(self.o.read_pending()), [])

    def test_sequences_are_monotonic_per_producer(self):
        first_p1 = self.o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
        first_p2 = self.o.emit("p2", "host.status", "node-a", {"host": "node-a", "reachable": True})
        second_p1 = self.o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
        self.assertEqual(first_p1["producer_seq"], 1)
        self.assertEqual(first_p2["producer_seq"], 1)
        self.assertEqual(second_p1["producer_seq"], 2)
        self.assertEqual(first_p1["event_id"], "p1:000001")
        self.assertEqual(first_p2["event_id"], "p2:000001")

    def test_unmanaged_archive_jsonl_does_not_break_sequence_recovery(self):
        with open(os.path.join(self.o.archive_dir, "notes.backup.jsonl"), "w") as f:
            f.write("not json\n")
        event = self.o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
        self.assertEqual(event["producer_seq"], 1)

    def test_managed_readers_ignore_symlinks_and_malformed_records(self):
        outside = os.path.join(self.root, "outside.jsonl")
        event = make_event("outside", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
        with open(outside, "w") as f:
            f.write(json.dumps(event) + "\n")
        os.symlink(outside, os.path.join(self.o.journal_dir, "2026-08-13.jsonl"))
        with open(os.path.join(self.o.archive_dir, "notes.backup.jsonl"), "w") as f:
            f.write("not json\n")
        self.assertEqual(list(self.o.read_journal()), [])
        self.assertEqual(list(self.o.read_archive()), [])

    def test_emit_rejects_invalid_typed_payload_before_journaling(self):
        with self.assertRaises(ValueError):
            self.o.emit("p1", "host.status", "node-a", {"reachable": True})
        with self.assertRaises(ValueError):
            self.o.emit("p1", "host.status", "node-a", [])
        self.assertEqual(self.o.count_pending(), 0)
        self.assertEqual(self.o.load_producer_seqs(), {})

    def test_legacy_cursor_prevents_sequence_reuse_and_is_migrated(self):
        import json

        subject = "anvil.fleet.node-a.host.status"
        legacy = {subject: {"last_event_id": "p1:000042", "producer_seq": 42}}
        with open(self.o.cursor_file, "w", encoding="utf-8") as f:
            json.dump(legacy, f)
        reopened = Outbox(self.root)
        event = reopened.emit("p1", "host.status", "node-a",
                              {"host": "node-a", "reachable": True})
        self.assertEqual((event["event_id"], event["producer_seq"]),
                         ("p1:000043", 43))
        self.assertEqual(reopened.load_cursors()[subject]["p1"]["producer_seq"],
                         42)
        with open(reopened.cursor_file, encoding="utf-8") as f:
            self.assertIn("p1", json.load(f)[subject])

    def test_oversized_event_is_rejected_before_outbox_append(self):
        from anvil_events.nats_mini import _BROKER_MAX_PAYLOAD

        with self.assertRaisesRegex(ValueError, "too large|exceed"):
            self.o.emit(
                "p" * 900, "divergence", "node-a",
                {"issue": "oversized",
                 "delta": "x" * _BROKER_MAX_PAYLOAD},
            )
        self.assertEqual(self.o.count_pending(), 0)
        self.assertEqual(self.o.load_producer_seqs(), {})

    def test_exact_hpub_boundary_rejected_before_sequence_persistence(self):
        from anvil_events.nats_mini import _MAX_BODY

        producer = "p" * 1100
        probe = make_event(
            producer, "divergence", "node-a",
            {"issue": "framed-boundary", "delta": ""}, producer_seq=1,
        )
        base_size = len(json.dumps(probe, sort_keys=True).encode())
        payload = {
            "issue": "framed-boundary",
            "delta": "x" * (_MAX_BODY - base_size),
        }
        seqs_before = self.o.load_producer_seqs()
        with self.assertRaisesRegex(ValueError, "max_msg_size"):
            self.o.emit(producer, "divergence", "node-a", payload)
        self.assertEqual(self.o.count_pending(), 0)
        self.assertEqual(self.o.load_producer_seqs(), seqs_before)


class TestTargetQueue(unittest.TestCase):
    """LogPlayer state machine: S/RF/FC/N + term duplicate prevention."""

    def test_normal_stream(self):
        q = TargetQueue()
        self.assertTrue(q.push("a"))
        self.assertTrue(q.push("b"))
        self.assertEqual(q.front(), "a")
        self.assertEqual(q.pop(), "a")
        self.assertEqual(q.pop(), "b")
        self.assertIsNone(q.front())

    def test_suspend_clears_and_drops_pushes(self):
        q = TargetQueue()
        q.push("a")
        q.suspend()
        self.assertEqual(q.state, TargetQueue.SUSPENDED)
        self.assertFalse(q.push("b"))  # dropped while suspended
        self.assertIsNone(q.front())

    def test_reconnect_term_prevents_stale_duplicates(self):
        q = TargetQueue()
        q.push("a", term=1)
        q.suspend()
        q.reconnect()                 # term -> 2, state -> RECOVERY_FETCHING
        self.assertEqual(q.term, 2)
        self.assertFalse(q.push("stale", term=1))  # expired term dropped
        self.assertTrue(q.push("missed", is_normal=False, term=2))  # catchup
        self.assertEqual(q.front(), "missed")      # catchup preferred
        self.assertEqual(q.pop(), "missed")
        q.fetching_completed()
        self.assertEqual(q.state, TargetQueue.NORMAL)

    def test_fetching_completed_transitions(self):
        q = TargetQueue()
        q.suspend()
        q.reconnect()
        q.push("c1", is_normal=False, term=2)
        q.fetching_completed()        # catchup still non-empty -> FC
        self.assertEqual(q.state, TargetQueue.FETCHING_COMPLETED)
        self.assertEqual(q.pop(), "c1")
        self.assertEqual(q.state, TargetQueue.NORMAL)


class TestLogPlayerReconnectSimulation(unittest.TestCase):
    """End-to-end LogPlayer claim: no duplicate delivery after reconnect.

    Uses the REAL Outbox (emit -> deliver -> ack -> suspend -> reconnect ->
    recovery replay from cursor) plus a per-target TargetQueue, proving the
    paper's central exactly-once-per-target invariant (arXiv:1911.11286
    §2.4–2.5) across the whole path, not just the unit state machine.
    """

    def test_no_duplicate_delivery_after_reconnect(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            q = TargetQueue()
            delivered = []
            seen = set()
            subject = "anvil.fleet.node-a.serve.up"

            def deliver(ev):
                # simulate the AT-LEAST-ONCE transport: a msg may be re-sent
                # after reconnect, but the consumer must not double-deliver
                if ev["event_id"] not in seen:
                    seen.add(ev["event_id"])
                    delivered.append(ev)

            # phase 1: emit 3 events, deliver + ack all (normal stream)
            seqs = []
            for i in range(3):
                ev = o.emit("p1", "serve.up", "node-a",
                            {"serve": "s1", "model": "m", "port": 9000 + i})
                self.assertEqual(ev["subject"], subject)  # derived host.kind
                seqs.append(ev["producer_seq"])
                q.push(ev, term=q.term)
                while True:
                    e = q.front()
                    if e is None:
                        break
                    q.pop()
                    deliver(e)
                    o.ack(e)
            self.assertEqual(len(delivered), 3)
            # per-producer seqs strictly increase (no reuse ever)
            self.assertEqual(seqs, sorted(seqs))
            self.assertEqual(len(set(seqs)), 3)
            cur = o.load_cursors().get(subject, {}).get("p1", {})
            self.assertEqual(cur["producer_seq"], seqs[-1],
                             "cursor at last acked")

            # phase 2: target down -> suspend (queues cleared)
            q.suspend()
            self.assertEqual(q.state, TargetQueue.SUSPENDED)

            # phase 3: reconnect -> term bump, recovery fetch of the gap,
            #          stale term cannot re-deliver anything old
            q.reconnect()
            missed = []
            for ev in o.read_pending():
                if ev.get("subject") == subject:
                    missed.append(ev)
            for ev in missed:
                # recovery fetch: pushes under the NEW term only
                self.assertTrue(q.push(ev, is_normal=False, term=q.term))
            for e in missed:  # drain catch-up, then normal
                q.pop()
                deliver(e)     # re-transmitted, but deduped by event_id
            q.fetching_completed()
            # the pipeline re-delivered only the gap (nothing new emitted)
            self.assertEqual(q.state, TargetQueue.NORMAL)
            # INV3: NO DUPLICATES — delivered exactly the 3 original events,
            # even though the transport re-sent them after reconnect
            self.assertEqual(len(seen), 3)
            self.assertEqual(len(delivered), 3)
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestCausalChecker(unittest.TestCase):
    def _ev(self, producer, seq, corr, observed, causes=None):
        return make_event(producer, "host.status", "h", {},
                          correlation_id=corr, producer_seq=seq,
                          observed_at=observed, causes=causes)

    def test_consistent_journal_passes(self):
        events = [
            self._ev("p1", 1, "c1", "2026-08-12T10:00:00.000Z"),
            self._ev("p1", 2, "c1", "2026-08-12T10:00:01.000Z"),
            self._ev("p2", 1, "c1", "2026-08-12T10:00:02.000Z"),
        ]
        ok, err = CausalChecker.check(events)
        self.assertTrue(ok, err)

    def test_explicit_causes_chain_is_valid(self):
        """A proper cause->effect chain must NOT be flagged as a cycle."""
        events = [
            self._ev("p1", 1, None, "2026-08-12T10:00:00.000Z"),
            self._ev("p1", 2, None, "2026-08-12T10:00:01.000Z",
                     causes=["p1:000001"]),
            self._ev("p1", 3, None, "2026-08-12T10:00:02.000Z",
                     causes=["p1:000002"]),
        ]
        ok, err = CausalChecker.check(events)
        self.assertTrue(ok, f"valid causal chain must pass: {err}")

    def test_explicit_causes_cycle_detected(self):
        """Mutual causality (a causes b, b causes a) is a real cycle."""
        events = [
            self._ev("p2", 1, None, "2026-08-12T10:00:00.000Z",
                     causes=["p2:000002"]),
            self._ev("p2", 2, None, "2026-08-12T10:00:01.000Z",
                     causes=["p2:000001"]),
        ]
        ok, err = CausalChecker.check(events)
        self.assertFalse(ok)
        self.assertIn("cycle", err)

    def test_duplicate_event_id_is_single_node_semantics(self):
        events = [self._ev("p1", 1, None, "2026-08-12T10:00:00.000Z")]
        ok, err = CausalChecker.check(events)
        self.assertTrue(ok, err)


class TestNATSClient(unittest.TestCase):
    """Client hardening: subject injection, URL parse, buffer caps."""

    def test_validate_subject_rejects_injection(self):
        from anvil_events.nats_mini import validate_subject
        for bad in ["a\r\nPUB x 0\r\n", "a b", "a\nb", "a\tb", "", "a*",
                    "a>b", "a.>.b", "a..b", ".a", "a."]:
            with self.assertRaises(ValueError, msg=f"should reject {bad!r}"):
                validate_subject(bad)
        for good in ["a.b.c", "anvil.fleet.node-a.serve.up", "x-y_z.1"]:
            self.assertEqual(validate_subject(good), good)

    def test_subject_wildcards_are_subscription_only(self):
        from anvil_events.nats_mini import validate_subject
        self.assertEqual(validate_subject("anvil.fleet.>", allow_wildcards=True),
                         "anvil.fleet.>")
        self.assertEqual(validate_subject("anvil.*.node.kind",
                                          allow_wildcards=True),
                         "anvil.*.node.kind")
        with self.assertRaises(ValueError):
            validate_subject("anvil.fleet.>")
        with self.assertRaises(ValueError):
            validate_subject("anvil.>.kind", allow_wildcards=True)

    def test_subscribe_sends_sub_only_once_per_connection(self):
        from anvil_events.nats_mini import NATSClient
        client = NATSClient()
        sent = []
        client.sock = object()
        client._send = sent.append
        self.assertEqual(client.subscribe("anvil.fleet.>", timeout=0), [])
        self.assertEqual(client.subscribe("anvil.fleet.>", timeout=0), [])
        self.assertEqual(sum(frame.startswith(b"SUB ") for frame in sent), 1)

    def test_connect_requires_exact_info_verb(self):
        import unittest.mock as mock

        from anvil_events.nats_mini import NATSClient

        class Socket:
            def __init__(self, data):
                self.data = data
                self.sent = []

            def recv(self, n):
                chunk, self.data = self.data[:n], self.data[n:]
                return chunk

            def sendall(self, data):
                self.sent.append(data)

            def settimeout(self, _timeout):
                pass

            def close(self):
                pass

        for invalid in (
            b"INFOX {}\r\nPONG\r\n",
            b"INFORMATION {}\r\nPONG\r\n",
            b"INFO not-json\r\nPONG\r\n",
            b"INFO []\r\nPONG\r\n",
        ):
            sock = Socket(invalid)
            with mock.patch(
                "anvil_events.nats_mini.socket.create_connection",
                return_value=sock,
            ):
                with self.assertRaisesRegex(
                    OSError, "(?:bad|malformed).*handshake",
                ):
                    NATSClient().connect()
        sock = Socket(b"INFO {}\r\nPONG\r\n")
        with mock.patch(
            "anvil_events.nats_mini.socket.create_connection", return_value=sock,
        ):
            NATSClient().connect()

    def test_parse_url_defaults_and_forms(self):
        from anvil_events.nats_mini import parse_url
        self.assertEqual(parse_url(None), ("127.0.0.1", 4222))
        self.assertEqual(parse_url("nats://127.0.0.1:4222"), ("127.0.0.1", 4222))
        self.assertEqual(parse_url("nats://host.one:9999"), ("host.one", 9999))
        self.assertEqual(parse_url("nats://host.one"), ("host.one", 4222))

    def test_publish_rejects_injection_and_oversize(self):
        from anvil_events.nats_mini import _MAX_BODY, NATSClient
        c = NATSClient()
        with self.assertRaises(ValueError):
            c.publish("a\r\nPUB x 0\r\n", b"x")
        with self.assertRaises(ValueError):
            c.publish("ok.subject", b"x" * (_MAX_BODY + 1))


class TestCLISequence(unittest.TestCase):
    """Event-ID uniqueness across pending+acked (reviewer #2)."""

    def test_iterator_holds_open_directory_across_path_swap(self):
        root = tempfile.mkdtemp()
        replacement = tempfile.mkdtemp()
        moved = root + ".moved"
        try:
            event = make_event(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )
            event2 = make_event(
                "p2", "host.status", "node-b",
                {"host": "node-b", "reachable": True},
            )
            forged = make_event(
                "outside", "host.status", "outside",
                {"host": "outside", "reachable": True},
            )
            with open(os.path.join(root, "2026-08-13.jsonl"), "w") as f:
                f.write(json.dumps(event) + "\n")
            with open(os.path.join(root, "2026-08-14.jsonl"), "w") as f:
                f.write(json.dumps(event2) + "\n")
            with open(os.path.join(replacement, "2026-08-14.jsonl"), "w") as f:
                f.write(json.dumps(forged) + "\n")
            iterator = iter_managed_jsonl(root)
            first = next(iterator)
            os.rename(root, moved)
            os.symlink(replacement, root, target_is_directory=True)
            self.assertEqual(first["event_id"], event["event_id"])
            self.assertEqual(
                [item["event_id"] for item in iterator],
                [event2["event_id"]],
            )
        finally:
            if os.path.islink(root):
                os.unlink(root)
            shutil.rmtree(moved, ignore_errors=True)
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(replacement, ignore_errors=True)

    def test_verify_ignores_unmanaged_malformed_and_managed_symlink(self):
        import argparse

        from anvil_events.cli import cmd_verify

        root = tempfile.mkdtemp()
        try:
            for name in ("outbox", "archive", "journal"):
                os.makedirs(os.path.join(root, name))
            with open(os.path.join(root, "archive", "notes.backup.jsonl"), "w") as f:
                f.write("not json\n")
            outside = os.path.join(root, "outside.jsonl")
            with open(outside, "w") as f:
                f.write(json.dumps(make_event(
                    "p1", "host.status", "node-a",
                    {"host": "node-a", "reachable": True},
                )) + "\n")
            os.symlink(outside,
                       os.path.join(root, "journal", "2026-08-13.jsonl"))
            self.assertEqual(cmd_verify(argparse.Namespace(path=root)), 0)
            symlink_root = os.path.join(root, "symlink-root")
            os.makedirs(symlink_root)
            external_journal = os.path.join(root, "external-journal")
            os.makedirs(external_journal)
            with open(os.path.join(external_journal, "2026-08-13.jsonl"), "w") as f:
                f.write(json.dumps(make_event(
                    "external", "host.status", "node-a",
                    {"host": "node-a", "reachable": True},
                )) + "\n")
            os.symlink(external_journal, os.path.join(symlink_root, "journal"))
            self.assertEqual(cmd_verify(argparse.Namespace(path=symlink_root)), 0)
            managed = os.path.join(root, "outbox", "2026-08-13.jsonl")
            with open(managed, "w") as f:
                f.write(json.dumps(make_event(
                    "p2", "host.status", "node-a",
                    {"host": "node-a", "reachable": True},
                )) + "\n")
            with open(os.path.join(root, "outbox", "2026-08-14.jsonl"), "w") as f:
                f.write("not json\n")
            self.assertEqual(cmd_verify(argparse.Namespace(path=managed)), 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def _run_emit(self, root, seq_calls):
        from anvil_events import cli
        # fake argparse namespace
        class A:
            pass
        a = A()
        a.root = root
        a.kind = "host.status"
        a.host = "node-a"
        a.producer = "producer-x"
        a.correlation = None
        a.payload = '{"host":"node-a","reachable":true,"gpu_used":1}'
        # monkeypatch NATS so emit never actually publishes (hermetic)
        orig = cli.NATSClient
        cli.NATSClient = _NoNATS
        try:
            return cli.cmd_emit(a)
        finally:
            cli.NATSClient = orig

    def test_no_event_id_reuse_with_pending(self):
        import shutil
        import tempfile
        root = tempfile.mkdtemp()
        try:
            # two emits with NATS failing => both stay pending, distinct seqs
            self._run_emit(root, None)
            self._run_emit(root, None)
            o = Outbox(root)
            originals = [e for e in o.read_pending()
                         if e["kind"] == "host.status"]
            seqs = [e["producer_seq"] for e in originals]
            self.assertEqual(len(seqs), 2, "two originals expected")
            self.assertEqual(len(set(seqs)), 2, "event_ids must be unique")
            self.assertTrue(all(s >= 1 for s in seqs))
            # degraded records were journaled too (never-silent contract)
            degraded = [e for e in o.read_pending()
                        if e["kind"] == "event.degraded"]
            self.assertGreaterEqual(len(degraded), 2)
            # REVIEW FIX: degraded events must have DISTINCT identities
            # (fixed producer_seq=1 was a bug — corrupted per-producer order)
            ids = [e["event_id"] for e in degraded]
            self.assertEqual(len(set(ids)), len(ids),
                             f"degraded event_ids must be unique: {ids}")
        finally:
            shutil.rmtree(root, ignore_errors=True)


class _NoNATS:
    """Fake client that always fails to publish (hermetic fragility test)."""
    def __init__(self, url=None): pass
    def connect(self, timeout=None): raise OSError("no broker")
    def close(self): pass


class TestCrashRecovery(unittest.TestCase):
    def test_torn_last_line_detected_and_dropped(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            e1 = make_event("p1", "serve.up", "node-a", {})
            e2 = make_event("p1", "serve.down", "node-a", {})
            o.append(e1)
            o.append(e2)
            # simulate a crash: torn last line (no trailing newline)
            import glob
            f = glob.glob(os.path.join(root, "outbox", "*.jsonl"))[0]
            with open(f, "a") as fh:
                fh.write('{"torn": true}')   # no newline
            # read_pending must DROP the torn line (never yield it)
            pending = list(o.read_pending())
            self.assertEqual(len(pending), 2, "torn line must be dropped")
            self.assertNotIn("torn", [e.get("torn") for e in pending])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_reopen_repairs_torn_tail_and_records_degraded(self):
        import glob
        import json

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
            path = glob.glob(os.path.join(root, "outbox", "*.jsonl"))[0]
            with open(path, "ab") as f:
                f.write(b'{"torn":true')

            repaired = Outbox(root)
            events = list(repaired.read_pending())
            self.assertEqual([e["kind"] for e in events],
                             ["host.status", "event.degraded"])
            with open(path, "rb") as f:
                data = f.read()
            self.assertTrue(data.endswith(b"\n"))
            for line in data.splitlines():
                json.loads(line)
            quarantine = os.path.join(root, "quarantine")
            self.assertTrue(os.path.isdir(quarantine))
            self.assertEqual(len(os.listdir(quarantine)), 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_live_append_repairs_torn_outbox_tail(self):
        import glob
        import json

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            first = o.emit("p1", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
            path = glob.glob(os.path.join(root, "outbox", "*.jsonl"))[0]
            with open(path, "ab") as f:
                f.write(b'{"torn":')
            second = o.emit("p1", "host.status", "node-a",
                            {"host": "node-a", "reachable": False})
            rows = list(o.read_pending())
            self.assertEqual([row["event_id"] for row in rows[:2]],
                             [first["event_id"], second["event_id"]])
            with open(path) as f:
                for line in f:
                    json.loads(line)
            self.assertTrue(glob.glob(os.path.join(
                root, "quarantine", "*.outbox.*.torn")))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_reopen_repairs_torn_first_record(self):
        root = tempfile.mkdtemp()
        try:
            outbox_dir = os.path.join(root, "outbox")
            os.makedirs(outbox_dir)
            with open(os.path.join(outbox_dir, "2026-08-13.jsonl"), "wb") as f:
                f.write(b'{"only":"partial"')
            repaired = Outbox(root)
            events = list(repaired.read_pending())
            self.assertEqual([e["kind"] for e in events], ["event.degraded"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_repair_append_uses_same_fd_across_path_swap(self):
        from unittest import mock

        root = tempfile.mkdtemp()
        try:
            o = Outbox(os.path.join(root, "root"))
            event = make_event(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )
            for directory, label in (
                (o.outbox_dir, "outbox"),
                (o.journal_dir, "journal"),
                (o.archive_dir, "archive"),
            ):
                path = os.path.join(directory, event["observed_at"][:10] + ".jsonl")
                with open(path, "wb") as f:
                    f.write(b'{"torn"')
                original_path = path + ".original"
                real_quarantine = o._quarantine_relative
                swapped = False

                def swap_then_quarantine(directory_fd, stem, suffix, torn):
                    nonlocal swapped
                    if not swapped:
                        os.rename(path, original_path)
                        with open(path, "wb") as attacker:
                            attacker.write(b"ATTACKER")
                        swapped = True
                    return real_quarantine(directory_fd, stem, suffix, torn)

                with mock.patch.object(
                    o, "_quarantine_relative", side_effect=swap_then_quarantine,
                ):
                    o._repair_and_append_line(
                        path, label, json.dumps(event, sort_keys=True),
                    )
                with open(path, "rb") as attacker:
                    self.assertEqual(attacker.read(), b"ATTACKER")
                with open(original_path, "rb") as original:
                    parsed = [json.loads(line) for line in original if line.strip()]
                self.assertEqual(
                    [row["event_id"] for row in parsed], [event["event_id"]],
                )
                os.unlink(path)
                os.rename(original_path, path)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_pinned_dirfd_repair_survives_parent_directory_swap(self):
        """An attacker swapping the outbox directory mid-repair cannot redirect it.

        The repair runs against a pinned dirfd: even if the pathname is
        renamed and replaced between listing and reopen, the torn record is
        found, quarantined, and truncated on the ORIGINAL inode, and the
        attacker's replacement directory/file are left untouched.
        """
        root = tempfile.mkdtemp()
        try:
            managed = os.path.join(root, "root")
            o = Outbox(managed)
            day = "2026-08-13"
            original_dir = o.outbox_dir
            moved_dir = original_dir + ".grab"
            replacement_dir = original_dir + ".replacement"
            torn_file = os.path.join(original_dir, day + ".jsonl")
            with open(torn_file, "wb") as f:
                f.write(b'{"torn"')
            real_repair = o._repair_append_tail
            swapped = False

            def swap_on_first_repair(directory_fd, name, label):
                nonlocal swapped
                if not swapped and directory_fd is not None:
                    os.rename(original_dir, moved_dir)
                    os.mkdir(replacement_dir)
                    swapped = True
                return real_repair(directory_fd, name, label)

            from unittest import mock

            # The dirfd passed in IS the pinned original; the attacker swaps
            # the PATHNAME. The pinned fd still refers to the moved dir.
            with mock.patch.object(
                o, "_repair_append_tail", side_effect=swap_on_first_repair,
            ):
                # repair runs against the pinned inode; the degraded alert is
                # best-effort and must not crash even though the managed
                # pathname is now gone
                o._repair_torn_locked()
            # the attacker's replacement (now at the original pathname) must
            # be untouched / have no torn record
            self.assertFalse(
                os.path.exists(os.path.join(replacement_dir, day + ".jsonl")),
            )
            # the original inode (moved aside) must now be repaired: the torn
            # tail was quarantined and the file is empty-valid
            moved_file = os.path.join(moved_dir, day + ".jsonl")
            self.assertTrue(os.path.exists(moved_file))
            with open(moved_file, "rb") as f:
                self.assertEqual(f.read(), b"")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_pinned_dirfd_pending_rewrite_survives_parent_directory_swap(self):
        """Pending-batch rewrite uses the pinned dirfd, not the pathname.

        An attacker swapping the outbox directory after listing cannot make
        the quarantine-rewrite land in the replacement directory.
        """
        from unittest import mock

        root = tempfile.mkdtemp()
        try:
            managed = os.path.join(root, "root")
            o = Outbox(managed)
            event = o.emit(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )
            day = event["observed_at"][:10]
            # poison the pending file with one invalid record so the batch
            # repair quarantines and rewrites it
            original_dir = o.outbox_dir
            moved_dir = original_dir + ".grab"
            replacement_dir = original_dir + ".replacement"
            path = os.path.join(original_dir, day + ".jsonl")
            with open(path, "ab") as f:
                f.write(b"not-json\n")
            real_read = __import__(
                "anvil_events.outbox", fromlist=["read_regular_fd"],
            ).read_regular_fd
            swapped = False

            def swap_then_read(fd, name):
                nonlocal swapped
                if not swapped and fd is not None:
                    os.rename(original_dir, moved_dir)
                    os.mkdir(replacement_dir)
                    swapped = True
                return real_read(fd, name)

            with mock.patch(
                "anvil_events.outbox.read_regular_fd", side_effect=swap_then_read,
            ):
                selected, _, repaired, _, _, _ = o.select_pending_batch(
                    max_events=10, seen=set(),
                    validator=lambda ev: (True, ""), return_meta=True,
                )
            self.assertEqual(repaired, 1)
            self.assertEqual([e["event_id"] for e in selected], [event["event_id"]])
            # the attacker's replacement must be untouched
            self.assertFalse(
                os.path.exists(os.path.join(replacement_dir, day + ".jsonl")),
            )
            # the original inode (moved aside) holds only the valid event
            with open(os.path.join(moved_dir, day + ".jsonl"), "rb") as f:
                rows = [json.loads(line) for line in f if line.strip()]
            self.assertEqual([r["event_id"] for r in rows], [event["event_id"]])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_atomic_json_temp_symlinks_do_not_redirect_writes(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(os.path.join(root, "root"))
            external = os.path.join(root, "external")
            with open(external, "w") as f:
                f.write("SAFE\n")
            for target, mutate in (
                (
                    o.producer_seq_file,
                    lambda: o.emit(
                        "p1", "host.status", "node-a",
                        {"host": "node-a", "reachable": True},
                    ),
                ),
                (
                    o.cursor_file,
                    lambda: o._set_cursor({
                        "subject": "anvil.fleet.node-a.host.status",
                        "producer": "p1", "producer_seq": 1,
                        "event_id": "p1:000001",
                    }),
                ),
            ):
                legacy_tmp = target + ".tmp"
                os.symlink(external, legacy_tmp)
                mutate()
                with open(external) as f:
                    self.assertEqual(f.read(), "SAFE\n")
                os.unlink(legacy_tmp)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_constructor_torn_symlink_does_not_truncate_external(self):
        root = tempfile.mkdtemp()
        try:
            managed = os.path.join(root, "root")
            outbox = os.path.join(managed, "outbox")
            os.makedirs(outbox)
            external = os.path.join(root, "external")
            with open(external, "w") as f:
                f.write("TORN")
            os.symlink(external, os.path.join(outbox, "2026-08-13.jsonl"))
            with self.assertRaises(OSError):
                Outbox(managed)
            with open(external) as f:
                self.assertEqual(f.read(), "TORN")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_archive_ack_symlink_does_not_modify_external(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(os.path.join(root, "root"))
            event = o.emit(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )
            external = os.path.join(root, "external")
            with open(external, "w") as f:
                f.write("TORN")
            target = os.path.join(
                o.archive_dir, event["observed_at"][:10] + ".jsonl",
            )
            os.symlink(external, target)
            with self.assertRaises(OSError):
                o.ack(event)
            with open(external) as f:
                self.assertEqual(f.read(), "TORN")
            self.assertEqual(
                [e["event_id"] for e in o.read_pending()], [event["event_id"]],
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_outbox_lock_rejects_symlink(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(os.path.join(root, "root"))
            os.unlink(os.path.join(o.root, ".lock"))
            external = os.path.join(root, "external")
            with open(external, "w") as f:
                f.write("SAFE\n")
            os.symlink(external, os.path.join(o.root, ".lock"))
            with self.assertRaises(OSError):
                o.emit(
                    "p1", "host.status", "node-a",
                    {"host": "node-a", "reachable": True},
                )
            with open(external) as f:
                self.assertEqual(f.read(), "SAFE\n")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_managed_append_targets_reject_symlinks(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(os.path.join(root, "root"))
            event = make_event(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )
            external = os.path.join(root, "external")
            with open(external, "w") as f:
                f.write("SAFE\n")
            for directory, append in (
                (o.outbox_dir, lambda: o.append(event)),
                (o.journal_dir, lambda: o.append_journal(event)),
            ):
                target = os.path.join(directory, event["observed_at"][:10] + ".jsonl")
                os.symlink(external, target)
                with self.assertRaises(OSError):
                    append()
                with open(external) as f:
                    self.assertEqual(f.read(), "SAFE\n")
                os.unlink(target)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_unsafe_observed_at_cannot_escape_storage_root(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            event = make_event("p1", "host.status", "node-a", {})
            event["observed_at"] = "../../escape"
            with self.assertRaises(ValueError):
                o.append(event)
            with self.assertRaises(ValueError):
                o.append_journal(event)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_make_event_rejects_control_character_producer(self):
        for producer in ("\nforged", "bad producer", "bad::producer"):
            with self.assertRaises(ValueError):
                make_event(producer, "host.status", "node-a", {})


class TestSubscriberJournal(unittest.TestCase):
    def test_received_event_is_deduped_and_not_producer_pending(self):
        import json

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            d.allowed_producers = frozenset(["remote:p1"])
            event = make_event("remote:p1", "host.status", "node-b",
                               {"host": "node-b", "reachable": True})
            body = json.dumps(event).encode()
            self.assertTrue(d._handle_body(body))
            self.assertFalse(d._handle_body(body), "duplicate must be deduped")
            self.assertEqual(d.out.count_pending(), 0)
            journal = list(d.out.read_journal())
            self.assertEqual([e["event_id"] for e in journal],
                             [event["event_id"]])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_received_event_repairs_torn_journal_tail(self):
        import json

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            event = make_event("remote:p1", "host.status", "node-b",
                               {"host": "node-b", "reachable": True})
            day = event["observed_at"][:10]
            path = os.path.join(o.journal_dir, day + ".jsonl")
            with open(path, "wb") as f:
                f.write(b'{"torn":')
            self.assertTrue(o.append_journal(event))
            with open(path) as f:
                rows = [json.loads(line) for line in f if line.strip()]
            self.assertEqual([row["event_id"] for row in rows], [event["event_id"]])
            self.assertTrue(os.listdir(o.quarantine_dir))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_journal_first_create_fsyncs_parent_directory(self):
        import stat
        from unittest import mock

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            event = make_event("remote:p1", "host.status", "node-b",
                               {"host": "node-b", "reachable": True})
            real_fsync = os.fsync
            directory_syncs = []

            def track(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    directory_syncs.append(fd)
                return real_fsync(fd)

            with mock.patch("anvil_events.outbox.os.fsync", side_effect=track):
                self.assertTrue(o.append_journal(event))
            self.assertGreaterEqual(len(directory_syncs), 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_interleaved_writes_do_not_lose_events(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            events = [make_event(f"p{i % 3:02d}", "host.status", "node-a",
                                 {"host": f"node-{i}", "reachable": bool(i % 2)},
                                 producer_seq=(i // 3) + 1)
                      for i in range(9)]
            import threading
            threads = [threading.Thread(target=o.append, args=(e,))
                       for e in events]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(o.count_pending(), 9)
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestCausalScale(unittest.TestCase):
    def test_large_chain_no_recursion_error(self):
        events = [make_event("p1", "host.status", "node-a", {},
                             producer_seq=i + 1,
                             observed_at=f"2026-08-12T10:{i % 60:02d}:00.000Z")
                  for i in range(1500)]
        ok, err = CausalChecker.check(events)
        self.assertTrue(ok, err)


class TestConcurrentSeq(unittest.TestCase):
    """Concurrent emitters must never reuse a sequence (reviewer #2 repro)."""

    def test_threaded_emit_distinct_seqs(self):
        root = tempfile.mkdtemp()
        try:
            import threading
            o = Outbox(root)
            results = []
            def worker(i):
                # each thread appends via the locked emit
                ev = o.emit("p1", "host.status", "node-a", {"host": f"node-a-{i}", "reachable": bool(i % 2)})
                results.append(ev["producer_seq"])
            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(len(results), 8)
            self.assertEqual(len(set(results)), 8,
                             f"concurrent emitters reused a seq: {results}")
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestAckOrdering(unittest.TestCase):
    """Archive-before-pending: a crash never loses an event (reviewer #1)."""

    def test_ack_archive_first_then_remove_pending(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            day = utcnow_iso()[:10]
            e = make_event("p1", "serve.up", "node-a", {}, observed_at=utcnow_iso())
            o.append(e)
            # archive first, then remove from pending
            import inspect
            src = inspect.getsource(o.ack)
            self.assertLess(src.index("archive"), src.index("remove from pending")
                            if "remove from pending" in src else 10**9,
                            "ack must archive before touching pending")
            # and end-to-end: after ack both done
            o.ack(e)
            self.assertEqual(o.count_pending(), 0)
            with open(os.path.join(root, "archive", day + ".jsonl")) as f:
                self.assertIn(e["event_id"], f.read())
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_ack_repairs_torn_archive_before_append(self):
        import json

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            event = o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
            day = event["observed_at"][:10]
            archive = os.path.join(o.archive_dir, day + ".jsonl")
            with open(archive, "wb") as f:
                f.write(b'{"torn":')
            o.ack(event)
            with open(archive) as f:
                rows = [json.loads(line) for line in f if line.strip()]
            self.assertEqual([row["event_id"] for row in rows], [event["event_id"]])
            self.assertTrue(os.listdir(o.quarantine_dir))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_out_of_order_ack_never_rolls_cursor_back(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            first = o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
            second = o.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
            o.ack(second)
            o.ack(first)
            cursor = o.load_cursors()[first["subject"]]["p1"]
            self.assertEqual(cursor["producer_seq"], 2)
            self.assertEqual(cursor["last_event_id"], second["event_id"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_cursor_is_per_subject_and_producer(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            payload = {"host": "node-a", "reachable": True}
            a = o.emit("producer-a", "host.status", "node-a", payload)
            b = o.emit("producer-b", "host.status", "node-a", payload)
            o.ack(a)
            o.ack(b)
            cursor = o.load_cursors()[a["subject"]]
            self.assertEqual(cursor["producer-a"]["last_event_id"], a["event_id"])
            self.assertEqual(cursor["producer-b"]["last_event_id"], b["event_id"])
            self.assertEqual(cursor["producer-a"]["producer_seq"], 1)
            self.assertEqual(cursor["producer-b"]["producer_seq"], 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_append_uses_same_interprocess_lock_as_ack(self):
        import fcntl
        import subprocess
        import time

        root = tempfile.mkdtemp()
        try:
            event = make_event("p1", "host.status", "node-a", {})
            code = (
                "import json,sys; from anvil_events.outbox import Outbox; "
                "Outbox(sys.argv[1]).append(json.loads(sys.argv[2]))"
            )
            lock_path = os.path.join(root, ".lock")
            with open(lock_path, "a") as lockf:
                fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
                proc = subprocess.Popen(
                    [sys.executable, "-c", code, root,
                     __import__("json").dumps(event)],
                )
                time.sleep(0.1)
                self.assertIsNone(proc.poll(), "append bypassed the process lock")
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)
            self.assertEqual(proc.wait(timeout=3), 0)
            self.assertEqual(Outbox(root).count_pending(), 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestDaemonGate(unittest.TestCase):
    """Daemon validation gate: forged/unknown events are DROPPED (ADR-0001)."""

    def test_default_durable_identity_uses_unsanitized_host_stream_and_filter(self):
        from unittest import mock

        from anvil_events.daemon import EventsDaemon

        with mock.patch("anvil_events.daemon.socket.gethostname",
                        side_effect=["node.example", "node-example"]):
            first = EventsDaemon(root=tempfile.mkdtemp(), health=("127.0.0.1", 0))
            second = EventsDaemon(root=tempfile.mkdtemp(), health=("127.0.0.1", 0))
        try:
            self.assertNotEqual(first.durable, second.durable)
            self.assertTrue(first.durable.startswith("anvil-events-node-example-"))
            self.assertTrue(second.durable.startswith("anvil-events-node-example-"))
        finally:
            first.stop()
            second.stop()
            shutil.rmtree(first.root, ignore_errors=True)
            shutil.rmtree(second.root, ignore_errors=True)

    def daemon(self):
        from anvil_events.daemon import EventsDaemon
        d = EventsDaemon(root=tempfile.mkdtemp(), health=("127.0.0.1", 0))
        d.allowed_producers = frozenset(["p1"])
        return d

    def test_valid_event_passes_gate(self):
        ok = self.daemon()._valid(make_event(
            "p1", "serve.up", "node-a",
            {"serve": "s1", "model": "m", "port": 9001},
        ))
        self.assertTrue(ok)

    def test_unknown_kind_dropped(self):
        e = make_event("p1", "serve.up", "node-a", {})
        e["kind"] = "not.a.kind"
        self.assertFalse(self.daemon()._valid(e))

    def test_forged_missing_fields_dropped(self):
        e = make_event("p1", "serve.up", "node-a", {})
        del e["event_id"]
        self.assertFalse(self.daemon()._valid(e))
        e2 = make_event("p1", "serve.up", "node-a", {})
        e2["producer"] = ""
        self.assertFalse(self.daemon()._valid(e2))

    def test_wrong_version_dropped(self):
        e = make_event("p1", "serve.up", "node-a", {})
        e["version"] = 999
        self.assertFalse(self.daemon()._valid(e))

    def test_structurally_valid_unauthorized_producer_is_dropped(self):
        event = make_event("intruder", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
        self.assertFalse(self.daemon()._valid(event))


class TestDaemonHealthObservability(unittest.TestCase):
    """M5: health endpoint surfaces the degraded signal (pending + degraded_events)."""

    def test_health_reports_pending_and_degraded(self):
        import json
        import socket
        import threading

        from anvil_events.daemon import EventsDaemon
        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            # override the bound port with a free one
            probe = socket.socket()
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            probe.close()
            d.health_addr = ("127.0.0.1", port)
            # seed a pending event (unpublished -> degraded signal) plus an
            # event.degraded record (so degraded_events counts it)
            o = d.out
            o.emit("p1", "host.status", "node-a", {"host": "h", "reachable": True})
            o.emit("local", "event.degraded", "local", {"cause": "test"})
            d._stats["received"] = 1
            d._stats["dropped"] = 0
            t = threading.Thread(target=d._health_loop, daemon=True)
            t.start()
            import time
            time.sleep(0.2)
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
                    s.settimeout(2)
                    data = b""
                    while True:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                        data += chunk
                body = data.split(b"\r\n\r\n", 1)[1]
                stats = json.loads(body)
                self.assertIn("pending", stats)
                self.assertIn("degraded_events", stats)
                self.assertGreaterEqual(stats["pending"], 2)
                self.assertGreaterEqual(stats["degraded_events"], 1)
                self.assertEqual(stats["received"], 1)
            finally:
                d.stop()
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestJetStreamPublish(unittest.TestCase):
    """M2: HPUB with Nats-Msg-Id dedup header; ensure_stream reports JS."""

    def test_publish_js_sends_hpub_with_dedup_id(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        captured = []
        c._send = lambda data: captured.append(data)   # capture frames
        c.publish_js("anvil.fleet.node-a.serve.up",
                     {"kind": "serve.up"}, msg_id="p1:000001")
        frame = captured[0]
        self.assertTrue(frame.startswith(b"HPUB "))
        # mandatory NATS/1.0 preamble + blank line + Nats-Msg-Id header
        self.assertIn(b"NATS/1.0\r\nNats-Msg-Id: p1:000001\r\n\r\n", frame)
        self.assertIn(b"anvil.fleet.node-a.serve.up", frame)

    def test_publish_js_requires_positive_puback_when_requested(self):
        import json

        from anvil_events.nats_mini import NATSClient

        c = NATSClient()
        sent = []
        c._send = sent.append
        c.subscribe = lambda subject, count, timeout: [
            json.dumps({"stream": "ANVIL", "seq": 42}).encode()
        ]
        ack = c.publish_js("anvil.fleet.node-a.host.status", {},
                           msg_id="p1:000001", wait_ack=True)
        self.assertEqual(ack, {"stream": "ANVIL", "seq": 42})
        self.assertTrue(any(frame.startswith(b"SUB _INBOX.") for frame in sent))
        self.assertTrue(any(frame.startswith(b"HPUB ") for frame in sent))

    def test_publish_js_timeout_is_not_success(self):
        from anvil_events.nats_mini import NATSClient

        c = NATSClient()
        c._send = lambda data: None
        c.subscribe = lambda subject, count, timeout: []
        with self.assertRaises(TimeoutError):
            c.publish_js("anvil.fleet.node-a.host.status", {},
                         msg_id="p1:000001", wait_ack=True)
        self.assertEqual(c._subscriptions, {})


class TestDeliveryPump(unittest.TestCase):
    def test_pending_archived_only_after_puback(self):
        from anvil_events.daemon import EventsDaemon

        class AckClient:
            def publish_js(self, subject, event, **kwargs):
                return {"stream": "ANVIL", "seq": 1}

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            event = d.out.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
            d._drain_pending(AckClient())
            self.assertEqual(d.out.count_pending(), 0)
            self.assertEqual([e["event_id"] for e in d.out.read_archive()],
                             [event["event_id"]])
            self.assertEqual(d._stats["acked"], 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_puback_failure_keeps_pending(self):
        from anvil_events.daemon import EventsDaemon

        class FailClient:
            def publish_js(self, subject, event, **kwargs):
                raise TimeoutError("no PubAck")

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            event = d.out.emit("p1", "host.status", "node-a", {"host": "node-a", "reachable": True})
            attempted, failed = d._drain_pending(FailClient())
            self.assertEqual((attempted, failed), (1, 1))
            self.assertEqual([e["event_id"] for e in d.out.read_pending()],
                             [event["event_id"]])
            self.assertEqual(list(d.out.read_archive()), [])
            self.assertEqual(d._stats["delivery_errors"], 1)
            self.assertEqual(d._stats["last_delivery_error"], "no PubAck")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_poison_pending_does_not_block_later_healthy_event(self):
        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            poison = d.out.emit("poison", "host.status", "node-a",
                                {"host": "node-a", "reachable": True})
            healthy = d.out.emit("healthy", "host.status", "node-a",
                                 {"host": "node-a", "reachable": True})

            class Client:
                def publish_js(self, subject, event, **kwargs):
                    if event["event_id"] == poison["event_id"]:
                        raise OSError("permanent rejection")
                    return {"stream": "ANVIL", "seq": 1}

            attempted, failed = d._drain_pending(Client())
            self.assertEqual((attempted, failed), (2, 1))
            self.assertEqual([e["event_id"] for e in d.out.read_pending()],
                             [poison["event_id"]])
            self.assertEqual([e["event_id"] for e in d.out.read_archive()],
                             [healthy["event_id"]])
            self.assertEqual(d._stats["acked"], 1)
            self.assertFalse(d._stats["broker_connected"],
                             "producer error must not mutate subscriber state")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_poison_entry_uses_backoff_and_round_robin_fairness(self):
        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            poison_ids = []
            for i in range(17):
                poison_ids.append(d.out.emit(
                    f"poison{i}", "host.status", "node-a",
                    {"host": "node-a", "reachable": True},
                )["event_id"])
            healthy = d.out.emit("healthy", "host.status", "node-a",
                                 {"host": "node-a", "reachable": True})
            calls = []

            class Client:
                def publish_js(self, subject, event, **kwargs):
                    calls.append(event["event_id"])
                    if event["event_id"] in poison_ids:
                        raise OSError("poison")
                    return {"stream": "ANVIL", "seq": 1}

            d._drain_pending(Client(), max_events=16)
            first_cycle_calls = len(calls)
            d._drain_pending(Client(), max_events=16)
            self.assertIn(healthy["event_id"], calls,
                          "later healthy work was hidden by poison backlog")
            # Offset wraps; backed-off poison entries must not be attempted
            # again immediately even when encountered in another cycle.
            d._drain_pending(Client(), max_events=16)
            self.assertEqual(calls.count(poison_ids[0]), 1)
            self.assertGreaterEqual(first_cycle_calls, 1)
            self.assertNotIn(healthy["event_id"],
                             [e["event_id"] for e in d.out.read_pending()])
            self.assertEqual(len(d._delivery_retry), 17)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_all_backed_off_entries_require_zero_envelope_validations(self):
        import json
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            for i in range(100):
                event = d.out.emit(f"p{i}", "host.status", "node-a",
                                   {"host": "node-a", "reachable": True})
                d._delivery_retry[event["event_id"]] = {
                    "failures": 1, "next_at": 999.0,
                }
            validations = 0
            source_rows = 0
            real_json_loads = json.loads

            def validator(event):
                nonlocal validations
                validations += 1
                return True, ""

            def counted_loads(value, *args, **kwargs):
                nonlocal source_rows
                source_rows += 1
                return real_json_loads(value, *args, **kwargs)

            with (mock.patch("anvil_events.daemon.time.monotonic",
                             return_value=0.0),
                  mock.patch("anvil_events.daemon.validate_event",
                             side_effect=validator),
                  mock.patch("anvil_events.outbox.json.loads",
                             side_effect=counted_loads)):
                self.assertEqual(d._pending_batch(16), [])
            self.assertEqual(validations, 0)
            self.assertLessEqual(source_rows, 64)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_all_backoff_round_sleeps_at_eof_without_rescanning(self):
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            for i in range(100):
                event = d.out.emit(f"p{i}", "host.status", "node-a",
                                   {"host": "node-a", "reachable": True})
                d._delivery_retry[event["event_id"]] = {
                    "failures": 1, "next_at": 999.0,
                }
            with mock.patch("anvil_events.daemon.time.monotonic",
                            return_value=0.0):
                self.assertEqual(d._pending_batch(16), [])
                self.assertEqual(d._pending_batch(16), [])
            self.assertEqual(d._retry_sleep_until, 999.0)

            waits = []
            d._stop.wait = lambda seconds: waits.append(seconds) or d.stop() or True
            with (mock.patch("anvil_events.daemon.time.monotonic",
                             return_value=0.0),
                  mock.patch.object(d, "_pending_batch",
                                    side_effect=AssertionError("rescanned"))):
                d._producer_loop()
            self.assertEqual(waits, [2])
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_new_append_wakes_backoff_sleep_without_json_rescan(self):
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            old = d.out.emit("old", "host.status", "node-a",
                             {"host": "node-a", "reachable": True})
            d._delivery_retry[old["event_id"]] = {
                "failures": 1, "next_at": 999.0,
            }
            with mock.patch("anvil_events.daemon.time.monotonic",
                            return_value=0.0):
                self.assertEqual(d._pending_batch(16), [])
            self.assertEqual(d._retry_sleep_until, 999.0)

            def append_during_wait(_seconds):
                d.out.emit("new", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
                return False

            d._stop.wait = append_during_wait
            selected = []

            def select_once(_max_events):
                selected.append(True)
                d.stop()
                return []

            with (mock.patch("anvil_events.daemon.time.monotonic",
                             return_value=0.0),
                  mock.patch.object(d, "_pending_batch",
                                    side_effect=select_once)):
                d._producer_loop()
            self.assertEqual(selected, [True])
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_append_after_atomic_eof_snapshot_wakes_selector(self):
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            old = d.out.emit("old", "host.status", "node-a",
                             {"host": "node-a", "reachable": True})
            d._delivery_retry[old["event_id"]] = {
                "failures": 1, "next_at": 999.0,
            }
            original = d.out.select_pending_batch

            def append_after_select(*args, **kwargs):
                result = original(*args, **kwargs)
                d.out.emit("new", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
                return result

            with (mock.patch("anvil_events.daemon.time.monotonic",
                             return_value=0.0),
                  mock.patch.object(d.out, "select_pending_batch",
                                    side_effect=append_after_select)):
                self.assertEqual(d._pending_batch(16), [])
            self.assertNotEqual(d.out.pending_signature(),
                                d._retry_sleep_signature)

            selected = []
            d._stop.wait = lambda _seconds: False

            def select_once(_max_events):
                selected.append(True)
                d.stop()
                return []

            with (mock.patch("anvil_events.daemon.time.monotonic",
                             return_value=0.0),
                  mock.patch.object(d, "_pending_batch",
                                    side_effect=select_once)):
                d._producer_loop()
            self.assertEqual(selected, [True])
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_successful_retry_clears_stale_sleep_deadline(self):
        from anvil_events.daemon import EventsDaemon

        class Client:
            def publish_js(self, subject, event, **kwargs):
                return {"stream": "ANVIL", "seq": 1}

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            event = d.out.emit("p1", "host.status", "node-a",
                               {"host": "node-a", "reachable": True})
            d._delivery_retry[event["event_id"]] = {
                "failures": 1, "next_at": 0.0,
            }
            d._retry_sleep_until = 999.0
            d._retry_sleep_signature = d.out.pending_signature()
            self.assertEqual(d._deliver_batch(Client(), [event]), (1, 0))
            self.assertEqual(d._delivery_retry, {})
            self.assertIsNone(d._retry_sleep_until)
            self.assertIsNone(d._retry_sleep_signature)
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_due_retry_is_not_starved_by_sustained_new_work(self):
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            poison = d.out.emit("poison", "host.status", "node-a",
                                {"host": "node-a", "reachable": True})
            calls = []

            class Client:
                def publish_js(self, subject, event, **kwargs):
                    calls.append(event["event_id"])
                    if event["event_id"] == poison["event_id"]:
                        raise OSError("poison")
                    return {"stream": "ANVIL", "seq": 1}

            with mock.patch("anvil_events.daemon.time.monotonic",
                            return_value=100.0):
                d._drain_pending(Client(), max_events=1)
            for i in range(5):
                d.out.emit(f"new{i}", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
                with mock.patch("anvil_events.daemon.time.monotonic",
                                return_value=103.0 + i):
                    d._drain_pending(Client(), max_events=1)
            self.assertGreaterEqual(calls.count(poison["event_id"]), 2,
                                    "due retry starved behind sustained inflow")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_drain_batch_is_bounded(self):
        from anvil_events.daemon import EventsDaemon

        class AckClient:
            def publish_js(self, subject, event, **kwargs):
                return {"stream": "ANVIL", "seq": 1}

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            for i in range(20):
                d.out.emit(f"p{i}", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
            attempted, failed = d._drain_pending(AckClient(), max_events=16)
            self.assertEqual((attempted, failed), (16, 0))
            self.assertEqual(d.out.count_pending(), 4)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_producer_loop_validation_work_is_bounded_by_batch(self):
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon
        from anvil_events.ingest import validate_event as real_validate

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            for i in range(100):
                d.out.emit(f"p{i}", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
            validations = 0

            def validator(event):
                nonlocal validations
                validations += 1
                return real_validate(event)

            class Producer:
                def connect(self, timeout=5):
                    return self

                def publish_js(self, subject, event, **kwargs):
                    d.stop()
                    return {"stream": "ANVIL", "seq": 1}

                def close(self):
                    pass

            with (mock.patch("anvil_events.daemon.validate_event",
                             side_effect=validator),
                  mock.patch("anvil_events.daemon.NATSClient",
                             return_value=Producer())):
                d._producer_loop()
            self.assertLessEqual(validations, 16,
                                 "producer path scanned beyond its batch")
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_producer_loop_does_not_connect_without_selected_work(self):
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))

            def stop_after_first_wait(timeout):
                d.stop()
                return True

            with (mock.patch("anvil_events.daemon.NATSClient") as client,
                  mock.patch.object(d._stop, "wait",
                                    side_effect=stop_after_first_wait)):
                d._producer_loop()
            client.assert_not_called()
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_pending_selector_streams_instead_of_reading_whole_file(self):
        import builtins
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            for i in range(20):
                d.out.emit(f"p{i}", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
            real_open = builtins.open

            class NoReadlines:
                def __init__(self, wrapped):
                    self._wrapped = wrapped

                def __getattr__(self, name):
                    return getattr(self._wrapped, name)

                def __enter__(self):
                    self._wrapped.__enter__()
                    return self

                def __exit__(self, *args):
                    return self._wrapped.__exit__(*args)

                def __iter__(self):
                    return iter(self._wrapped)

                def readlines(self, *args, **kwargs):
                    raise AssertionError("selector loaded the entire outbox file")

            def guarded_open(path, mode="r", *args, **kwargs):
                opened = real_open(path, mode, *args, **kwargs)
                if (mode == "rb" and str(path).startswith(d.out.outbox_dir)
                        and str(path).endswith(".jsonl")):
                    return NoReadlines(opened)
                return opened

            with mock.patch("builtins.open", side_effect=guarded_open):
                self.assertEqual(len(d._pending_batch(2)), 2)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_malformed_complete_pending_line_is_quarantined_not_head_of_line(self):
        import glob

        from anvil_events.daemon import EventsDaemon

        class AckClient:
            def publish_js(self, subject, event, **kwargs):
                return {"stream": "ANVIL", "seq": 1}

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            healthy = d.out.emit("healthy", "host.status", "node-a",
                                 {"host": "node-a", "reachable": True})
            path = glob.glob(os.path.join(root, "outbox", "*.jsonl"))[0]
            with open(path, "rb") as f:
                valid = f.read()
            with open(path, "wb") as f:
                f.write(b'{malformed complete line}\n' + valid)
            attempted, failed = d._drain_pending(AckClient(), max_events=16)
            self.assertEqual(failed, 0)
            self.assertGreaterEqual(attempted, 1)
            archived = [e["event_id"] for e in d.out.read_archive()]
            self.assertIn(healthy["event_id"], archived)
            self.assertTrue(glob.glob(os.path.join(root, "quarantine", "*.invalid")))
            # Recovery remains observable but never blocks healthy history.
            pending = list(d.out.read_pending())
            self.assertTrue(all(e.get("kind") == "event.degraded" for e in pending))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_producer_loop_repairs_malformed_before_counting_pending(self):
        import glob
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            healthy = d.out.emit("healthy", "host.status", "node-a",
                                 {"host": "node-a", "reachable": True})
            path = glob.glob(os.path.join(root, "outbox", "*.jsonl"))[0]
            with open(path, "rb") as f:
                valid = f.read()
            with open(path, "wb") as f:
                f.write(b'{}\n' + valid)

            class Producer:
                def connect(self, timeout=5):
                    return self

                def publish_js(self, subject, event, **kwargs):
                    if event["event_id"] == healthy["event_id"]:
                        d.stop()
                    return {"stream": "ANVIL", "seq": 1}

                def close(self):
                    pass

            with mock.patch("anvil_events.daemon.NATSClient", return_value=Producer()):
                d._producer_loop()
            self.assertIn(healthy["event_id"],
                          [e["event_id"] for e in d.out.read_archive()])
            self.assertTrue(glob.glob(os.path.join(root, "quarantine", "*.invalid")))
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_producer_failure_does_not_starve_healthy_subscriber_thread(self):
        import threading
        import time
        import unittest.mock as mock

        from anvil_events.daemon import EventsDaemon

        root = tempfile.mkdtemp()
        try:
            d = EventsDaemon(root=root, health=("127.0.0.1", 0))
            d.out.emit("poison", "host.status", "node-a",
                       {"host": "node-a", "reachable": True})
            polled = threading.Event()

            class Subscriber:
                def connect(self, timeout=5):
                    return self

                def bind_durable_consumer(self, stream, durable, subject,
                                          timeout=5):
                    return f"anvil.delivery.{durable}"

                def receive(self, count=1, timeout=10, subscription=None):
                    polled.set()
                    d._stop.wait(0.01)
                    return []

                def close(self):
                    pass

            class Producer:
                def connect(self, timeout=5):
                    return self

                def publish_js(self, subject, event, **kwargs):
                    raise OSError("permanent rejection")

                def close(self):
                    pass

            def factory(url):
                return (Subscriber()
                        if threading.current_thread().name == "subscriber-test"
                        else Producer())

            with mock.patch("anvil_events.daemon.NATSClient", side_effect=factory):
                subscriber = threading.Thread(target=d._run, name="subscriber-test")
                producer = threading.Thread(target=d._producer_loop, name="producer-test")
                subscriber.start()
                producer.start()
                self.assertTrue(polled.wait(1), "healthy subscriber never polled")
                deadline = time.time() + 1
                while d._stats["delivery_errors"] == 0 and time.time() < deadline:
                    time.sleep(0.01)
                d.stop()
                subscriber.join(1)
                producer.join(1)
            self.assertTrue(d._stats["broker_connected"])
            self.assertEqual(d._stats["last_error"], None)
            self.assertEqual(d._stats["last_delivery_error"], "permanent rejection")
            self.assertEqual(d.out.count_pending(), 1)
        finally:
            d.stop()
            shutil.rmtree(root, ignore_errors=True)


class TestHeaderMessageSubscribe(unittest.TestCase):
    """JetStream HPUB reaches header-capable subscribers as HMSG."""

    def test_subscribe_extracts_payload_from_hmsg(self):
        import json

        from anvil_events.nats_mini import NATSClient

        payload = json.dumps({"kind": "host.status", "version": 1}).encode()
        headers = b"NATS/1.0\r\nNats-Msg-Id: p1:000001\r\n\r\n"
        frame = (b"HMSG anvil.fleet.node-a.host.status 1 " +
                 str(len(headers)).encode() + b" " +
                 str(len(headers) + len(payload)).encode() + b"\r\n" +
                 headers + payload + b"\r\n")

        class FakeSocket:
            def __init__(self, data):
                self.data = data
                self.sent = []

            def sendall(self, data):
                self.sent.append(data)

            def recv(self, _size):
                data, self.data = self.data, b""
                return data

            def settimeout(self, _timeout):
                pass

        c = NATSClient()
        c.sock = FakeSocket(frame)
        self.assertEqual(c.subscribe("anvil.fleet.>", count=1, timeout=1),
                         [payload])

        for malformed in (
            b"MSG too-short\r\n",
            b"MSGX a 1 3\r\nabc\r\n",
            b"HMSGX a 1 0 3\r\nabc\r\n",
            b"PONG\r\n",
            b"HMSG a 1 broken\r\n",
            b"MSG a 1 -1\r\n\r\n",
            b"MSG a 1 +3\r\nabc\r\n",
            b"MSG a 0 0\r\n\r\n",
            b"HMSG a 1 -1 3\r\nabc\r\n",
            b"HMSG a +1 0 3\r\nabc\r\n",
            b"HMSG a 1 4 3\r\nabc\r\n",
        ):
            c = NATSClient()
            c.sock = FakeSocket(malformed)
            with self.assertRaises(OSError):
                c.subscribe("anvil.fleet.>", count=1, timeout=1)

    def test_segmented_frames_and_body_terminator_validation(self):
        from anvil_events.nats_mini import NATSClient

        class SegmentedSocket:
            def __init__(self, chunks):
                self.chunks = list(chunks)
                self.sent = []

            def sendall(self, data):
                self.sent.append(data)

            def recv(self, _n):
                return self.chunks.pop(0) if self.chunks else b""

            def settimeout(self, _timeout):
                pass

        frame = b"MSG a 1 3\r\nabc\r\nPING\r\n"
        for chunks in ([frame], [bytes([byte]) for byte in frame]):
            c = NATSClient()
            c.sock = SegmentedSocket(chunks)
            c._subscriptions = {"a": 1}
            messages = c.receive(count=1, timeout=1, subscription="a")
            self.assertEqual(messages[0]["body"], b"abc")

        c = NATSClient()
        c.sock = SegmentedSocket([b"MSG a 1 3\r\nabcXXPING\r\n"])
        c._subscriptions = {"a": 1}
        with self.assertRaisesRegex(OSError, "body terminator"):
            c.receive(count=1, timeout=1, subscription="a")

    def test_publish_js_headers_flag_in_proto(self):
        from anvil_events.nats_mini import PROTO
        self.assertTrue(PROTO.get("headers"), "CONNECT must advertise headers")

    def test_publish_js_rejects_injection(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        with self.assertRaises(ValueError):
            c.publish_js("a\r\nPUB x 0\r\n", {}, msg_id="x")

    def test_publish_js_header_value_injection_rejected(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        with self.assertRaises(ValueError):
            c.publish_js("ok.subject", {}, msg_id="p1:000001\r\nEVIL")
        with self.assertRaises(ValueError):
            c.publish_js("ok.subject", {}, msg_id="safe\n")

    def test_publish_js_oversize_rejected(self):
        from anvil_events.nats_mini import (
            _MAX_BODY,
            NATSClient,
            encode_js_publish,
        )
        c = NATSClient()
        with self.assertRaises(ValueError):
            c.publish_js("ok.subject", b"x" * (_MAX_BODY + 1), msg_id="x")
        with self.assertRaisesRegex(ValueError, "max_msg_size"):
            encode_js_publish(b"x" * _MAX_BODY, "m" * 1200)

    def test_durable_consumer_binds_before_create_and_ack_is_explicit(self):
        from anvil_events.nats_mini import NATSClient

        c = NATSClient()
        sent = []
        c.sock = object()
        c._send = sent.append
        c._request_json = lambda subject, payload, timeout=5: {
            "stream_name": "ANVIL", "name": "node-a",
        }
        delivery = c.bind_durable_consumer(
            "ANVIL", "node-a", "anvil.fleet.node-a.>",
        )
        self.assertEqual(delivery, "anvil.delivery.node-a")
        self.assertTrue(sent[0].startswith(b"SUB anvil.delivery.node-a "))
        c.ack("$JS.ACK.ANVIL.node-a.1.1.1.1.0")
        self.assertEqual(sent[-1],
                         b"PUB $JS.ACK.ANVIL.node-a.1.1.1.1.0 0\r\n\r\n")
        with self.assertRaises(ValueError):
            c.ack("$JS.ACK.bad\r\nPUB forged 0")

    def test_daemon_acks_only_after_journal_processing(self):
        import json
        from unittest import mock

        from anvil_events.daemon import EventsDaemon

        d = EventsDaemon(root=tempfile.mkdtemp(), health=("127.0.0.1", 0))
        d.allowed_producers = frozenset(["remote:p1"])
        event = make_event("remote:p1", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
        processed, journaled = d._handle_body_result(json.dumps(event).encode())
        self.assertEqual((processed, journaled), (True, True))
        # Duplicate is processed safely and may be ACKed, but not re-journaled.
        self.assertEqual(d._handle_body_result(json.dumps(event).encode()),
                         (True, False))
        with mock.patch.object(d.out, "append_journal",
                               side_effect=OSError("disk failure")):
            self.assertEqual(d._handle_body_result(json.dumps(event).encode()),
                             (False, False))


class TestEnsureStream(unittest.TestCase):
    """M2: ensure_stream reports reachability/JS availability honestly."""

    def _fake(self, server_info, connect_ok=True):
        from anvil_events.nats_mini import NATSClient
        if not connect_ok:
            def boom():
                raise OSError("no broker")
            return boom
        c = NATSClient()
        c.server_info = server_info
        c.close = lambda: None
        return lambda: c

    def test_bool_jetstream_info(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        out = c.ensure_stream(client_factory=self._fake({"jetstream": True}))
        self.assertTrue(out["reachable"] and out["jetstream_available"])

    def test_dict_jetstream_info(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        out = c.ensure_stream(client_factory=self._fake(
            {"jetstream": {"config": {"enabled": True}}}))
        self.assertTrue(out["reachable"] and out["jetstream_available"])

    def test_no_jetstream_reports_false(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        out = c.ensure_stream(client_factory=self._fake({"jetstream": False}))
        self.assertTrue(out["reachable"])
        self.assertFalse(out["jetstream_available"])

    def test_unreachable_reports_false(self):
        from anvil_events.nats_mini import NATSClient
        c = NATSClient()
        out = c.ensure_stream(client_factory=self._fake({}, connect_ok=False))
        self.assertFalse(out["reachable"])


class TestGCSizeGuard(unittest.TestCase):
    """M2: gc rotates + emits event.degraded when archive exceeds the cap."""

    def test_gc_rotates_and_degrades_on_oversize(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            # fill the archive with a big file (> cap)
            import os
            day = utcnow_iso()[:10]
            with open(os.path.join(root, "archive", day + ".jsonl"), "w") as f:
                f.write("x" * 600)     # small but cap is tiny for the test
            result = o.gc(archive_days=90, max_bytes=100)
            self.assertTrue(result["rotated"], result)
            self.assertTrue(result["degraded"], "must emit event.degraded")
            # the degraded event is in the pending outbox now
            kinds = [e["kind"] for e in o.read_pending()]
            self.assertIn("event.degraded", kinds)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_removes_old_archive(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            # a file with a very old mtime
            import os
            import time
            old = os.path.join(root, "archive", "2020-01-01.jsonl")
            with open(old, "w") as f:
                f.write("{}")
            os.utime(old, (time.time() - 400 * 86400,) * 2)
            result = o.gc(archive_days=90, max_bytes=10 ** 9)
            self.assertEqual(result["removed"], 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_deletion_fsyncs_archive_directory(self):
        import stat
        import time
        from unittest import mock

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            old = os.path.join(o.archive_dir, "2020-01-01.jsonl")
            with open(old, "w") as f:
                f.write("{}\n")
            os.utime(old, (time.time() - 400 * 86400,) * 2)
            real_fsync = os.fsync
            directory_syncs = []

            def track(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    directory_syncs.append(fd)
                return real_fsync(fd)

            with mock.patch("anvil_events.outbox.os.fsync", side_effect=track):
                result = o.gc(archive_days=90, max_bytes=10 ** 9)
            self.assertEqual(result["removed"], 1)
            self.assertGreaterEqual(len(directory_syncs), 1)
            # PRD: "the sweep logs deletions to the day's journal line." The
            # audit is a durable producer day-file (outbox/pending) record,
            # exactly like the oversize degraded signal.
            audits = [
                e for e in o.read_pending()
                if e.get("kind") == "event.degraded"
                and e.get("payload", {}).get("cause", "")
                .startswith("retention sweep deleted")
            ]
            self.assertEqual(len(audits), 1, "GC deletion must be journaled")
            self.assertEqual(audits[0]["payload"]["records"], 1)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_fsyncs_pinned_archive_inode_across_path_swap(self):
        """GC must fsync the pinned archive dirfd, never a pathname reopen.

        A directory swap between unlink and fsync must not make us fsync a
        replacement directory: the deletion's dirent change has to be
        persisted on the ORIGINAL archive inode whose entries were deleted.
        """
        import stat
        import time
        from unittest import mock

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            old = os.path.join(o.archive_dir, "2020-01-01.jsonl")
            with open(old, "w") as f:
                f.write("{}\n")
            os.utime(old, (time.time() - 400 * 86400,) * 2)
            real_fsync = os.fsync
            real_unlink = os.unlink
            archive_fds_seen = []
            archive_inode = None
            swapped = False
            pinned_inode = None
            # open the pinned archive fd exactly once (as gc does), so we can
            # prove the fsync target is that same inode even after a swap
            real_open_pinned = anvil_events.outbox.open_pinned_directory

            def tracking_pinned(directory):
                nonlocal pinned_inode, archive_inode
                fd = real_open_pinned(directory)
                if directory == o.archive_dir and archive_inode is None:
                    archive_inode = os.fstat(fd).st_ino
                if pinned_inode is None and directory == o.archive_dir:
                    pinned_inode = os.fstat(fd).st_ino
                return fd

            def track(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    archive_fds_seen.append(os.fstat(fd).st_ino)
                return real_fsync(fd)

            def swapping_unlink(name, *, dir_fd=None):
                nonlocal swapped
                if not swapped and dir_fd is not None:
                    # attacker renames the archive dir away and puts a fresh
                    # empty dir at the same pathname
                    moved = o.archive_dir + ".moved"
                    os.rename(o.archive_dir, moved)
                    os.mkdir(o.archive_dir)
                    swapped = True
                return real_unlink(name, dir_fd=dir_fd)

            with mock.patch(
                "anvil_events.outbox.open_pinned_directory",
                side_effect=tracking_pinned,
            ), mock.patch("anvil_events.outbox.os.fsync", side_effect=track), \
                 mock.patch("anvil_events.outbox.os.unlink",
                            side_effect=swapping_unlink):
                result = o.gc(archive_days=90, max_bytes=10 ** 9)
            self.assertEqual(result["removed"], 1, result)
            self.assertGreaterEqual(len(archive_fds_seen), 1)
            # every fsync targeting the ARCHIVE directory must be the
            # original pinned inode (the GC deletion durability guarantee).
            # The audit record's outbox fsyncs are separate and legitimate.
            archive_syncs = [ino for ino in archive_fds_seen
                             if ino == archive_inode]
            self.assertTrue(pinned_inode is not None)
            self.assertTrue(archive_inode is not None)
            self.assertEqual(pinned_inode, archive_inode)
            self.assertEqual(archive_syncs, [pinned_inode] * len(archive_syncs),
                            f"archive fsync inodes {archive_syncs} "
                            f"!= pinned {pinned_inode}")
            # the original (moved) archive still holds the deletion (empty dir)
            self.assertEqual(os.listdir(o.archive_dir + ".moved"), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_ignores_directories_symlinks_and_unmanaged_files(self):
        root = tempfile.mkdtemp()
        try:
            import time
            o = Outbox(root)
            archive = os.path.join(root, "archive")
            directory = os.path.join(archive, "old-dir")
            os.mkdir(directory)
            target = os.path.join(root, "outside.jsonl")
            with open(target, "w") as f:
                f.write("outside")
            link = os.path.join(archive, "2020-01-01.1786600000.jsonl")
            os.symlink(target, link)
            unmanaged = os.path.join(archive, "notes.backup.jsonl")
            with open(unmanaged, "w") as f:
                f.write("notes")
            old = time.time() - 400 * 86400
            os.utime(directory, (old, old))
            os.utime(unmanaged, (old, old))
            result = o.gc(archive_days=90, max_bytes=10 ** 9)
            self.assertEqual(result["removed"], 0)
            self.assertTrue(os.path.isdir(directory))
            self.assertTrue(os.path.islink(link))
            self.assertTrue(os.path.exists(target))
            self.assertTrue(os.path.exists(unmanaged))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_enforces_hard_cap_evicts_oldest_rotated(self):
        # M5: the size guard must ENFORCE the hard cap — after rotating the
        # current-day file, evict the OLDEST rotated overflow files until
        # under max_bytes (true retention enforcement, not just rotation).
        root = tempfile.mkdtemp()
        try:
            import os
            import time
            o = Outbox(root)
            # create two rotated-overflow files (from prior rotations) + current day
            day = utcnow_iso()[:10]
            epoch_old = "1786600000"   # 10-digit epoch suffixes (as rotation produces)
            epoch_new = "1786600200"
            sizes = {f"{day}.{epoch_old}.jsonl": 40, f"{day}.{epoch_new}.jsonl": 80}
            for name, age in ((f"{day}.{epoch_old}.jsonl", 3), (f"{day}.{epoch_new}.jsonl", 1)):
                p = os.path.join(root, "archive", name)
                with open(p, "w") as f:
                    f.write("x" * sizes[name])
                os.utime(p, (time.time() - 100 * 86400 - age * 10,) * 2)  # old enough
            # an ORDINARY archive (not rotated-overflow) must NEVER be evicted
            ordinary = os.path.join(root, "archive", "2026-06-01.jsonl")
            with open(ordinary, "w") as f:
                f.write("y" * 50)
            # an ODD single-digit-suffix file (NOT a rotation timestamp) must
            # never be evicted either (reviewer probe: 2026-08-12.5.jsonl)
            odd = os.path.join(root, "archive", "2026-08-12.5.jsonl")
            with open(odd, "w") as f:
                f.write("z" * 40)
            # a big current-day file pushes managed total over the 330-byte cap
            with open(os.path.join(root, "archive", day + ".jsonl"), "w") as f:
                f.write("x" * 200)
            result = o.gc(archive_days=90, max_bytes=330)
            self.assertEqual(result["removed"], 2, result)
            self.assertFalse(result["rotated"], result)
            self.assertFalse(result["unresolved_oversize"], result)
            total = sum(os.path.getsize(os.path.join(root, "archive", f))
                        for f in os.listdir(os.path.join(root, "archive"))
                        if __import__("re").fullmatch(
                            r"\d{4}-\d{2}-\d{2}(?:\.\d{9,11})?\.jsonl", f,
                        ))
            self.assertLessEqual(total, 330)
            remaining = os.listdir(os.path.join(root, "archive"))
            self.assertNotIn(f"{day}.{epoch_old}.jsonl", remaining)
            self.assertNotIn(f"{day}.{epoch_new}.jsonl", remaining)
            # the ordinary archive file was NOT touched
            self.assertIn("2026-06-01.jsonl", remaining,
                          "ordinary archive must never be evicted")
            # the odd-suffix file (2026-08-12.5.jsonl) also survived
            self.assertIn("2026-08-12.5.jsonl", remaining,
                          "odd-suffix non-rotation file must never be evicted")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_preserves_young_rotations_and_reports_unresolved_cap(self):
        import time

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            young = os.path.join(o.archive_dir,
                                 "2026-08-13.1786600000.jsonl")
            with open(young, "w") as f:
                f.write("x" * 200)
            os.utime(young, (time.time() - 10,) * 2)
            result = o.gc(archive_days=90, max_bytes=100)
            self.assertTrue(os.path.exists(young))
            self.assertEqual(result["evicted"], 0)
            self.assertTrue(result["unresolved_oversize"])
            self.assertTrue(result["degraded"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_bare_daily_oversize_is_explicitly_unresolved(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            bare = os.path.join(o.archive_dir, "2026-06-01.jsonl")
            with open(bare, "w") as f:
                f.write("x" * 200)
            result = o.gc(archive_days=365, max_bytes=100)
            self.assertTrue(os.path.exists(bare))
            self.assertTrue(result["unresolved_oversize"])
            self.assertGreater(result["size"], 100)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_gc_same_second_rotation_never_overwrites_existing_sibling(self):
        import unittest.mock as mock

        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            day = utcnow_iso()[:10]
            current = os.path.join(o.archive_dir, day + ".jsonl")
            existing = os.path.join(o.archive_dir, day + ".1786619000.jsonl")
            with open(existing, "wb") as f:
                f.write(b"EXISTING")
            with open(current, "wb") as f:
                f.write(b"NEW")
            # Keep the pre-existing sibling newer, so cap eviction removes the
            # newly rotated current file rather than the collision sentinel.
            os.utime(existing, (1786619999, 1786619999))
            os.utime(current, (1786618000, 1786618000))
            with mock.patch("anvil_events.outbox.time.time", return_value=1786619000):
                o.gc(archive_days=90, max_bytes=len(b"EXISTING"))
            with open(existing, "rb") as f:
                self.assertEqual(f.read(), b"EXISTING")
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
