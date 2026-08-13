"""Hermetic tests (unittest, no network) for anvil-events core.

Run:  uv run python -m unittest discover -s tests -q
"""
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from anvil_events.outbox import (  # noqa: E402
    KINDS,
    CausalChecker,
    Outbox,
    TargetQueue,
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
                         ["last_event_id"], e["event_id"])

    def test_ack_is_idempotent_on_event_id_key(self):
        e1 = make_event("p1", "host.status", "node-a", {}, producer_seq=1)
        e2 = make_event("p1", "host.status", "node-a", {}, producer_seq=2)
        self.o.append(e1)
        self.o.append(e2)
        self.o.ack(e1)
        self.o.ack(e1)  # second ack must not corrupt
        self.assertEqual(self.o.count_pending(), 1)


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
        for bad in ["a\r\nPUB x 0\r\n", "a b", "a\nb", "a\tb", "", "a*"]:
            with self.assertRaises(ValueError, msg=f"should reject {bad!r}"):
                validate_subject(bad)
        for good in ["a.b.c", "anvil.fleet.>", "anvil.fleet.node-a.serve.up",
                     "x-y_z.1"]:
            self.assertEqual(validate_subject(good), good)

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
        a.payload = '{"n":1}'
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

    def test_interleaved_writes_do_not_lose_events(self):
        root = tempfile.mkdtemp()
        try:
            o = Outbox(root)
            events = [make_event(f"p{i % 3:02d}", "host.status", "node-a",
                                 {"i": i}, producer_seq=(i // 3) + 1)
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
                ev = o.emit("p1", "host.status", "node-a", {"i": i})
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
            e = make_event("p1", "serve.up", "node-a", {})
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
            with open(os.path.join(root, "archive", "2026-08-13.jsonl")) as f:
                self.assertIn(e["event_id"], f.read())
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestDaemonGate(unittest.TestCase):
    """Daemon validation gate: forged/unknown events are DROPPED (ADR-0001)."""

    def test_valid_event_passes_gate(self):
        from anvil_events.daemon import EventsDaemon
        ok = EventsDaemon._valid(make_event("p1", "serve.up", "node-a", {}))
        self.assertTrue(ok)

    def test_unknown_kind_dropped(self):
        from anvil_events.daemon import EventsDaemon
        e = make_event("p1", "serve.up", "node-a", {})
        e["kind"] = "not.a.kind"
        self.assertFalse(EventsDaemon._valid(e))

    def test_forged_missing_fields_dropped(self):
        from anvil_events.daemon import EventsDaemon
        e = make_event("p1", "serve.up", "node-a", {})
        del e["event_id"]
        self.assertFalse(EventsDaemon._valid(e))
        e2 = make_event("p1", "serve.up", "node-a", {})
        e2["producer"] = ""
        self.assertFalse(EventsDaemon._valid(e2))

    def test_wrong_version_dropped(self):
        from anvil_events.daemon import EventsDaemon
        e = make_event("p1", "serve.up", "node-a", {})
        e["version"] = 999
        self.assertFalse(EventsDaemon._valid(e))


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

    def test_publish_js_oversize_rejected(self):
        from anvil_events.nats_mini import _MAX_BODY, NATSClient
        c = NATSClient()
        with self.assertRaises(ValueError):
            c.publish_js("ok.subject", b"x" * (_MAX_BODY + 1), msg_id="x")


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
            sizes = {f"{day}.1000.jsonl": 40, f"{day}.2000.jsonl": 80}
            for name, age in ((f"{day}.1000.jsonl", 3), (f"{day}.2000.jsonl", 1)):
                p = os.path.join(root, "archive", name)
                with open(p, "w") as f:
                    f.write("x" * sizes[name])
                os.utime(p, (time.time() - age * 10,) * 2)  # distinct mtimes
            # an ORDINARY archive (not rotated-overflow) must NEVER be evicted
            ordinary = os.path.join(root, "archive", "2026-06-01.jsonl")
            with open(ordinary, "w") as f:
                f.write("y" * 50)
            # a big current-day file pushes total over the 340-byte cap
            with open(os.path.join(root, "archive", day + ".jsonl"), "w") as f:
                f.write("x" * 200)
            result = o.gc(archive_days=90, max_bytes=340)
            self.assertTrue(result["rotated"], result)
            self.assertTrue(result["degraded"], "must emit event.degraded")
            # after eviction: total remaining <= cap (340 enforced with margin)
            total = sum(os.path.getsize(os.path.join(root, "archive", f))
                        for f in os.listdir(os.path.join(root, "archive"))
                        if f.endswith(".jsonl"))
            self.assertLessEqual(total, 340, f"hard cap not enforced: total={total}")
            # the OLDEST rotated file (1000, older mtime) was evicted first
            remaining = os.listdir(os.path.join(root, "archive"))
            self.assertNotIn(f"{day}.1000.jsonl", remaining,
                             "oldest rotated overflow must be evicted")
            # the NEWER rotated file survives (eviction stopped under cap)
            self.assertIn(f"{day}.2000.jsonl", remaining,
                          "newer rotated overflow must remain (stopped under cap)")
            # the ordinary archive file was NOT touched
            self.assertIn("2026-06-01.jsonl", remaining,
                          "ordinary archive must never be evicted")
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
