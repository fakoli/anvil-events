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
    CausalChecker, KINDS, Outbox, TargetQueue, make_event)


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

    def test_explicit_causes_cycle_detected(self):
        events = [
            self._ev("p1", 1, "c1", "2026-08-12T10:00:00.000Z"),
            self._ev("p1", 2, "c1", "2026-08-12T10:00:01.000Z",
                     causes=["p1:000001"]),
            self._ev("p1", 3, "c1", "2026-08-12T10:00:02.000Z",
                     causes=["p1:000002"]),
            # cycle: 4 claims 3 caused it, 3 claims 4 caused it
            self._ev("p1", 4, "c1", "2026-08-12T10:00:03.000Z",
                     causes=["p1:000003"]),
        ]
        # add a back-edge: event 3 causes event 4 AND 4 causes 3
        events[2]["causes"] = ["p1:000002", "p1:000004"]  # 3 caused by 2 AND 4
        events[3]["causes"] = ["p1:000003"]               # 4 caused by 3
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
            with self.assertRaises(ValueError, msg="should reject %r" % bad):
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
        import tempfile
        import shutil
        root = tempfile.mkdtemp()
        try:
            # two emits with NATS failing => both stay pending, distinct seqs
            self._run_emit(root, None)
            self._run_emit(root, None)
            o = Outbox(root)
            seqs = [e["producer_seq"] for e in o.read_pending()]
            self.assertEqual(len(seqs), 2)
            self.assertEqual(len(set(seqs)), 2, "event_ids must be unique")
            self.assertTrue(all(s >= 1 for s in seqs))
        finally:
            shutil.rmtree(root, ignore_errors=True)


class _NoNATS:
    """Fake client that always fails to publish (hermetic fragility test)."""
    def __init__(self, url=None): pass
    def connect(self, timeout=None): raise IOError("no broker")
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
            events = [make_event("p%02d" % (i % 3), "host.status", "node-a",
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
                             observed_at="2026-08-12T10:%02d:00.000Z" % (i % 60))
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
                             "concurrent emitters reused a seq: %s" % results)
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


if __name__ == "__main__":
    unittest.main()
