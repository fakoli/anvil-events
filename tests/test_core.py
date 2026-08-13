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

from anvil_events.outbox import (  # noqa: E402
    CausalChecker, KINDS, Outbox, TargetQueue, make_event)


class TestEnvelope(unittest.TestCase):
    def test_make_event_fields(self):
        e = make_event("fakoli-dark:serves", "promote.applied", "fakoli-dark",
                       {"tier": "primary-local"}, correlation_id="c1",
                       producer_seq=7)
        self.assertEqual(e["event_id"], "fakoli-dark:serves:000007")
        self.assertEqual(e["subject"], "anvil.fleet.fakoli-dark.promote.applied")
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
        e = make_event("p1", "serve.up", "dark", {"port": 9001})
        self.o.append(e)
        self.assertEqual(self.o.count_pending(), 1)
        self.o.ack(e)
        self.assertEqual(self.o.count_pending(), 0)
        self.assertEqual(self.o.load_cursors()["anvil.fleet.dark.serve.up"]
                         ["last_event_id"], e["event_id"])

    def test_ack_is_idempotent_on_event_id_key(self):
        e1 = make_event("p1", "host.status", "dark", {}, producer_seq=1)
        e2 = make_event("p1", "host.status", "dark", {}, producer_seq=2)
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
    def _ev(self, producer, seq, corr, observed):
        return make_event(producer, "host.status", "h", {},
                          correlation_id=corr, producer_seq=seq,
                          observed_at=observed)

    def test_consistent_journal_passes(self):
        events = [
            self._ev("p1", 1, "c1", "2026-08-12T10:00:00.000Z"),
            self._ev("p1", 2, "c1", "2026-08-12T10:00:01.000Z"),
            self._ev("p2", 1, "c1", "2026-08-12T10:00:02.000Z"),
        ]
        ok, err = CausalChecker.check(events)
        self.assertTrue(ok, err)

    def test_skew_cycle_detected(self):
        events = [
            self._ev("p1", 1, "c1", "2026-08-12T10:00:00.000Z"),
            self._ev("p1", 5, "c1", "2026-08-12T09:59:00.000Z"),  # seq up, time back
        ]
        ok, err = CausalChecker.check(events)
        self.assertFalse(ok)
        self.assertIn("cycle", err)

    def test_duplicate_event_id_is_single_node_semantics(self):
        events = [self._ev("p1", 1, None, "2026-08-12T10:00:00.000Z")]
        ok, err = CausalChecker.check(events)
        self.assertTrue(ok, err)


if __name__ == "__main__":
    unittest.main()
