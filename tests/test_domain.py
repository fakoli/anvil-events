from __future__ import annotations

import copy
import json
import math
import unittest
from pathlib import Path

from helpers import desired_event, desired_payload

from anvil_events.dependency_graph import DependencyGraphChecker
from anvil_events.domain import make_event, validate_event
from anvil_events.domain_v2 import make_event_v2, validate_event_v2


class V2DomainTests(unittest.TestCase):
    def test_desired_event_round_trip(self):
        event = desired_event()
        self.assertEqual((True, ""), validate_event_v2(event))
        self.assertEqual("anvil.events.v2.node-a.state.desired", event["subject"])

    def test_custom_dotted_kind_is_extensible(self):
        event = make_event_v2(
            "node-a:plugin", "plugin.refreshed", "node-a", {"count": 2},
            producer_seq=3,
        )
        self.assertTrue(validate_event_v2(event)[0])

    def test_producer_must_belong_to_node(self):
        with self.assertRaisesRegex(ValueError, "belong"):
            make_event_v2(
                "node-b:router", "state.desired", "node-a", desired_payload(),
                producer_seq=1,
            )
        event = desired_event()
        event["producer"] = "node-b:router"
        event["event_id"] = "node-b:router:000001"
        self.assertIn("belong", validate_event_v2(event)[1])

    def test_identity_and_subject_are_derived(self):
        event = desired_event()
        for field, value in (
            ("event_id", "wrong:000001"),
            ("subject", "anvil.events.v2.node-b.state.desired"),
        ):
            changed = {**event, field: value}
            self.assertFalse(validate_event_v2(changed)[0])

    def test_credentials_are_rejected_at_any_depth(self):
        for key in (
            "api_key", "apiKey", "client-secret", "authorization",
            "private_key", "privateKey", "ssh_private_key", "access_key_id",
        ):
            payload = {"outer": [{key: "never-store-this"}]}
            with self.assertRaisesRegex(ValueError, "credential-shaped"):
                make_event_v2(
                    "node-a:plugin", "plugin.changed", "node-a", payload,
                    producer_seq=1,
                )

    def test_private_key_material_is_rejected_even_under_generic_key(self):
        with self.assertRaisesRegex(ValueError, "credential-shaped"):
            make_event_v2(
                "node-a:plugin", "plugin.changed", "node-a",
                {"value": "-----BEGIN PRIVATE KEY-----\nsynthetic\n"},
                producer_seq=1,
            )

    def test_network_urls_are_not_event_control_data(self):
        for value in (
            "https://controller.example/artifact?token=x",
            "nats://broker.internal:4222",
        ):
            with self.assertRaisesRegex(ValueError, "URL"):
                make_event_v2(
                    "node-a:plugin", "plugin.changed", "node-a", {"source": value},
                    producer_seq=1,
                )

    def test_logical_identifiers_cannot_alias_paths(self):
        for update in (
            {"resource": "routing/../other"},
            {"artifact": "routing//clients"},
            {"revision": "../rev-1"},
            {"revision": "refs/heads/main"},
        ):
            with self.subTest(update=update):
                with self.assertRaises(ValueError):
                    make_event_v2(
                        "node-a:router", "state.desired", "node-a",
                        {**desired_payload(), **update}, producer_seq=1,
                    )

    def test_non_json_numbers_are_rejected(self):
        for value in (math.nan, math.inf, -math.inf):
            with self.assertRaisesRegex(ValueError, "JSON"):
                make_event_v2(
                    "node-a:plugin", "plugin.changed", "node-a", {"v": value},
                    producer_seq=1,
                )

    def test_excessive_nesting_is_rejected(self):
        value = "leaf"
        for _ in range(70):
            value = [value]
        with self.assertRaisesRegex(ValueError, "nesting"):
            make_event_v2(
                "node-a:plugin", "plugin.changed", "node-a", {"v": value},
                producer_seq=1,
            )

    def test_targets_are_unique_safe_nodes(self):
        for targets in ([], ["node-b", "node-b"], ["bad.node"]):
            with self.assertRaisesRegex(ValueError, "targets"):
                make_event_v2(
                    "node-a:router", "state.desired", "node-a",
                    desired_payload(targets=targets), producer_seq=1,
                )
        self.assertTrue(validate_event_v2(
            desired_event(targets=["node-b", "node-c"]),
        )[0])

    def test_timestamps_are_real_rfc3339_values(self):
        event = desired_event()
        event["observed_at"] = "2026-99-99T99:99:99Z"
        self.assertIn("RFC 3339", validate_event_v2(event)[1])

    def test_causes_must_be_unique(self):
        event = desired_event()
        event["causes"] = ["node-a:x:000001", "node-a:x:000001"]
        self.assertIn("duplicates", validate_event_v2(event)[1])

    def test_envelope_is_closed(self):
        event = desired_event()
        event["surprise"] = True
        self.assertIn("extra", validate_event_v2(event)[1])

    def test_schema_metadata_matches_runtime_contract(self):
        schema = json.loads(Path("schemas/events-v2.json").read_text())
        event = desired_event()
        self.assertEqual(schema["$id"], event["schema"])
        self.assertEqual(set(schema["required"]), set(event))


class CompatibilityAndGraphTests(unittest.TestCase):
    def test_v1_is_still_readable(self):
        event = make_event(
            "node-a:serve", "serve.down", "node-a", {"serve": "primary"},
        )
        self.assertTrue(validate_event(event)[0])

    def test_v2_routes_through_shared_validator(self):
        self.assertTrue(validate_event(desired_event())[0])

    def test_dependency_cycle_is_detected(self):
        first = desired_event(sequence=1)
        second = desired_event(sequence=2, generation=2, revision="rev-2")
        first["causes"] = [second["event_id"]]
        second["causes"] = [first["event_id"]]
        ok, error = DependencyGraphChecker.check([first, second])
        self.assertFalse(ok)
        self.assertIn("cycle", error)

    def test_identical_duplicates_are_collapsed(self):
        event = desired_event()
        self.assertEqual((True, None), DependencyGraphChecker.check(
            [event, copy.deepcopy(event)],
        ))

    def test_conflicting_duplicates_fail_closed(self):
        event = desired_event()
        conflict = copy.deepcopy(event)
        conflict["payload"]["revision"] = "different"
        ok, error = DependencyGraphChecker.check([event, conflict])
        self.assertFalse(ok)
        self.assertIn("conflicting", error)


if __name__ == "__main__":
    unittest.main()
