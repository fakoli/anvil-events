"""JSON merge adapter tests."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from anvil_events.reconciliation.contracts import Artifact
from anvil_events.reconciliation.json_merge_adapter import JSONMergeAdapter


def desired(data):
    return {
        "payload": {
            "resource": "routing/openclaw",
            "content_sha256": hashlib.sha256(data).hexdigest(),
        },
    }


class JSONMergeAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "client.json"

    def tearDown(self):
        self.temporary.cleanup()

    def test_merge_preserves_unmentioned_and_protected_values(self):
        self.path.write_text(json.dumps({
            "models": {"providers": {"anvil": {
                "credential": {"source": "file", "id": "unchanged"},
                "models": [{"id": "old"}],
            }}},
            "channels": {"enabled": True},
        }), encoding="utf-8")
        patch = json.dumps({
            "models": {"providers": {"anvil": {
                "models": [{"id": "llm.primary", "context": 393216}],
            }}},
        }).encode()
        adapter = JSONMergeAdapter(
            "openclaw", self.path,
            protected_paths=["models.providers.anvil.credential"],
        )

        preview = adapter.preview(desired(patch), Artifact(patch, "rev-1"))
        adapter.apply(desired(patch), Artifact(patch, "rev-1"))

        current = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual("unchanged", current["models"]["providers"]["anvil"]["credential"]["id"])
        self.assertTrue(current["channels"]["enabled"])
        self.assertEqual("llm.primary", current["models"]["providers"]["anvil"]["models"][0]["id"])
        self.assertTrue(adapter.verify(desired(patch), Artifact(patch, "rev-1")))
        self.assertNotIn("unchanged", " ".join(preview.changes))

    def test_rollback_restores_exact_previous_bytes(self):
        before = b'{"format":  "preserved", "value": 1}\n'
        self.path.write_bytes(before)
        patch = b'{"value":2}'
        adapter = JSONMergeAdapter("client", self.path)
        adapter.preview(desired(patch), Artifact(patch, "rev-1"))
        adapter.apply(desired(patch), Artifact(patch, "rev-1"))
        adapter.rollback(desired(patch))
        self.assertEqual(before, self.path.read_bytes())

    def test_protected_leaf_and_ancestor_replacements_fail_closed(self):
        adapter = JSONMergeAdapter(
            "client", self.path,
            protected_paths=["models.providers.anvil.endpoint"],
        )
        for value in (
            {"models": {"providers": {"anvil": {"endpoint": "changed"}}}},
            {"models": None},
            {"models": []},
        ):
            patch = json.dumps(value).encode()
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "protected"):
                adapter.preview(desired(patch), Artifact(patch, "rev-1"))

    def test_credential_shaped_patch_key_is_rejected(self):
        patch = b'{"provider":{"apiKey":"do-not-store"}}'
        adapter = JSONMergeAdapter("client", self.path)
        with self.assertRaisesRegex(ValueError, "credential-shaped"):
            adapter.preview(desired(patch), Artifact(patch, "rev-1"))

    def test_protected_paths_require_unambiguous_array_entries(self):
        for value in ("models.endpoint", ["models..endpoint"], [""]):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "dotted"):
                JSONMergeAdapter("client", self.path, protected_paths=value)

    def test_destination_change_after_preview_is_not_overwritten(self):
        self.path.write_text('{"value":1}\n', encoding="utf-8")
        patch = b'{"value":2}'
        adapter = JSONMergeAdapter("client", self.path)
        adapter.preview(desired(patch), Artifact(patch, "rev-1"))
        self.path.write_text('{"value":3}\n', encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "changed after preview"):
            adapter.apply(desired(patch), Artifact(patch, "rev-1"))
        self.assertEqual(3, json.loads(self.path.read_text())["value"])


if __name__ == "__main__":
    unittest.main()
