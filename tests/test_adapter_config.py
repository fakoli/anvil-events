"""Node-manifest composition for built-in adapter types."""

import tempfile
import unittest
from pathlib import Path

from anvil_events.reconciliation import (
    CommandConfigAdapter,
    JSONMergeAdapter,
    load_node_runtime,
)
from anvil_events.storage import SQLiteStore


class AdapterConfigTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "artifacts").mkdir()
        self.store = SQLiteStore(self.root / "store")

    def tearDown(self):
        self.temporary.cleanup()

    def test_manifest_composes_json_merge_and_command_config(self):
        config = self.root / "node.toml"
        config.write_text(
            """\
version = 1
node = "node-b"
producer = "node-b:events"
allowed_producers = ["node-a:router"]

[artifact_source]
type = "directory"
root = "artifacts"

[[adapters]]
name = "openclaw"
type = "json_merge"
destination = "state/openclaw.json"
protected_paths = ["models.providers.anvil.credential"]
auto_apply_resources = ["routing/openclaw"]
authority_producers = ["node-a:router"]

[[adapters]]
name = "hermes"
type = "command_config"
command = ["/opt/hermes", "config"]
get_args = ["get"]
set_args = ["set"]
unset_args = ["unset"]
missing_returncode = 1
missing_stderr_prefix = "Config key not set:"
allowed_keys = ["model.default", "model.context_length"]
auto_apply_resources = ["routing/hermes"]
authority_producers = ["node-a:router"]
""",
            encoding="utf-8",
        )

        runtime = load_node_runtime(config, self.store)

        self.assertIsInstance(
            runtime.processor.engine.adapters.get("openclaw"), JSONMergeAdapter,
        )
        self.assertIsInstance(
            runtime.processor.engine.adapters.get("hermes"), CommandConfigAdapter,
        )

    def test_unknown_adapter_type_fails_startup(self):
        config = self.root / "node.toml"
        config.write_text(
            """\
version = 1
node = "node-b"
producer = "node-b:events"
allowed_producers = ["node-a:router"]
[artifact_source]
type = "directory"
root = "artifacts"
[[adapters]]
name = "unsafe"
type = "shell"
""",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "adapter type"):
            load_node_runtime(config, self.store)


if __name__ == "__main__":
    unittest.main()
