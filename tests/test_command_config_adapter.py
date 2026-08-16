"""Allowlisted command-config adapter tests."""

import json
import subprocess
import unittest

from anvil_events.reconciliation.command_config_adapter import CommandConfigAdapter
from anvil_events.reconciliation.contracts import Artifact

DESIRED = {"payload": {"resource": "routing/hermes"}}


class ConfigCLI:
    def __init__(self, values=None, fail_key=None, read_error_key=None):
        self.values = dict(values or {})
        self.fail_key = fail_key
        self.read_error_key = read_error_key
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        action, key = argv[2:4]
        if action == "get":
            if key == self.read_error_key:
                return subprocess.CompletedProcess(argv, 1, "", "database locked")
            if key in self.values:
                return subprocess.CompletedProcess(argv, 0, str(self.values[key]) + "\n", "")
            return subprocess.CompletedProcess(
                argv, 1, "", f"Config key not set: {key}\n",
            )
        if key == self.fail_key:
            return subprocess.CompletedProcess(argv, 2, "", "refused")
        if action == "set":
            self.values[key] = argv[4]
        else:
            self.values.pop(key, None)
        return subprocess.CompletedProcess(argv, 0, "", "")


class CommandConfigAdapterTests(unittest.TestCase):
    def adapter(self, cli):
        return CommandConfigAdapter(
            "hermes", ["/opt/hermes", "config"],
            allowed_keys=["model.default", "model.context_length"],
            missing_stderr_prefix="Config key not set:", runner=cli,
        )

    def test_apply_verify_and_rollback_use_argv_without_shell(self):
        cli = ConfigCLI({"model.default": "old", "model.context_length": "100"})
        adapter = self.adapter(cli)
        data = json.dumps({
            "model.default": "llm.primary", "model.context_length": 393216,
        }).encode()
        artifact = Artifact(data, "rev-1")

        preview = adapter.preview(DESIRED, artifact)
        adapter.apply(DESIRED, artifact)

        self.assertTrue(adapter.verify(DESIRED, artifact))
        self.assertEqual("llm.primary", cli.values["model.default"])
        self.assertEqual("393216", cli.values["model.context_length"])
        self.assertNotIn("llm.primary", " ".join(preview.changes))
        self.assertTrue(all(kwargs["shell"] is False for _, kwargs in cli.calls))
        adapter.rollback(DESIRED)
        self.assertEqual({"model.default": "old", "model.context_length": "100"}, cli.values)

    def test_partial_failure_restores_already_changed_keys(self):
        cli = ConfigCLI(
            {"model.default": "old", "model.context_length": "100"},
            fail_key="model.context_length",
        )
        adapter = self.adapter(cli)
        artifact = Artifact(json.dumps({
            "model.default": "new", "model.context_length": 200,
        }).encode(), "rev-1")
        adapter.preview(DESIRED, artifact)
        with self.assertRaisesRegex(RuntimeError, "rejected an update"):
            adapter.apply(DESIRED, artifact)
        self.assertEqual("old", cli.values["model.default"])

    def test_event_cannot_select_an_unapproved_key(self):
        cli = ConfigCLI()
        adapter = self.adapter(cli)
        artifact = Artifact(b'{"channels.discord":"changed"}', "rev-1")
        with self.assertRaisesRegex(ValueError, "unapproved"):
            adapter.preview(DESIRED, artifact)
        self.assertEqual([], cli.calls)

    def test_missing_key_contract_does_not_hide_command_failure(self):
        cli = ConfigCLI(read_error_key="model.default")
        adapter = self.adapter(cli)
        artifact = Artifact(b'{"model.default":"llm.primary"}', "rev-1")
        with self.assertRaisesRegex(RuntimeError, "rejected a read"):
            adapter.preview(DESIRED, artifact)

    def test_manifest_cannot_allow_credential_shaped_keys(self):
        with self.assertRaisesRegex(ValueError, "credential-shaped"):
            CommandConfigAdapter(
                "unsafe", ["app", "config"], allowed_keys=["provider.api_key"],
            )

    def test_event_value_cannot_be_reinterpreted_as_an_option(self):
        cli = ConfigCLI()
        adapter = self.adapter(cli)
        artifact = Artifact(b'{"model.default":"--force"}', "rev-1")
        with self.assertRaisesRegex(ValueError, "safe bounded argument"):
            adapter.preview(DESIRED, artifact)
        self.assertEqual([], cli.calls)

    def test_manifest_argv_fields_must_be_arrays(self):
        with self.assertRaisesRegex(ValueError, "string array"):
            CommandConfigAdapter(
                "unsafe", "app config", allowed_keys=["model.default"],
            )


if __name__ == "__main__":
    unittest.main()
