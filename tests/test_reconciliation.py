from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from helpers import desired_event

from anvil_events.reconciliation.artifacts import (
    ArtifactUnavailable,
    DirectoryArtifactResolver,
    HTTPSArtifactResolver,
)
from anvil_events.reconciliation.config import load_node_runtime
from anvil_events.reconciliation.contracts import (
    AdapterRegistry,
    AllowBindingsPolicy,
    Artifact,
    Preview,
)
from anvil_events.reconciliation.engine import ReconcileEngine
from anvil_events.reconciliation.file_adapter import ManagedFileAdapter
from anvil_events.reconciliation.processor import DesiredStateProcessor
from anvil_events.reconciliation.resource_lock import (
    ResourceBusy,
    resource_lock,
)
from anvil_events.storage import SQLiteStore


class FakeResolver:
    def __init__(self, data=b"router = 'node-a'\n", revision="rev-1",
                 error=None):
        self.data = data
        self.revision = revision
        self.error = error

    def resolve(self, reference, revision):
        if self.error:
            raise self.error
        return Artifact(self.data, self.revision)


class FakeAdapter:
    name = "router_config"

    def __init__(self, *, apply_error=None, preview_error=None, verified=True):
        self.apply_error = apply_error
        self.preview_error = preview_error
        self.verified = verified
        self.applied = 0
        self.rolled_back = 0

    def preview(self, desired, artifact):
        if self.preview_error:
            raise self.preview_error
        return Preview("replace router client config", ("digest changed",))

    def apply(self, desired, artifact):
        self.applied += 1
        if self.apply_error:
            raise self.apply_error

    def verify(self, desired, artifact):
        return self.verified

    def rollback(self, desired):
        self.rolled_back += 1


class BlockingAdapter(FakeAdapter):
    def __init__(self):
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def apply(self, desired, artifact):
        self.applied += 1
        self.entered.set()
        if not self.release.wait(5):
            raise TimeoutError("test did not release blocking adapter")


class RepairingAdapter(FakeAdapter):
    def __init__(self):
        super().__init__()
        self.drifted = True

    def apply(self, desired, artifact):
        super().apply(desired, artifact)
        self.drifted = False

    def verify(self, desired, artifact):
        return not self.drifted


class ReconcileEngineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(self.temporary.name)
        self.adapter = FakeAdapter()
        self.registry = AdapterRegistry()
        self.registry.register(self.adapter)

    def tearDown(self):
        self.temporary.cleanup()

    def _engine(self, *, resolver=None, allow=True):
        bindings = {
            ("node-a:router", "routing/clients", "router_config"),
        } if allow else set()
        return ReconcileEngine(
            "node-b",
            self.store,
            self.registry,
            resolver or FakeResolver(),
            AllowBindingsPolicy(bindings),
        )

    def _journal(self, event):
        self.store.append_journal(event)
        return event

    def test_apply_verify_and_durable_outcome(self):
        event = self._journal(desired_event(targets=["node-b"]))
        processor = DesiredStateProcessor(
            self._engine(), self.store,
            producer="node-b:reconciler", node="node-b",
        )
        result = processor.process(event)
        self.assertEqual("applied", result.state)
        self.assertEqual(1, self.adapter.applied)
        outcomes = [
            item for item in self.store.read_pending()
            if item["kind"] == "reconcile.applied"
        ]
        self.assertEqual([event["event_id"]], outcomes[0]["causes"])

    def test_duplicate_processing_does_not_reapply_or_duplicate_outcome(self):
        event = self._journal(desired_event(targets=["node-b"]))
        processor = DesiredStateProcessor(
            self._engine(), self.store,
            producer="node-b:reconciler", node="node-b",
        )
        processor.process(event)
        processor.process(event)
        self.assertEqual(1, self.adapter.applied)
        self.assertEqual(1, sum(
            item["kind"] == "reconcile.applied"
            for item in self.store.read_pending()
        ))

    def test_applied_generation_repairs_adapter_drift(self):
        adapter = RepairingAdapter()
        registry = AdapterRegistry()
        registry.register(adapter)
        engine = ReconcileEngine(
            "node-b", self.store, registry, FakeResolver(),
            AllowBindingsPolicy({
                ("node-a:router", "routing/clients", "router_config"),
            }),
        )
        processor = DesiredStateProcessor(
            engine, self.store,
            producer="node-b:reconciler", node="node-b",
        )
        event = self._journal(desired_event(targets=["node-b"]))
        processor.process(event)
        adapter.drifted = True

        result = processor.process(event)

        self.assertEqual("applied", result.state)
        self.assertEqual(2, adapter.applied)

    def test_startup_reconciliation_repairs_stored_applied_resource(self):
        adapter = RepairingAdapter()
        registry = AdapterRegistry()
        registry.register(adapter)
        engine = ReconcileEngine(
            "node-b", self.store, registry, FakeResolver(),
            AllowBindingsPolicy({
                ("node-a:router", "routing/clients", "router_config"),
            }),
        )
        processor = DesiredStateProcessor(
            engine, self.store,
            producer="node-b:reconciler", node="node-b",
        )
        event = self._journal(desired_event(targets=["node-b"]))
        processor.process(event)
        adapter.drifted = True

        results = processor.reconcile_stored()

        self.assertEqual(["applied"], [result.state for result in results])
        self.assertEqual(2, adapter.applied)

    def test_applied_resource_keeps_retrying_when_artifact_is_unavailable(self):
        event = self._journal(desired_event(targets=["node-b"]))
        self._engine().process(event)
        unavailable = self._engine(
            resolver=FakeResolver(error=ArtifactUnavailable("not yet")),
        )

        with self.assertRaises(ArtifactUnavailable):
            unavailable.process(event)

        self.assertEqual("applied", self._engine().process(event).state)

    def test_deny_by_default_waits_without_apply(self):
        event = self._journal(desired_event(targets=["node-b"]))
        result = self._engine(allow=False).process(event)
        self.assertEqual("awaiting-approval", result.state)
        self.assertEqual(0, self.adapter.applied)

    def test_not_targeted_is_a_noop(self):
        event = self._journal(desired_event(targets=["node-c"]))
        result = self._engine().process(event)
        self.assertEqual("not-targeted", result.state)
        self.assertEqual(0, self.adapter.applied)

    def test_allowed_but_non_authoritative_producer_cannot_auto_apply(self):
        event = desired_event(targets=["node-b"])
        event["producer"] = "node-a:other"
        event["event_id"] = "node-a:other:000001"
        self.store.append_journal(event)
        result = self._engine().process(event)
        self.assertEqual("awaiting-approval", result.state)
        self.assertEqual(0, self.adapter.applied)

    def test_digest_mismatch_is_permanent_failure(self):
        event = self._journal(desired_event(targets=["node-b"]))
        result = self._engine(resolver=FakeResolver(data=b"wrong")).process(event)
        self.assertEqual("failed", result.state)
        self.assertIn("digest", result.payload["error"])

    def test_revision_mismatch_is_permanent_failure(self):
        event = self._journal(desired_event(targets=["node-b"]))
        result = self._engine(
            resolver=FakeResolver(revision="other"),
        ).process(event)
        self.assertEqual("failed", result.state)
        self.assertIn("revision", result.payload["error"])

    def test_unavailable_artifact_remains_retryable(self):
        event = self._journal(desired_event(targets=["node-b"]))
        engine = self._engine(
            resolver=FakeResolver(error=ArtifactUnavailable("not yet")),
        )
        with self.assertRaises(ArtifactUnavailable):
            engine.process(event)

    def test_apply_exception_is_indeterminate_and_not_retried(self):
        self.adapter.apply_error = OSError("connection disappeared")
        event = self._journal(desired_event(targets=["node-b"]))
        engine = self._engine()
        first = engine.process(event)
        second = engine.process(event)
        self.assertEqual("indeterminate", first.state)
        self.assertEqual("indeterminate", second.state)
        self.assertEqual(1, self.adapter.applied)

    def test_adapter_exception_path_is_not_published_in_outcome(self):
        self.adapter.preview_error = OSError(
            "permission denied: /private/operator/client.toml"
        )
        event = self._journal(desired_event(targets=["node-b"]))
        result = self._engine().process(event)
        self.assertEqual("adapter preview failed", result.payload["error"])
        self.assertNotIn("/private", result.payload["error"])

    def test_verification_failure_rolls_back(self):
        self.adapter.verified = False
        event = self._journal(desired_event(targets=["node-b"]))
        result = self._engine().process(event)
        self.assertEqual("failed", result.state)
        self.assertEqual(1, self.adapter.rolled_back)

    def test_stale_generation_is_superseded(self):
        newer = self._journal(desired_event(
            sequence=1, generation=2, revision="rev-2", targets=["node-b"],
        ))
        older = self._journal(desired_event(
            sequence=2, generation=1, revision="rev-1", targets=["node-b"],
        ))
        engine = self._engine(resolver=FakeResolver(revision="rev-2"))
        self.assertEqual("applied", engine.process(newer).state)
        self.assertEqual("superseded", engine.process(older).state)

    def test_generation_cannot_be_reused_for_different_content(self):
        first = self._journal(desired_event(sequence=1, targets=["node-b"]))
        second = self._journal(desired_event(
            sequence=2, revision="other", targets=["node-b"],
        ))
        engine = self._engine()
        engine.process(first)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            engine.process(second)

    def test_same_event_cannot_apply_concurrently(self):
        event = self._journal(desired_event(targets=["node-b"]))
        self.adapter = BlockingAdapter()
        self.registry = AdapterRegistry()
        self.registry.register(self.adapter)
        engine = self._engine()
        result = {}

        def apply():
            result["value"] = engine.process(event)

        thread = threading.Thread(target=apply)
        thread.start()
        self.assertTrue(self.adapter.entered.wait(2))
        with self.assertRaises(ResourceBusy):
            engine.process(event)
        self.adapter.release.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual("applied", result["value"].state)
        self.assertEqual(1, self.adapter.applied)

    def test_newer_generation_waits_for_inflight_apply(self):
        older = self._journal(desired_event(
            sequence=1, generation=1, targets=["node-b"],
        ))
        newer = self._journal(desired_event(
            sequence=2, generation=2, targets=["node-b"],
        ))
        self.adapter = BlockingAdapter()
        self.registry = AdapterRegistry()
        self.registry.register(self.adapter)
        engine = self._engine()
        thread = threading.Thread(target=lambda: engine.process(older))
        thread.start()
        self.assertTrue(self.adapter.entered.wait(2))
        with self.assertRaises(ResourceBusy):
            engine.process(newer)
        self.adapter.release.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual("applied", engine.process(newer).state)
        with self.store.database.connect() as connection:
            generation = connection.execute(
                "SELECT generation FROM reconcile_resources",
            ).fetchone()[0]
        self.assertEqual(2, generation)
        self.assertEqual(2, self.adapter.applied)

    def test_abandoned_applying_attempt_is_indeterminate_not_replayed(self):
        event = self._journal(desired_event(targets=["node-b"]))
        engine = self._engine()
        with resource_lock(
                self.store.root, "node-b", event["payload"]["resource"]):
            state, operation_id = engine.state.claim("node-b", event)
            self.assertEqual("prepared", state)
            engine.state.set_preview(
                operation_id, Preview("synthetic", ("change",)),
            )
            engine.state.begin_apply(operation_id)
        result = engine.process(event)
        self.assertEqual("indeterminate", result.state)
        self.assertEqual(0, self.adapter.applied)
        with self.store.database.connect() as connection:
            applications = connection.execute(
                "SELECT COUNT(*) FROM reconcile_applications",
            ).fetchone()[0]
        self.assertEqual(0, applications)

    def test_resource_lock_files_are_bounded_by_fixed_shards(self):
        for index in range(200):
            with resource_lock(
                    self.store.root, "node-b", f"resource/{index}"):
                pass
        locks = list((Path(self.store.root) / "reconcile-locks").glob("*.lock"))
        self.assertLessEqual(len(locks), 64)


class BuiltInAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_managed_file_is_atomic_and_verifiable(self):
        destination = self.root / "client.toml"
        destination.write_bytes(b"old = true\n")
        data = b"router = 'node-a'\n"
        event = desired_event(targets=["node-b"])
        adapter = ManagedFileAdapter("router_config", destination)
        artifact = Artifact(data, "rev-1")
        adapter.preview(event, artifact)
        adapter.apply(event, artifact)
        self.assertTrue(adapter.verify(event, artifact))
        adapter.rollback(event)
        self.assertEqual(b"old = true\n", destination.read_bytes())

    def test_directory_resolver_uses_exact_reference_and_revision(self):
        path = self.root / "artifacts" / "routing" / "clients"
        path.mkdir(parents=True)
        (path / "rev-1").write_bytes(b"payload")
        resolver = DirectoryArtifactResolver(self.root / "artifacts")
        self.assertEqual(b"payload", resolver.resolve(
            "routing/clients", "rev-1",
        ).data)
        with self.assertRaises(ValueError):
            resolver.resolve("../../escape", "rev-1")

    def test_directory_resolver_rejects_non_regular_artifacts(self):
        artifact_root = self.root / "artifacts"
        directory_revision = artifact_root / "routing" / "clients" / "rev-1"
        directory_revision.mkdir(parents=True)
        resolver = DirectoryArtifactResolver(artifact_root)
        with self.assertRaisesRegex(ValueError, "regular file"):
            resolver.resolve("routing/clients", "rev-1")

    def test_directory_resolver_rejects_artifact_symlink(self):
        artifact_root = self.root / "artifacts"
        path = artifact_root / "routing" / "clients"
        path.mkdir(parents=True)
        target = path / "target"
        target.write_bytes(b"payload")
        link = path / "rev-1"
        try:
            link.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"symlink unavailable: {exc}")
        resolver = DirectoryArtifactResolver(artifact_root)
        with self.assertRaisesRegex(ValueError, "symbolic link"):
            resolver.resolve("routing/clients", "rev-1")

    def test_node_config_composes_a_working_reconciler(self):
        artifact = b"router = 'node-a'\n"
        artifact_path = self.root / "artifacts" / "routing" / "clients"
        artifact_path.mkdir(parents=True)
        (artifact_path / "rev-1").write_bytes(artifact)
        config = self.root / "node.toml"
        config.write_text(
            """version = 1
node = "node-b"
producer = "node-b:reconciler"
allowed_producers = ["node-a:router"]

[artifact_source]
type = "directory"
root = "artifacts"

[[adapters]]
name = "router_config"
type = "managed_file"
destination = "state/client.toml"
validator = "toml"
auto_apply_resources = ["routing/clients"]
authority_producers = ["node-a:router"]
""",
            encoding="utf-8",
        )
        store = SQLiteStore(self.root / "store")
        runtime = load_node_runtime(config, store)
        event = desired_event(data=artifact, targets=["node-b"])
        store.append_journal(event)
        result = runtime.processor.process(event)
        self.assertEqual("applied", result.state)
        self.assertEqual(artifact, (self.root / "state/client.toml").read_bytes())
        self.assertIn("node-b:reconciler", runtime.allowed_producers)

    def test_auto_apply_requires_an_exact_allowed_authority(self):
        artifact_path = self.root / "artifacts"
        artifact_path.mkdir()
        config = self.root / "node.toml"
        config.write_text(
            """version = 1
node = "node-b"
producer = "node-b:reconciler"
allowed_producers = ["node-a:router"]
[artifact_source]
type = "directory"
root = "artifacts"
[[adapters]]
name = "router_config"
type = "managed_file"
destination = "state/client.toml"
auto_apply_resources = ["routing/clients"]
authority_producers = ["node-c:router"]
""",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "allowed_producers"):
            load_node_runtime(config, SQLiteStore(self.root / "store"))

    def test_managed_file_refuses_destination_symlink(self):
        target = self.root / "target.toml"
        target.write_text("value = 1\n", encoding="utf-8")
        link = self.root / "link.toml"
        try:
            link.symlink_to(target)
        except OSError as exc:
            self.skipTest(f"symlink unavailable: {exc}")
        adapter = ManagedFileAdapter("router_config", link)
        with self.assertRaisesRegex(OSError, "symlink"):
            adapter.preview(desired_event(), Artifact(b"value = 2\n", "rev-1"))


class ArtifactHTTPTests(unittest.TestCase):
    class Response:
        def __init__(self, data=b"artifact", revision="rev-1"):
            self.data = data
            self.headers = {
                "Content-Length": str(len(data)),
                "X-Anvil-Revision": revision,
            }

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, size):
            return self.data

    class Opener:
        def __init__(self, response):
            self.response = response
            self.request = None

        def open(self, request, timeout):
            self.request = request
            return self.response

    def test_fleet_source_requires_auth_reference(self):
        with self.assertRaisesRegex(ValueError, "token_env"):
            HTTPSArtifactResolver("https://controller.example/artifacts")

    def test_exact_https_artifact_uses_runtime_token(self):
        opener = self.Opener(self.Response())
        resolver = HTTPSArtifactResolver(
            "https://controller.example/artifacts",
            token_env="ANVIL_TEST_ARTIFACT_TOKEN",
            opener=opener,
        )
        with patch.dict(
                "os.environ", {"ANVIL_TEST_ARTIFACT_TOKEN": "secret-value"},
                clear=True):
            artifact = resolver.resolve("routing/clients", "rev-1")
        self.assertEqual(b"artifact", artifact.data)
        self.assertEqual("Bearer secret-value", opener.request.get_header(
            "Authorization",
        ))
        self.assertNotIn("secret-value", opener.request.full_url)

    def test_missing_runtime_token_is_retryable(self):
        resolver = HTTPSArtifactResolver(
            "https://controller.example/artifacts",
            token_env="ANVIL_TEST_ARTIFACT_TOKEN",
            opener=self.Opener(self.Response()),
        )
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(ArtifactUnavailable):
                resolver.resolve("routing/clients", "rev-1")


if __name__ == "__main__":
    unittest.main()
