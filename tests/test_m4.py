"""M4 tests: `anvil events sync-repo` (commit-push adapter) + `ingest` (validated ingestion)."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from anvil_events import cli, ingest
from anvil_events.ingest import fact_store_add
from anvil_events.outbox import Outbox, make_event


def _fake_git(log):
    """Return a git_sync-style fake: (repo_dir, push=) -> (rc, details).

    Records stored git commands in `log` (used to assert add/commit/push ran).
    """
    def git(repo_dir, push=False):
        log.append(["git", "status", "--porcelain"])
        log.append(["git", "add", "-A"])
        log.append(["git", "commit", "-m", "ops: adopt recorded state"])
        if push:
            log.append(["git", "push"])
        return 0, {"committed": True, "pushed": push}
    return git


class TestSyncRepo(unittest.TestCase):
    """sync-repo: commit + optional push + correlation-linked config.adopted/repo.synced."""

    def test_requires_correlation_and_existing_dir(self):
        with tempfile.TemporaryDirectory() as d:
            args = cli.argparse.Namespace(
                dir=os.path.join(d, "missing"), correlation=None,
                push=False, root=d, host="node-a")
            with self.assertRaises(ValueError):
                cli.cmd_sync_repo(args)

    def test_commits_changes_and_emits_correlation_linked_events(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            repo = Path(d) / "repo"
            repo.mkdir()
            (repo / "serves.toml").write_text("a = 1\n")
            log = []
            with mock.patch.object(ingest, "cli_cmd_emit") as emit:
                emit.return_value = 0
                args = cli.argparse.Namespace(
                    dir=str(repo), correlation="promote-123",
                    push=False, root=str(root), host="node-a")
                rc = cli.cmd_sync_repo(args, _git=_fake_git(log))
                self.assertEqual(rc, 0)
            # git add + commit happened
            add = [a for a in log if a[:2] == ["git", "add"]]
            commit = [a for a in log if a[:2] == ["git", "commit"]]
            self.assertTrue(add, "git add must run")
            self.assertTrue(commit, "git commit must run")
            # exactly two emits: config.adopted + repo.synced, correlation-linked
            self.assertEqual(emit.call_count, 2)
            kinds = [c.args[0] if c.args else c.kwargs.get("kind")
                     for c in emit.call_args_list]
            self.assertEqual(kinds, ["config.adopted", "repo.synced"])
            for c in emit.call_args_list:
                kw = c.kwargs if c.kwargs else {}
                corr = kw.get("correlation") or (c.args[2] if len(c.args) > 2 else None)
                self.assertEqual(corr, "promote-123")
                host = kw.get("host") or (c.args[3] if len(c.args) > 3 else None)
                self.assertEqual(host, "node-a")
            # repo.synced carries ok=True
            synced_kw = emit.call_args_list[1].kwargs or {}
            synced_payload = synced_kw.get("payload") or (emit.call_args_list[1].args[1] if len(emit.call_args_list[1].args) > 1 else {})
            self.assertEqual(synced_payload.get("ok"), True)

    def test_push_flag_runs_git_push(self):
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            repo = Path(d) / "repo"
            repo.mkdir()
            log = []
            with mock.patch.object(ingest, "cli_cmd_emit"):
                args = cli.argparse.Namespace(
                    dir=str(repo), correlation="promote-123",
                    push=True, root=str(root), host="node-a")
                cli.cmd_sync_repo(args, _git=_fake_git(log))
            push = [a for a in log if a[:2] == ["git", "push"]]
            self.assertTrue(push, "git push must run when --push")

    def test_git_failure_returns_nonzero_no_traceback_and_emits_both(self):
        # Reviewer M4-2: a git add/commit/push failure must return non-zero
        # WITHOUT a traceback, and must still emit BOTH events with ok=False.
        import unittest.mock as mock
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            repo = Path(d) / "repo"
            repo.mkdir()
            emitted = []
            def failing_git(repo_dir, push=False):
                return 128, {"committed": False, "pushed": False,
                             "error": "fatal: not a git repository"}
            with mock.patch.object(ingest, "cli_cmd_emit") as emit:
                emit.side_effect = lambda kind, payload, corr, host, root: emitted.append((kind, payload, corr)) or 0
                args = cli.argparse.Namespace(
                    dir=str(repo), correlation="promote-123",
                    push=True, root=str(root), host="node-a")
                rc = cli.cmd_sync_repo(args, _git=failing_git)
            self.assertEqual(rc, 128)
            self.assertEqual([k for k, _, _ in emitted], ["config.adopted", "repo.synced"])
            synced = emitted[1][1]
            self.assertIs(synced["ok"], False)

    def test_git_status_failure_is_error_not_clean_noop(self):
        # Reviewer M4-3: a non-zero git status (repo unreadable) must be an
        # error (rc non-zero), NOT a clean-tree success.
        from subprocess import CompletedProcess
        with tempfile.TemporaryDirectory() as d:
            repo = Path(d) / "repo"
            repo.mkdir()
            def status_fails(argv, **kwargs):
                return CompletedProcess(argv, 128, "", "fatal: not a git repository")
            rc, details = ingest.git_sync(str(repo), _run=status_fails)
            self.assertEqual(rc, 128, "failed git status must be an error")
            self.assertIs(details["committed"], False)
            self.assertIn("error", details)


class TestIngest(unittest.TestCase):
    """ingest: validated, deduplicated ingestion into the journal/fact store."""

    def setUp(self):
        self.allowed = mock.patch.dict(
            os.environ, {"ANVIL_EVENTS_ALLOWED_PRODUCERS":
                         "p1,remote:p1,node-a:serves"},
        )
        self.allowed.start()

    def tearDown(self):
        self.allowed.stop()

    def _write_event(self, root, ev):
        o = Outbox(str(root))
        o.append(ev)

    def test_forged_kind_is_dropped_not_stored(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            root.mkdir()
            o = Outbox(str(root))
            ev = make_event("p1", "host.status", "node-a", {"x": 1})
            ev["kind"] = "forged.kind"
            o.append(ev)
            stored = []
            rc = cli.cmd_ingest(str(root), fact_store=lambda ev: stored.append(ev) or ev)
            self.assertEqual(stored, [], "forged kind must not be stored")
            self.assertEqual(rc, 1, "dropped events surface non-zero")

    def test_valid_events_are_fact_store_and_journaled(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            root.mkdir()
            o = Outbox(str(root))
            o.append(make_event("p1", "serve.up", "node-a",
                                {"serve": "s1", "model": "m", "port": 9001}))
            stored = []
            cli.cmd_ingest(str(root), fact_store=lambda ev: stored.append(ev) or ev)
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0]["kind"], "serve.up")

    def test_duplicate_event_id_is_ingested_once_across_stores(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            root.mkdir()
            o = Outbox(str(root))
            ev = make_event("p1", "host.status", "node-a",
                            {"host": "node-a", "reachable": True})
            o.append(ev)
            o.append_journal(ev)
            stored = []
            cli.cmd_ingest(str(root),
                           fact_store=lambda event: stored.append(event) or event)
            self.assertEqual([event["event_id"] for event in stored],
                             [ev["event_id"]])

    def test_default_store_is_idempotent_across_ingest_runs(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            root.mkdir()
            ev = make_event("p1", "host.status", "node-a",
                            {"host": "node-a", "reachable": True})
            Outbox(str(root)).append_journal(ev)
            self.assertEqual(cli.cmd_ingest(str(root)), 0)
            self.assertEqual(cli.cmd_ingest(str(root)), 0)
            facts = [line for line in (root / "facts.jsonl").read_text().splitlines()
                     if line.strip()]
            self.assertEqual(len(facts), 1)

    def test_incomplete_or_inconsistent_envelope_is_rejected(self):
        from anvil_events.ingest import validate_event

        ev = make_event("p1", "host.status", "node-a",
                        {"host": "node-a", "reachable": True})
        for field in ("event_id", "producer", "producer_seq", "host",
                      "subject", "schema", "observed_at", "emitted_at"):
            broken = dict(ev)
            broken.pop(field)
            self.assertFalse(validate_event(broken)[0], field)
        mismatched = dict(ev)
        mismatched["subject"] = "anvil.fleet.node-b.host.status"
        self.assertFalse(validate_event(mismatched)[0])
        for field, value in (
            ("schema", "https://invalid.example/schema"),
            ("host", "../escape"),
            ("observed_at", "not-a-date"),
            ("producer_seq", 0),
            ("causes", [""]),
            ("correlation_id", 42),
        ):
            broken = dict(ev)
            broken[field] = value
            self.assertFalse(validate_event(broken)[0], field)
        for missing in ("version", "correlation_id"):
            broken = dict(ev)
            broken.pop(missing)
            self.assertFalse(validate_event(broken)[0], missing)
        date_only = dict(ev)
        date_only["observed_at"] = "2026-08-13"
        self.assertFalse(validate_event(date_only)[0])
        extra = dict(ev)
        extra["unexpected"] = True
        self.assertFalse(validate_event(extra)[0])
        producer_control = dict(ev)
        producer_control["producer"] = "\nforged"
        producer_control["event_id"] = "\nforged:000001"
        self.assertFalse(validate_event(producer_control)[0])

    def test_v1_without_optional_causes_remains_compatible(self):
        from anvil_events.ingest import validate_event

        event = make_event("p1", "host.status", "node-a",
                           {"host": "node-a", "reachable": True})
        event.pop("causes")
        self.assertEqual(validate_event(event), (True, ""))

    def test_false_boolean_required_fields_accepted(self):
        # Reviewer M4-1: `ok=False` / `reachable=False` are VALID failure-state
        # events; required-field checking must NOT use truthiness.
        from anvil_events.ingest import validate_event
        ok, reason = validate_event(make_event(
            "p1", "repo.synced", "node-a", {"repo": "r", "ok": False},
        ))
        self.assertTrue(ok, f"False bool must be valid: {reason}")
        ok, reason = validate_event(make_event(
            "p1", "host.status", "node-a",
            {"host": "h", "reachable": False},
        ))
        self.assertTrue(ok, f"False bool must be valid: {reason}")

    def test_payload_allowlist_rejects_unknown_keys_and_wrong_types(self):
        from anvil_events.ingest import fact_store_add, validate_event

        forged = make_event(
            "remote:p1", "host.status", "node-a",
            {"host": 123, "reachable": "yes",
             "arbitrary": {"command": "run"}},
        )
        ok, reason = validate_event(forged)
        self.assertFalse(ok)
        self.assertIn("unknown fields", reason)
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(fact_store_add(os.path.join(d, "facts.jsonl"),
                                              forged))

        wrong_type = make_event(
            "remote:p1", "host.status", "node-a",
            {"host": "node-a", "reachable": "yes"},
        )
        ok, reason = validate_event(wrong_type)
        self.assertFalse(ok)
        self.assertIn("must be a boolean", reason)

    def test_structurally_valid_unauthorized_producer_is_not_stored(self):
        from anvil_events.ingest import fact_store_add, validate_event

        event = make_event(
            "intruder", "host.status", "node-a",
            {"host": "node-a", "reachable": True},
        )
        allowed = frozenset(["p1"])
        self.assertEqual(validate_event(event, allowed_producers=allowed),
                         (False, "producer is not authorized"))
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "facts.jsonl"
            self.assertIsNone(fact_store_add(
                path, event, allowed_producers=allowed,
            ))
            self.assertFalse(path.exists())

    def test_anvil_serving_lifecycle_payloads_match_frozen_requirements(self):
        from anvil_events.ingest import validate_event

        samples = {
            "serve.up": {"serve": "s", "model": "m", "port": 1},
            "serve.down": {"serve": "s", "graceful": True},
            "profile.enter": {"mode": "exclusive", "profile": "p"},
            "profile.leave": {"mode": "exclusive", "profile": "p"},
            "promote.applied": {"tier": "primary", "model": "m"},
            "promote.rolled_back": {"tier": "primary", "restored_model": "m"},
        }
        for kind, payload in samples.items():
            event = make_event("node-a:serves", kind, "node-a", payload)
            self.assertEqual(validate_event(event), (True, ""), kind)

    def test_sensitive_fields_are_dropped_case_insensitively(self):
        from anvil_events.ingest import fact_store_add
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "facts.jsonl")
            fact = fact_store_add(store, make_event(
                "p1", "divergence", "node-a",
                {"issue": "drift", "declared": {"Token": "secret",
                                                   "API_KEY": "k",
                                                   "password": "p",
                                                   "kept": "yes"}},
            ))
            self.assertIsNotNone(fact)
            payload = fact["payload"]["declared"]
            self.assertNotIn("Token", payload)
            self.assertNotIn("API_KEY", payload)
            self.assertNotIn("password", payload)
            # lowercase-insensitive drop keys: uppercase in payload removed
            self.assertNotIn("token", {k.lower() for k in payload})

    def test_missing_payload_fields_are_dropped(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            root.mkdir()
            o = Outbox(str(root))
            ev = make_event("p1", "serve.up", "node-a",
                            {"serve": "s1", "model": "m", "port": 9001})
            del ev["payload"]["serve"]
            o.append(ev)
            stored = []
            cli.cmd_ingest(str(root), fact_store=lambda ev: stored.append(ev) or ev)
            self.assertEqual(stored, [], "invalid payload must be dropped")

    def test_invalid_duplicate_does_not_shadow_later_valid_event(self):
        import json

        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            archive = root / "archive"
            journal = root / "journal"
            archive.mkdir(parents=True)
            journal.mkdir()
            valid = make_event("p1", "host.status", "node-a",
                               {"host": "node-a", "reachable": True})
            invalid = dict(valid)
            invalid["subject"] = "anvil.fleet.node-b.host.status"
            (archive / "2026-08-13.jsonl").write_text(json.dumps(invalid) + "\n")
            (journal / "2026-08-13.jsonl").write_text(json.dumps(valid) + "\n")
            stored = []
            rc = ingest.cmd_ingest(
                {"root": str(root), "count": None, "store": None},
                fact_store=lambda ev: stored.append(ev) or ev,
            )
            self.assertEqual(rc, 1)
            self.assertEqual([ev["event_id"] for ev in stored], [valid["event_id"]])

    def test_nested_sensitive_payload_fields_are_removed(self):
        import json

        with tempfile.TemporaryDirectory() as d:
            event = make_event("p1", "divergence", "node-a", {
                "issue": "drift",
                "delta": {"token": "SECRET", "access_token": "SECRET",
                           "client_secret": "SECRET", "accessToken": "SECRET",
                           "clientSecret": "SECRET", "clientApiKey": "SECRET",
                           "kept": [
                    {"password": "SECRET", "db_password": "SECRET",
                     "dbPassword": "SECRET", "value": 1},
                ]},
            })
            path = Path(d) / "facts.jsonl"
            ingest.fact_store_add(path, event)
            fact = json.loads(path.read_text())
            self.assertNotIn("token", fact["payload"]["delta"])
            self.assertNotIn("access_token", fact["payload"]["delta"])
            self.assertNotIn("client_secret", fact["payload"]["delta"])
            self.assertNotIn("accessToken", fact["payload"]["delta"])
            self.assertNotIn("clientSecret", fact["payload"]["delta"])
            self.assertNotIn("clientApiKey", fact["payload"]["delta"])
            self.assertNotIn("password", fact["payload"]["delta"]["kept"][0])
            self.assertNotIn("db_password", fact["payload"]["delta"]["kept"][0])
            self.assertNotIn("dbPassword", fact["payload"]["delta"]["kept"][0])
            self.assertEqual(fact["payload"]["delta"]["kept"][0]["value"], 1)

    def test_fact_store_repairs_torn_tail_before_append(self):
        import json

        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "facts.jsonl"
            path.write_bytes(b'{"event_id":"torn"')
            event = make_event("p1", "host.status", "node-a",
                               {"host": "node-a", "reachable": True})
            ingest.fact_store_add(path, event)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row["event_id"] for row in rows], [event["event_id"]])
            self.assertTrue(list(Path(d).glob("facts.jsonl.*.torn")))

    def test_fact_store_first_create_fsyncs_parent_directory(self):
        import stat
        from unittest import mock

        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "facts.jsonl"
            event = make_event("p1", "host.status", "node-a",
                               {"host": "node-a", "reachable": True})
            real_fsync = os.fsync
            directory_syncs = []

            def track(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    directory_syncs.append(fd)
                return real_fsync(fd)

            with mock.patch("anvil_events.ingest.os.fsync", side_effect=track):
                self.assertIsNotNone(ingest.fact_store_add(path, event))
            self.assertGreaterEqual(len(directory_syncs), 1)

    def test_fact_store_rejects_symlinked_data_and_lock_targets(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            external = root / "external"
            external.write_text("SAFE\n")
            event = make_event(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )
            store = root / "facts.jsonl"
            store.symlink_to(external)
            with self.assertRaises(OSError):
                fact_store_add(store, event, allowed_producers={"p1"})
            self.assertEqual(external.read_text(), "SAFE\n")
            store.unlink()
            lock = root / "facts.jsonl.lock"
            lock.unlink()
            lock.symlink_to(external)
            with self.assertRaises(OSError):
                fact_store_add(store, event, allowed_producers={"p1"})
            self.assertEqual(external.read_text(), "SAFE\n")

    def test_unmanaged_jsonl_files_are_not_ingested(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "archive").mkdir()
            (root / "archive" / "notes.backup.jsonl").write_text("not json\n")
            self.assertEqual(ingest._OutboxForIngest(str(root)).read_all(), [])

    def test_managed_symlink_is_not_ingested(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            journal = root / "journal"
            journal.mkdir()
            outside = root / "outside.jsonl"
            outside.write_text(json.dumps(make_event(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )) + "\n")
            (journal / "2026-08-13.jsonl").symlink_to(outside)
            self.assertEqual(ingest._OutboxForIngest(str(root)).read_all(), [])

    def test_symlinked_managed_directory_is_not_ingested(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "root"
            outside = Path(d) / "outside"
            root.mkdir()
            outside.mkdir()
            (outside / "2026-08-13.jsonl").write_text(json.dumps(make_event(
                "p1", "host.status", "node-a",
                {"host": "node-a", "reachable": True},
            )) + "\n")
            (root / "journal").symlink_to(outside, target_is_directory=True)
            self.assertEqual(ingest._OutboxForIngest(str(root)).read_all(), [])


if __name__ == "__main__":
    unittest.main()


class TestSyncRepoRealGit(unittest.TestCase):
    """sync-repo against a REAL temp git repo (proves actual git commands)."""

    def test_real_git_commit_and_emits(self):
        import subprocess
        with tempfile.TemporaryDirectory() as d:
            repo = Path(d) / "repo"
            repo.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.email", "t@t"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "t"], cwd=repo, check=True)
            # Keep the temp repository hermetic when the host's global Git
            # config enables commit signing but has no signer on PATH.
            subprocess.run(["git", "config", "commit.gpgsign", "false"],
                           cwd=repo, check=True)
            (repo / "state.toml").write_text("x = 1\n")
            subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
            # now a pending change
            (repo / "state.toml").write_text("x = 2\n")
            root = Path(d) / "root"
            root.mkdir()
            emitted = []
            from anvil_events import ingest
            real_emit = ingest.cli_cmd_emit
            def captured(kind, payload, correlation, host, root):
                emitted.append((kind, payload, correlation))
                return real_emit(kind, payload, correlation, host, root)
            args = cli.argparse.Namespace(
                dir=str(repo), correlation="promote-e2e", push=False,
                root=str(root), host="node-a")
            rc = cli.cmd_sync_repo(args, _git=ingest.git_sync, _emit=captured)
            self.assertEqual(rc, 0)
            # git log shows the new commit
            log = subprocess.run(["git", "log", "--oneline", "-2"], cwd=repo,
                                 capture_output=True, text=True, check=True).stdout
            self.assertIn("ops: adopt recorded state", log)
            # both events, correlation-linked, repo.synced ok=True
            self.assertEqual([k for k, _, _ in emitted],
                             ["config.adopted", "repo.synced"])
            self.assertTrue(all(c == "promote-e2e" for _, _, c in emitted))
            synced = emitted[1][1]
            self.assertTrue(synced["ok"])
            self.assertTrue(synced["committed"])
