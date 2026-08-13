"""M4 tests: `anvil events sync-repo` (commit-push adapter) + `ingest` (validated ingestion)."""

import os
import tempfile
import unittest
from pathlib import Path

from anvil_events import cli, ingest
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

    def test_false_boolean_required_fields_accepted(self):
        # Reviewer M4-1: `ok=False` / `reachable=False` are VALID failure-state
        # events; required-field checking must NOT use truthiness.
        from anvil_events.ingest import validate_event
        ok, reason = validate_event({
            "version": 1, "kind": "repo.synced", "host": "node-a",
            "payload": {"repo": "r", "ok": False},
        })
        self.assertTrue(ok, f"False bool must be valid: {reason}")
        ok, reason = validate_event({
            "version": 1, "kind": "host.status", "host": "node-a",
            "payload": {"host": "h", "reachable": False},
        })
        self.assertTrue(ok, f"False bool must be valid: {reason}")

    def test_sensitive_fields_are_dropped_case_insensitively(self):
        from anvil_events.ingest import fact_store_add
        with tempfile.TemporaryDirectory() as d:
            store = os.path.join(d, "facts.jsonl")
            fact = fact_store_add(store, {
                "version": 1, "kind": "host.status", "host": "node-a",
                "payload": {"host": "h", "reachable": True,
                            "Token": "secret", "API_KEY": "k", "password": "p"},
            })
            self.assertIsNotNone(fact)
            payload = fact["payload"]
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
