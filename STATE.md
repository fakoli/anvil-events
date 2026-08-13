# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M4 — private operator adapter (commit-push-on-promote) + Hermes validated ingestion
- **Current commit:** 1dd68d7 (anvil-events main; M4 verbs merged). Private wrapper: ops-private @ 03a8ccc.
- **In progress:** M4 implementation DONE + live-proven. Public: `sync-repo`+`ingest`
  verbs (44 tests, ruff clean, real-git E2E, live commit+emit probe, live
  validated ingest with forged-drop). Private: `scripts/promote-commit-push.sh`
  + runbook. Hermes: cron `5fb3e7110183` (every 4h) validated ingestion to
  `~/.anvil/events/facts.jsonl` (2 facts already stored from the live journal).
  Remaining: M4 boundary review gate, then PR/merge + STATE.md pointer.
- **Open review:** M4 boundary review (pending dispatch)
- **Next action:** M4 boundary adversarial review (code-correctness-only rule) →
  fix findings → PR/merge → mark M4 ✅ → M5.
- **Tests/quality:** 44 pass (37 baseline + 7 M4) · ruff clean · wheel builds ·
  live ingest stored 2 facts, dropped forged.
- **Notes:** Mini daemon healthy (`{"received":5,"journaled":1,"dropped":4}`);
  anvil-events uv-tool reinstalled with M4 verbs; nats-server still NOT a
  persistent service (gap → M5).

---

## Milestone log

- **M1** ✅ 2026-08-12/13: public repo; PRD/vocab/ADR-0001; code (outbox,
  nats_mini, cli, daemon, deploy/); 4-round review; deployed Mini daemon.
- **M2** ✅ 2026-08-12: durable outbox + HPUB JetStream-compatible publish +
  degraded signaling + flock GC; 37 tests; gate APPROVE (deleg_e5afc93e).
- **M3** ✅ 2026-08-12/13: anvil-serving `[events]` seam (optional events.toml);
  exact no-op boundaries; PR #402 merge 31ab847; gate APPROVE (deleg_eb46ba6a).
- **M4** 🔄 2026-08-13: sync-repo + ingest verbs merged 1dd68d7; private
  wrapper + runbook @ 03a8ccc; Hermes validated-ingestion cron live.
