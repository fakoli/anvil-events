# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M4 — private operator adapter (commit-push-on-promote) + validated Hermes subscriber/ingestion
- **Current commit:** bdda02e (anvil-events main; M3 seam landed in fakoli/anvil-serving@31ab847)
- **In progress:** M3 ✅ complete (feat(serves): add anvil-events lifecycle seam, PR #402,
  squash 31ab847, final gate APPROVE deleg_eb46ba6a, 2026-08-12/13). M4 starting:
  private operator adapter = commit/push on promote + repo.synced/config.adopted +
  validated Hermes subscription/ingestion.
- **Open review:** none (M3 closed APPROVE)
- **Next action:** implement M4 in the private operator workflow: the operator
  repo commit-push-on-promote hook (config.adopted / repo.synced events) and the
  Hermes subscriber that validates producer/envelope/kind before persisting.
- **Tests/quality:** anvil-events 37 pass · ruff clean (M2 baseline). anvil-serving
  4016 pass, 17 skipped (M3 baseline; controller-auth tests require the token env
  at runtime).
- **Notes:** deploy daemon on Mini (launchd) done + verified (KeepAlive).
  nats-server is NOT yet a persistent service (gap → fix in M2-finish or M5).

---

## Milestone log

- **M1** ✅ 2026-08-12/13: repo created public; PRD/vocab/ADR-0001; code
  (outbox, nats_mini, cli, daemon, deploy/, research library + origin story);
  4-round adversarial review loop (Reject → REQUEST CHANGES ×2 → APPROVE).
  Final commit 178a694; deployed daemon on Mini as launchd agent.
- **M2** ✅ 2026-08-12: durable outbox + HPUB JetStream-compatible publish +
  degraded signaling + shared-flock GC rotation + honest ensure_stream;
  37 tests, ruff clean, live-proven against Mini broker; gate APPROVE
  (deleg_e5afc93e). Public main at e84dd92.
- **M3** ✅ 2026-08-12/13: anvil-serving `[events]` seam (optional
  `$ANVIL_SERVING_HOME/events.toml`); serve up/down, profile enter/leave,
  promote applied/rolled_back recorded outbox-first with exact no-op
  boundaries (running-compose no-op records nothing; explicit --recreate
  records once). PR #402 merge 31ab847; gate APPROVE (deleg_eb46ba6a).
