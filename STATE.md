# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M5 — rollout + observability (implementation done, gate pending)
- **Current commit:** 5a7f9ae (anvil-events main; M5 code merged). Broker: dedicated
  LaunchAgent `com.fakoli.nats-server` (config, JetStream, KeepAlive).
- **In progress:** M5 implementation DONE + deployed. Hard-cap retention
  enforcement (`gc` evicts oldest rotated overflow), health surfaces
  `pending`+`degraded_events` (degraded signal), broker is now a persistent
  service (JetStream with persisted storage). 50 tests, CI green.
- **Open review:** M5 boundary review (pending dispatch)
- **Next action:** M5 boundary adversarial review → fix findings → mark M5 ✅.
- **Tests/quality:** 50 pass (48 + hard-cap eviction + health observability) ·
  ruff clean · CI green (5a7f9ae).
- **Notes:** host daemon healthy incl. degraded observability
  (`pending:6` = unpublished events visible); uv tool reinstalled (cache
  busted) with M5 code; Hermes ingestion cron `5fb3e7110183` live.

---

## Milestone log

- **M1** ✅ 2026-08-12/13: public repo; PRD/vocab/ADR-0001; code (outbox,
  nats_mini, cli, daemon, deploy/); 4-round review; deployed host daemon.
- **M2** ✅ 2026-08-12: durable outbox + HPUB JetStream-compatible publish +
  degraded signaling + flock GC; 37 tests; gate APPROVE (deleg_e5afc93e).
- **M3** ✅ 2026-08-12/13: anvil-serving `[events]` seam (optional events.toml);
  exact no-op boundaries; PR #402 merge 31ab847; gate APPROVE (deleg_eb46ba6a).
- **M4** ✅ 2026-08-13: sync-repo + ingest verbs merged 1dd68d7; private
  wrapper + runbook @ 03a8ccc; Hermes validated-ingestion cron live;
  fixes for 4 review blockers (false-boolean, git failure semantics,
  status-failure, public-safety) in 10ec6a3; gate APPROVE (deleg_0100c2a2).
- **M5** 🔄 2026-08-13: retention enforcement (gc hard-cap eviction) +
  health observability (pending/degraded_events) + broker persistence
  (LaunchAgent + JetStream) merged 5a7f9ae; deployed; gate pending.
