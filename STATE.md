# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** ✅ COMPLETE — M1–M5 all approved (anvil-events v1.0)
- **Current commit:** fb7c645 (anvil-events main; M5 APPROVED). Broker: dedicated
  LaunchAgent `com.fakoli.nats-server` (config, JetStream, KeepAlive).
- **In progress:** None. Full milestone chain closed: M1 public release, M2
  durable outbox, M3 anvil-serving seam (PR #402), M4 operator adapter +
  Hermes ingestion, M5 retention enforcement + observability + broker
  persistence. All gates APPROVE.
- **Open review:** none.
- **Next action:** maintain — periodic `anvil events gc` / cron ingestion
  monitor degraded signal; consider rolling the private operator wrapper
  into a standing deployment.
- **Tests/quality:** 50 pass · ruff clean · CI green (fb7c645).
- **Notes:** host daemon healthy incl. degraded observability
  (`pending:6` = unpublished events visible); broker persistent (KeepAlive
  verified); Hermes ingestion cron `5fb3e7110183` live (validated, dedup,
  fact-store hook).

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
- **M5** ✅ 2026-08-13: retention enforcement (gc hard-cap eviction) +
  health observability (pending/degraded_events) + broker persistence
  (LaunchAgent + JetStream) merged 5a7f9ae; two review-fix rounds
  (strict epoch regex in 20ab720 + fb7c645); gate APPROVE (deleg_31aaaedf).
  **M1–M5 complete — v1.0.**
