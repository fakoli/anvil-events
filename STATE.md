# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M5 — rollout + observability (event.degraded monitoring, retention enforcement)
- **Current commit:** 10ec6a3 (anvil-events main; M4 APPROVED). Private wrapper: ops-private @ 03a8ccc.
- **In progress:** M4 ✅ complete (final gate APPROVE deleg_0100c2a2, 2026-08-13).
  M5 starting: `anvil events status` on each host, event.degraded monitoring,
  retention/rotation enforced, nats-server as a persistent service.
- **Open review:** none (M4 closed APPROVE)
- **Next action:** M5 rollout + observability (status on hosts, degraded
  monitoring, retention enforcement, broker persistence).
- **Tests/quality:** 48 pass (37 baseline + 11 M4) · ruff clean · CI green.
- **Notes:** host daemon healthy; anvil-events uv-tool has M4 verbs; Hermes
  ingestion cron `5fb3e7110183` live (validated, dedup, fact-store hook);
  nats-server still NOT a persistent service (gap → M5).

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
