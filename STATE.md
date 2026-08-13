# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M2 (finish) — Core CLI + outbox + JetStream + degraded + gc
- **Current commit:** 9cee593 (public main; daemon + deploy/ added)
- **In progress:** M2-finish items — JetStream stream/consumer config, dedup
  publish (event_id), `event.degraded` wiring, gc size guard
- **Open review:** none currently (M1 review loop closed with APPROVE)
- **Next action:** implement M2-finish, then PR + merge + adversarial review
- **Tests/quality:** 26 pass · ruff clean (expected baseline for M2-finish)
- **Notes:** deploy daemon on Mini (launchd) done + verified (KeepAlive).
  nats-server is NOT yet a persistent service (gap → fix in M2-finish or M5).

---

## Milestone log

- **M1** ✅ 2026-08-12/13: repo created public; PRD/vocab/ADR-0001; code
  (outbox, nats_mini, cli, daemon, deploy/, research library + origin story);
  4-round adversarial review loop (Reject → REQUEST CHANGES ×2 → APPROVE).
  Final commit 178a694; deployed daemon on Mini as launchd agent.
- **M2** 🔄 started: CLI pub/sub/emit/verify/gc + outbox (archive-before-pending,
  flock, torn-line) + nats client hardening + causal checker (explicit causes).
  Remaining: JetStream config, dedup publish, event.degraded, gc size guard.
