# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M3 — anvil-serving `[events]` seam (in fakoli/anvil-serving)
- **Current commit:** e84dd92 (anvil-events main; M2 APPROVED)
- **In progress:** M2 ✅ complete (final gate APPROVE, deleg_e5afc93e, 2026-08-12).
  M3 starting: config-gated [events] seam in anvil-serving lifecycle commands
  (serves up/down, profile enter/leave, promote) -> shell out to anvil events
  emit, outbox-first, best-effort, stdlib-only.
- **Open review:** none (M2 closed APPROVE)
- **Next action:** implement M3 seam in ~/anvil-serving-t007, PR + merge, adversarial review (boundary rule: code correctness only)
- **Tests/quality:** anvil-events 37 pass · ruff clean (M2 baseline). anvil-serving suite must stay green.
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
