# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M2 (finish) — Core CLI + outbox + JetStream + degraded + gc
- **Current commit:** [SET AT COMMIT] (public main)
- **In progress:** M2-finish done (JetStream HPUB dedup publish, gc size guard +
  rotate + event.degraded, degraded-on-emit-failure). Awaiting adversarial
  review of the merged M2.
- **Open review:** pending GPT-5.6 adversarial review of M2-finish (dispatch after commit)
- **Next action:** dispatch review → fix feedback → update STATE → M3
- **Tests/quality:** 31 pass · ruff clean
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
