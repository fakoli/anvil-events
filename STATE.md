# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** M2 (finish) — awaiting re-review of review fixes
- **Current commit:** [SET AT COMMIT] (public main)
- **In progress:** M2-finish review fixes done (HPUB framing + headers:true +
  header validation; honest ensure_stream; gc lock + dir fsync + unique
  degraded; degraded via o.emit unique seq). Awaiting GPT-5.6 re-review.
- **Open review:** M2 adversarial review returned REQUEST CHANGES (6 findings,
  all fixed); re-review dispatched after commit
- **Next action:** re-review verdict → fix any residual → update STATE → M3
- **Tests/quality:** 33 pass · ruff clean · live HPUB accepted by real broker
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
