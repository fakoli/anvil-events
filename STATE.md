# STATE.md — anvil-events position pointer

> Updated at every milestone boundary. The single source of truth for "what
> is done / what is next" across context compactions. See AGENTS.md §3.

- **Current milestone:** ✅ COMPLETE — M1–M5 all approved (anvil-events v1.0)
- **Current commit:** 7c69b86 (review improvements pending commit; M1–M5 remain
  approved). Broker: dedicated
  LaunchAgent `com.fakoli.nats-server` (config, JetStream, KeepAlive).
- **In progress:** final correction-gate review of post-v1.0 durability,
  liveness, schema, and security improvements. Full milestone chain remains
  closed: M1 public release, M2
  durable outbox, M3 anvil-serving seam (PR #402), M4 operator adapter +
  Hermes ingestion, M5 retention enforcement + observability + broker
  persistence. All gates APPROVE.
- **Open review:** exact-tree independent correction gates on the final
  candidate. The final gate found one remaining pathname-based dirfd gap
  (list-then-reopen by path in repair/pending/GC) — fixed by pinning every
  managed directory fd for list/stat/open/replace/unlink, converting all
  quarantine writes to a pinned-dirfd helper, and adding two dir-swap race
  regressions. A follow-up gate then found the GC deletion path still fsynced
  the archive directory via a pathname reopen (`_fsync_directory`); fixed by
  fsyncing the pinned archive dirfd and removing the dead helper, with a
  regression proving every directory fsync targets the original pinned inode
  across a path swap. A fresh exact-tree gate is running on the corrected
  tree.
- **Next action:** maintain — periodic `anvil events gc` / cron ingestion
  monitor degraded signal; consider rolling the private operator wrapper
  into a standing deployment.
- **Tests/quality:** 133 pass locally with JSON Schema format validation;
  Python 3.11/3.12/3.13 compatibility and repeated-suite stress verified;
  both independent durability/liveness probe packs pass; exact real-broker
  framed-size and independent-durable replay probes pass; ruff and diff clean;
  final CI pending commit.
- **Notes:** exact-tree two-macOS-host proof verified remote PubAck/archive,
  JetStream persistence, subscriber journaling/dedup, causal checking, and
  validated fact ingestion. Host daemon healthy incl. PubAck/retry
  observability (`pending:0`, broker connected); broker persistent (KeepAlive
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
