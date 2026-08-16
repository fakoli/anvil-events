# STATE.md — current position

- **Base revision:** `a1c099f53b90c2d4ee1cb2dbd7671298d607a30a`
  (`origin/main`, synchronized 2026-08-16).
- **Working branch:** `feat/fleet-event-core-v2` in the isolated
  `anvil-events-wt-fleet-redesign` worktree.
- **Current milestones:** R1 and R2 implemented locally; no commit,
  push, PR, merge, package installation, service restart, route change, or live
  deployment yet.
- **Formal milestone review:** not dispatched. Independent baseline storage and
  transport reviews both returned DO NOT SHIP; their valid findings drove the
  redesign.
- **Local evidence:** 117 hermetic tests pass natively on Windows with
  `ResourceWarning` promoted to error and in WSL Linux (one Windows-only test
  skipped); `ruff check .`, the deterministic gate router, CLI hygiene scan,
  source/wheel build, and 79% line coverage pass. Clean Compose proves stream
  create/exact re-verification, desired apply, pending durability during broker
  outage, and automatic catch-up; its synthetic volumes were removed. The
  ephemeral fleet broker also proves mTLS identity mapping, mapped
  publish/cross-node delivery, exact ACK, foreign-prefix and foreign-consumer
  denial, and unmapped-certificate rejection. Remote CI and live fleet
  acceptance are still pending.
- **Architecture:** ADR-0003. V2 uses SQLite, asynchronous PubAck delivery,
  authenticated TLS fleet mode, node-bound producers/subjects, logical desired
  artifacts, monotonic resource generation, narrow adapters, policy,
  verification/rollback, and durable outcomes. V1 is migration/read
  compatibility only.
- **Research correction:** the removed runtime did not implement LogPlayer and
  the dependency DAG checker is not a causal-consistency proof.
- **Reference deployment observation:** the pre-redesign package was present on
  only one inspected node, its events health endpoint was not live, and the
  observed broker was not authenticated/TLS. These are baseline observations,
  not current deployment claims.
- **Public/private boundary:** no private operator file has been changed. Real
  topology, addresses, active routes, manifests, credentials, and rollout
  evidence remain private and require a clean private worktree plus the
  separate deployment gate.

## Next actions

1. Commit/push/PR/merge the R1/R2 source milestone according to `AGENTS.md`.
2. Obtain Windows/macOS/Linux and Compose results on the exact PR head.
3. Dispatch the one formal GPT-5.6 adversarial code review on the merged exact
   tree and fix every real correctness/security/durability/race finding.
4. Design R3 changes in the public Anvil Serving seam. Do not install or deploy
   until the separate private canary gate is authorized.
