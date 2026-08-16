# STATE.md — current position

- **Redesign baseline:** `a1c099f53b90c2d4ee1cb2dbd7671298d607a30a`.
- **R1/R2 implementation revision:**
  `af688436216a84225eb494fb08f7c7a3057f1349` on `main` (PRs
  [#1](https://github.com/fakoli/anvil-events/pull/1) and
  [#2](https://github.com/fakoli/anvil-events/pull/2), 2026-08-16).
- **Current milestones:** R1 audit/research correction/composable storage and
  R2 secure transport/reconciliation/portable runtime are complete in source.
  No package installation, service restart, route change, private manifest, or
  live deployment was performed.
- **Formal milestone review:** closed. The one merged-tree `gpt-5.6-sol`
  review of `734d450` returned `REQUEST_CHANGES` with eight code blockers:
  external-apply serialization/recovery, operation intent resolution,
  journal-only corruption recovery, SQLite initialization crash safety,
  post-create stream verification, wrong-stream PubAck, credential-shaped
  payloads, and idle health shutdown. PR #2 reproduced and fixed all eight.
  Independent recall-mode verification also found and fixed simultaneous WAL
  negotiation contention and bounded lock-file growth.
- **Evidence:** 130 hermetic tests pass natively on Windows and Linux with
  `ResourceWarning` promoted to error (one Windows-only test is skipped on
  Linux); measured line coverage is 80%. `ruff`, deterministic gate routing,
  CLI hygiene, wheel/sdist build, installed-wheel smoke, and exact-head
  Windows/macOS/Linux CI pass. Clean Compose proves stream create plus
  post-create verification, desired apply, pending durability during broker
  outage, and automatic catch-up. The ephemeral fleet broker proves mTLS
  identity mapping, mapped publish/cross-node delivery, exact ACK,
  foreign-prefix and foreign-consumer denial, and unmapped-certificate
  rejection. Synthetic containers and volumes were removed.
- **Architecture:** ADR-0003. V2 uses SQLite, asynchronous exact-stream PubAck
  delivery, authenticated TLS fleet mode, node-bound producers/subjects,
  immutable logical artifacts, monotonic resource generations, policy-bound
  adapters, cross-process apply serialization, durable indeterminate recovery,
  verification/rollback, and durable outcomes. V1 is migration/read
  compatibility only.
- **Research correction:** the removed runtime did not implement LogPlayer and
  the dependency DAG checker is not a causal-consistency proof.
- **Public/private boundary:** no private operator file changed. Real topology,
  addresses, active routes, manifests, credentials, and rollout evidence remain
  private and require a clean private worktree plus a separate deployment gate.

## Next actions

1. Design and implement R3 in the public Anvil Serving lifecycle seam at the
   exact reviewed Anvil Events revision.
2. Author R4 private node manifests and canary evidence only after separate
   operator authorization.
3. Do not install, restart, change routing, or claim live fleet convergence
   until the private canary and staged-rollout gates pass.
