# Baseline maturity and live-readiness assessment

- Assessed revision: `a1c099f53b90c2d4ee1cb2dbd7671298d607a30a`
- Date: 2026-08-16
- Verdict: **do not ship as fleet software**

## What was proven

- The current GitHub CI is green on Linux with Python 3.11, 3.12, and 3.13.
- In a clean Linux environment with declared development extras, 134 hermetic
  tests pass.
- The legacy POSIX store has unusually strong crash-tail, symlink, directory
  swap, PubAck, deduplication, and retention regression tests.
- A wheel and source distribution build successfully.
- A read-only reference-deployment inspection found a running JetStream broker
  with retained messages and consumers.

## What was not proven

- Native Windows execution: the baseline test suite produced 81 errors and two
  failures, primarily because `Outbox` and the fact store import `fcntl` and use
  POSIX directory-descriptor operations. The wheel is incorrectly tagged as
  platform-independent despite that runtime behavior.
- Fleet propagation: only one reference node had the package installed, its
  events health endpoint refused connections, and the other inspected nodes did
  not have the package installed.
- Secure producer identity: the observed broker and client did not require TLS
  or authentication, so the producer field was a claim inside attacker-controlled
  JSON rather than an authenticated principal.
- Lifecycle coverage: the public seam is disabled when its optional config is
  absent, and the reference private operator homes did not contain a tracked
  enabling configuration.
- Safe repository synchronization: the adapter stages every change with
  `git add -A` and may push it.

## Code structure

At the assessed revision, the six source modules contain approximately 2,570
lines. `outbox.py` alone contains 1,181 lines and owns unrelated concerns:
schema vocabulary, envelope construction, low-level file primitives, locking,
pending selection, acknowledgement, cursors, retention, a research queue
prototype, and graph verification. `ingest.py` combines validation, redaction,
fact persistence, source replay, Git mutation, and CLI registration.

High-complexity paths include event validation, pending-batch selection,
retention, fact persistence, and NATS receive parsing. Hot paths repeatedly scan
or rewrite complete retained files, producing quadratic growth across sequential
emits, subscriber deduplication, facts, and acknowledgements.

## Research alignment

The research mapping overclaims two results:

1. The LogPlayer target queue exists only as an isolated class/test. The daemon
   does not use it, the event log lacks the paper's single global entry index,
   and JetStream already implements durable-consumer recovery.
2. The causal checker detects cycles in a dependency DAG. The cited database
   causal-consistency algorithm additionally requires program-order,
   write-read, return-value, and bad-pattern semantics. DAG acyclicity alone is
   not causal-consistency conformance.

ADR-0003 narrows the claims and maps the useful research guarantees to the
actual JetStream-plus-reconciler architecture.

## Maturity score

| Category | Score | Evidence |
|---|---:|---|
| Arithmetic | Moderate (2/4) | Size/sequence checks exist; negative retention was accepted. |
| Auditing | Moderate (2/4) | Events and health exist; live service and incident signals drifted. |
| Authentication/access | Weak (1/4) | Payload allowlist without authenticated transport identity. |
| Complexity | Weak (1/4) | God modules, duplicated validation, and scan/rewrite hot paths. |
| Decentralization/resilience | Moderate (2/4) | Local-first avoids producer loss; one broker is a liveness dependency. |
| Documentation | Moderate (2/4) | Rich ADRs/tests; state, platform, security, and research claims drifted. |
| Ordering/transactions | Satisfactory (3/4) | Producer sequences, PubAck gating, and dedup are explicit. |
| Low-level manipulation | Moderate (2/4) | Strong POSIX tests, but bespoke protocol/filesystem code is platform-specific. |
| Testing | Moderate (2/4) | Strong Linux suite; no native Windows/macOS CI or current fleet acceptance. |

Overall: **17/36 (1.89/4)**. The baseline is a serious prototype, not a fleet
product.

## Redesign acceptance boundary

The next release is not complete until it demonstrates:

- small composable domain, store, transport, reconciliation, and runtime modules;
- indexed transactional storage on Windows, macOS, and Linux;
- explicit, lossless legacy migration with source hashes unchanged;
- authenticated TLS fleet transport and subject-scoped principals;
- local recording that never waits for the broker;
- registered adapters with preview, policy, verify, rollback, and outcome events;
- one reference desired-state revision converging across a canary and then every
  intended node through separately approved private rollout gates.
