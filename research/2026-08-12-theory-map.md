# Research-to-design map, audited for v2

- Original spike: 2026-08-12
- Corrected: 2026-08-16
- Status: bounded design input, not an equivalence or formal-proof claim

The local paper texts are in `research/papers/`. This note states exactly what
the product adopts, what it does not implement, and which executable property
stands in its place.

## Summary

| Paper | Useful idea | V2 decision | Claim we do not make |
|---|---|---|---|
| LogPlayer (arXiv:1911.11286) | Separate safety, liveness, recovery, and target cursor contracts. | JetStream owns retained-log recovery; node state owns idempotent resource generations and apply attempts. | The unused v1 queue was not LogPlayer, and external apply is not globally exactly once. |
| Delivery, consistency, determinism (arXiv:1907.06250) | “Exactly once” is insufficient without consistency and determinism semantics. | State the product contract on separate delivery, consistency, determinism, and liveness axes. | A PubAck is not proof of a consistent or exactly-once external side effect. |
| Checking Causal Consistency (arXiv:2011.09753) | Conformance can be reduced to bad-pattern/cycle checks over a precisely defined history graph. | Keep a dependency-DAG checker for explicit causes and producer order. | DAG acyclicity alone is not causal-consistency conformance. |
| Lamport’s Arrow of Time (arXiv:2602.21730, preprint) | Logical order should not be confused with a universal physical order. | Use producer sequence and resource generation; reject a fleet-global-clock claim. | The preprint is not a correctness proof for this implementation. |

## 1. LogPlayer

LogPlayer plays entries from one indexed WAL to storage targets. Its target
queue algorithm assumes terms, one recovery fetch range, per-target normal and
catch-up queues, and a target that can consume an entry while durably storing
its last consumed index. Those assumptions are not the v1 or v2 event model.

The v1 `TargetQueue` copied the S/RF/FC/N state machine but the daemon never
called it. V1 event IDs were per producer, not a single WAL index. Calling that
class an implementation—and inferring exactly-once fleet delivery from its unit
tests—was incorrect.

V2 adopts only the contract decomposition:

- **Safety:** one resource generation cannot name conflicting desired bytes;
  one event identity cannot name conflicting envelopes.
- **Recovery:** JetStream retains publishes and redelivers an unacknowledged
  durable consumer after disconnection.
- **Target state:** a node durably records its attempt and applied generation.
- **External apply:** an adapter must be idempotent/reconcilable, or expose an
  indeterminate result after a crash it cannot classify.
- **Liveness:** after source, broker, node, and policy faults stop, the latest
  desired generation can be fetched and applied.

No second in-memory recovery queue is built. The relevant executable probes
are duplicate event/generation conflicts, PubAck-gated pending state, durable
consumer redelivery, idempotent outcome recording, and crash/fault injection
around adapter apply and verify.

## 2. Delivery, consistency, and determinism

The paper’s key contribution here is taxonomy. V2 uses it directly:

- **Delivery:** local acceptance is durable; broker and consumer delivery are
  at least once.
- **Consistency:** a desired revision is bound to exact artifact bytes; an
  applied outcome follows adapter verification.
- **Determinism:** canonical JSON, immutable identities, monotonic generation
  rules, and exact resource-to-adapter policy make a replay decision stable.
- **External nondeterminism:** controller calls, service reloads, and crashes
  can still be indeterminate. They are not hidden behind an exactly-once label.

The normative table is in `prd.md`. Tests cover local idempotency, identity
equivocation, PubAck evidence, artifact mismatch, apply indeterminacy,
verification rollback, and replay.

## 3. Checking causal consistency

The cited algorithm needs a declared object model and a history containing
program order, reads, writes, returned values, and model-specific bad patterns.
The repository has events with explicit predecessor IDs and producer order; it
does not have those database semantics.

`DependencyGraphChecker` therefore proves only:

1. identical duplicate envelopes collapse;
2. conflicting duplicate identities fail;
3. explicit `causes` plus producer-order edges contain no cycle.

The CLI says `dependency graph: OK`, not `causal consistency: OK`. A future
causal-consistency claim would first define resources as objects, define read
and write operations/values, select the target consistency model, implement
its bad-pattern checks, and validate against the paper’s examples.

## 4. Logical ordering

The 2026 preprint is treated as design context. Independent of its broader
argument, the implementation has no defensible need for a global sequence:
unrelated resource updates may be observed in different orders without
violating convergence.

V2 uses two scoped orders:

- `producer_seq` gives identity and program order for one producer;
- `generation` gives desired-state order for one resource authority.

Wall-clock timestamps are diagnostics. They never decide generation conflict,
authorization, or whether an adapter may apply.

## Evidence boundary

Passing hermetic tests proves code behavior under the tested faults. It does
not prove JetStream configuration, TLS/ACL enforcement, private manifest
correctness, application reload behavior, or live fleet convergence. Those are
separate Compose, secured-broker negative, canary, and staged live-acceptance
gates in `prd.md`.
