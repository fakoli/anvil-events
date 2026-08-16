# Product requirements: generic fleet convergence events

## Problem

A small fleet can have one system that owns routing or model desired state and
several heterogeneous systems that consume it: agent gateways, compute nodes,
evaluation workers, or specialist services. Today an operator often visits
each host after a model promotion or route change and edits client state by
hand. Missed updates leave a fleet internally inconsistent.

The product must let an authority record one immutable desired revision and let
each node converge the resource it owns. The private Anvil Serving deployment
is the reference design, not product-specific code.

## Goals

1. Record lifecycle and desired-state events durably before attempting network
   delivery.
2. Deliver through a recoverable fleet log and catch up after disconnection.
3. Reconcile narrow node-owned resources with preview, policy, verification,
   rollback, and durable outcomes.
4. Run from one stdlib-only package on Windows, macOS, and Linux, natively or
   in a thin container.
5. Keep public product contracts separate from private topology, credentials,
   active routes, and deployment evidence.

## Non-goals

- Distributed consensus, leader election, or multi-writer merging.
- Raw file synchronization or arbitrary command execution.
- Replacing an artifact/source repository or the serving control plane.
- Git add/commit/push from an event consumer.
- A global total order or globally exactly-once external side effects.
- Treating tailnet membership as producer authentication.

## Reference roles

- `route-authority`: owns a resource generation and exact desired artifact.
- `gateway-agent`: consumes routes and updates agent/provider client state.
- `compute-service`: consumes the subset of serving configuration it owns.
- `evaluation-worker`: observes revisions without hosting model state.
- `specialist-service`: consumes a narrow voice/media/other resource.

No public artifact may contain real role assignments, node addresses, active
routes, credentials, or operator paths.

## Architecture

### Domain

V2 envelopes are closed at the top level but extensible by dotted `kind`.
`producer` is node-owned (`node-a:router` belongs to `node-a`), identity is
`producer + producer_seq`, and `subject` is derived from node and kind.

`state.desired` requires:

- `resource`: portable logical identity;
- `generation`: positive authority-assigned integer;
- `revision`: immutable source revision;
- `content_sha256`: digest of the exact artifact bytes;
- `adapter`: locally registered narrow adapter;
- `artifact`: logical reference resolved through a configured source;
- optional `targets`: explicit node tokens.

Credential-shaped keys, non-JSON numbers, capability URLs in event-controlled
fields, arbitrary local paths, duplicate causes, and oversized payloads fail
validation.

### Local state and acceptance

SQLite owns the event journal, producer sequences, pending delivery, PubAck
evidence, cursors, operation records, reconciliation attempts, applied
generations, facts, quarantine, and migration provenance. It uses WAL,
`synchronous=FULL`, foreign keys, application ownership, and immediate write
transactions.

`record` commits the idempotency key and canonical event in one transaction.
It never contacts the broker. A conflicting event ID, producer sequence,
idempotency key, resource generation, or PubAck fails closed.

Legacy POSIX JSONL is read-only. Explicit migration locks or requires an
offline source, rejects torn/malformed/equivocating input, fingerprints the
source, imports in one transaction, records provenance, verifies SQLite
integrity, re-fingerprints the source, and never deletes it.

### Transport

An independent worker selects indexed pending rows, publishes with
`Nats-Msg-Id=event_id`, and changes pending state only while recording positive
PubAck stream/sequence evidence and the producer cursor in one transaction.
Failure leaves the canonical event pending with bounded diagnostic state.

- Development mode permits `nats://` on literal loopback or an explicitly
  allowlisted single-label host in an isolated container network.
- Fleet mode requires `tls://`, server-name verification, and username/password
  or mTLS. TLS-first is explicit; there is no downgrade.
- The actual broker subject must equal the envelope subject.
- Per-node server ACLs bind principals to node subject prefixes and fixed
  durable-consumer control subjects.

JetStream is the replicated log, deduplication window, and recovery mechanism.
The default stream does not expire history because a newly enrolled node or a
node offline beyond an arbitrary window must still observe current desired
state. Finite retention is valid only after a tested desired-state snapshot or
per-resource compaction mechanism exists. Referenced immutable artifacts must
remain available for every replayable desired generation.
The product does not reimplement the LogPlayer paper's in-memory target queue.

### Reconciliation algorithm

For each desired event on a node:

1. Validate schema, authorized producer, actual subject, target, resource,
   generation, revision, adapter, and digest.
2. Journal the desired event before external work.
3. Claim `(node, resource, generation)` durably. Identical replay is a no-op;
   stale generations are superseded; generation equivocation fails closed.
4. Resolve the logical artifact from the locally configured directory or
   authenticated HTTPS controller source.
5. Require exact revision and SHA-256 bytes.
6. Run adapter preview and exact authority-producer/resource/adapter policy.
7. If policy denies, record `reconcile.awaiting_approval` and leave the broker
   delivery unacknowledged for later policy change/redelivery.
8. Apply only locally configured state, verify observed bytes/state, and roll
   back a verification failure.
9. Durably record applied/failed/indeterminate state and an idempotent outcome
   event caused by the desired event.
10. ACK the desired delivery only after durable local processing.

The built-in managed-file adapter uses a configured destination, validates
JSON/TOML when requested, refuses destination symlinks, writes a same-directory
temporary file, fsyncs, atomically replaces, verifies the digest, and can roll
back within the running attempt. A crash during a non-atomic external adapter
is `INDETERMINATE` and is never silently retried.

## Reliability contract

| Axis | Contract |
|---|---|
| Local acceptance | Exactly one canonical event per idempotency key. |
| Broker delivery | At least once; PubAck proves stream persistence. |
| Local journal | One canonical row per event ID; conflicts rejected. |
| Ordering | Per producer plus per-resource generation; no global sequence. |
| External application | Idempotent at least once unless an adapter proves stronger atomicity. |
| Consistency | Desired bytes are revision- and digest-bound; outcome follows verify/rollback. |
| Determinism | Canonical JSON and conflict rules make replay decisions repeatable. |
| Liveness | After faults stop, policy permits, and replayable artifacts remain available, healthy subscribed nodes converge. |

## Research alignment

- **LogPlayer (2019):** provides a useful safety/liveness/target-cursor
  vocabulary. Its algorithm assumes a single indexed WAL and atomic target
  consumed index. This product instead delegates replay/redelivery to
  JetStream and keeps an idempotent node apply ledger. No LogPlayer
  implementation or exactly-once side-effect claim is made.
- **Delivery, consistency, determinism (2019):** motivates stating guarantees
  by separate axes. The table above is normative.
- **Checking Causal Consistency (2020/2021):** the implemented checker only
  tests acyclicity of explicit causes and producer order. Without object
  reads/writes, returned values, and model-specific bad patterns, it is
  dependency-DAG integrity—not causal-consistency conformance.
- **Lamport's Arrow of Time (2026 preprint):** reinforces the decision not to
  turn local clocks into global truth. It is context, not a correctness proof.

See [`research/2026-08-12-theory-map.md`](research/2026-08-12-theory-map.md).

## Security requirements

1. Event bodies contain no credential values or capability-bearing URLs.
2. Endpoint URLs contain no userinfo, query, or fragment.
3. Fleet transport has TLS verification and an authenticated node principal.
4. Principal ACLs restrict publish node prefix, inboxes, fixed delivery
   subject, fixed durable-create API, and ACK subjects.
5. Node config binds exact authority producers and resources to adapters and
   configured destinations.
6. Artifact HTTP rejects redirects, bounds response size, requires exact
   revision metadata, and obtains any bearer token from a named environment
   variable at request time.
7. Health reports local acceptance separately from fleet readiness and does
   not print broker URLs or credentials.
8. Public and private repository ownership boundaries are mandatory.

## Acceptance criteria

### Source gates

- No Python module exceeds 300 lines without a documented exception.
- `ruff check .` passes.
- Hermetic `unittest` suite passes with `ResourceWarning` promoted to error.
- Native CI passes on Windows, macOS, and Linux for Python 3.11 and 3.13.
- Wheel and source distribution build and the installed wheel CLI starts.
- Legacy source hashes are unchanged after success and every failed migration
  probe.

### Broker and failure gates

- Development Compose creates or exactly verifies `ANVIL_EVENTS`, then proves
  local record → PubAck → durable consumer → reconcile → outcome.
- Broker outage preserves pending work; restart catches up without duplicate
  external apply.
- Forged producer, actual/envelope subject mismatch, unknown adapter,
  conflicting generation, wrong revision, and wrong digest all fail closed.
- TLS standard-upgrade and TLS-first paths both pass; a plaintext fleet
  endpoint and a fleet principal without auth fail before CONNECT credentials.
- Negative ACL probes prove each node cannot publish another node's prefix.

### Private rollout gates

1. Resolve public/private workspace ownership and pin exact public revision.
2. Author private manifests and credentials references in a clean private
   worktree; no real topology enters public files.
3. Preview every adapter and prove rollback without altering active routing.
4. Install a non-authoritative canary and record disconnect/catch-up evidence.
5. Enable the route authority lifecycle seam only after local record failure
   semantics pass on its actual OS/runtime.
6. Stage gateway/agent, worker, and specialist roles separately.
7. Prove one desired route generation yields verified local client state and a
   correlated outcome on every intended node.
8. Keep install/restart/route/promotion authorization separate from source
   merge.

## Delivery milestones

| Milestone | Scope | Current status |
|---|---|---|
| R1 | Baseline audit, research correction, composable domain/storage | Implemented locally; review and CI pending |
| R2 | Secure transport, runtime, reconciliation, portable deployment | Implemented locally; Compose fault proof passed; review/CI pending |
| R3 | Public Anvil Serving lifecycle seam using local `record` | Pending |
| R4 | Private node manifests, artifact/controller adapter, canary | Pending separate approval |
| R5 | Staged fleet rollout and live acceptance | Pending separate approval |
