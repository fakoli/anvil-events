# ADR-0003: Fleet convergence, not file synchronization

- Status: accepted for the v2 redesign
- Date: 2026-08-16
- Deployment status: not deployed

## Context

The useful product is not a broadcast CLI. It is a convergence system for a
heterogeneous fleet. An authoritative service records a desired-state revision;
every interested node eventually observes it, applies only the configuration it
owns, verifies the result, and reports an outcome. A temporarily disconnected
node must catch up without an operator visiting it manually.

The current repository does not implement that product. It couples a large
POSIX JSONL outbox, a hand-written NATS client, a daemon, Git mutation, fact
projection, retention, and research prototypes. The public lifecycle seam is
optional and silently disabled when configuration is absent. The only operator
adapter stages an entire repository with `git add -A`. The live reference
deployment does not have a healthy events daemon or an installed node on every
host.

The reference topology is useful, but it is not the public product model. Its
portable roles are:

- a route authority that owns model and routing desired state;
- gateway/agent nodes that must update client/provider configuration;
- compute or service nodes that own local serving configuration;
- evaluation nodes that consume revisions without hosting models;
- optional specialist nodes, such as voice or media services.

Real identities, addresses, routes, credentials, operator homes, and raw
evidence remain in the private operator repository.

## Research audit

### LogPlayer

LogPlayer is a WAL-to-storage-shard player. It assumes a single indexed log,
per-target queues, a target that atomically consumes an entry and persists the
consumed index, and a recovery fetcher that can request an exact missed range.
Its S/RF/FC/N queue and term algorithms are meaningful inside that architecture.

The v1 code copied the queue state machine into `TargetQueue`, but the runtime
never uses it. The product has per-producer event IDs rather than the paper's
single log index, and JetStream already owns durable-consumer recovery and
redelivery. Calling that unused class a LogPlayer implementation overstates the
evidence.

The reusable result is the guarantee boundary:

- safety: a target never applies one logical revision out of resource order or
  applies a conflicting identity;
- liveness: after faults stop, every subscribed target eventually converges;
- target contract: applying a revision and persisting its local completion
  record must be atomic, or the apply operation must be idempotent and
  reconcilable after an indeterminate crash.

JetStream is the replicated log and recovery fetcher. The node reconciler is the
target. We do not reimplement a second in-memory recovery queue.

### Causal-consistency checker

The database paper cited by v1 reasons about histories containing program order,
write-read relations, returned values, and model-specific bad patterns. The v1
checker only verifies that explicit `causes` plus per-producer sequence edges
form a DAG. That is useful dependency-graph integrity, but it is not a proof of
causal consistency as defined by the paper.

V2 calls this property `dependency DAG integrity`. A future causal-consistency
claim requires a declared object model, read/write semantics, and the relevant
bad-pattern checks.

## Decision

Build five composable boundaries.

### 1. Domain

The domain package owns envelopes, event-type registration, canonical encoding,
identity conflict detection, dependency edges, and redaction. It has no storage,
transport, subprocess, or host-topology code.

Desired-state events carry generic fields:

- `resource`: a stable logical resource identifier;
- `generation`: a monotonically increasing integer assigned by that resource's
  authority;
- `revision`: an immutable source revision;
- `content_sha256`: the digest of the exact desired artifact;
- `adapter`: a registered portable adapter type;
- `operation_id` and `correlation_id` for idempotency and traceability.

The event carries a reference and digest, not credentials, capability-bearing
URLs, or an arbitrary host filesystem path.

### 2. Local transactional state

New nodes use a stdlib SQLite store with WAL, full synchronous commits, unique
event and `(producer, sequence)` constraints, indexed pending/journal/fact
roles, PubAck evidence, target cursors, desired generations, apply attempts,
and an operation ledger.

Legacy JSONL is an explicit read-only migration source. Migration validates a
stable source snapshot, detects identity conflicts, imports transactionally,
records provenance, and never deletes the source.

An external lifecycle side effect cannot be part of a SQLite transaction. The
operation ledger therefore records `PREPARED` before the side effect and
`APPLIED`, `FAILED`, or `INDETERMINATE` afterward. Recovery never silently
equates an absent event with an absent side effect.

### 3. Transport

Local recording never waits for the broker. A delivery worker asynchronously
publishes pending events and records positive JetStream PubAck evidence in one
local transaction.

Two modes exist with no implicit downgrade:

- development: plaintext NATS only on literal loopback or an explicit
  single-label host in an isolated container network;
- fleet: authenticated TLS, hostname verification, and a per-node principal.

The intended reference network is a Tailscale tailnet, not the Telnet protocol.
Tailnet reachability is an additional network boundary; it does not replace
NATS authentication or subject permissions.

### 4. Reconciliation

A node agent owns a registry of narrow adapters. An adapter declares:

- the event type and resources it accepts;
- the local configuration paths or controller operations it owns;
- preview, apply, verify, and rollback functions;
- whether policy permits automatic apply or requires approval.

Automatic apply is granted only by an exact
`(authority producer, resource, adapter)` node-manifest binding. Merely being
an allowed producer does not grant authority over every local resource.

For one desired-state event, the agent:

1. validates authority, schema, generation, revision, and digest;
2. records a durable apply attempt and idempotency key;
3. fetches the exact artifact through an authenticated configured source;
4. verifies the digest and builds a preview;
5. evaluates local policy;
6. applies only adapter-owned state, atomically where possible;
7. verifies observed state;
8. commits its cursor/outcome and emits `reconcile.applied`,
   `reconcile.failed`, or `reconcile.awaiting_approval`.

An event does not execute arbitrary shell, Git, or filesystem operations. Git
commit/push remains in a separately reviewed source workflow. Remote execution
uses the authenticated controller path; SSH is bootstrap/recovery only.

### 5. Runtime and packaging

The runtime composes the domain, store, transport, and reconciler through small
interfaces. Health reports local acceptance separately from fleet-delivery and
reconciliation readiness. Startup fails synchronously when required local
resources cannot initialize.

Public artifacts include portable launchd, systemd, and container templates.
The private repository supplies actual node manifests, principals, controller
references, policies, service selection, staged rollout, and acceptance
evidence.

## Ordering and convergence

There is no fleet-global sequence. Each producer has a monotonic sequence for
identity, and each desired resource has one authority-assigned generation for
convergence. Nodes may observe unrelated resources in different orders.

For a given resource, a node applies only a generation greater than its durable
observed generation. A duplicate with identical canonical content is a no-op. A
duplicate identity or generation with different content is a conflict and is
never acknowledged as applied. Superseded pending generations may be compacted
only after the latest desired artifact is verified and policy allows skipping
intermediate states.

## Delivery claims

- Local acceptance: exactly one durable local event per idempotency key.
- Broker delivery: at least once; PubAck proves the stream stored a publish.
- Local journal: exactly one canonical row per event identity; conflicting
  duplicates fail closed.
- External apply: idempotent at least once unless an adapter proves an atomic
  apply-plus-cursor transaction.
- Convergence: after faults stop and policy permits, every subscribed healthy
  node eventually reaches the latest accepted generation, provided the exact
  artifact remains resolvable.

The default stream retains history indefinitely. A finite expiry would strand
new nodes and nodes offline beyond that window; it is safe only after a tested
desired-state snapshot or per-resource compaction mechanism is added.

No component claims globally exactly-once side effects.

## Rollout boundary

Source delivery is separate from installation and live acceptance. A rollout
requires, in order:

1. exact package revision and schema compatibility;
2. private per-node manifest and credential references;
3. native local-record/store tests on every operating-system class;
4. one non-authoritative canary consumer;
5. broker-disconnect/catch-up and conflicting-authority tests;
6. adapter preview and rollback proof;
7. staged remaining nodes;
8. end-to-end desired revision, apply, verify, and outcome evidence.

No install, restart, route change, promotion, or automatic apply is authorized
by this ADR.

## Consequences

- V1 compatibility remains available for migration, but is not the architecture
  for new nodes.
- `sync-repo` is removed from the product.
- The unused LogPlayer queue prototype is removed from the runtime claim.
- `verify` is renamed/reworded around dependency integrity.
- Event types and reconcilers become registered components rather than branches
  in one frozen global function.
- Fleet rollout becomes repeatable through private manifests without making the
  public project topology-specific.
