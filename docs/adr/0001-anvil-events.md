# ADR-0001 — anvil-events: fleet lifecycle event bus and journal

- **Status:** Accepted; implemented through M1–M5 and post-v1.0 correction
  reviews.
- **Date:** 2026-08-12
- **Relates to:** anvil (orchestration), anvil-serving (serving); ADR-0035
  (config reconciliation) and ADR-0036 (voice relocation) in anvil-serving
- **Amends:** none (new repo)

## Context

The Anvil family spans anvil (coordination, leases, evidence-gated state) and
anvil-serving (model serves + capability gateway). A recurring operator pain:
**"what changed on node-a, and when?"** requires git archaeology; a promotion run
directly on a host (e.g. the operator CLI-driven r33-tp2 change) never reaches the
repo or any other host until a human pushes. The repo is a passive journal; it
only knows what's committed, and nothing auto-commits a promotion.

The 2026-08-12 live-vs-checked-in reconciliation proved the drift: the live
router served a newer primary tier than the checked-in operator home still
declared (rollback, vision, and voice tiers likewise differed), and the
promotion had never been committed or propagated. The gap is a *notification
and journaling* problem, not a *consensus* problem: any single act has one writer
at a time (whoever runs it), so there is no multi-writer leader election to
solve. K3s/etcd/ZooKeeper would add cluster weight (on WSL2 + macOS) for a
pub/sub problem.

An independent second-model review (2026-08-12) rejected the
first draft: the "best-effort silent no-op" publisher contradicted the
"durable journal" promise; NATS semantics (durability, wildcards) were
unspecified; ordering was undefined under multi-host producers; vocabulary
drifted across docs; journal authority/atomicity was absent; security was
reduced to secret placement; milestones built integrations before the repo.
This revision absorbs those findings.

## Decision

1. **Create a third family repo, `anvil-events`** (after M1 contract freeze —
   see milestone reorder). The need is cross-cutting (anvil and anvil-serving
   both consume it) and deserves one home, one version, one contract.
2. **Events are records of what happened, never desired state.** The repo
   remains the declared spec. A live-vs-repo mismatch is a *recorded*
   `divergence` event, never an automatic correction.
3. **Durability: local-first outbox + JetStream mirror.**
   *Status: implemented. The local outbox is authoritative producer history;
   the checked-in JetStream stream is the seven-day fleet mirror.*
   - The producer's **local transactional outbox** (fsync'd append-only JSONL)
     is the authoritative record; write it BEFORE publishing.
   - An outbox write failure while enabled **fails the operation** (no silent
     loss).
   - A journaled-but-undelivered event is `pending`/`failed`, surfaced via
     `anvil events status` and an `event.degraded` event — never silently
     dropped.
   - **JetStream** (NATS) is the fleet durable mirror. The checked-in `ANVIL`
     stream uses file storage, seven-day retention, `DiscardOld`, and
     `Nats-Msg-Id=event_id` deduplication. Producers archive only after a
     positive PubAck; the daemon retries pending events after reconnect.
   - "No event" is distinguishable from "delivery failed": outbox empty + no
     degraded = nothing happened; non-empty outbox = delivery pending/failed.
4. **Transport: NATS JetStream.** nats-server 2.14.x, loopback where possible,
   proven spike 2026-08-12 on node-b. K3s/etcd explicitly rejected (single-writer
   per act). Subjects use `>` wildcards (multi-token), not `*`.
5. **Ordering: per-producer, explicit.** `event_id`, `producer`,
   `producer_seq`, `observed_at`/`emitted_at`, `correlation_id`; consumers
   reconstruct causal order; idempotent on `event_id`. No global sequencer.
6. **Stdlib-only core, optional adapters.** anvil + anvil-serving each gain an
   optional `events` extra that shells out to the `anvil events` CLI if
   present (mirrors the `voice` extra pattern). No hard dependency.
7. **Credentials are env-var names + threat model.** `[events]` carries
   `ANVIL_EVENTS_*` names (NATS URL, per-host producer token), never values.
   Threat model documented: tailnet-only TLS by default, producer identity via
   per-host token, per-subject publish ACLs on JetStream, the gateway adapter
   validates producer/kind/payload allowlist before any memory mutation
   (forged events dropped, not stored), journal mode/ownership enforced.
8. **Best-effort ON TOP of the outbox, never a substitute.** Publishing never
   fails/delays a lifecycle operation; the outbox is the durability guarantee.
9. **Milestone order: contract first.** M1 repo+schema+CI → M2 CLI+outbox+
   JetStream → M3 anvil-serving seam → M4 private adapter (with rollback +
   observability) → M5 rollout/observability.

## Consequences

- **Positive.** "What's running on node-a?" becomes a journal query, not
  archaeology. the gateway on node-b subscribes and refreshes memory on
  `anvil.fleet.>` (validated). The commit-push-on-promote hook is the first real
  adapter and closes today's drift.
- **Negative.** A third repo is a maintenance commitment; the vocabulary must
  stay frozen + versioned (schema bumps gated + compatibility tests); NATS
  JetStream adds one daemon (15 MB) and a configuration surface; the outbox
  adds a write per lifecycle change.
- **Risk.** Vocabulary drift / generic-ness ("yet another pub/sub" with no Anvil
  value). Mitigation: kinds tied to anvil-serving/anvil verbs; each new kind
  requires a schema bump + compatibility test. Security misconfiguration (weak
  ACLs) would poison the gateway memory — mitigated by validation gate + drop, not
  store.
