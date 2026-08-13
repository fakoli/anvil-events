# Project: anvil-events — fleet lifecycle event bus and journal

## Summary

A typed, append-only **event bus + journal** for the Anvil family (anvil,
anvil-serving). Every lifecycle change on any host — serve up/down, profile
enter/leave, promotion applied, config adopted, repo synced — publishes a
versioned JSON event to a subject every subscriber can hear, and appends the
same record to a durable journal. It is the third wheel the family is missing:
anvil coordinates *who*, anvil-serving serves *what*, anvil-events answers
*"what happened, when, and who needs to know"*.

The event log is the **journal**, never the desired state. The repo (declared
spec) and the event log (journal of what actually happened) do not compete —
events fill the gap that today requires a 20-minute git-archaeology session to
answer "what's actually running on node-a?"

## Goals

- **G1 — Publish on lifecycle change.** `serve.up/down`, `profile.enter/leave`,
  `promote.applied`, `config.adopted`, `repo.synced` emit a typed event.
- **G2 — Subscribe from anywhere.** Any host (node-a, node-b, node-c, an agent)
  can subscribe to subjects it cares about and react — including the gateway
  host refreshing its fact/memory on `anvil.fleet.>`.
- **G3 — Durable journal.** Events are append-only and replayable; a late
  subscriber can catch up on recent history, independent of git state.
- **G4 — Zero hard dependency.** `anvil-events` is stdlib-only (like
  anvil-serving). anvil and anvil-serving get an optional `events` extra that
  shells out to it if present; everything works when it's absent.
- **G5 — Boundaries stay.** Credentials (NATS auth, tokens) are env-var names,
  never values in config or code. Public repo never holds operator identity.

## Reliability contract (revised)

The journal is authoritative and local-first; publishing is best-effort ON
TOP of the journal, never a substitute for it. Every producer writes the
event to its **local transactional outbox** (append-only JSONL, fsync'd) as the
*durable record that the change happened*, BEFORE any publish attempt. The
publish is then attempted; if it fails, the event remains in the outbox and a
**visible `degraded` status** is exposed (`anvil events status` reports
pending/failed outbox entries). Lost events are therefore *distinguishable*
from "nothing happened": if the outbox is empty and the producer succeeded,
there is genuinely nothing; if an event failed to deliver, it is
`pending`/`failed` — **never silently discarded while enabled.**

A subscriber that never acks (JetStream consumer ACK) does NOT affect the
producer outbox — publish persistence and consumer delivery are independent;
an unacked consumer is that consumer's backlog to replay, not a producer
failure.

- An enabled event that cannot be journaled (disk failure) **fails the
  operation** (or aborts with a clear error) rather than proceeding silently.
- A journaled event that cannot be published is `pending` — surfaced, retried
  with backoff, and never dropped while `enabled`.
- `[events] enabled: false` is the ONLY way a change produces no event, and it
  is explicit, not default-silent.

## Non-goals

- **Not a generic pub/sub platform.** NATS/Kafka exist; the value is the Anvil
  lifecycle contract + journal, not the transport.
- **Not a new source of truth.** The repo remains the declared spec; events
  never silently override it. A divergence (repo vs live) is a *recorded*
  event, not an automatic correction.
- **Not K3s/etcd/ZooKeeper.** This is single-writer-at-a-time push-on-event,
  not multi-writer consensus. No leader election, no quorum.
- **Not a config store.** No CRDT merging, no distributed files. Files stay in
  the repo; events describe their lifecycle.

## Ordering and identity (revised)

Events are **per-producer ordered**, not globally ordered. Each event carries:

- `event_id` — unique (host-producer-seq), idempotent replay key.
- `producer` — stable producer identity (host + role, e.g. `node-a:serves`).
- `producer_seq` — monotonically increasing per producer.
- `observed_at` / `emitted_at` — event-time vs publish-time, so clock skew is
  explicit.
- `correlation_id` — links cause/effect (e.g. a `promote.applied` and its
  `config.adopted` + `repo.synced` siblings share one).
- `schema` — schema URI.

Consumers that need global ordering must reconstruct it via `observed_at` +
per-producer causal chains; no global sequencer is claimed. Duplicate
delivery is handled by idempotent consumers on `event_id` (last-write-wins by
`producer_seq`).

## Outbox atomicity and retention (revised)

**Atomicity.** The word "transactional" on the outbox does NOT imply a
distributed transaction. The concrete semantics:

- The producer serializes the lifecycle side effect and the journal append
  **in one critical section** (a single per-producer file lock). Tens to
  hundreds of milliseconds of lock hold; no second phase.
- Order: (1) acquire lock, (2) apply lifecycle side effect, (3) append +
  fsync the event, (4) release lock.
- If (3) fails (disk error), the operation **aborts with a clear error** — the
  side effect is either rolled back by the caller or explicitly declared
  applied-but-unjournaled in the CLI output; it is never silent.
- The outbox file is append-only; a torn last line (crash) is detected on next
  open (trailing newline check) and the partial line is dropped, with a
  `event.degraded` record.

**Retention (concrete).**

- In M2 the durable record is the **local outbox** (fsync'd append); entries
  move to `events/archive/` via `ack()` when a consumer confirms them. The
  JetStream mirror + server PUB-ACK ("pending until acked by JetStream") and
  the retry-with-backoff producer path are **M4** (operator adapter) — M2
  reports `sent` on socket write and keeps the record in the outbox.
- Archive files are retained **90 days** and then deleted by a daily sweep
  (`anvil events gc` / the operator cron). JetStream stream retention: **7
  days** of history for late subscribers (configurable, M4), with `max_age` +
  `DiscardOld`; the archive is the long-term source, JetStream is the
  short-term window.
- No event is deleted from the archive before its retention age; the sweep
  logs deletions to the day's journal line.
- Size guard: if the archive exceeds 500 MB, the sweep rotates the current-day
  file and flags `event.degraded` (rotation + alerting; the rotated file stays
  in `archive/` under the long-term retention policy; true cap enforcement is
  M4).

## Requirements

- **R001 — Typed event vocabulary (normative).** Events are versioned JSON with
  the envelope above. Kinds are frozen in the vocabulary spec (see
  `docs/event-vocabulary.md`): `serve.up`, `serve.down`, `profile.enter`,
  `profile.leave`, `promote.applied`, `promote.rolled_back`,
  `config.adopted`, `repo.synced`, `host.status`, `divergence`,
  `event.degraded`. The vocabulary is the single canonical kind list; adding a
  kind requires a schema bump + compatibility test across the repos.
- **R002 — Publish on lifecycle change (outbox-first).** Lifecycle commands in
  anvil-serving (`serves up/down`, `profile enter/leave`, `promote`) append to
  the outbox and publish best-effort; `[events]` gate is explicit. Never blocks
  or fails the operation EXCEPT when the local outbox write fails while
  enabled.
- **R003 — Subscribe from anywhere.** `anvil events sub <subject>` receives
  events; `--count`/`--timeout` for bounded use.
- **R004 — Durable journal + outbox.** The outbox is the authoritative local
  journal (IMPLEMENTED, M1); JetStream (NATS, PLANNED M2) provides durable
  server-side subjects for late subscribers and multi-host replay. Journal
  authority is the **producer's local outbox**; JetStream is a replicated
  mirror for fleet consumers. Both are append-only, rotation + retention
  defined (see ADR-0001).
- **R005 — the gateway/node-b adapter (validated).** A lightweight subscriber on node-b
  ingests `anvil.fleet.>` into the gateway fact_store/memory via a script + cron,
  but only after validating `producer`/`event_id`/kind against the vocabulary
  and a payload allowlist; forged/unknown events are dropped, not stored.
- **R006 — Repo-sync event.** After a promotion that changes the operator home,
  the commit-push (config-adopt) emits `config.adopted` + `repo.synced` sharing
  a `correlation_id`; the transaction links them (the promote either yields
  both or records the partial state).
- **R007 — No credential in config.** `[events]` carries env-var *names*
  (`ANVIL_EVENTS_NATS_URL` etc.), never values.
- **R008 — Hermetic tests.** Unit tests use fakes/stdlib; no real network in CI.
  Compatibility tests pin the vocabulary across anvil + anvil-serving.

## Verification / acceptance

- **V1 — Outbox-then-publish round-trip.** Publish `promote.applied` with the
  outbox write; simulate a publish failure → event stays `pending`, `status`
  shows `degraded`, retry delivers, outbox clears.
- **V2 — No-event is distinguishable.** With the publisher down and events
  enabled, `anvil events status` reports `pending`/`failed` — never silent.
- **V3 — Lifecycle integration.** `anvil-serving serves up` emits `serve.up`
  (outbox-first) with correct host/kind/model; a disabled `[events]` produces
  nothing AND is explicit.
- **V4 — Late subscriber catch-up (JetStream).** After N events, a new
  subscriber replays them in per-producer order via JetStream durable subjects.
- **V5 — Validation gate.** A forged `host.status` (bad producer/unknown kind)
  is dropped by the the gateway adapter, not stored.
- **V6 — Drift flag.** A live-vs-repo mismatch emits a `divergence` event
  (recorded, not corrected).

## Milestones (revised — repo & contract FIRST)

1. **M1 — Repository + normative schema + CI.** Create `anvil-events` repo;
   freeze the v1 JSON Schema + kind list; add vocabulary compatibility tests;
   set up CI (lint, unit, schema conformance).
2. **M2 — Core CLI (stdlib) + outbox.** `anvil events pub/sub/replay/status`
   with local outbox (fsync'd JSONL), retry/backoff, degraded status; JetStream
   stream/consumer config; hermetic tests (fakes, no network). **Includes the
   `serve` daemon verb (subscriber + journal) and `deploy/` sample runtimes —
   launchd/systemd units + Dockerfile + compose (ADR-0002).**
3. **M3 — anvil-serving `[events]` seam.** Outbox-first best-effort publish
   after lifecycle commands; compatible with M2; docs + CLI audit; tests.
4. **M4 — Private operator adapter.** Real NATS publisher (dev on node-b),
   commit-push-on-promote wrapper emitting correlation-linked
   `config.adopted`+`repo.synced`, the gateway subscriber with validation gate +
   rollback + observability. Deployed after M2/M3 prove out.
5. **M5 — Rollout + observability.** `anvil events status` on each host,
   monitoring of `event.degraded`, retention/rotation policy enforced.

## Open questions

- **O1 — Transport:** NATS JetStream (chosen; proven spike) vs pure
  tailnet+git-bundle. JetStream is the default; revisit if a host cannot run it.
- **O2 — Journal home:** producer-local outbox (authoritative) + JetStream
  mirror (fleet). Retention: default 30d or N events; operator-configurable.
- **O3 — Repo-vs-journal precedence** when they disagree: journal wins for
  *what happened*; repo wins for *desired*; divergence is a recorded event.
  Needs operator sign-off.
- **O4 — Security posture:** tailnet-only TLS by default; producer identity via
  per-host token (env); per-subject ACLs on JetStream. Threat model in ADR.
