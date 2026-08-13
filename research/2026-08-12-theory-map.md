# Theory map — published research → anvil-events design

**Date:** 2026-08-12 · **Status:** working note (feeds PRD + implementation)
**Method:** arXiv search + exact-ID lookup + full-text extraction (pdftotext),
papers downloaded to `research/papers/`.

Three papers directly upgrade the anvil-events design. Each has a
**paper → design decision → build item** mapping. This is the "test published
theory in personal software" angle: anvil-events is the lab bench.

---

## 1. LogPlayer — exactly-once outbox delivery, TLA+-verified

**Paper:** Roohitavaf, Ren, Zhang, Ben-Romdhane (eBay/Netskope), 2019.
"LogPlayer: Fault-tolerant Exactly-once Delivery using gRPC Asynchronous
Streaming." arXiv:1911.11286. Full text: `research/papers/logplayer.txt`.

**Claim.** A component that replays transactional mutations from a WAL to
backend shards with **in-order exactly-once delivery**, even under target
failures. Key mechanisms:
- per-target **queues + completion cursors** (last-acked entry index per target),
- **recovery streams** on reconnect: resume from the last ack, catch up, then
  resume normal streaming,
- **health checks** that stop enqueueing to a dead target (no unbounded growth),
- **TLA+ specification** model-checking the exactly-once/in-order property.

**Why it matters for anvil-events.** Our PRD says "transactional outbox +
ack + pending/failed status" but leaves the *protocol* implicit. LogPlayer is
the precise semantics: per-target cursors, resume-on-reconnect, dedup on entry
index, no reorder. It is exactly the "exactly-once delivery of a journaled
event" contract our R004/V1–V2 acceptance tests describe.

**Design decision adopted.** The outbox becomes a LogPlayer-style journal:
`event_id` = the entry index; per-destination last-acked cursors; on
JetStream-reconnect, a recovery stream resumes from the last ack and replays
the gap before normal streaming. Degraded/pending = target unreachable or
unacked past the cursor, surfaced by `anvil events status`.

**Build item (M2/M3).** Implement the recovery-stream protocol + per-target
cursor; write the TLA+-style invariants (at minimum a formal invariant list in
the repo): (a) each event delivered to each target exactly once, (b) order
preserved per target, (c) no event lost while `enabled`.

**LogPlayer's protocol (from full text, §2.4–2.5) — directly reusable:**

- **Terms + per-target cursors.** Each target has a monotonically increasing
  `term`, incremented on every disconnect/reconnect. Entries are pushed with a
  term stamp; an expired term causes the queue to drop the entry — this is the
  *duplicate-prevention* mechanism (a stale term can't deliver an old event
  again after reconnect).
- **States.** `S` (suspended — target down, queues cleared), `RF` (recovery
  fetching — missed entries being fetched), `FC` (fetching completed — fetched
  but not yet sent), `N` (normal streaming). Front/pop logic prefers the
  catch-up queue while in RF/FC, transitions `FC → N` when the catch-up queue
  empties.
- **Health checker → suspend.** On target down: clear normal + catch-up queues,
  set state `S`, stop enqueueing. On reconnect: spill a recovery stream that
  fetches the missed window, then `fetchingCompleted` re-enters `N`.
- **Batching.** Entries are sent in batches up to a size cap, tracking a
  `popped` map, to cut per-message gRPC calls — same idea applies to
  JetStream publishes.
- **Reference algorithm:** Target-Queue state machine with `push/front/pop/
  suspend/fetchingCompleted` under a lock (Algorithm 1 in the paper).

**Implementation mapping (anvil-events):** JetStream consumer = the "target";
per-consumer `last_acked_seq` = the cursor; reconnect → recovery stream
replays `[last_acked, current)`; a stale-term guard prevents duplicate delivery
after reconnect; `event_id` is the dedup key at the consumer side.

**Fit note.** LogPlayer targets backend storage shards; anvil-events targets
JetStream subjects + the gateway/node-b. The protocol generalizes (the paper says
"can be used with other asynchronous streaming platforms").

---

## 2. Delivery, consistency, determinism — the guarantee taxonomy

**Paper:** Trofimov, Kuralenok, Marshalkin, Novikov (JetBrains Research/Yandex/
VK/HSE), 2019. "Delivery, consistency, and determinism: rethinking guarantees
in distributed stream processing." arXiv:1907.06250. Full text:
`research/papers/delivery-consistency.txt`.

**Claim.** "Exactly-once" as usually stated is not formally defined and doesn't
capture all properties. They split guarantees into **three axes** — delivery
(at-most/at-least/exactly-once), **consistency** (state/output agreement), and
**determinism** (repeated runs reproduce the same result) — and show that
exactly-once with near-zero overhead is attainable only on a **deterministic**
engine. Nondeterministic systems that claim exactly-once pay a latency lower
bound equal to state snapshotting.

**Why it matters for anvil-events.** Our "no event vs delivery failed is
distinguishable" boundary is a *determinism* property: a replayable journal
whose replay is deterministic is what makes "nothing happened" provable. The
paper gives us the vocabulary to state guarantees per axis instead of vaguely
("best-effort").

**Design decision adopted.** anvil-events documents its contract on all three
axes, not just delivery:
- **Delivery:** at-least-once to JetStream (persisted once acked); exactly-once
  delivered *to a target* via LogPlayer-style cursors + dedup.
- **Consistency:** journal append and lifecycle side effect are atomic under
  the outbox lock (PRD "Outbox atomicity"); replay is consistent with state.
- **Determinism:** replay of the journal is deterministic (same input → same
  order); the "empty outbox + success = nothing happened" claim is only valid
  because the journal is deterministic.

**Build item.** Add a "Guarantees by axis" section to `docs/event-vocabulary.md`
(and the PRD verification), stating delivery/consistency/determinism per
consumer. This converts the reviewer's "reliability is internally impossible"
charge into a testable contract.

---

## 3. Lamport's Arrow of Time — logical clocks critique (2026)

**Paper:** Borrill, 2026. "Lamport's Arrow of Time: The Category Mistake in
Logical Clocks." arXiv:2602.21730. Full text:
`research/papers/lamport-arrow.txt`.

**Claim.** Lamport's happens-before formalism quietly assumes causality induces
a **globally well-defined DAG** (a forward-in-time-only, FITO structure). The
author argues this conflates an **epistemic** ordering (the logical ordering of
messages) with an **ontic** claim (physical causality is globally acyclic and
monotonic), tracing it through Shannon's channel model, TLA+, Bell's theorem,
and FLP/CAP. Recent indefinite-causal-order work shows nature admits
correlations with no well-defined causal order.

**Why it matters for anvil-events.** Our design claims per-producer ordering
(`producer_seq`, `observed_at`/`emitted_at`) and explicitly disclaims a global
sequencer. This 2026 paper is the **theoretical justification for that
disclaimer**: global causal order is an assumption, not a given. In a fleet
spread across Windows/macOS Docker hosts, per-producer clocks are epistemic
ordering tools — never global truth.

**Design decision adopted.** Keep per-producer ordering, *and* state the
epistemic/ontic boundary in the vocabulary (new §"Ordering is per-producer and
epistemic"). Never derive global order from local clocks; consumers that want
causal chains reconstruct them via `correlation_id` + per-producer seq, same
as the paper's "epistemic ordering" reading of Lamport.

**Build item.** A short doc note + the explicit "no global arrow" statement in
the PRD/vocabulary, with the paper cited as rationale. (No code change needed —
the design is already aligned; this is a correctness *footing*.)

---

## 4. Checking Causal Consistency — automatic conformance checking

**Paper:** Zennou, Biswas, Bouajjani, Enea, Erradi, 2020/2021. "Checking Causal
Consistency of Distributed Databases." arXiv:2011.09753. Full text:
`research/papers/causal-consistency.txt`.

**Claim.** Causal consistency is one of the weak consistency models that can be
implemented while staying available + partition-tolerant (CAP). They reduce
the problem of *checking* whether a given computation conforms to causal
consistency to **checking the absence of cycles in a suitably defined graph**,
covering three practical variants (causal convergence, causal memory,
causal consistency proper). They provide verification/testing algorithms.

**Why it matters for anvil-events.** Our consumers reconstruct causal order
from `correlation_id` + per-producer `producer_seq`. This paper is the
**checkable correctness property** for that claim: a replayed journal
conforms to causal consistency iff its per-producer-order graph is acyclic.
We can *test* it (build a checker on the journal replay), not just assert it.

**Design decision adopted.** Add an `anvil events verify` (or test-suite)
that replays a journal window, builds the causal graph (edges from
`correlation_id` + `producer_seq`), and fails if a cycle is found. This
operationalizes "no global sequencer but causal chains are consistent."

**Build item (M2/M3).** Journal-replay cycle-checker using the paper's
reduction (cycle existence in the causality graph); a hermetic test with a
crafted cyclic and acyclic journal.

---

## Cross-cutting: the reliability contract (reviewer gap #1)

The reviewer's original Reject charged "best-effort silent publisher vs durable
journal" as internally impossible. The three papers together resolve it:
- LogPlayer gives the **delivery protocol** (cursors + recovery + dedup).
- Delivery-consistency-determinism gives the **guarantee axes** (determinism is
  what makes "nothing happened" provable).
- Lamport's Arrow gives the **ordering honesty** (per-producer, epistemic).

## Implementation status (2026-08-13)

The spike is implemented and exercised (all three options from the research
thread landed):

- **`anvil_events/outbox.py`** — `Outbox` (fsync'd JSONL outbox + archive +
  cursors), `TargetQueue` (LogPlayer S/RF/FC/N state machine + term duplicate
  prevention), `CausalChecker` (cycle check per Zennou et al.).
- **`anvil_events/nats_mini.py`** — minimal stdlib NATS core client
  (CONNECT/PING/PUB/SUB/blocking-MSG) for the live loopback proof.
- **`anvil_events/cli.py`** — `init/emit/pub/sub/status/replay/verify/gc`,
  outbox-first emit (write-then-publish, undelivered stays pending).
- **`tests/test_core.py`** — 12 hermetic tests, all passing.
- **Live E2E (2026-08-13):** outbox-first emit of `promote.applied` +
  `config.adopted` over loopback NATS (wildcard `anvil.fleet.>` subscriber
  received both), status pending=0, replay correlation-linked,
  `verify` = causal consistency **OK (2 events)**.
- **`research/references.bib`** + `research/README.md` — durable citation
  library for all four papers.

Remaining roadmap (M3+): the anvil-serving `[events]` seam, the private
operator adapter (commit-push-on-promote + the gateway subscriber), and rollout —
all tracked in `prd.md` Milestones.

## Sources

- `research/papers/logplayer.pdf/.txt` (arXiv:1911.11286)
- `research/papers/delivery-consistency.pdf/.txt` (arXiv:1907.06250)
- `research/papers/lamport-arrow.pdf/.txt` (arXiv:2602.21730)
- arXiv metadata + abstracts via the arxiv skill (export.arxiv.org API)
