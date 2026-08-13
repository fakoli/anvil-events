# anvil-events

> **Fleet lifecycle event bus and journal for the Anvil family.**

Every lifecycle change on any Anvil host — serve up/down, profile enter/leave,
promotion applied, config adopted, repo synced — publishes a versioned JSON
event and appends it to a durable journal. It answers the question that git
archaeology can't: **"what changed on that box, and when?"**

> **📖 Why this exists** — the story that started it: one fleet spread across
> several machines, each earning its keep differently, all needing to know
> about each other. Experiments that ran but never told anyone; agents that
> couldn't see a change that had happened a box away; a simple reframe —
> **the promotion is the event.** Read it: [`docs/origin-story.md`](docs/origin-story.md).

- **anvil** coordinates *who* does what.
- **anvil-serving** serves *what* models on which tiers.
- **anvil-events** records *what happened* and tells everyone who needs to know.

Backed by published distributed-systems theory — see
[`research/README.md`](research/README.md) (LogPlayer exactly-once delivery,
causal-consistency checking, logical-clock semantics).

## Status

**M1 baseline (2026-08-13, public).** PRD and ADR revised through two
independent gpt-5.6-sol reviews (Reject → Approve-with-changes → residuals
fixed). Core CLI + transactional outbox + LogPlayer-style recovery +
causal-consistency verify implemented (stdlib-only, `dependencies = []`,
12 hermetic tests) and exercised end-to-end over NATS on node-b (nats-server, loopback :4222). CI runs the hermetic suite on 3.11/3.12/3.13.

PRD: [`prd.md`](prd.md) · Decision: [`docs/adr/0001-anvil-events.md`](docs/adr/0001-anvil-events.md) ·
Vocabulary: [`docs/event-vocabulary.md`](docs/event-vocabulary.md) ·
Research: [`research/README.md`](research/README.md).

## Why not ZooKeeper / K3s / etcd

Because the fleet has **one writer at a time** (whoever runs the promotion).
That's a push-on-event problem, not a multi-writer consensus problem — NATS
(15 MB, stdlib-friendly, subscription-only) covers it without cluster weight.

## Concepts

- **Event** — versioned JSON `{version, ts, host, kind, subject, payload}`.
- **Subject** — `anvil.fleet.<host>.<kind>` (canonical bus) or
  `anvil.<product>.<host>.<kind>`.
- **Journal** — append-only `events/<YYYY-MM-DD>.jsonl` in the operator home;
  replayable. The journal is *what happened*; the repo remains *declared* state.
- **Adapters** — anvil + anvil-serving each get an optional `events` extra
  that shells out to this tool if present. Best-effort, never blocking.

## CLI (planned)

```
anvil events pub <subject> '<json>'      # publish (default nats://127.0.0.1:4222)
anvil events sub <subject> [--count N]   # subscribe (bounded)
anvil events emit <kind> --host H ...      # outbox-first + publish
anvil events replay [--lines N]           # replay the journal
anvil events verify --root <dir>          # causal-consistency check
```

Transport: a minimal stdlib NATS core client (`anvil_events/nats_mini.py`)
speaks the wire protocol directly; a `nats-server` (or any NATS broker) is
required at runtime. The package ships stdlib-only (`dependencies = []`).

## Roadmap

- M1 — vocabulary + `anvil events pub/sub/replay` (stdlib)
- M2 — anvil-serving `[events]` seam (best-effort publish after lifecycle)
- M3 — operator adapter: real publisher + commit-push-on-promote + the gateway
  subscriber on node-b
- M4 — graduate to own repo with PRD, ADR, vocabulary, tests, CI

See [`prd.md`](prd.md) for the full plan, acceptance criteria, and open
questions.
