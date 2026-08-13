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

**v1.0 (M1–M5 complete).** The stdlib-only implementation includes the typed
v1 schema, transactional producer outbox, JetStream PubAck/retry delivery,
deduplicated subscriber journal, validated/idempotent ingestion,
LogPlayer-style recovery, causal verification, retention, and health. The
hermetic suite runs on Python 3.11/3.12/3.13; cross-host transport has been
exercised against the same public code path.

PRD: [`prd.md`](prd.md) · Decision: [`docs/adr/0001-anvil-events.md`](docs/adr/0001-anvil-events.md) ·
Vocabulary: [`docs/event-vocabulary.md`](docs/event-vocabulary.md) ·
Research: [`research/README.md`](research/README.md).

## Why not ZooKeeper / K3s / etcd

Because the fleet has **one writer at a time** (whoever runs the promotion).
That's a push-on-event problem, not a multi-writer consensus problem — NATS
(15 MB, stdlib-friendly, subscription-only) covers it without cluster weight.

## Concepts

- **Event** — the versioned envelope in `schemas/events-v1.json`.
- **Subject** — `anvil.fleet.<host>.<kind>`.
- **Journal** — producer outbox/archive plus a separate deduplicated subscriber
  journal. The journal is *what happened*; the repo remains *declared* state.
- **Adapters** — anvil + anvil-serving each get an optional `events` extra
  that shells out to this tool if present. Best-effort, never blocking.

## Deployment

anvil-events is **one artifact, two runtimes** (ADR-0002): `anvil events serve`
runs as a **native daemon** on hosts without Docker (launchd on macOS, systemd
on Linux) or as a **thin container** where a container runtime is already
required (Windows + Docker Desktop). The journal root (outbox/archive/cursors)
is volume-mounted in container mode and lives on the host — never inside the
container. The same code path serves both.

- Daemon: `anvil events serve` + sample launchd/systemd units in `deploy/`
- Container: `deploy/Dockerfile` + `deploy/compose.yml` (volume + env)
- Broker: nats-server co-deploys the same way per host; cross-host reachable
  over tailnet

## CLI

```
anvil events pub <subject> '<json>'      # publish (default nats://127.0.0.1:4222)
anvil events sub <subject> [--count N]   # subscribe (bounded)
anvil events emit <kind> --host H ...      # outbox-first + publish
anvil events serve                        # daemon (subscriber + journal)
anvil events replay [--lines N]           # replay the journal
anvil events verify --root <dir>          # causal-consistency check
```

Transport: a minimal stdlib NATS/JetStream client (`anvil_events/nats_mini.py`)
speaks PubAck plus durable explicit-ACK consumer wire protocols directly.
The package ships stdlib-only (`dependencies = []`). See `deploy/README.md` for
the required stream configuration.

## Roadmap

- M1–M5 — complete.
- `anvil-events sub` and the daemon bind stable JetStream durable consumers,
  replay seven-day retained history, and ACK only after local processing.

See [`prd.md`](prd.md) for the full plan, acceptance criteria, and open
questions.
