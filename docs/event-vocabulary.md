# anvil-events — event vocabulary (v1)

Versioned, append-only lifecycle events for the Anvil family. A single
`anvil-events` JSON document is the contract; `version` is the schema version,
`kind` is enumerated, `subject` is the NATS-style hierarchical topic,
`payload` is free-form JSON describing the change.

## Event envelope (v1 — normative)

```json
{
  "version": 1,
  "event_id": "fakoli-dark:serves:000123",
  "producer": "fakoli-dark:serves",
  "producer_seq": 123,
  "observed_at": "2026-08-13T02:08:55.000Z",
  "emitted_at": "2026-08-13T02:08:55.120Z",
  "correlation_id": "promote-r33-20260812-01",
  "schema": "https://anvil.dev/schemas/events/v1.json",
  "host": "fakoli-dark",
  "kind": "promote.applied",
  "subject": "anvil.fleet.fakoli-dark.promote.applied",
  "payload": {
    "tier": "primary-local",
    "model": "deepseek-v4-flash-0731-r33-...-tp2-393k",
    "context": 393216,
    "rollback": "qwen35-122b-a10b-nvfp4",
    "repo": "ops-private",
    "repo_rev": "3d6861c",
    "repo_synced": true
  }
}
```

## Kinds (canonical, frozen)

| kind | subject (suffix) | payload highlights | emitted by |
|---|---|---|---|
| `serve.up` | `.serve.up` | serve name, model, port, gpu_roles, residency | `anvil-serving serves up` |
| `serve.down` | `.serve.down` | serve name, graceful | `anvil-serving serves down` |
| `profile.enter` | `.profile.enter` | mode (split/exclusive), profile id, exclusive_target | `serves profile enter` |
| `profile.leave` | `.profile.leave` | mode, profile id, restore group | `serves profile leave` |
| `promote.applied` | `.promote.applied` | tier, model, context, rollback, revision | `serves promote` |
| `promote.rolled_back` | `.promote.rolled_back` | tier, restored model | `serves promote --rollback` |
| `config.adopted` | `.config.adopted` | file(s), repo, rev; `correlation_id` links to promote | commit-push-on-promote wrapper |
| `repo.synced` | `.repo.synced` | repo, pushed rev, ok/failed; `correlation_id` links | the ops-private commit-push hook |
| `host.status` | `.host.status` | host, reachable, gpu used/free | periodic host probe / `host status` |
| `divergence` | `.divergence` | declared vs live mismatch, delta | reconciliation probe |
| `event.degraded` | `.event.degraded` | cause (outbox full, publish failed), pending entries | event subsystem itself |

`config.adopted_mirror` was removed in v1 (redundant with `config.adopted`).
`router-updated` is not a kind; router moves are `promote.applied` /
`config.adopted` / `divergence`.

## Subject grammar (corrected wildcards)

```
anvil.fleet.>                         # ALL fleet events (multi-token)
anvil.fleet.<host>.>                  # all events for one host (multi-token)
anvil.fleet.<host>.<kind>             # one host, one kind
anvil.<product>.<host>.<kind>         # product-scoped (anvil / anvil-serving)
```

`>` matches one-or-more tokens; `*` matches exactly one. Use `>` for fleet-wide
and host-wide subscriptions.

## Journal + outbox

- **Producer-local outbox** (authoritative): append-only JSONL
  `events/outbox/<YYYY-MM-DD>.jsonl`, fsync'd on write, per producer.
  This is the durable record that the change happened.
- **JetStream mirror** (fleet): replicated subjects for late subscribers and
  multi-host replay; retention 30d / N events (configurable).
- **Replay:** `anvil events replay [--since]` reads outbox-first, then
  JetStream. Per-producer order only; duplicates resolved by `event_id`.
- **Rotation/retention:** outbox rotated on completion (published entries move
  to `events/archive/`); policy in ADR-0001.
