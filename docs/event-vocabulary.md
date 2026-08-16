# Event vocabulary and envelope

V2 is the normative contract for new producers. V1 remains readable only for
compatibility and explicit legacy migration.

## V2 envelope

```json
{
  "version": 2,
  "event_id": "node-a:router:000123",
  "producer": "node-a:router",
  "producer_seq": 123,
  "observed_at": "2026-08-16T18:00:00.000Z",
  "emitted_at": "2026-08-16T18:00:00.000Z",
  "correlation_id": "route-change-42",
  "schema": "https://anvil.dev/schemas/events/v2.json",
  "node": "node-a",
  "kind": "state.desired",
  "subject": "anvil.events.v2.node-a.state.desired",
  "payload": {
    "resource": "routing/clients",
    "generation": 42,
    "revision": "immutable-revision",
    "content_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "adapter": "router_config",
    "artifact": "routing/clients",
    "targets": ["node-b"]
  },
  "causes": []
}
```

`producer` must start with the exact `node` token. `event_id` and `subject` are
derived. Timestamps are RFC 3339 diagnostics; ordering decisions use producer
sequence and resource generation.

Payloads are JSON-only, bounded, and may not contain credential-shaped keys or
network URLs. Desired events carry logical references; locally configured
manifests own real endpoints, paths, and credential environment-variable
names.

## Normative convergence kinds

| Kind | Required payload | Meaning |
|---|---|---|
| `state.desired` | resource, generation, revision, content SHA-256, adapter, artifact | An authority declares exact desired bytes. |
| `reconcile.applied` | resource, generation, revision, content SHA-256, adapter | A node applied and verified the generation. |
| `reconcile.failed` | resource, generation, revision, adapter, error | A node rejected, failed, rolled back, or classified an attempt. |
| `reconcile.awaiting_approval` | resource, generation, revision, adapter | Local policy did not permit automatic apply. |
| `operation.indeterminate` | operation ID, error | An external side effect cannot be classified safely. |
| `delivery.degraded` | event ID, error | Delivery remains pending after a bounded failure. |

Other dotted lowercase kinds are permitted for generic lifecycle facts. New
kinds do not automatically gain reconciliation behavior; a registered
processor/adapter and tests are required.

## Subject and ACL shape

```text
anvil.events.v2.>                    all v2 events
anvil.events.v2.<node>.>             all events emitted by one node
anvil.events.v2.<node>.<kind>        one node and kind
```

The authenticated principal for `<node>` may publish only that node prefix.
The subscriber rejects a body whose subject differs from the actual broker
subject. Durable delivery and ACK/API permissions are separately scoped in the
fleet NATS configuration.

## Local and broker state

- SQLite is the producer and subscriber state store.
- A newly recorded event is `pending` until a positive JetStream PubAck.
- PubAck stream/sequence evidence and the producer cursor commit atomically.
- Subscriber journaling is idempotent by canonical event ID.
- JetStream retains history indefinitely by default so new and long-offline
  nodes can replay current desired state. Finite retention requires a separate
  tested snapshot/compaction path.
- Local archive retention defaults to 90 days and emits a local audit event
  into the local journal, not the fleet outbox, when records expire.

## V1 compatibility

The frozen v1 kinds and schema remain in `domain.py` and
`schemas/events-v1.json`. New code must not use v1 to declare desired state.
The removed JSONL writer cannot be reopened; use `migrate-legacy`.
