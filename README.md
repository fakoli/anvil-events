# anvil-events

`anvil-events` is a stdlib-only desired-state convergence agent for small,
heterogeneous fleets. An authority records an immutable desired revision once;
each node catches up, updates only the resource it owns, verifies the result,
and records an outcome.

The current Anvil Serving topology motivated the product, but it is only a
reference design. Public code uses generic roles and identities. Real hosts,
addresses, routes, credentials, and operator state belong in a private
deployment repository.

## Status

Version 0.2 is a source redesign and is **not deployed**. The composable core,
SQLite store, secured NATS client, managed-file reconciler, migration path, and
130 hermetic tests run natively on Windows and Linux. Clean container probes
also prove exact apply, broker-outage catch-up, mTLS identity mapping, and
negative cross-node ACLs. Exact-head CI passes on Windows, macOS, and Linux;
live fleet acceptance remains a separate private deployment gate.

The prior 0.1 implementation is assessed in
[`docs/assessment/2026-08-16-baseline-assessment.md`](docs/assessment/2026-08-16-baseline-assessment.md).
The accepted architecture is
[`ADR-0003`](docs/adr/0003-fleet-convergence-architecture.md).

## Model

```text
lifecycle command
  -> local SQLite acceptance (never waits for network)
  -> asynchronous JetStream publish + PubAck evidence
  -> durable per-node consumer
  -> desired generation + exact artifact digest
  -> preview/policy -> narrow adapter -> verify/rollback
  -> reconcile.applied | reconcile.failed | reconcile.awaiting_approval
```

The event is a notification and integrity contract, not a shell command or a
configuration blob. It contains a logical resource, authority-assigned
generation, immutable revision, SHA-256 digest, adapter name, and artifact
reference. The node manifest—not the event—owns local paths, controller URLs,
credentials, and automatic-apply policy.

## Guarantees

- Local acceptance: one durable canonical event per idempotency key.
- Broker delivery: at least once; only a PubAck from the configured stream
  completes local pending work.
- Journal identity: one canonical row per event ID; equivocation fails closed.
- Resource order: generations are monotonic per resource and authority.
- Auto-apply authority: an exact producer/resource/adapter binding is owned by
  each node manifest; a node-wide producer allowlist is insufficient.
- External apply: one serialized automatic attempt. A crash after the durable
  applying marker is indeterminate and requires explicit recovery; it is never
  silently replayed.
- Convergence: after faults stop and policy permits, a healthy subscribed node
  reaches the latest accepted generation.

There is no fleet-global sequence and no globally exactly-once side-effect
claim. `verify` checks dependency-DAG integrity; it does not claim database
causal-consistency conformance.

## Network and identity

The reference network is a Tailscale **tailnet**, not Telnet. Tailnet
reachability does not authenticate event producers.

- `development`: plaintext NATS is accepted on literal loopback or an
  explicitly allowlisted single-label host inside an isolated container
  network. Tailnet IPs and dotted LAN names are rejected.
- `fleet`: `tls://`, hostname verification, and username/password or mTLS are
  required. TLS-first is an explicit option because standard NATS TLS normally
  upgrades after the initial `INFO` line.
- A producer such as `node-a:router` must belong to envelope node `node-a`.
  The subscriber also verifies the actual broker subject equals the envelope
  subject. Fleet NATS ACLs can therefore bind a node principal to
  `anvil.events.v2.<node>.>`.

See [`deploy/nats-fleet.example.conf`](deploy/nats-fleet.example.conf) for a
sanitized mTLS identity-map and ACL shape. Never place credential values in
this repository.

## Local development proof

The Compose stack uses an isolated, unexposed broker network and is insecure
outside that development boundary. It creates and exactly verifies the JetStream stream,
starts a node reconciler, and exposes readiness at `127.0.0.1:9877`.

```powershell
docker compose -f deploy/compose.yml up -d --build

$payload = @'
{"resource":"routing/clients","generation":1,"revision":"rev-1","content_sha256":"781a9e745454e30551907d683c956be69055528219e5f567a1ba1afe245e2c17","adapter":"router_config","artifact":"routing/clients","targets":["node-b"]}
'@
$payload | docker compose -f deploy/compose.yml exec -T events `
  anvil-events --root /var/lib/anvil/events record state.desired `
  --node node-a --producer node-a:router --operation-key demo-routing-1

docker compose -f deploy/compose.yml exec events `
  python -c "print(open('/var/lib/anvil/events/managed/router-client.toml').read())"
```

This proves the development path only. It does not prove fleet TLS, private
manifests, client reload behavior, or a live rollout.

## CLI

```text
anvil-events init
anvil-events record <dotted-kind> --node N --producer N:ROLE --operation-key K
anvil-events serve --config /path/to/node.toml --durable node-events
# Authority only, behind authenticated private HTTPS ingress:
anvil-events serve --artifact-root /srv/anvil-artifacts \
  --artifact-auth-env ANVIL_ARTIFACT_AUTH
anvil-events status --json
anvil-events replay --lines 20
anvil-events verify <store-or-legacy-jsonl>
anvil-events migrate-legacy <legacy-root> [--offline-source]
anvil-events broker-init deploy/nats-stream.json
```

`record` reads one JSON object from standard input and performs no broker I/O.
The raw publish/subscribe commands and the Git-mutating `sync-repo` command
were removed. Git publication and host deployment remain separate managed
workflows.

## Repository boundaries

- `domain*`: v1 compatibility plus the extensible v2 envelope.
- `storage/`: SQLite transactions, delivery evidence, retention, operations,
  facts, and fail-closed legacy migration.
- `transport/`: endpoint security, NATS framing, and JetStream client.
- `reconciliation/`: artifact sources, adapter contracts, policy, state, and
  managed-file, JSON-merge, and allowlisted command-config implementations.
- `runtime/`: subscriber, delivery pump, health, stats, and composition.
- `deploy/`: portable development and fleet templates only.

The package has no runtime dependencies. Tests use `unittest` and no real
network. The project contract and remaining rollout gates are in
[`prd.md`](prd.md).

The default stream does not expire history: a newly enrolled node must still
see the current desired generation. Operators may introduce finite retention
only with a tested desired-state snapshot/compaction mechanism, and immutable
artifacts must remain resolvable while their generation can be replayed.
