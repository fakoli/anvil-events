# anvil-events deployment — one artifact, two runtimes (ADR-0002)

The controller set the pattern: **native process where no container runtime
exists, Docker container where the stack requires it.** anvil-events does the
same. `anvil events serve` is the single daemon verb; `deploy/` holds the thin
wrappers.

## When to use which

| Host kind | Runtime available | Shape | Files |
|---|---|---|---|
| macOS (no Docker needed) | — | **native daemon** (launchd) | `ai.anvil.events.plist` |
| Linux (no Docker) | — | **native daemon** (systemd) | `anvil-events.service` |
| Windows (Docker Desktop required) | Docker | **container** | `Dockerfile` + `compose.yml` |

## Daemon mode (no Docker)

```bash
# macOS — install the launchd unit
pip install anvil-events
cp deploy/ai.anvil.events.plist ~/Library/LaunchAgents/
# edit the REPLACE_ME username in the plist, then:
launchctl load ~/Library/LaunchAgents/ai.anvil.events.plist   # or bootstrap

# Linux — install the systemd unit
cp deploy/anvil-events.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now anvil-events
```

## Container mode (Docker present — the Windows hosts)

```bash
docker compose -f deploy/compose.yml up -d --build
```

- The **journal lives on the host** (`anvil-events-root` volume), never inside
  the container — the container is stateless and rebuild-safe.
- Composed with a nats-server broker; cross-host, point
  `ANVIL_EVENTS_NATS_URL` at the fleet broker over tailnet.

## Broker

nats-server follows the same rule: native daemon where no Docker (or a
lightweight loopback broker), container where Docker is mandatory.

JetStream being enabled is not sufficient: the broker must have the checked-in
file-backed stream that captures the fleet subjects. Provision it idempotently
with the NATS CLI after starting the broker:

```bash
nats stream add --config deploy/nats-stream.json
nats stream info ANVIL --json
```

The contract in `deploy/nats-stream.json` is `ANVIL` → `anvil.fleet.>`, file
storage, `DiscardOld`, 7-day history, and a 2-minute message-ID deduplication
window. Producers retain their local outbox entry until JetStream returns a
positive PubAck; the daemon retries pending entries after reconnect.

Set `ANVIL_EVENTS_ALLOWED_PRODUCERS` to a comma-separated list of exact
producer identities. The daemon and validated ingestion are default-deny when
this allowlist is empty; broker publish ACLs remain defense in depth.

## Verify

```bash
anvil events serve --help            # the verb is the same in both shapes
curl -s http://127.0.0.1:9877/       # includes pending, PubAck retries, broker state
```
