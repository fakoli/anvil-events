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

## Verify

```bash
anvil events serve --help            # the verb is the same in both shapes
curl -s http://127.0.0.1:9877/       # loopback health: {"received":N,"journaled":N,"dropped":N}
```
