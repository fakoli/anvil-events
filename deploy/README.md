# Deployment shapes

The package has one `serve` runtime. The files here wrap it as a native macOS
launchd service, native Linux systemd service, or a thin container. These are
portable templates, not authorization to install or restart a real node.

## Development Compose

```bash
docker compose -f deploy/compose.yml up -d --build
docker compose -f deploy/compose.yml ps
curl http://127.0.0.1:9877/ready
```

The broker is reachable only inside the isolated Compose network; no client
port is published to the host. `stream-init` creates `ANVIL_EVENTS` or fails if
an existing stream differs. The events service loads
`development-node.toml`, consumes desired events, and applies the single
synthetic `routing/clients` resource under its volume.

This shape is intentionally unauthenticated and must not be exposed to a LAN or
tailnet. It is a reproducible development proof only.

## Fleet mode

Fleet mode requires all of the following:

1. one managed JetStream broker/recovery log reachable through the intended
   private network;
2. `tls://` with server-name verification;
3. mTLS with certificate identity mapping per node in the supplied template;
4. publish ACL bound to `anvil.events.v2.<node>.>`;
5. fixed durable name and matching consumer API, delivery, inbox, and ACK ACLs;
6. a private node TOML with real artifact/controller source, exact producer
   allowlist, exact authority-producer/resource/adapter binding, destination,
   validation, and policy;
7. separately managed service environment and credential files.

`nats-fleet.example.conf` shows a sanitized two-node mTLS identity-map shape.
Each client certificate identity must map exactly to its configured NATS user.
Real broker configuration, identities, addresses, credential values, and node
manifests belong in the private operator repository.

For the TLS-first sample, clients set:

```text
ANVIL_EVENTS_TRANSPORT_MODE=fleet
ANVIL_EVENTS_NATS_URL=tls://broker.example.invalid:4222
ANVIL_EVENTS_TLS_HANDSHAKE_FIRST=true
ANVIL_EVENTS_TLS_CA_FILE=/path/to/ca.pem
ANVIL_EVENTS_TLS_CERT_FILE=/path/to/node-b.pem
ANVIL_EVENTS_TLS_KEY_FILE=/path/to/node-b-key.pem
```

The URL never contains credentials. In this template the verified certificate
identity maps to the NATS user, so the client does not send a separate username
or password. The client also supports username/password for brokers configured
with that separate authentication design; do not mix it with the supplied
certificate-identity template without testing the broker's mapping and ACL
behavior.

## Native services

The systemd unit expects `/etc/anvil-events/events.env` and
`/etc/anvil-events/node.toml`. The launchd template uses mTLS path references
and a placeholder broker name because launchd has no systemd-style
`EnvironmentFile`.

Before enabling either unit:

- install the exact reviewed wheel;
- run `anvil-events --root <root> init` as the service user;
- validate the private node config without changing an active destination;
- create/verify the stream with the separate admin principal;
- test local `record`, broker-offline pending status, and reconnect;
- obtain the separate install/restart gate.

## Health semantics

- `/live`: local store is readable and workers are alive.
- `/ready`: `/live` plus the durable subscriber is connected.
- `/`: full bounded JSON snapshot.

Readiness does not prove an adapter changed a real application or that the
application reloaded it. A rollout needs a desired event, exact local observed
state, and correlated outcome evidence.
