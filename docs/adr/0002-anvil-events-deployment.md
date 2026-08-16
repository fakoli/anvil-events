# ADR-0002 — anvil-events deployment: local daemon or container

- **Status:** Superseded in detail by ADR-0003; the one-artifact/two-runtime
  decision remains accepted.
- **Date:** 2026-08-13
- **Relates to:** ADR-0001 (event bus, journal, JetStream); the controller's
  dual deployment (native process on macOS hosts, Docker container on Windows
  hosts); the gateway/dashboard launchd precedent on the ingress host

## Context

> Historical note: v2 replaces the JSONL runtime, per-host broker assumption,
> and plaintext fleet posture. Current templates use SQLite, one managed
> JetStream recovery log, and authenticated TLS in fleet mode.

The family's operator layer already solves a two-shape deployment problem: the
controller runs as a **native process** (launchd) on hosts without a container
runtime, and as a **Docker container** where the stack requires Docker Desktop
(the Windows GPU hosts cannot run serves outside containers). Hosts are
mixed:

- macOS hosts: no Docker requirement; native daemons (launchd) are the norm —
  the gateway and dashboard already run this way.
- Windows hosts: Docker Desktop is mandatory (the GPU serving stack lives in
  containers); a bare process is not a supported shape there.

anvil-events must offer the same choice, or it becomes un-deployable on half
the fleet (no daemon path on macOS) and awkward on the other half (no
container path on Windows).

## Decision

1. **One artifact, two runtimes.** The package ships an `anvil events serve`
   daemon verb — subscriber + journal writer + loopback health/status. The
   container image is a **thin wrapper over the same verb** (Dockerfile in
   `deploy/`); there is no separate code path.
2. **Daemon mode (no Docker).** `anvil events serve` runs as a native service:
   launchd on macOS / systemd on Linux. Sample units live in `deploy/`.
   Used on all macOS hosts.
3. **Container mode (Docker present).** The same `serve` runs as a container
   where the host already requires Docker (Windows + Docker Desktop). Compose
   file in `deploy/`; the events root (outbox/archive/cursors) and config are
   supplied by a **mounted volume + environment**, so journal durability lives
   on the host, never inside the container.
4. **Bus co-deployment.** nats-server follows the same rule per host (daemon
   where no Docker, container where Docker), with cross-host reachability over
   tailnet; each host sets `ANVIL_EVENTS_NATS_URL` accordingly.
5. **Parity.** Same code path in both shapes — behavior cannot drift. Hermetic
   tests run natively; CI builds the image and smoke-tests it.

## Consequences

- **Positive.** Deployable everywhere: launchd daemon on macOS, container on
  Windows — matching each host's existing operational model. Single code path
  means one set of tests and no mode-specific bugs. Journal (volume-mounted in
  container mode) survives container rebuilds.
- **Negative.** One more service to run per host (offset: it is loopback-only
  and small); CI gains an image build + smoke step; container mode must be
  explicit about where the events root lives.
- **Risk.** The daemon verb could grow into "just another server to secure."
  Mitigation: loopback-only health surface, same validation gates as the CLI,
  no new auth surface in M2/M3 (operator adapter ships auth in M4 per the
  threat model in ADR-0001).
