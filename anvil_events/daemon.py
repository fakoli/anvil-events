"""`anvil events serve` — the anvil-events daemon (subscriber + journal).

One artifact, two runtimes (ADR-0002): run as a launchd/systemd daemon on
hosts without Docker, or as a thin container where Docker Desktop is
required. Same code path both ways.

Responsibilities:
- subscribe to `anvil.fleet.>` (or a configured subject) on the broker,
- append received events to the separate deduplicated subscriber journal,
- retry producer-pending outbox events and archive only after JetStream PubAck,
- validate producers/kinds against the vocabulary before journaling (drop
  forged/unknown events — the validation gate from ADR-0001),
- expose a loopback-only health/status endpoint (127.0.0.1).

The CLI (`anvil events emit`) is the synchronous publisher path; the daemon is
both subscriber/journal and asynchronous pending-delivery pump.
"""
import argparse
import hashlib
import json
import os
import re
import socket
import threading
import time

from .ingest import parse_allowed_producers, validate_event
from .nats_mini import NATSClient
from .outbox import Outbox

DEFAULT_URL = os.environ.get("ANVIL_EVENTS_NATS_URL", "nats://127.0.0.1:4222")
DEFAULT_ROOT = os.path.expanduser("~/.anvil/events")
DEFAULT_HEALTH = ("127.0.0.1", 9877)   # loopback only (never 0.0.0.0)


class EventsDaemon:
    """Subscribe -> validate (kind/producer) -> journal append, loopback health."""

    def __init__(self, root=DEFAULT_ROOT, url=DEFAULT_URL,
                 subject="anvil.fleet.>", health=DEFAULT_HEALTH,
                 stream="ANVIL", durable=None):
        self.root = root
        self.url = url
        self.subject = subject
        self.stream = stream
        raw_host = socket.gethostname()
        host_token = re.sub(r"[^A-Za-z0-9_-]+", "-", raw_host).strip("-")
        identity = "\0".join(("subscriber", raw_host, stream, subject))
        identity_token = hashlib.sha256(identity.encode()).hexdigest()[:12]
        self.durable = durable or f"anvil-events-{host_token or 'host'}-{identity_token}"
        self.allowed_producers = parse_allowed_producers()
        self.health_addr = health
        self.out = Outbox(root)
        self._stop = threading.Event()
        self._delivery_retry = {}
        self._delivery_seen = set()
        self._delivery_position = None
        self._retry_sleep_until = None
        self._retry_sleep_signature = None
        self._stats = {"received": 0, "journaled": 0, "dropped": 0,
                       "retried": 0, "acked": 0, "broker_connected": False,
                       "last_error": None, "producer_connected": False,
                       "delivery_errors": 0, "last_delivery_error": None,
                       "started": time.time()}
        self._logged_url = None

    def log_banner(self, url=None):
        """Log the daemon banner once per broker URL (state-change dedup).

        The reconnect loop in _run() previously re-emitted the full banner on
        every round, producing dozens of identical lines in the daemon log.
        Now the banner prints only on first start and when the effective
        broker URL changes (e.g. env override picked up after a restart).
        """
        url = url if url is not None else self.url
        if self._logged_url == url:
            return
        print(f"anvil events serve: subject={self.subject} root={self.root} "
              f"url={url} health={self.health_addr[0]}:{self.health_addr[1]}",
              flush=True)
        self._logged_url = url

    # -- validation gate -------------------------------------------------
    def _valid(self, e):
        """Drop events outside the frozen, structurally valid v1 envelope."""
        return validate_event(e, allowed_producers=self.allowed_producers)[0]

    # -- broker loop ------------------------------------------------------
    def _handle_body(self, body):
        """Compatibility wrapper returning whether a new row was journaled."""
        return self._handle_body_result(body)[1]

    def _handle_body_result(self, body):
        """Return (processed, newly_journaled); I/O failure is not processed."""
        self._stats["received"] += 1
        try:
            event = json.loads(body)
        except Exception:
            self._stats["dropped"] += 1
            return True, False
        if not self._valid(event):
            self._stats["dropped"] += 1
            return True, False
        try:
            journaled = self.out.append_journal(event)
        except Exception:
            self._stats["dropped"] += 1
            return False, False
        if journaled:
            self._stats["journaled"] += 1
        return True, journaled

    def _pending_batch(self, max_events):
        """Select one bounded fair round over a mutating pending set."""
        now = time.monotonic()

        def eligible(event):
            event_id = event.get("event_id")
            retry = self._delivery_retry.get(event_id) if event_id else None
            return retry is None or now >= retry["next_at"]

        batch, reached_eof, repaired, position, scanned, signature = (
            self.out.select_pending_batch(
                max_events, self._delivery_seen, validate_event, eligible,
                start_after=self._delivery_position, max_scan=max_events * 4,
                return_meta=True,
            )
        )
        self._delivery_position = position
        if repaired:
            self._delivery_position = None
        if reached_eof:
            self._delivery_seen.clear()
            self._delivery_position = None
        if not batch and reached_eof and self._delivery_retry:
            self._retry_sleep_until = min(
                retry["next_at"] for retry in self._delivery_retry.values()
            )
            self._retry_sleep_signature = signature
        return batch

    def _drain_pending(self, client, max_events=16):
        """Attempt one bounded/fair batch; poison entries back off independently."""
        return self._deliver_batch(client, self._pending_batch(max_events))

    def _deliver_batch(self, client, batch):
        """Deliver a batch already selected under the outbox lock."""
        attempted = 0
        failed = 0
        now = time.monotonic()
        for event in batch:
            if self._stop.is_set():
                break
            event_id = event["event_id"]
            retry = self._delivery_retry.get(event_id)
            if retry and now < retry["next_at"]:
                continue
            attempted += 1
            self._stats["retried"] += 1
            try:
                client.publish_js(event["subject"], event,
                                  msg_id=event["event_id"], wait_ack=True,
                                  timeout=5)
                self.out.ack(event)
                self._stats["acked"] += 1
                self._delivery_retry.pop(event_id, None)
            except Exception as exc:
                failed += 1
                # A failed event must become selectable again when its
                # backoff expires, even if new work prevents an EOF round.
                self._delivery_seen.discard(event_id)
                failures = (retry or {}).get("failures", 0) + 1
                delay = min(60, 2 ** min(failures, 6))
                self._delivery_retry[event_id] = {
                    "failures": failures, "next_at": now + delay,
                }
                self._stats["delivery_errors"] += 1
                self._stats["last_delivery_error"] = str(exc)[:300]
                continue
        # ack() rewrites JSONL files, invalidating line positions. Stable
        # event-id `seen` state survives, but the physical scan cursor resets.
        if attempted:
            self._delivery_position = None
        if self._delivery_retry:
            self._retry_sleep_until = min(
                item["next_at"] for item in self._delivery_retry.values()
            )
        else:
            self._retry_sleep_until = None
            self._retry_sleep_signature = None
        return attempted, failed

    def _producer_loop(self):
        """Independent, bounded producer pump; never tears down the subscriber."""
        while not self._stop.is_set():
            if self._retry_sleep_until is not None:
                delay = self._retry_sleep_until - time.monotonic()
                if (delay > 0 and not self._delivery_seen
                        and self._retry_sleep_signature is not None):
                    self._stop.wait(min(delay, 2))
                    if self.out.pending_signature() == self._retry_sleep_signature:
                        continue
                    self._retry_sleep_until = None
                    self._retry_sleep_signature = None
                if delay <= 0:
                    self._retry_sleep_until = None
                    self._retry_sleep_signature = None
            client = None
            try:
                batch = self._pending_batch(16)
                if batch:
                    client = NATSClient(self.url).connect(timeout=5)
                    self._stats["producer_connected"] = True
                    attempted, failed = self._deliver_batch(client, batch)
                    if attempted and failed == 0:
                        self._stats["last_delivery_error"] = None
            except Exception as exc:
                self._stats["producer_connected"] = False
                self._stats["delivery_errors"] += 1
                self._stats["last_delivery_error"] = str(exc)[:300]
            finally:
                try:
                    if client is not None:
                        client.close()
                except Exception:
                    pass
            self._stop.wait(2)

    def _run(self):
        # reconnect loop: keep subscribing across broker restarts
        while not self._stop.is_set():
            client = None
            try:
                client = NATSClient(self.url).connect(timeout=5)
                client.bind_durable_consumer(
                    self.stream, self.durable, self.subject, timeout=5,
                )
                self._stats["broker_connected"] = True
                self._stats["last_error"] = None
                # Durable JetStream consumer resumes from its persisted ACK
                # floor; ACK only after local handling has completed durably.
                while not self._stop.is_set():
                    got = client.receive(count=1, timeout=10,
                                         subscription=f"anvil.delivery.{self.durable}")
                    for message in got:
                        processed, _ = self._handle_body_result(message["body"])
                        if processed and message.get("reply"):
                            client.ack(message["reply"])
            except Exception as exc:
                self._stats["broker_connected"] = False
                self._stats["last_error"] = str(exc)[:300]
                if self._stop.is_set():
                    break
                time.sleep(2)   # broker down: retry with backoff
            finally:
                try:
                    if client is not None:
                        client.close()
                except Exception:
                    pass

    # -- health server (loopback only) -------------------------------------
    def _health_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(self.health_addr)
        srv.listen(4)
        srv.settimeout(0.5)
        while not self._stop.is_set():
            try:
                conn, _ = srv.accept()
                stats = dict(self._stats)
                # M5 observability: surface the degraded signal live.
                # pending > 0 means events could not be published (degraded);
                # count event.degraded records seen since start.
                try:
                    stats["pending"] = self.out.count_pending()
                    stats["degraded_events"] = sum(
                        1 for e in self.out.read_pending()
                        if e.get("kind") == "event.degraded")
                    stats["delivery_backoff_events"] = len(self._delivery_retry)
                except Exception:
                    stats["pending"] = -1
                    stats["degraded_events"] = -1
                    stats["delivery_backoff_events"] = -1
                body = json.dumps(stats).encode()
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                             b"Content-Length: %d\r\nConnection: close\r\n\r\n"
                             % len(body) + body)
                conn.close()
            except TimeoutError:
                continue
            except Exception:
                pass
        srv.close()

    # -- lifecycle -----------------------------------------------------------
    def start(self):
        t1 = threading.Thread(target=self._run, daemon=True)
        t2 = threading.Thread(target=self._health_loop, daemon=True)
        t3 = threading.Thread(target=self._producer_loop, daemon=True)
        t1.start()
        t2.start()
        t3.start()
        return t1, t2, t3

    def stop(self):
        self._stop.set()


def main(argv=None):
    p = argparse.ArgumentParser(prog="anvil events serve",
                                description="anvil-events daemon (subscriber + journal)")
    p.add_argument("--root", default=DEFAULT_ROOT)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--subject", default="anvil.fleet.>")
    p.add_argument("--stream", default="ANVIL")
    p.add_argument("--durable", default=None)
    p.add_argument("--health-port", type=int, default=DEFAULT_HEALTH[1])
    p.add_argument("--once", action="store_true",
                   help="receive one batch then exit (for tests)")
    args = p.parse_args(argv)

    d = EventsDaemon(root=args.root, url=args.url, subject=args.subject,
                     health=("127.0.0.1", args.health_port), stream=args.stream,
                     durable=args.durable)
    d.log_banner()
    if args.once:
        # bounded run: keep appending until SIGINT (tests use --once + timeout)
        d.start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            d.stop()
        return
    d.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        d.stop()


if __name__ == "__main__":
    main()
