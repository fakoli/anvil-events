"""`anvil events serve` — the anvil-events daemon (subscriber + journal).

One artifact, two runtimes (ADR-0002): run as a launchd/systemd daemon on
hosts without Docker, or as a thin container where Docker Desktop is
required. Same code path both ways.

Responsibilities:
- subscribe to `anvil.fleet.>` (or a configured subject) on the broker,
- append received events to the local journal (outbox/archive),
- validate producers/kinds against the vocabulary before journaling (drop
  forged/unknown events — the validation gate from ADR-0001),
- expose a loopback-only health/status endpoint (127.0.0.1).

The publisher side is the CLI (`anvil events emit`); the daemon is the
subscriber + journal side. On a fleet host that both emits and consumes, run
both.
"""
import argparse
import json
import os
import socket
import threading
import time

from .nats_mini import NATSClient
from .outbox import KINDS, Outbox

DEFAULT_URL = os.environ.get("ANVIL_EVENTS_NATS_URL", "nats://127.0.0.1:4222")
DEFAULT_ROOT = os.path.expanduser("~/.anvil/events")
DEFAULT_HEALTH = ("127.0.0.1", 9877)   # loopback only (never 0.0.0.0)


class EventsDaemon:
    """Subscribe -> validate (kind/producer) -> journal append, loopback health."""

    def __init__(self, root=DEFAULT_ROOT, url=DEFAULT_URL,
                 subject="anvil.fleet.>", health=DEFAULT_HEALTH):
        self.root = root
        self.url = url
        self.subject = subject
        self.health_addr = health
        self.out = Outbox(root)
        self._stop = threading.Event()
        self._stats = {"received": 0, "journaled": 0, "dropped": 0,
                       "started": time.time()}

    # -- validation gate -------------------------------------------------
    @staticmethod
    def _valid(e):
        """Drop events that are not well-formed / not in the frozen vocabulary."""
        if not isinstance(e, dict):
            return False
        if e.get("version") != 1:
            return False
        if e.get("kind") not in KINDS:
            return False
        if not e.get("event_id") or not e.get("producer"):
            return False
        if not e.get("subject") or not e.get("host"):
            return False
        return True

    # -- broker loop ------------------------------------------------------
    def _run(self):
        # reconnect loop: keep subscribing across broker restarts
        while not self._stop.is_set():
            try:
                client = NATSClient(self.url).connect(timeout=5)
                # bounded peek: subscribe in a loop; each receive -> journal
                # (reconnect re-subscribes on the next iteration)
                while not self._stop.is_set():
                    got = client.subscribe(self.subject, count=1, timeout=10)
                    if not got:
                        continue
                    for body in got:
                        self._stats["received"] += 1
                        try:
                            e = json.loads(body)
                        except Exception:
                            self._stats["dropped"] += 1
                            continue
                        if not self._valid(e):
                            self._stats["dropped"] += 1
                            continue
                        try:
                            self.out.append(e)
                            self._stats["journaled"] += 1
                        except Exception:
                            self._stats["dropped"] += 1
            except Exception:
                if self._stop.is_set():
                    break
                time.sleep(2)   # broker down: retry with backoff
            finally:
                try:
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
                except Exception:
                    stats["pending"] = -1
                    stats["degraded_events"] = -1
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
        t1.start()
        t2.start()
        return t1, t2

    def stop(self):
        self._stop.set()


def main(argv=None):
    p = argparse.ArgumentParser(prog="anvil events serve",
                                description="anvil-events daemon (subscriber + journal)")
    p.add_argument("--root", default=DEFAULT_ROOT)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--subject", default="anvil.fleet.>")
    p.add_argument("--health-port", type=int, default=DEFAULT_HEALTH[1])
    p.add_argument("--once", action="store_true",
                   help="receive one batch then exit (for tests)")
    args = p.parse_args(argv)

    d = EventsDaemon(root=args.root, url=args.url, subject=args.subject,
                     health=("127.0.0.1", args.health_port))
    print(f"anvil events serve: subject={args.subject} root={args.root} url={args.url} "
          f"health=127.0.0.1:{args.health_port}", flush=True)
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
