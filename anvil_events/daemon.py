"""Long-running anvil-events service entry point."""

from __future__ import annotations

import argparse
import os
import signal
import threading

from .runtime.service import EventsService

DEFAULT_URL = os.environ.get("ANVIL_EVENTS_NATS_URL", "nats://127.0.0.1:4222")
DEFAULT_ROOT = os.path.expanduser("~/.anvil/events")


def build_parser():
    parser = argparse.ArgumentParser(prog="anvil-events serve")
    parser.add_argument("--root", default=DEFAULT_ROOT)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--subject", default="anvil.events.v2.>")
    parser.add_argument("--stream", default="ANVIL_EVENTS")
    parser.add_argument("--durable")
    parser.add_argument("--backend", choices=("auto", "sqlite"), default="auto")
    parser.add_argument("--config", help="node reconciliation TOML")
    parser.add_argument("--health-port", type=int, default=9877)
    parser.add_argument(
        "--health-host", choices=("127.0.0.1", "0.0.0.0"),
        default="127.0.0.1",
        help="0.0.0.0 is intended only for an isolated container namespace",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    service = EventsService(
        args.root,
        args.url,
        args.subject,
        args.stream,
        (args.health_host, args.health_port),
        durable=args.durable,
        store_backend=args.backend,
        node_config=args.config,
    )
    service.log_banner()
    terminate = threading.Event()

    def request_stop(_signum, _frame):
        terminate.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    service.start()
    try:
        while not terminate.wait(1):
            failed = service.failed_workers()
            if failed:
                raise RuntimeError(f"service worker exited unexpectedly: {failed}")
    finally:
        service.stop()
    return 0


if __name__ == "__main__":
    main()
