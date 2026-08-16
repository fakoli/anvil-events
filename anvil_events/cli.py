"""Command-line interface for local acceptance and fleet delivery."""

from __future__ import annotations

import argparse
import os
import sys

from .commands import local
from .commands.broker import initialize as initialize_broker
from .daemon import main as serve
from .ingest import cmd_ingest
from .ingest import register as register_ingest
from .store import BACKENDS

DEFAULT_ROOT = os.path.expanduser("~/.anvil/events")


def build_parser():
    parser = argparse.ArgumentParser(prog="anvil-events", description=__doc__)
    parser.add_argument("--root", default=DEFAULT_ROOT)
    parser.add_argument(
        "--backend", choices=sorted(BACKENDS),
        default=os.environ.get("ANVIL_EVENTS_STORE_BACKEND", "auto"),
    )
    commands = parser.add_subparsers(dest="cmd", required=True)
    commands.add_parser("init")
    record = commands.add_parser(
        "record", help="idempotently accept one local v2 event from JSON stdin",
    )
    record.add_argument("kind", help="dotted v2 event kind")
    record.add_argument("--node", required=True)
    record.add_argument("--operation-key", required=True)
    record.add_argument(
        "--producer",
        default=os.environ.get("ANVIL_EVENTS_PRODUCER"),
        required=os.environ.get("ANVIL_EVENTS_PRODUCER") is None,
        help="node-owned producer identity, for example node-a:router",
    )
    record.add_argument("--correlation")
    daemon = commands.add_parser("serve")
    daemon.add_argument("--url")
    daemon.add_argument("--subject", default="anvil.events.v2.>")
    daemon.add_argument("--stream", default="ANVIL_EVENTS")
    daemon.add_argument("--durable")
    daemon.add_argument("--config", help="node reconciliation TOML")
    daemon.add_argument("--artifact-root")
    daemon.add_argument("--artifact-auth-env")
    daemon.add_argument("--health-port", type=int, default=9877)
    daemon.add_argument(
        "--health-host", choices=("127.0.0.1", "0.0.0.0"),
        default="127.0.0.1",
    )
    commands.add_parser("status").add_argument("--json", action="store_true")
    replay = commands.add_parser("replay")
    replay.add_argument("--lines", type=int, default=20)
    verify = commands.add_parser("verify")
    verify.add_argument("path")
    collect = commands.add_parser("gc")
    collect.add_argument("--archive-days", type=int, default=90)
    migrate = commands.add_parser("migrate-legacy")
    migrate.add_argument("legacy_root")
    migrate.add_argument("--offline-source", action="store_true")
    broker = commands.add_parser(
        "broker-init", help="create or exactly verify a JetStream stream",
    )
    broker.add_argument("stream_config")
    broker.add_argument("--url")
    broker.add_argument("--wait", type=int, default=30)
    register_ingest(commands)
    return parser


def _serve(args):
    arguments = [
        "--root", args.root,
        "--backend", args.backend,
        "--subject", args.subject,
        "--stream", args.stream,
        "--health-host", args.health_host,
        "--health-port", str(args.health_port),
    ]
    if args.url:
        arguments += ["--url", args.url]
    if args.durable:
        arguments += ["--durable", args.durable]
    if args.config:
        arguments += ["--config", args.config]
    if args.artifact_root:
        arguments += ["--artifact-root", args.artifact_root]
    if args.artifact_auth_env:
        arguments += ["--artifact-auth-env", args.artifact_auth_env]
    return serve(arguments)


def main(argv=None):
    args = build_parser().parse_args(argv)
    handlers = {
        "init": local.init,
        "record": local.record,
        "serve": _serve,
        "status": local.status,
        "replay": local.replay,
        "verify": local.verify,
        "gc": local.gc,
        "migrate-legacy": local.migrate_legacy,
        "broker-init": initialize_broker,
        "ingest": cmd_ingest,
    }
    return handlers[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
