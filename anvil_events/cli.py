"""anvil events — CLI (emit/pub/sub/status/replay/verify/gc).

Usage:
  anvil-events init
  anvil-events emit <kind> --host H [--correlation C] <payload-json>
  anvil-events pub <subject> <payload-json>
  anvil-events sub <subject> [--count N] [--timeout S]
  anvil-events status
  anvil-events replay [--lines N]
  anvil-events verify <dir-or-file>    # causal-consistency cycle check
  anvil-events gc [--archive-days 90]
  anvil-events sync-repo --dir DIR --correlation C [--push]   # commit+push operator repo, emit config.adopted+repo.synced
  anvil-events ingest [--root R] [--store S] [--count N]      # validated fact ingestion (drop forged)
"""
import argparse
import hashlib
import json
import os
import sys

from .daemon import main as daemon_main
from .ingest import cmd_ingest, cmd_sync_repo
from .ingest import register as register_m4
from .nats_mini import NATSClient
from .outbox import (
    _ARCHIVE_JSONL,
    _DAILY_JSONL,
    KINDS,
    CausalChecker,
    Outbox,
    iter_managed_jsonl,
)

DEFAULT_ROOT = os.path.expanduser("~/.anvil/events")
DEFAULT_URL = os.environ.get("ANVIL_EVENTS_NATS_URL", "nats://127.0.0.1:4222")


def _outbox(root):
    return Outbox(root)


def _pub(root, subject, payload):
    client = NATSClient(DEFAULT_URL).connect(timeout=3)
    try:
        client.publish(subject, payload)
    finally:
        client.close()
    # note: real producer path is `emit` (outbox-first). pub is for ad-hoc use.


def cmd_init(args):
    o = _outbox(args.root)
    print(f"outbox:  {o.outbox_dir}")
    print(f"archive: {o.archive_dir}")


def cmd_emit(args):
    """Outbox-FIRST: write to the durable outbox, then attempt publish.

    On publish failure, an `event.degraded` record is emitted to the local
    journal — the failure is never silent (PRD reliability contract).
    """
    o = _outbox(args.root)
    event = o.emit(args.producer, args.kind, args.host,
                   json.loads(args.payload or "{}"),
                   correlation_id=args.correlation)
    path = os.path.join(o.outbox_dir, ".")
    sent = False
    client = None
    try:
        client = NATSClient(DEFAULT_URL).connect(timeout=3)
        # JetStream publish with Nats-Msg-Id dedup (event_id)
        client.publish_js(event["subject"], event, msg_id=event["event_id"],
                          wait_ack=True, timeout=3)
        # Archive only after JetStream confirms durable stream storage.
        o.ack(event)
        sent = True
    except Exception as e:
        # never silent: record the degradation in the journal with a UNIQUE
        # identity (via the outbox's locked sequence, not fixed seq=1)
        try:
            o.emit("local:emit", "event.degraded", event["host"],
                   {"cause": str(e), "event_id": event["event_id"]},
                   correlation_id=event["correlation_id"])
        except Exception:
            pass
        print(f"WARN: publish failed ({e}) -> event stays pending in {path}")
    finally:
        if client is not None:
            client.close()
    print(f"emitted {event['event_id']} -> {event['subject']} seq={event['producer_seq']} sent={sent}")


def cmd_pub(args):
    payload = json.loads(args.payload)
    _pub(args.root, args.subject, payload)
    print(f"published {args.subject}")


def cmd_sub(args):
    durable = args.durable or (
        "anvil-events-cli-" + hashlib.sha256(args.subject.encode()).hexdigest()[:12]
    )
    client = NATSClient(DEFAULT_URL).connect(timeout=3)
    try:
        client.bind_durable_consumer(args.stream, durable, args.subject,
                                     timeout=3)
        got = client.receive(count=args.count, timeout=args.timeout,
                             subscription=f"anvil.delivery.{durable}")
        for message in got:
            body = message["body"]
            try:
                e = json.loads(body)
                print("{} {} {}".format(e.get("event_id", "?"), e.get("kind", "?"),
                                    e.get("subject", args.subject)))
            except Exception:
                print(body.decode(errors="replace")[:200])
            if message.get("reply"):
                client.ack(message["reply"])
    finally:
        client.close()


def cmd_status(args):
    o = _outbox(args.root)
    pending = o.count_pending()
    cursors = o.load_cursors()
    print(f"pending:   {pending}")
    print(f"degraded:  {'yes (pending > 0)' if pending else 'no'}")
    print(f"cursors:   {len(cursors)} target(s)")


def cmd_replay(args):
    o = _outbox(args.root)
    events = list(o.read_pending())
    events.extend(o.read_archive())
    events.extend(o.read_journal())
    events = list({e.get("event_id"): e for e in events}.values())
    events.sort(key=lambda e: (e.get("observed_at", ""), e.get("producer_seq", 0)))
    for e in events[-args.lines:]:
        print("{} {} {} corr={}".format(e.get("observed_at", "?")[:19],
                                    e.get("event_id"), e.get("kind"),
                                    e.get("correlation_id")))


def cmd_verify(args):
    events = []
    if os.path.isdir(args.path):
        # read BOTH outbox and archive (the full replayed journal)
        for subdir, pattern in (
            ("outbox", _DAILY_JSONL),
            ("archive", _ARCHIVE_JSONL),
            ("journal", _DAILY_JSONL),
        ):
            d = os.path.join(args.path, subdir)
            events.extend(iter_managed_jsonl(d, pattern))
    else:
        parent, name = os.path.split(os.path.abspath(args.path))
        pattern = _ARCHIVE_JSONL if _ARCHIVE_JSONL.fullmatch(name) else _DAILY_JSONL
        if not pattern.fullmatch(name):
            raise ValueError("verify input is not a managed JSONL filename")
        events.extend(iter_managed_jsonl(parent, pattern, managed_name=name))
    events.sort(key=lambda e: (e.get("observed_at", ""), e.get("producer_seq", 0)))
    ok, err = CausalChecker.check(events)
    print(f"causal consistency: {'OK' if ok else 'VIOLATED'} ({len(events)} events)")
    if err:
        print(f"  {err}")
        return 1
    return 0


def cmd_gc(args):
    result = _outbox(args.root).gc(archive_days=args.archive_days)
    print(f"gc: removed {result['removed']} old archive file(s)")
    if result.get("evicted"):
        print(f"gc: evicted {result['evicted']} rotated overflow file(s) (hard cap)")
    if result["rotated"]:
        print("gc: archive exceeded 500MB -> rotated ({})".format(result["size"]))
    if result["degraded"]:
        print("gc: emitted event.degraded {}".format(result["degraded"]))
    if result.get("unresolved_oversize"):
        print("gc: archive remains above hard cap; retained history is too young")
        return 1
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="anvil-events", description=__doc__)
    p.add_argument("--root", default=DEFAULT_ROOT,
                   help=f"events root (default {DEFAULT_ROOT})")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    e = sub.add_parser("emit")
    e.add_argument("kind", choices=sorted(KINDS))
    e.add_argument("--host", required=True)
    e.add_argument("--producer", default=os.environ.get("ANVIL_EVENTS_PRODUCER", "local:cli"))
    e.add_argument("--correlation")
    e.add_argument("payload")
    pub = sub.add_parser("pub")
    pub.add_argument("subject")
    pub.add_argument("payload")
    s = sub.add_parser("sub")
    s.add_argument("subject")
    s.add_argument("--count", type=int, default=1)
    s.add_argument("--timeout", type=int, default=10)
    s.add_argument("--stream", default="ANVIL")
    s.add_argument("--durable", default=None)
    serve = sub.add_parser("serve")
    serve.add_argument("--root", default=None)
    serve.add_argument("--url", default=None)
    serve.add_argument("--subject", default="anvil.fleet.>")
    serve.add_argument("--stream", default="ANVIL")
    serve.add_argument("--durable", default=None)
    serve.add_argument("--health-port", type=int, default=9877)
    serve.add_argument("--once", action="store_true")
    sub.add_parser("status")
    r = sub.add_parser("replay")
    r.add_argument("--lines", type=int, default=20)
    v = sub.add_parser("verify")
    v.add_argument("path")
    g = sub.add_parser("gc")
    g.add_argument("--archive-days", type=int, default=90)
    register_m4(sub)
    args = p.parse_args(argv)
    if args.cmd == "serve":
        # local argv: --root/--url default from the real defaults
        sv = ["--subject", args.subject, "--health-port", str(args.health_port)]
        if args.root:
            sv += ["--root", args.root]
        if args.url:
            sv += ["--url", args.url]
        if args.stream:
            sv += ["--stream", args.stream]
        if args.durable:
            sv += ["--durable", args.durable]
        if args.once:
            sv += ["--once"]
        return daemon_main(sv)
    return {"init": cmd_init, "emit": cmd_emit, "pub": cmd_pub,
            "sub": cmd_sub, "status": cmd_status, "replay": cmd_replay,
            "verify": cmd_verify, "gc": cmd_gc,
            "sync-repo": cmd_sync_repo, "ingest": cmd_ingest}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
