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
"""
import argparse
import json
import os
import sys

from .nats_mini import NATSClient
from .daemon import main as daemon_main
from .outbox import CausalChecker, KINDS, Outbox

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
    print("outbox:  %s" % o.outbox_dir)
    print("archive: %s" % o.archive_dir)


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
        client.publish_js(event["subject"], event, msg_id=event["event_id"])
        # Core-style: write is NOT an ack. We report `sent` honestly (reached
        # the server socket); durability = outbox + JetStream mirror (M2).
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
        print("WARN: publish failed (%s) -> event stays pending in %s"
              % (e, path))
    finally:
        if client is not None:
            client.close()
    print("emitted %s -> %s seq=%d sent=%s"
          % (event["event_id"], event["subject"], event["producer_seq"], sent))


def cmd_pub(args):
    payload = json.loads(args.payload)
    _pub(args.root, args.subject, payload)
    print("published %s" % args.subject)


def cmd_sub(args):
    client = NATSClient(DEFAULT_URL).connect(timeout=3)
    try:
        got = client.subscribe(args.subject, count=args.count,
                               timeout=args.timeout)
    finally:
        client.close()
    for body in got:
        try:
            e = json.loads(body)
            print("%s %s %s" % (e.get("event_id", "?"), e.get("kind", "?"),
                                e.get("subject", args.subject)))
        except Exception:
            print(body.decode(errors="replace")[:200])


def cmd_status(args):
    o = _outbox(args.root)
    pending = o.count_pending()
    cursors = o.load_cursors()
    print("pending:   %d" % pending)
    print("degraded:  %s" % ("yes (pending > 0)" if pending else "no"))
    print("cursors:   %d target(s)" % len(cursors))


def cmd_replay(args):
    o = _outbox(args.root)
    events = list(o.read_pending())
    for fn in sorted(os.listdir(o.archive_dir)):
        if fn.endswith(".jsonl"):
            with open(os.path.join(o.archive_dir, fn), encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        events.append(json.loads(line))
    events.sort(key=lambda e: (e.get("observed_at", ""), e.get("producer_seq", 0)))
    for e in events[-args.lines:]:
        print("%s %s %s corr=%s" % (e.get("observed_at", "?")[:19],
                                    e.get("event_id"), e.get("kind"),
                                    e.get("correlation_id")))


def cmd_verify(args):
    events = []
    if os.path.isdir(args.path):
        # read BOTH outbox and archive (the full replayed journal)
        for subdir in ("outbox", "archive"):
            d = os.path.join(args.path, subdir)
            if not os.path.isdir(d):
                continue
            for fn in sorted(os.listdir(d)):
                if fn.endswith(".jsonl"):
                    with open(os.path.join(d, fn), encoding="utf-8") as f:
                        events += [json.loads(line_) for line_ in f if line_.strip()]
    else:
        with open(args.path, encoding="utf-8") as f:
            events = [json.loads(line_) for line_ in f if line_.strip()]
    events.sort(key=lambda e: (e.get("observed_at", ""), e.get("producer_seq", 0)))
    ok, err = CausalChecker.check(events)
    print("causal consistency: %s (%d events)" % ("OK" if ok else "VIOLATED", len(events)))
    if err:
        print("  %s" % err)
        return 1
    return 0


def cmd_gc(args):
    result = _outbox(args.root).gc(archive_days=args.archive_days)
    print("gc: removed %d old archive file(s)" % result["removed"])
    if result["rotated"]:
        print("gc: archive exceeded 500MB -> rotated (%s)" % result["size"])
    if result["degraded"]:
        print("gc: emitted event.degraded %s" % result["degraded"])


def main(argv=None):
    p = argparse.ArgumentParser(prog="anvil-events", description=__doc__)
    p.add_argument("--root", default=DEFAULT_ROOT,
                   help="events root (default %s)" % DEFAULT_ROOT)
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
    serve = sub.add_parser("serve")
    serve.add_argument("--root", default=None)
    serve.add_argument("--url", default=None)
    serve.add_argument("--subject", default="anvil.fleet.>")
    serve.add_argument("--health-port", type=int, default=9877)
    serve.add_argument("--once", action="store_true")
    sub.add_parser("status")
    r = sub.add_parser("replay")
    r.add_argument("--lines", type=int, default=20)
    v = sub.add_parser("verify")
    v.add_argument("path")
    g = sub.add_parser("gc")
    g.add_argument("--archive-days", type=int, default=90)
    args = p.parse_args(argv)
    if args.cmd == "serve":
        # local argv: --root/--url default from the real defaults
        sv = ["--subject", args.subject, "--health-port", str(args.health_port)]
        if args.root:
            sv += ["--root", args.root]
        if args.url:
            sv += ["--url", args.url]
        if args.once:
            sv += ["--once"]
        return daemon_main(sv)
    return {"init": cmd_init, "emit": cmd_emit, "pub": cmd_pub,
            "sub": cmd_sub, "status": cmd_status, "replay": cmd_replay,
            "verify": cmd_verify, "gc": cmd_gc}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
