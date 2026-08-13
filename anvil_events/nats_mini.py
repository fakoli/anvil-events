"""Minimal stdlib NATS client (core protocol, loopback).

Implements just enough for the anvil-events spike: CONNECT handshake,
PING/PONG, PUB, SUB, blocking MSG read with --count/--timeout.
JetStream (durable subjects) is the M2 upgrade path; the local outbox is the
durability mechanism for this spike (per PRD reliability contract).
"""
import json
import re
import socket

PROTO = {"verbose": False, "pedantic": False, "tls_required": False,
         "headers": True, "name": "anvil-events"}
_MAX_BODY = 8 * 1024 * 1024          # 8 MiB publish cap
_VALID_SUBJECT = re.compile(r"^[A-Za-z0-9._>\-]+$")
_VALID_HEADER_VALUE = re.compile(r"^[^\r\n]+$")   # header values: no CR/LF


def parse_url(url):
    """Parse nats://[host][:port] (127.0.0.1:4222 default)."""
    rest = (url or "").replace("nats://", "")
    if not rest:
        return "127.0.0.1", 4222
    if ":" in rest:
        host, port = rest.rsplit(":", 1)
        return (host or "127.0.0.1"), int(port)
    return rest, 4222


def validate_subject(subject):
    """Reject subjects that could inject CRLF/space into the protocol."""
    if not subject or not _VALID_SUBJECT.match(subject):
        raise ValueError("invalid subject %r" % subject)
    return subject


class NATSClient:
    def __init__(self, url="nats://127.0.0.1:4222"):
        self.url = url
        self.sock = None
        self._buf = b""

    def connect(self, timeout=5):
        host, port = parse_url(self.url)
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        info = self._readline()
        if not info.strip().startswith(b"INFO"):
            raise IOError("bad handshake: %r" % info[:80])
        self._send(b"CONNECT " + json.dumps(PROTO).encode() + b"\r\n")
        self._send(b"PING\r\n")
        while self._readline().strip() != b"PONG":
            pass
        return self

    def _send(self, data):
        self.sock.sendall(data)

    def _readline(self):
        while b"\r\n" not in self._buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise IOError("connection closed")
            self._buf += chunk
            if len(self._buf) > 2 * _MAX_BODY:
                raise IOError("protocol buffer overflow")
        line, self._buf = self._buf.split(b"\r\n", 1)
        if line.startswith(b"-ERR"):
            raise IOError(line.decode(errors="replace"))
        return line

    def _read_n(self, n):
        if n > _MAX_BODY:
            raise IOError("frame too large: %d" % n)
        while len(self._buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise IOError("connection closed")
            self._buf += chunk
        body, self._buf = self._buf[:n], self._buf[n + 2:]
        return body

    def publish(self, subject, payload):
        validate_subject(subject)
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload).encode()
        if len(payload) > _MAX_BODY:
            raise ValueError("payload too large")
        self._send(b"PUB " + subject.encode() + b" " +
                   str(len(payload)).encode() + b"\r\n" + payload + b"\r\n")

    # -- JetStream (M2: durable subjects, dedup by Nats-Msg-Id) ------------
    def publish_js(self, subject, payload, msg_id=None):
        """Publish with JetStream headers (dedup on Nats-Msg-Id).

        Frame: ``HPUB <subject> <hdrsize> <total>`` followed by the header
        block ``NATS/1.0<CRLF><headers><CRLF><CRLF>`` then the payload.

        Returns WITHOUT a server PUB-ACK (Core-style fire-and-forget). That is
        an honest, documented limitation of the minimal client: durability and
        retry are the local outbox's job (reliability contract, ADR-0001), and
        stream creation + PUB-ACK are the operator adapter's job (M4). A
        publish to a subject with no matching stream is NOT captured — callers
        must ensure a JetStream stream exists for the subject (see
        ``ensure_stream``).
        """
        validate_subject(subject)
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload).encode()
        if len(payload) > _MAX_BODY:
            raise ValueError("payload too large")
        hdrs = [b"NATS/1.0"]
        if msg_id:
            if not isinstance(msg_id, str) or not _VALID_HEADER_VALUE.match(msg_id):
                raise ValueError("invalid Nats-Msg-Id header value")
            hdrs.append(b"Nats-Msg-Id: " + msg_id.encode("utf-8"))
        header_block = b"\r\n".join(hdrs) + b"\r\n\r\n"
        hdrsize = len(header_block)
        total = hdrsize + len(payload)
        self._send(b"HPUB " + subject.encode() + b" " +
                   str(hdrsize).encode() + b" " + str(total).encode() +
                   b"\r\n" + header_block + payload + b"\r\n")

    def ensure_stream(self, stream="ANVIL", subjects=("anvil.fleet.>",),
                      max_age_secs=7 * 86400, timeout=5):
        """Check JetStream is available; report stream existence honestly.

        The minimal client does NOT create streams (that is the operator
        adapter's job in M4 — it requires the JetStream request/reply API).
        This sends the server ``INFO`` and reports whether JetStream is
        available and whether the requested stream already exists, so callers
        can fail loudly (not silently drop) when a stream is missing.

        Returns {"available": bool, "stream": name-or-None, "subjects": [...]}.
        """
        self._send(b"INFO\r\n")
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self._readline()
            if line.startswith(b"INFO"):
                info = json.loads(line[4:].decode())
                cfg = info.get("jetstream", {}).get("config", {})
                if not bool(info.get("jetstream")) or not cfg.get("enabled"):
                    return {"available": False, "stream": None,
                            "subjects": []}
                streams = cfg.get("known_streams") or []
                exists = stream in streams
                return {"available": True,
                        "stream": stream if exists else None,
                        "subjects": list(subjects)}
            if line.strip() == b"PONG":
                continue
        return {"available": False, "stream": None, "subjects": []}

    def subscribe(self, subject, count=1, timeout=10):
        """Block until `count` messages (or timeout); yield payloads."""
        validate_subject(subject)
        got = []
        self._send(b"SUB " + subject.encode() + b" 1\r\n")
        import time
        deadline = time.time() + timeout
        try:
            while len(got) < count and time.time() < deadline:
                self.sock.settimeout(max(0.1, deadline - time.time()))
                line = self._readline()
                if line.strip() == b"PING":
                    self._send(b"PONG\r\n")
                    continue
                if line.startswith(b"MSG"):
                    parts = line.split(b" ")
                    nbytes = int(parts[-1])
                    body = self._read_n(nbytes)
                    got.append(body)
        except socket.timeout:
            pass
        return got

    def close(self):
        if self.sock:
            try:
                self._send(b"PING\r\n")
            except Exception:
                pass
            self.sock.close()
            self.sock = None
