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
         "name": "anvil-events"}
_MAX_BODY = 8 * 1024 * 1024          # 8 MiB publish cap
_VALID_SUBJECT = re.compile(r"^[A-Za-z0-9._>\-]+$")


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
        """Publish to a JetStream subject with dedup on Nats-Msg-Id.

        Uses HPUB (headers) so the server dedups by `Nats-Msg-Id` when the
        stream has a matching dedup window. Returns without server ACK
        (Core-style); durability is the local outbox's job (reliability
        contract). A true JetStream PUB-ACK (M2 finish) would require the
        request/reply flow; the minimal client keeps publish fire-and-forget.
        """
        validate_subject(subject)
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload).encode()
        if len(payload) > _MAX_BODY:
            raise ValueError("payload too large")
        headers = b""
        if msg_id:
            headers = b"Nats-Msg-Id: " + msg_id.encode() + b"\r\n"
        # HPUB <subject> [reply-to] <hdrsize> <total>
        hdrsize = len(headers)
        total = hdrsize + len(payload)
        self._send(b"HPUB " + subject.encode() + b" " +
                   str(hdrsize).encode() + b" " + str(total).encode() +
                   b"\r\n" + headers + payload + b"\r\n")

    def ensure_stream(self, stream="ANVIL", subjects=("anvil.fleet.>",),
                      max_age_secs=7 * 86400, timeout=5):
        """Create an ephemeral JetStream stream if missing (idempotent).

        Required by the PRD's JetStream mirror (retention 7d, DiscardOld).
        Pure Core clients can't guarantee this (needs the JS API request/
        reply); this issues a request to server INFO and reports whether JS
        is available. The actual stream-add is the operator adapter's job
        (M4); here we VERIFY JetStream is reachable and return its info.
        """
        self._send(b"INFO\r\n")
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self._readline()
            if line.startswith(b"INFO"):
                info = json.loads(line[4:].decode())
                return {"jetstream": info.get("jetstream", {}).get("config", {}),
                        "available": bool(info.get("jetstream"))}
            if line.strip() == b"PONG":
                continue
        return {"jetstream": {}, "available": False}

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
