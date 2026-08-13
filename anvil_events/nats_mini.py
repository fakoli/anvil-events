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
        raise ValueError(f"invalid subject {subject!r}")
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
        info_line = self._readline()
        if not info_line.strip().startswith(b"INFO"):
            raise OSError(f"bad handshake: {info_line[:80]!r}")
        try:
            self.server_info = json.loads(info_line[4:].strip())
        except Exception:
            self.server_info = {}
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
                raise OSError("connection closed")
            self._buf += chunk
            if len(self._buf) > 2 * _MAX_BODY:
                raise OSError("protocol buffer overflow")
        line, self._buf = self._buf.split(b"\r\n", 1)
        if line.startswith(b"-ERR"):
            raise OSError(line.decode(errors="replace"))
        return line

    def _read_n(self, n):
        if n > _MAX_BODY:
            raise OSError(f"frame too large: {n}")
        while len(self._buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise OSError("connection closed")
            self._buf += chunk
        body, self._buf = self._buf[:n], self._buf[n + 2:]
        return body

    def publish(self, subject, payload):
        validate_subject(subject)
        if isinstance(payload, dict | list):
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
        if isinstance(payload, dict | list):
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
                      max_age_secs=7 * 86400, timeout=5, client_factory=None):
        """Report broker reachability + JetStream availability honestly.

        The minimal client does NOT create or inventory streams — that is the
        operator adapter's job in M4 (it requires the JetStream request/reply
        API). This checks the server's INFO (received during connect) and
        reports whether JetStream is enabled so callers can fail loudly (not
        silently drop) when the broker has no JetStream.

        `client_factory` is injectable for tests (default: connect to self.url).

        Returns {"reachable": bool, "jetstream_available": bool}.
        """
        try:
            if client_factory is None:
                def _default_factory():
                    return NATSClient(self.url).connect(timeout=timeout)
                client_factory = _default_factory
            c = client_factory()
            info = getattr(c, "server_info", {})
            js = info.get("jetstream", False)
            # server INFO shape: `jetstream: true` (bool) OR
            # `jetstream: {"config": {"enabled": true, ...}}` (dict)
            if isinstance(js, dict):
                enabled = bool(js.get("config", {}).get("enabled"))
            else:
                enabled = bool(js)
            c.close()
            return {"reachable": True, "jetstream_available": enabled}
        except Exception:
            return {"reachable": False, "jetstream_available": False}

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
        except TimeoutError:
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
