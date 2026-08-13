"""Minimal stdlib NATS client (core protocol, loopback).

Implements just enough for the anvil-events spike: CONNECT handshake,
PING/PONG, PUB, SUB, blocking MSG read with --count/--timeout.
JetStream (durable subjects) is the M2 upgrade path; the local outbox is the
durability mechanism for this spike (per PRD reliability contract).
"""
import json
import socket

PROTO = {"verbose": False, "pedantic": False, "tls_required": False,
         "name": "anvil-events"}


def parse_url(url):
    host = url.replace("nats://", "").split(":")[0] or "127.0.0.1"
    port = 4222
    if ":" in url.replace("nats://", ""):
        port = int(url.replace("nats://", "").split(":")[1])
    return host, port


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
            chunk = self.sock.recv(4096)
            if not chunk:
                raise IOError("connection closed")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\r\n", 1)
        return line

    def _read_n(self, n):
        while len(self._buf) < n + 2:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise IOError("connection closed")
            self._buf += chunk
        body, self._buf = self._buf[:n], self._buf[n + 2:]
        return body

    def publish(self, subject, payload):
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload).encode()
        self._send(b"PUB " + subject.encode() + b" " +
                   str(len(payload)).encode() + b"\r\n" + payload + b"\r\n")

    def subscribe(self, subject, count=1, timeout=10):
        """Block until `count` messages (or timeout); yield payloads."""
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
                    # MSG <subject> <sid> [reply] <nbytes>
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
