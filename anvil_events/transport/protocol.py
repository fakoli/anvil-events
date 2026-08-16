"""Strict NATS wire framing with no endpoint or credential policy."""

from __future__ import annotations

import json
import re
import time

BROKER_MAX_PAYLOAD = 1024 * 1024
PUBLISH_OVERHEAD = 1024
MAX_BODY = BROKER_MAX_PAYLOAD - PUBLISH_OVERHEAD
VALID_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_VALID_HEADER_VALUE = re.compile(r"[^\r\n]+")


def encode_js_publish(payload, msg_id=None):
    """Encode a payload and optional JetStream deduplication header."""
    if isinstance(payload, dict | list):
        payload = json.dumps(payload, sort_keys=True, allow_nan=False).encode()
    elif isinstance(payload, str):
        payload = payload.encode()
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes, str, dict, or list")
    if len(payload) > MAX_BODY:
        raise ValueError("payload too large for JetStream stream")
    headers = [b"NATS/1.0"]
    if msg_id:
        if not isinstance(msg_id, str) or not _VALID_HEADER_VALUE.fullmatch(msg_id):
            raise ValueError("invalid Nats-Msg-Id header value")
        headers.append(b"Nats-Msg-Id: " + msg_id.encode())
    header_block = b"\r\n".join(headers) + b"\r\n\r\n"
    total = len(header_block) + len(payload)
    if total > MAX_BODY:
        raise ValueError("HPUB headers and payload exceed stream max_msg_size")
    if total > BROKER_MAX_PAYLOAD:
        raise ValueError("HPUB headers and payload exceed broker max_payload")
    return payload, header_block


def validate_subject(subject, *, allow_wildcards=False):
    """Validate NATS subjects; wildcards are subscription-only."""
    if not isinstance(subject, str) or not subject:
        raise ValueError(f"invalid subject {subject!r}")
    tokens = subject.split(".")
    if any(not token for token in tokens):
        raise ValueError(f"invalid subject {subject!r}")
    for index, token in enumerate(tokens):
        if token in ("*", ">"):
            if not allow_wildcards or (token == ">" and index != len(tokens) - 1):
                raise ValueError(f"invalid subject {subject!r}")
        elif not VALID_TOKEN.fullmatch(token):
            raise ValueError(f"invalid subject {subject!r}")
    return subject


def validate_wire_subject(subject, *, allow_wildcards=False):
    """Validate broker-generated/system subjects using the NATS wire grammar."""
    if not isinstance(subject, str) or not subject:
        raise ValueError(f"invalid subject {subject!r}")
    tokens = subject.split(".")
    if any(not token for token in tokens):
        raise ValueError(f"invalid subject {subject!r}")
    for index, token in enumerate(tokens):
        if token in ("*", ">"):
            if not allow_wildcards or (token == ">" and index != len(tokens) - 1):
                raise ValueError(f"invalid subject {subject!r}")
        elif any(character.isspace() or character in ".*>" for character in token):
            raise ValueError(f"invalid subject {subject!r}")
    return subject


def parse_uint(token, label, *, positive=False):
    if not isinstance(token, bytes) or not token or not token.isdigit():
        raise OSError(f"malformed {label}")
    value = int(token)
    if positive and value == 0:
        raise OSError(f"invalid {label}")
    return value


class NATSWire:
    """One connected NATS socket and its protocol parser state."""

    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""
        self.subscriptions = {}
        self.next_sid = 1
        self.pending_messages = []

    def replace_socket(self, sock):
        if self.buffer:
            raise OSError("cannot start TLS with buffered protocol bytes")
        self.sock = sock

    def send(self, data):
        self.sock.sendall(data)

    def readline(self):
        while b"\r\n" not in self.buffer:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise OSError("connection closed")
            self.buffer += chunk
            if len(self.buffer) > 2 * MAX_BODY:
                raise OSError("protocol buffer overflow")
        line, self.buffer = self.buffer.split(b"\r\n", 1)
        if line.startswith(b"-ERR"):
            raise OSError(line.decode(errors="replace"))
        return line

    def read_body(self, size):
        if size < 0 or size > MAX_BODY:
            raise OSError(f"frame too large: {size}")
        while len(self.buffer) < size + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise OSError("connection closed")
            self.buffer += chunk
        body = self.buffer[:size]
        if self.buffer[size:size + 2] != b"\r\n":
            raise OSError("malformed message body terminator")
        self.buffer = self.buffer[size + 2:]
        return body

    def subscribe(self, subject):
        validate_subject(subject, allow_wildcards=True)
        if subject not in self.subscriptions:
            sid = self.next_sid
            self.next_sid += 1
            self.subscriptions[subject] = sid
            self.send(
                b"SUB " + subject.encode() + b" " + str(sid).encode() + b"\r\n"
            )
        return self.subscriptions[subject]

    def receive(self, count=1, timeout=10, subscription=None):
        if isinstance(count, bool) or count < 1:
            raise ValueError("count must be positive")
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        wanted_sid = self.subscriptions.get(subscription) if subscription else None
        got, keep = [], []
        for message in self.pending_messages:
            if len(got) < count and (
                    wanted_sid is None or message["sid"] == wanted_sid):
                got.append(message)
            else:
                keep.append(message)
        self.pending_messages = keep
        deadline = time.monotonic() + timeout
        try:
            while len(got) < count and time.monotonic() < deadline:
                self.sock.settimeout(max(0.01, deadline - time.monotonic()))
                line = self.readline()
                if line == b"PING":
                    self.send(b"PONG\r\n")
                    continue
                if line == b"+OK" or line.startswith(b"INFO "):
                    continue
                message = self._decode_message(line)
                if wanted_sid is None or message["sid"] == wanted_sid:
                    got.append(message)
                else:
                    self.pending_messages.append(message)
        except TimeoutError:
            pass
        return got

    def _decode_message(self, line):
        if line.startswith(b"MSG "):
            parts = line.split(b" ")
            if len(parts) not in (4, 5):
                raise OSError(f"malformed MSG: {line[:80]!r}")
            size = parse_uint(parts[-1], "MSG size")
            body = self.read_body(size)
            reply = parts[3].decode() if len(parts) == 5 else None
        elif line.startswith(b"HMSG "):
            parts = line.split(b" ")
            if len(parts) not in (5, 6):
                raise OSError(f"malformed HMSG: {line[:80]!r}")
            header_size = parse_uint(parts[-2], "HMSG header size")
            total_size = parse_uint(parts[-1], "HMSG total size")
            if total_size < header_size:
                raise OSError(f"invalid HMSG sizes: {line[:80]!r}")
            body = self.read_body(total_size)[header_size:]
            reply = parts[3].decode() if len(parts) == 6 else None
        else:
            raise OSError(f"unknown NATS protocol line: {line[:80]!r}")
        subject = parts[1].decode()
        validate_wire_subject(subject)
        if reply is not None:
            validate_wire_subject(reply)
        return {
            "subject": subject,
            "sid": parse_uint(parts[2], "message sid", positive=True),
            "reply": reply,
            "body": body,
        }

    def close(self):
        if self.sock is None:
            return
        try:
            self.send(b"PING\r\n")
            while self.readline().strip() != b"PONG":
                pass
        except Exception:
            pass
        finally:
            self.sock.close()
            self.sock = None
