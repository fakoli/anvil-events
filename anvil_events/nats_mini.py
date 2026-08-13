"""Minimal stdlib NATS + JetStream PubAck client.

Implements CONNECT, PING/PONG, PUB/HPUB, SUB, MSG/HMSG reads, message-ID
deduplication headers, and JetStream PubAck request/reply. The producer-local
outbox remains authoritative; PubAck gates archival (per the PRD reliability
contract).
"""
import json
import re
import secrets
import socket

PROTO = {"verbose": False, "pedantic": False, "tls_required": False,
         "headers": True, "name": "anvil-events"}
# nats-server's default max_payload is 1 MiB. Reserve room for HPUB headers
# and framing so anything accepted into the durable outbox is publishable.
_BROKER_MAX_PAYLOAD = 1024 * 1024
_PUBLISH_OVERHEAD = 1024
_MAX_BODY = _BROKER_MAX_PAYLOAD - _PUBLISH_OVERHEAD
_VALID_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_VALID_HEADER_VALUE = re.compile(r"[^\r\n]+")   # used with fullmatch


def encode_js_publish(payload, msg_id=None):
    """Return `(body, headers)` only when the exact HPUB payload is publishable."""
    if isinstance(payload, dict | list):
        payload = json.dumps(payload, sort_keys=True).encode()
    elif isinstance(payload, str):
        payload = payload.encode()
    if not isinstance(payload, bytes):
        raise TypeError("payload must be bytes, str, dict, or list")
    if len(payload) > _MAX_BODY:
        raise ValueError("payload too large for JetStream stream")
    headers = [b"NATS/1.0"]
    if msg_id:
        if (not isinstance(msg_id, str)
                or not _VALID_HEADER_VALUE.fullmatch(msg_id)):
            raise ValueError("invalid Nats-Msg-Id header value")
        headers.append(b"Nats-Msg-Id: " + msg_id.encode("utf-8"))
    header_block = b"\r\n".join(headers) + b"\r\n\r\n"
    total = len(header_block) + len(payload)
    if total > _MAX_BODY:
        raise ValueError("HPUB headers and payload exceed stream max_msg_size")
    if total > _BROKER_MAX_PAYLOAD:
        raise ValueError("HPUB headers and payload exceed broker max_payload")
    return payload, header_block


def parse_url(url):
    """Parse nats://[host][:port] (127.0.0.1:4222 default)."""
    rest = (url or "").replace("nats://", "")
    if not rest:
        return "127.0.0.1", 4222
    if ":" in rest:
        host, port = rest.rsplit(":", 1)
        return (host or "127.0.0.1"), int(port)
    return rest, 4222


def validate_subject(subject, *, allow_wildcards=False):
    """Validate NATS tokens; wildcards are subscription-only and standalone."""
    if not isinstance(subject, str) or not subject:
        raise ValueError(f"invalid subject {subject!r}")
    tokens = subject.split(".")
    if any(not token for token in tokens):
        raise ValueError(f"invalid subject {subject!r}")
    for index, token in enumerate(tokens):
        if token in ("*", ">"):
            if not allow_wildcards or (token == ">" and index != len(tokens) - 1):
                raise ValueError(f"invalid subject {subject!r}")
        elif not _VALID_TOKEN.fullmatch(token):
            raise ValueError(f"invalid subject {subject!r}")
    return subject


def _parse_uint(token, label, *, positive=False):
    """Parse an unsigned decimal NATS protocol token with strict grammar."""
    if not isinstance(token, bytes) or not token or not token.isdigit():
        raise OSError(f"malformed {label}")
    value = int(token)
    if positive and value == 0:
        raise OSError(f"invalid {label}")
    return value


class NATSClient:
    def __init__(self, url="nats://127.0.0.1:4222"):
        self.url = url
        self.sock = None
        self._buf = b""
        self._subscriptions = {}
        self._next_sid = 1
        self._pending_messages = []

    def connect(self, timeout=5):
        host, port = parse_url(self.url)
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        info_line = self._readline()
        if not info_line.startswith(b"INFO "):
            raise OSError(f"bad handshake: {info_line[:80]!r}")
        try:
            self.server_info = json.loads(info_line[5:])
        except json.JSONDecodeError as exc:
            raise OSError("malformed INFO handshake") from exc
        if not isinstance(self.server_info, dict):
            raise OSError("malformed INFO handshake")
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
        if n < 0 or n > _MAX_BODY:
            raise OSError(f"frame too large: {n}")
        while len(self._buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise OSError("connection closed")
            self._buf += chunk
        body = self._buf[:n]
        terminator = self._buf[n:n + 2]
        if terminator != b"\r\n":
            raise OSError("malformed message body terminator")
        self._buf = self._buf[n + 2:]
        return body

    def publish(self, subject, payload):
        validate_subject(subject)
        if isinstance(payload, dict | list):
            payload = json.dumps(payload).encode()
        if len(payload) > _MAX_BODY:
            raise ValueError("payload too large")
        self._send(b"PUB " + subject.encode() + b" " +
                   str(len(payload)).encode() + b"\r\n" + payload + b"\r\n")

    def _subscribe_subject(self, subject):
        validate_subject(subject, allow_wildcards=True)
        if subject not in self._subscriptions:
            sid = self._next_sid
            self._next_sid += 1
            self._subscriptions[subject] = sid
            self._send(b"SUB " + subject.encode() + b" " + str(sid).encode() + b"\r\n")
        return self._subscriptions[subject]

    def _request_json(self, subject, payload, timeout=5):
        """Request a JetStream API subject and decode one JSON response."""
        if not subject.startswith("$JS.API.") or any(c in subject for c in "\r\n \t"):
            raise ValueError(f"invalid JetStream API subject {subject!r}")
        inbox = "_INBOX.anvil_events." + secrets.token_hex(12)
        sid = self._subscribe_subject(inbox)
        self._send(b"UNSUB " + str(sid).encode() + b" 1\r\n")
        body = json.dumps(payload, separators=(",", ":")).encode()
        self._send(b"PUB " + subject.encode() + b" " + inbox.encode() + b" " +
                   str(len(body)).encode() + b"\r\n" + body + b"\r\n")
        try:
            messages = self.receive(count=1, timeout=timeout, subscription=inbox)
        finally:
            self._subscriptions.pop(inbox, None)
        if not messages:
            raise TimeoutError(f"JetStream API request timed out: {subject}")
        try:
            response = json.loads(messages[0]["body"])
        except (json.JSONDecodeError, TypeError) as exc:
            raise OSError("malformed JetStream API response") from exc
        if response.get("error"):
            raise OSError(f"JetStream API error: {response['error']}")
        return response

    def bind_durable_consumer(self, stream, durable, filter_subject,
                              timeout=5):
        """Create/update a stable explicit-ACK push consumer from history."""
        for token in (stream, durable):
            if not isinstance(token, str) or not _VALID_TOKEN.fullmatch(token):
                raise ValueError(f"invalid JetStream name {token!r}")
        validate_subject(filter_subject, allow_wildcards=True)
        deliver_subject = f"anvil.delivery.{durable}"
        self._subscribe_subject(deliver_subject)
        request = {
            "stream_name": stream,
            "config": {
                "durable_name": durable,
                "name": durable,
                "deliver_subject": deliver_subject,
                "deliver_policy": "all",
                "ack_policy": "explicit",
                "ack_wait": 30_000_000_000,
                "filter_subject": filter_subject,
                "replay_policy": "instant",
            },
        }
        self._request_json(
            f"$JS.API.CONSUMER.DURABLE.CREATE.{stream}.{durable}",
            request, timeout=timeout,
        )
        return deliver_subject

    def ack(self, reply_subject):
        """Explicitly ACK one JetStream delivery after durable local handling."""
        if (not isinstance(reply_subject, str)
                or not reply_subject.startswith("$JS.ACK.")
                or any(c in reply_subject for c in "\r\n \t")):
            raise ValueError("invalid JetStream ACK subject")
        self._send(b"PUB " + reply_subject.encode() + b" 0\r\n\r\n")

    # -- JetStream: durable subjects + PubAck + Nats-Msg-Id dedup ----------
    def publish_js(self, subject, payload, msg_id=None, wait_ack=False, timeout=5):
        """Publish with JetStream headers and optionally require a PubAck.

        ``wait_ack=True`` uses NATS request/reply: a unique inbox subscription
        is installed before HPUB, and success is returned only for a JSON
        PubAck containing ``stream`` and positive ``seq``. Callers must retain
        their durable outbox entry on timeout or error.
        """
        validate_subject(subject)
        payload, header_block = encode_js_publish(payload, msg_id)
        hdrsize = len(header_block)
        total = hdrsize + len(payload)
        inbox = None
        if wait_ack:
            inbox = "_INBOX.anvil_events." + secrets.token_hex(12)
            sid = self._next_sid
            self._next_sid += 1
            self._subscriptions[inbox] = sid
            self._send(b"SUB " + inbox.encode() + b" " + str(sid).encode() + b"\r\n")
            self._send(b"UNSUB " + str(sid).encode() + b" 1\r\n")
        control = b"HPUB " + subject.encode()
        if inbox:
            control += b" " + inbox.encode()
        self._send(control + b" " + str(hdrsize).encode() + b" " +
                   str(total).encode() + b"\r\n" + header_block + payload + b"\r\n")
        if not wait_ack:
            return None
        try:
            ack_bodies = self.subscribe(inbox, count=1, timeout=timeout)
        finally:
            self._subscriptions.pop(inbox, None)
        if not ack_bodies:
            raise TimeoutError("JetStream PubAck timed out")
        try:
            ack = json.loads(ack_bodies[0])
        except (json.JSONDecodeError, TypeError) as exc:
            raise OSError("malformed JetStream PubAck") from exc
        if ack.get("error"):
            raise OSError(f"JetStream PubAck error: {ack['error']}")
        if not ack.get("stream") or int(ack.get("seq", 0)) <= 0:
            raise OSError(f"invalid JetStream PubAck: {ack!r}")
        return ack

    def ensure_stream(self, stream="ANVIL", subjects=("anvil.fleet.>",),
                      max_age_secs=7 * 86400, timeout=5, client_factory=None):
        """Report broker reachability + JetStream availability honestly.

        Stream provisioning is deliberately declarative (`deploy/nats-stream.json`).
        This checks server INFO and reports whether JetStream is enabled so
        callers can fail loudly when the broker cannot durably mirror events.

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

    def receive(self, count=1, timeout=10, subscription=None):
        """Receive messages with payload, subject, and optional reply subject."""
        got = []
        wanted_sid = self._subscriptions.get(subscription) if subscription else None
        keep = []
        for message in self._pending_messages:
            if len(got) < count and (wanted_sid is None or message["sid"] == wanted_sid):
                got.append(message)
            else:
                keep.append(message)
        self._pending_messages = keep
        import time
        deadline = time.time() + timeout
        try:
            while len(got) < count and time.time() < deadline:
                self.sock.settimeout(max(0.1, deadline - time.time()))
                line = self._readline()
                if line == b"PING":
                    self._send(b"PONG\r\n")
                    continue
                if line == b"+OK" or line.startswith(b"INFO "):
                    continue
                if line.startswith(b"MSG "):
                    parts = line.split(b" ")
                    if len(parts) not in (4, 5):
                        raise OSError(f"malformed MSG: {line[:80]!r}")
                    nbytes = _parse_uint(parts[-1], "MSG size")
                    body = self._read_n(nbytes)
                    message = {
                        "subject": parts[1].decode(),
                        "sid": _parse_uint(parts[2], "MSG sid", positive=True),
                        "reply": parts[3].decode() if len(parts) == 5 else None,
                        "body": body,
                    }
                    if wanted_sid is None or message["sid"] == wanted_sid:
                        got.append(message)
                    else:
                        self._pending_messages.append(message)
                elif line.startswith(b"HMSG "):
                    # HMSG <subject> <sid> [reply] <hdr-bytes> <total-bytes>
                    # Header-capable subscribers receive HPUB as HMSG. The
                    # daemon validates JSON, so return only the payload after
                    # the NATS header block.
                    parts = line.split(b" ")
                    if len(parts) not in (5, 6):
                        raise OSError(f"malformed HMSG: {line[:80]!r}")
                    header_bytes = _parse_uint(parts[-2], "HMSG header size")
                    total_bytes = _parse_uint(parts[-1], "HMSG total size")
                    if total_bytes < header_bytes:
                        raise OSError(f"invalid HMSG sizes: {line[:80]!r}")
                    body = self._read_n(total_bytes)
                    message = {
                        "subject": parts[1].decode(),
                        "sid": _parse_uint(parts[2], "HMSG sid", positive=True),
                        "reply": parts[3].decode() if len(parts) == 6 else None,
                        "body": body[header_bytes:],
                    }
                    if wanted_sid is None or message["sid"] == wanted_sid:
                        got.append(message)
                    else:
                        self._pending_messages.append(message)
                else:
                    raise OSError(f"unknown NATS protocol line: {line[:80]!r}")
        except TimeoutError:
            pass
        return got

    def subscribe(self, subject, count=1, timeout=10):
        """Core subscription compatibility wrapper returning payload bytes."""
        self._subscribe_subject(subject)
        return [message["body"] for message in self.receive(
            count, timeout, subscription=subject,
        )]

    def close(self):
        if self.sock:
            try:
                self._send(b"PING\r\n")
            except Exception:
                pass
            self.sock.close()
            self.sock = None
