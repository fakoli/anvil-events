"""Composable stdlib NATS and JetStream client."""

from __future__ import annotations

import json
import os
import secrets
import socket

from .jetstream import validate_stream_config, verify_stream_config
from .protocol import (
    MAX_BODY,
    VALID_TOKEN,
    NATSWire,
    encode_js_publish,
    validate_subject,
    validate_wire_subject,
)
from .security import SecurityConfig


class NATSClient:
    def __init__(self, url="nats://127.0.0.1:4222", **security_options):
        self.url = url
        self.security = SecurityConfig.from_environment(**security_options)
        self.wire = None
        self.server_info = {}

    @classmethod
    def from_env(cls, url=None):
        return cls(url or os.environ.get(
            "ANVIL_EVENTS_NATS_URL", "nats://127.0.0.1:4222",
        ))

    @property
    def sock(self):
        return self.wire.sock if self.wire else None

    def connect(self, timeout=5):
        scheme, host, port = self.security.validate(self.url)
        raw_socket = socket.create_connection((host, port), timeout=timeout)
        self.wire = NATSWire(raw_socket)
        try:
            if scheme == "tls" and self.security.handshake_first:
                self._start_tls(host)
                info_line = self.wire.readline()
            else:
                info_line = self.wire.readline()
                if scheme == "tls":
                    self._load_server_info(info_line)
                    if not (
                        self.server_info.get("tls_required")
                        or self.server_info.get("tls_available")
                    ):
                        raise OSError("server did not advertise TLS")
                    self._start_tls(host)
            self._load_server_info(info_line)
            if self.server_info.get("tls_required") and scheme != "tls":
                raise OSError("server requires TLS; refusing a plaintext connection")
            has_certificate = bool(
                self.security.cert_file and self.security.key_file
            )
            if self.server_info.get("auth_required") and not (
                self.security.token or self.security.username or has_certificate
            ):
                raise OSError("server requires authentication")
            options = self.security.connect_options(scheme == "tls")
            self.wire.send(
                b"CONNECT " + json.dumps(options, separators=(",", ":")).encode()
                + b"\r\nPING\r\n"
            )
            while self.wire.readline().strip() != b"PONG":
                pass
            return self
        except Exception:
            self.close()
            raise

    def _start_tls(self, host):
        if self.wire.buffer:
            raise OSError("cannot start TLS with buffered protocol bytes")
        context = self.security.tls_context()
        wrapped = context.wrap_socket(
            self.wire.sock,
            server_hostname=self.security.server_name or host,
        )
        self.wire.replace_socket(wrapped)

    def _load_server_info(self, line):
        if not line.startswith(b"INFO "):
            raise OSError(f"bad handshake: {line[:80]!r}")
        try:
            info = json.loads(line[5:])
        except json.JSONDecodeError as exc:
            raise OSError("malformed INFO handshake") from exc
        if not isinstance(info, dict):
            raise OSError("malformed INFO handshake")
        self.server_info = info

    def publish(self, subject, payload):
        validate_subject(subject)
        if isinstance(payload, dict | list):
            payload = json.dumps(payload, allow_nan=False).encode()
        elif isinstance(payload, str):
            payload = payload.encode()
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes, str, dict, or list")
        if len(payload) > MAX_BODY:
            raise ValueError("payload too large")
        self.wire.send(
            b"PUB " + subject.encode() + b" " + str(len(payload)).encode()
            + b"\r\n" + payload + b"\r\n"
        )

    def _request_json(self, subject, payload, timeout=5):
        response = self._request_json_raw(subject, payload, timeout)
        if response.get("error"):
            raise OSError(f"JetStream API error: {response['error']}")
        return response

    @staticmethod
    def _decode_response(body, label):
        try:
            response = json.loads(body)
        except (json.JSONDecodeError, TypeError) as exc:
            raise OSError(f"malformed {label} response") from exc
        if not isinstance(response, dict):
            raise OSError(f"malformed {label} response")
        if response.get("error"):
            raise OSError(f"{label} error: {response['error']}")
        return response

    def _request_json_raw(self, subject, payload, timeout=5):
        """Request one JetStream API response without interpreting its error."""
        if not subject.startswith("$JS.API.") or any(c in subject for c in "\r\n \t"):
            raise ValueError(f"invalid JetStream API subject {subject!r}")
        inbox = "_INBOX.anvil_events." + secrets.token_hex(12)
        sid = self.wire.subscribe(inbox)
        self.wire.send(b"UNSUB " + str(sid).encode() + b" 1\r\n")
        body = json.dumps(
            payload, separators=(",", ":"), allow_nan=False,
        ).encode()
        self.wire.send(
            b"PUB " + subject.encode() + b" " + inbox.encode() + b" "
            + str(len(body)).encode() + b"\r\n" + body + b"\r\n"
        )
        try:
            messages = self.receive(1, timeout, subscription=inbox)
        finally:
            self.wire.subscriptions.pop(inbox, None)
        if not messages:
            raise TimeoutError(f"JetStream API request timed out: {subject}")
        try:
            response = json.loads(messages[0]["body"])
        except (json.JSONDecodeError, TypeError) as exc:
            raise OSError("malformed JetStream API response") from exc
        if not isinstance(response, dict):
            raise OSError("malformed JetStream API response")
        return response

    def configure_stream(self, config, timeout=5):
        """Create a stream or verify that the existing stream matches exactly."""
        name = validate_stream_config(config)
        existing = self._request_json_raw(
            f"$JS.API.STREAM.INFO.{name}", {}, timeout,
        )
        error = existing.get("error")
        if not error:
            actual = verify_stream_config(config, existing, "stream info")
            return {"created": False, "config": actual}
        if not isinstance(error, dict) or error.get("code") != 404:
            code = error.get("code") if isinstance(error, dict) else "malformed"
            error_code = (
                error.get("err_code") if isinstance(error, dict) else "malformed"
            )
            raise OSError(
                f"JetStream stream info failed: code={code} "
                f"err_code={error_code}"
            )
        created = self._request_json_raw(
            f"$JS.API.STREAM.CREATE.{name}", config, timeout,
        )
        if created.get("error"):
            raise OSError(f"JetStream stream create failed: {created['error']}")
        verify_stream_config(config, created, "create response")
        confirmed = self._request_json_raw(
            f"$JS.API.STREAM.INFO.{name}", {}, timeout,
        )
        if confirmed.get("error"):
            raise OSError(
                "JetStream created stream could not be verified: "
                f"{confirmed['error']}"
            )
        actual = verify_stream_config(config, confirmed, "post-create info")
        return {"created": True, "config": actual}

    def bind_durable_consumer(self, stream, durable, filter_subject, timeout=5):
        for token in (stream, durable):
            if not isinstance(token, str) or not VALID_TOKEN.fullmatch(token):
                raise ValueError(f"invalid JetStream name {token!r}")
        validate_subject(filter_subject, allow_wildcards=True)
        deliver_subject = f"anvil.delivery.{durable}"
        self.wire.subscribe(deliver_subject)
        self._request_json(
            f"$JS.API.CONSUMER.DURABLE.CREATE.{stream}.{durable}",
            {
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
            },
            timeout,
        )
        return deliver_subject

    def ack(self, reply_subject):
        if not isinstance(reply_subject, str) or not reply_subject.startswith(
                "$JS.ACK."):
            raise ValueError("invalid JetStream ACK subject")
        validate_wire_subject(reply_subject)
        self.wire.send(b"PUB " + reply_subject.encode() + b" 0\r\n\r\n")

    def publish_js(self, subject, payload, msg_id=None, wait_ack=False, timeout=5):
        validate_subject(subject)
        payload, headers = encode_js_publish(payload, msg_id)
        inbox = None
        if wait_ack:
            inbox = "_INBOX.anvil_events." + secrets.token_hex(12)
            sid = self.wire.subscribe(inbox)
            self.wire.send(b"UNSUB " + str(sid).encode() + b" 1\r\n")
        control = b"HPUB " + subject.encode()
        if inbox:
            control += b" " + inbox.encode()
        total = len(headers) + len(payload)
        self.wire.send(
            control + b" " + str(len(headers)).encode() + b" "
            + str(total).encode() + b"\r\n" + headers + payload + b"\r\n"
        )
        if not wait_ack:
            return None
        try:
            messages = self.receive(1, timeout, subscription=inbox)
        finally:
            self.wire.subscriptions.pop(inbox, None)
        if not messages:
            raise TimeoutError("JetStream PubAck timed out")
        ack = self._decode_response(messages[0]["body"], "JetStream PubAck")
        sequence = ack.get("seq")
        if (not isinstance(sequence, int) or isinstance(sequence, bool)
                or sequence < 1):
            raise OSError("invalid JetStream PubAck sequence")
        if not isinstance(ack.get("stream"), str) or not ack["stream"]:
            raise OSError(f"invalid JetStream PubAck: {ack!r}")
        return ack

    def receive(self, count=1, timeout=10, subscription=None):
        return self.wire.receive(count, timeout, subscription)

    def subscribe(self, subject, count=1, timeout=10):
        self.wire.subscribe(subject)
        return [message["body"] for message in self.receive(
            count, timeout, subscription=subject,
        )]

    def close(self):
        if self.wire:
            self.wire.close()
            self.wire = None

    def abort(self):
        """Immediately interrupt a blocking receive during service shutdown."""
        if self.wire and self.wire.sock:
            try:
                self.wire.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.wire.sock.close()
            except OSError:
                pass
            self.wire.sock = None
