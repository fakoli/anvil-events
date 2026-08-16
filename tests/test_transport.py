from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from anvil_events.transport.client import NATSClient
from anvil_events.transport.protocol import (
    NATSWire,
    encode_js_publish,
    validate_subject,
    validate_wire_subject,
)
from anvil_events.transport.security import SecurityConfig, parse_endpoint


class FakeSocket:
    def __init__(self, *chunks):
        self.chunks = list(chunks)
        self.sent = bytearray()
        self.closed = False
        self.timeout = None

    def recv(self, size):
        if not self.chunks:
            raise TimeoutError("no scripted input")
        return self.chunks.pop(0)

    def sendall(self, data):
        self.sent.extend(data)

    def settimeout(self, timeout):
        self.timeout = timeout

    def close(self):
        self.closed = True

    def shutdown(self, how):
        self.closed = True


class FakeTLSContext:
    def __init__(self):
        self.calls = []

    def wrap_socket(self, sock, server_hostname):
        self.calls.append((sock, server_hostname))
        return sock


class FakeWire:
    def __init__(self, messages=None):
        self.sent = []
        self.subscriptions = {}
        self.messages = list(messages or [])
        self.closed = False
        self.sock = FakeSocket()

    def send(self, data):
        self.sent.append(data)

    def subscribe(self, subject):
        self.subscriptions[subject] = 1
        return 1

    def receive(self, count=1, timeout=10, subscription=None):
        return self.messages[:count]

    def close(self):
        self.closed = True


class SecurityPolicyTests(unittest.TestCase):
    def test_endpoint_rejects_credentials_and_url_data(self):
        for value in (
            "nats://user:pass@localhost:4222",
            "nats://localhost:4222/path",
            "nats://localhost:4222?token=x",
            "http://localhost:4222",
        ):
            with self.assertRaises(ValueError):
                parse_endpoint(value)

    def test_plaintext_is_loopback_development_only(self):
        config = SecurityConfig.from_environment(mode="development")
        self.assertEqual(
            ("nats", "127.0.0.1", 4222),
            config.validate("nats://127.0.0.1:4222"),
        )
        with self.assertRaisesRegex(ValueError, "loopback"):
            config.validate("nats://100.64.0.10:4222")

    def test_isolated_container_host_requires_explicit_single_label(self):
        config = SecurityConfig.from_environment(
            mode="development", development_hosts=["nats"],
        )
        self.assertEqual("nats", config.validate("nats://nats:4222")[1])
        with self.assertRaises(ValueError):
            SecurityConfig.from_environment(
                development_hosts=["broker.internal"],
            )

    def test_fleet_requires_tls_and_identity(self):
        with self.assertRaisesRegex(ValueError, "tls"):
            SecurityConfig.from_environment(
                mode="fleet", username="node-a", password="secret",
            ).validate("nats://broker.internal:4222")
        with self.assertRaisesRegex(ValueError, "requires"):
            SecurityConfig.from_environment(mode="fleet").validate(
                "tls://broker.internal:4222",
            )
        config = SecurityConfig.from_environment(
            mode="fleet", username="node-a", password="secret",
        )
        self.assertEqual("tls", config.validate(
            "tls://broker.internal:4222",
        )[0])

    def test_security_repr_cannot_disclose_secret(self):
        config = SecurityConfig.from_environment(
            username="node-a", password="top-secret-value",
        )
        self.assertNotIn("top-secret-value", repr(config))

    def test_handshake_first_environment_is_strict(self):
        with patch.dict(os.environ, {
            "ANVIL_EVENTS_TLS_HANDSHAKE_FIRST": "true",
        }, clear=True):
            self.assertTrue(SecurityConfig.from_environment().handshake_first)
        with patch.dict(os.environ, {
            "ANVIL_EVENTS_TLS_HANDSHAKE_FIRST": "sometimes",
        }, clear=True):
            with self.assertRaisesRegex(ValueError, "true or false"):
                SecurityConfig.from_environment()


class ProtocolTests(unittest.TestCase):
    def test_subject_policy_separates_product_and_system_subjects(self):
        self.assertEqual("anvil.events.node-a", validate_subject(
            "anvil.events.node-a",
        ))
        with self.assertRaises(ValueError):
            validate_subject("$JS.ACK.stream")
        self.assertEqual("$JS.ACK.stream", validate_wire_subject(
            "$JS.ACK.stream",
        ))

    def test_wildcards_are_subscription_only(self):
        with self.assertRaises(ValueError):
            validate_subject("anvil.events.>")
        self.assertEqual("anvil.events.>", validate_subject(
            "anvil.events.>", allow_wildcards=True,
        ))
        with self.assertRaises(ValueError):
            validate_subject("anvil.>.events", allow_wildcards=True)

    def test_jetstream_encoding_has_dedup_header(self):
        payload, headers = encode_js_publish({"value": 1}, "node-a:x:000001")
        self.assertEqual(b'{"value": 1}', payload)
        self.assertIn(b"Nats-Msg-Id: node-a:x:000001", headers)

    def test_message_and_system_reply_are_decoded(self):
        socket = FakeSocket(
            b"MSG anvil.events.node-a 1 $JS.ACK.stream.consumer 5\r\n",
            b"hello\r\n",
        )
        wire = NATSWire(socket)
        wire.subscriptions["anvil.events.>"] = 1
        message = wire.receive(1, 1, "anvil.events.>")[0]
        self.assertEqual(b"hello", message["body"])
        self.assertEqual("$JS.ACK.stream.consumer", message["reply"])

    def test_header_message_returns_payload_only(self):
        headers = b"NATS/1.0\r\nKey: value\r\n\r\n"
        payload = b"body"
        socket = FakeSocket(
            f"HMSG anvil.events.node-a 1 {len(headers)} "
            f"{len(headers) + len(payload)}\r\n".encode(),
            headers + payload + b"\r\n",
        )
        wire = NATSWire(socket)
        wire.subscriptions["anvil.events.>"] = 1
        self.assertEqual(payload, wire.receive(
            1, 1, "anvil.events.>",
        )[0]["body"])

    def test_malformed_frame_is_rejected(self):
        wire = NATSWire(FakeSocket(b"MSG subject nope 1\r\nx\r\n"))
        with self.assertRaises(OSError):
            wire.receive(1, 1)


class ClientHandshakeTests(unittest.TestCase):
    def test_plaintext_handshake_sends_connect_then_ping(self):
        fake = FakeSocket(b'INFO {"auth_required":false}\r\n', b"PONG\r\n")
        with patch("socket.create_connection", return_value=fake):
            client = NATSClient("nats://127.0.0.1:4222").connect()
        sent = bytes(fake.sent)
        self.assertLess(sent.index(b"CONNECT "), sent.index(b"PING\r\n"))
        client.abort()

    def test_authentication_is_in_connect_options(self):
        fake = FakeSocket(b'INFO {"auth_required":true}\r\n', b"PONG\r\n")
        with patch("socket.create_connection", return_value=fake):
            client = NATSClient(
                "nats://127.0.0.1:4222", username="node-a", password="secret",
            ).connect()
        connect_line = bytes(fake.sent).split(b"\r\n", 1)[0]
        options = json.loads(connect_line.removeprefix(b"CONNECT "))
        self.assertEqual("node-a", options["user"])
        self.assertEqual("secret", options["pass"])
        client.abort()

    def test_default_tls_upgrades_after_info(self):
        fake = FakeSocket(b'INFO {"tls_required":true}\r\n', b"PONG\r\n")
        context = FakeTLSContext()
        with (
            patch("socket.create_connection", return_value=fake),
            patch.object(SecurityConfig, "tls_context", return_value=context),
        ):
            client = NATSClient(
                "tls://broker.internal:4222", mode="fleet",
                username="node-a", password="secret",
            ).connect()
        self.assertEqual("broker.internal", context.calls[0][1])
        client.abort()

    def test_tls_first_wraps_before_reading_info(self):
        fake = FakeSocket(b'INFO {"tls_required":true}\r\n', b"PONG\r\n")
        context = FakeTLSContext()
        with (
            patch("socket.create_connection", return_value=fake),
            patch.object(SecurityConfig, "tls_context", return_value=context),
        ):
            client = NATSClient(
                "tls://broker.internal:4222", mode="fleet",
                username="node-a", password="secret", handshake_first=True,
            ).connect()
        self.assertEqual(1, len(context.calls))
        client.abort()

    def test_tls_url_refuses_server_without_tls_advertisement(self):
        fake = FakeSocket(b"INFO {}\r\n")
        with patch("socket.create_connection", return_value=fake):
            with self.assertRaisesRegex(OSError, "advertise TLS"):
                NATSClient(
                    "tls://broker.internal:4222", mode="fleet",
                    username="node-a", password="secret",
                ).connect()
        self.assertTrue(fake.closed)

    def test_server_tls_requirement_rejects_plaintext(self):
        fake = FakeSocket(b'INFO {"tls_required":true}\r\n')
        with patch("socket.create_connection", return_value=fake):
            with self.assertRaisesRegex(OSError, "requires TLS"):
                NATSClient("nats://127.0.0.1:4222").connect()

    def test_auth_required_without_credentials_fails_before_connect(self):
        fake = FakeSocket(b'INFO {"auth_required":true}\r\n')
        with patch("socket.create_connection", return_value=fake):
            with self.assertRaisesRegex(OSError, "authentication"):
                NATSClient("nats://127.0.0.1:4222").connect()
        self.assertNotIn(b"CONNECT", bytes(fake.sent))

    def test_from_env_uses_configured_url(self):
        with patch.dict(os.environ, {
            "ANVIL_EVENTS_NATS_URL": "nats://localhost:4555",
        }, clear=True):
            self.assertEqual(
                "nats://localhost:4555", NATSClient.from_env().url,
            )


class ClientOperationTests(unittest.TestCase):
    def test_publish_and_ack_frames(self):
        client = NATSClient()
        client.wire = FakeWire()
        client.publish("anvil.events.node-a", {"value": 1})
        client.ack("$JS.ACK.stream.consumer")
        framed = b"".join(client.wire.sent)
        self.assertIn(b"PUB anvil.events.node-a", framed)
        self.assertIn(b"PUB $JS.ACK.stream.consumer 0", framed)

    def test_publish_js_parses_strict_puback(self):
        client = NATSClient()
        client.wire = FakeWire([{
            "subject": "_INBOX.x", "sid": 1, "reply": None,
            "body": b'{"stream":"ANVIL_EVENTS","seq":9,"duplicate":false}',
        }])
        ack = client.publish_js(
            "anvil.events.v2.node-a.state.desired", {"value": 1},
            msg_id="node-a:router:000001", wait_ack=True,
        )
        self.assertEqual(9, ack["seq"])
        self.assertIn(b"HPUB ", b"".join(client.wire.sent))

    def test_publish_js_rejects_invalid_puback_sequence(self):
        client = NATSClient()
        client.wire = FakeWire([{
            "subject": "_INBOX.x", "sid": 1, "reply": None,
            "body": b'{"stream":"ANVIL_EVENTS","seq":true}',
        }])
        with self.assertRaisesRegex(OSError, "sequence"):
            client.publish_js(
                "anvil.events.v2.node-a.plugin.changed", b"{}", wait_ack=True,
            )

    def test_publish_js_timeout_retains_caller_control(self):
        client = NATSClient()
        client.wire = FakeWire()
        with self.assertRaises(TimeoutError):
            client.publish_js(
                "anvil.events.v2.node-a.plugin.changed", b"{}", wait_ack=True,
            )

    def test_durable_consumer_request_is_bounded(self):
        client = NATSClient()
        client.wire = FakeWire()
        requests = []
        client._request_json = lambda *args: requests.append(args) or {}
        subject = client.bind_durable_consumer(
            "ANVIL_EVENTS", "node-b-events", "anvil.events.v2.>",
        )
        self.assertEqual("anvil.delivery.node-b-events", subject)
        self.assertIn("DURABLE.CREATE.ANVIL_EVENTS.node-b-events", requests[0][0])

    def test_raw_request_rejects_malformed_response(self):
        client = NATSClient()
        client.wire = FakeWire([{
            "subject": "_INBOX.x", "sid": 1, "reply": None, "body": b"[]",
        }])
        with self.assertRaisesRegex(OSError, "malformed"):
            client._request_json_raw("$JS.API.STREAM.INFO.A", {})

    def test_close_and_abort_are_idempotent(self):
        client = NATSClient()
        wire = FakeWire()
        client.wire = wire
        client.close()
        self.assertTrue(wire.closed)
        client.abort()


class StreamConfigurationTests(unittest.TestCase):
    @staticmethod
    def _config():
        return {
            "name": "ANVIL_EVENTS",
            "subjects": ["anvil.events.v2.>"],
            "storage": "file",
        }

    def test_stream_creation(self):
        client = NATSClient()
        responses = iter([
            {"error": {"code": 404, "description": "not found"}},
            {"config": self._config()},
            {"config": self._config()},
        ])
        client._request_json_raw = lambda *args: next(responses)
        self.assertTrue(client.configure_stream(self._config())["created"])

    def test_stream_create_response_drift_is_rejected(self):
        client = NATSClient()
        responses = iter([
            {"error": {"code": 404, "description": "not found"}},
            {"config": {**self._config(), "storage": "memory"}},
        ])
        client._request_json_raw = lambda *args: next(responses)
        with self.assertRaisesRegex(OSError, "storage"):
            client.configure_stream(self._config())

    def test_existing_identical_stream_is_verified(self):
        client = NATSClient()
        client._request_json_raw = lambda *args: {"config": self._config()}
        self.assertFalse(client.configure_stream(self._config())["created"])

    def test_existing_drift_is_rejected(self):
        client = NATSClient()
        client._request_json_raw = lambda *args: {
            "config": {**self._config(), "storage": "memory"},
        }
        with self.assertRaisesRegex(OSError, "storage"):
            client.configure_stream(self._config())

    def test_stream_info_failure_other_than_not_found_fails_closed(self):
        client = NATSClient()
        calls = []

        def response(subject, *args):
            calls.append(subject)
            return {"error": {"code": 503, "err_code": 10008}}

        client._request_json_raw = response
        with self.assertRaisesRegex(OSError, "code=503"):
            client.configure_stream(self._config())
        self.assertEqual(1, len(calls))


if __name__ == "__main__":
    unittest.main()
