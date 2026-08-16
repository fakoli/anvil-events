from __future__ import annotations

import io
import json
import socket
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from helpers import desired_event, desired_payload

from anvil_events.cli import build_parser, main
from anvil_events.runtime.delivery import DeliveryPump
from anvil_events.runtime.health import HealthServer
from anvil_events.runtime.service import EventsService
from anvil_events.runtime.stats import RuntimeStats
from anvil_events.runtime.subscriber import Subscriber
from anvil_events.storage import SQLiteStore


class FakePublisher:
    def __init__(self, error=None, stream="ANVIL_EVENTS"):
        self.error = error
        self.stream = stream
        self.published = []

    def publish_js(self, subject, event, **options):
        self.published.append((subject, event, options))
        if self.error:
            raise self.error
        return {"stream": self.stream, "seq": 11, "duplicate": False}

    def close(self):
        pass


class RuntimeBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(self.temporary.name)
        self.stop = threading.Event()
        self.stats = RuntimeStats()

    def tearDown(self):
        self.temporary.cleanup()

    def _subscriber(self, processor=None):
        return Subscriber(
            self.store,
            "nats://127.0.0.1:4222",
            "ANVIL_EVENTS",
            "node-b-events",
            "anvil.events.v2.>",
            {"node-a:router"},
            self.stop,
            self.stats,
            processor=processor,
        )

    def test_subscriber_journals_valid_matching_subject(self):
        event = desired_event()
        processed, journaled = self._subscriber().handle(
            json.dumps(event).encode(), event["subject"],
        )
        self.assertTrue(processed)
        self.assertTrue(journaled)

    def test_subscriber_drops_broker_subject_mismatch(self):
        event = desired_event()
        processed, journaled = self._subscriber().handle(
            json.dumps(event).encode(), "anvil.events.v2.node-b.state.desired",
        )
        self.assertTrue(processed)
        self.assertFalse(journaled)
        self.assertEqual(1, self.stats.snapshot()["dropped"])

    def test_subscriber_drops_unauthorized_producer(self):
        event = desired_event()
        subscriber = self._subscriber()
        subscriber.allowed_producers = {"node-c:router"}
        self.assertEqual((True, False), subscriber.handle(
            json.dumps(event).encode(), event["subject"],
        ))

    def test_awaiting_approval_is_not_ackable(self):
        class Processor:
            def process(self, event):
                return SimpleNamespace(state="awaiting-approval")

        event = desired_event()
        processed, journaled = self._subscriber(Processor()).handle(
            json.dumps(event).encode(), event["subject"],
        )
        self.assertFalse(processed)
        self.assertTrue(journaled)

    def test_processor_failure_is_not_ackable(self):
        class Processor:
            def process(self, event):
                raise OSError("disk failed")

        event = desired_event()
        self.assertEqual((False, True), self._subscriber(Processor()).handle(
            json.dumps(event).encode(), event["subject"],
        ))

    def test_delivery_records_puback(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        pump = DeliveryPump(
            self.store, "nats://127.0.0.1:4222", self.stop, self.stats,
        )
        attempted, failed = pump.deliver(FakePublisher(), [event])
        self.assertEqual((1, 0), (attempted, failed))
        self.assertEqual(0, self.store.count_pending())

    def test_delivery_failure_preserves_pending_with_backoff(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        pump = DeliveryPump(
            self.store, "nats://127.0.0.1:4222", self.stop, self.stats,
        )
        attempted, failed = pump.deliver(
            FakePublisher(OSError("broker down")), [event],
        )
        self.assertEqual((1, 1), (attempted, failed))
        self.assertEqual(1, self.store.count_pending())
        self.assertIn(event["event_id"], pump.retry)

    def test_wrong_stream_puback_preserves_pending(self):
        event = self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        pump = DeliveryPump(
            self.store, "nats://127.0.0.1:4222", self.stop, self.stats,
        )
        attempted, failed = pump.deliver(
            FakePublisher(stream="WRONG_STREAM"), [event],
        )
        self.assertEqual((1, 1), (attempted, failed))
        self.assertEqual(1, self.store.count_pending())

    def test_delivery_run_connects_and_drains(self):
        self.store.emit_v2(
            "node-a:router", "state.desired", "node-a", desired_payload(),
        )
        stop = threading.Event()

        class Client(FakePublisher):
            def connect(inner_self, timeout):
                return inner_self

            def publish_js(inner_self, *args, **kwargs):
                result = super().publish_js(*args, **kwargs)
                stop.set()
                return result

        pump = DeliveryPump(
            self.store, "nats://127.0.0.1:4222", stop, self.stats,
            client_factory=lambda url: Client(),
        )
        pump.run()
        self.assertEqual(0, self.store.count_pending())

    def test_subscriber_run_acks_after_handling(self):
        event = desired_event()
        stop = threading.Event()

        class Client:
            def __init__(inner_self):
                inner_self.acks = []

            def connect(inner_self, timeout):
                return inner_self

            def bind_durable_consumer(inner_self, *args, **kwargs):
                return "anvil.delivery.node-b-events"

            def receive(inner_self, **kwargs):
                stop.set()
                return [{
                    "body": json.dumps(event).encode(),
                    "subject": event["subject"],
                    "reply": "$JS.ACK.stream.consumer",
                }]

            def ack(inner_self, reply):
                inner_self.acks.append(reply)

            def close(inner_self):
                pass

        client = Client()
        subscriber = Subscriber(
            self.store, "nats://127.0.0.1:4222", "ANVIL_EVENTS",
            "node-b-events", "anvil.events.v2.>", {"node-a:router"},
            stop, self.stats, client_factory=lambda url: client,
        )
        subscriber.run()
        self.assertEqual(["$JS.ACK.stream.consumer"], client.acks)
        self.assertEqual(1, len(list(self.store.read_journal())))


class ServiceCompositionTests(unittest.TestCase):
    def test_artifact_publisher_configuration_is_all_or_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            for options in (
                {"artifact_root": root},
                {"artifact_auth_env": "ANVIL_ARTIFACT_AUTH"},
            ):
                with self.subTest(options=options), self.assertRaisesRegex(
                        ValueError, "requires root and auth env"):
                    EventsService(
                        root, "nats://127.0.0.1:4222", "anvil.events.v2.>",
                        "ANVIL_EVENTS", ("127.0.0.1", 0), **options,
                    )

    def test_service_starts_health_and_joins_workers(self):
        with tempfile.TemporaryDirectory() as root:
            service = EventsService(
                root, "nats://127.0.0.1:4222", "anvil.events.v2.>",
                "ANVIL_EVENTS", ("127.0.0.1", 0),
            )

            def idle_worker():
                service.stop_event.wait()

            service.subscriber.run = idle_worker
            service.delivery.run = idle_worker
            service.start()
            snapshot = service.health_snapshot()
            self.assertTrue(snapshot["local_accepting"])
            self.assertFalse(snapshot["fleet_delivery_ready"])
            service.store.emit_v2(
                "node-a:router", "state.desired", "node-a", desired_payload(),
            )
            service.stats.update(
                broker_connected=True, last_delivery_error="permissions denied",
            )
            snapshot = service.health_snapshot()
            self.assertFalse(snapshot["delivery_healthy"])
            self.assertFalse(snapshot["fleet_delivery_ready"])
            service.stop()
            self.assertEqual([], [
                thread.name for thread in service.threads if thread.is_alive()
            ])


class HealthTests(unittest.TestCase):
    @staticmethod
    def _snapshot(ready=True):
        return {
            "local_accepting": True,
            "fleet_delivery_ready": ready,
            "pending": 0,
        }

    def test_health_binds_synchronously_and_reports_readiness(self):
        stop = threading.Event()
        health = HealthServer(("127.0.0.1", 0), lambda: self._snapshot(False), stop)
        health.start()
        client = socket.create_connection(health.address, timeout=2)
        client.sendall(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
        response = client.recv(4096)
        client.close()
        stop.set()
        health.close()
        self.assertIn(b"503 Service Unavailable", response)

    def test_bind_failure_is_reported_by_start(self):
        occupied = socket.socket()
        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)
        health = HealthServer(
            occupied.getsockname(), lambda: self._snapshot(), threading.Event(),
        )
        try:
            with self.assertRaises(OSError):
                health.start()
        finally:
            occupied.close()

    def test_health_rejects_non_get_and_malformed_requests(self):
        stop = threading.Event()
        health = HealthServer(
            ("127.0.0.1", 0), lambda: self._snapshot(), stop,
        )
        health.start()
        try:
            for request, expected in (
                (b"POST /live HTTP/1.1\r\nHost: localhost\r\n\r\n", b"405"),
                (b"GET /live\r\nHost: localhost\r\n\r\n", b"400"),
                (b"GET /live HTTP/1.1\r\nHost: one\r\nHost: two\r\n\r\n", b"400"),
            ):
                with self.subTest(request=request):
                    client = socket.create_connection(health.address, timeout=2)
                    client.sendall(request)
                    response = client.recv(4096)
                    client.close()
                    self.assertIn(expected, response)
        finally:
            health.close()

    def test_idle_health_client_cannot_block_shutdown(self):
        stop = threading.Event()
        health = HealthServer(
            ("127.0.0.1", 0), lambda: self._snapshot(), stop,
        )
        thread = health.start()
        client = socket.create_connection(health.address, timeout=2)
        try:
            health.close(timeout=2)
            self.assertFalse(thread.is_alive())
        finally:
            client.close()


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = self.temporary.name

    def tearDown(self):
        self.temporary.cleanup()

    def test_command_surface_has_no_raw_publish_or_git_sync(self):
        parser = build_parser()
        commands = next(
            action for action in parser._actions
            if action.__class__.__name__ == "_SubParsersAction"
        ).choices
        self.assertFalse({"pub", "sub", "emit", "sync-repo"} & set(commands))

    def test_record_is_local_and_idempotent(self):
        payload = json.dumps(desired_payload())
        arguments = [
            "--root", self.root,
            "record", "state.desired",
            "--node", "node-a",
            "--producer", "node-a:router",
            "--operation-key", "router-update-1",
        ]
        outputs = []
        for _ in range(2):
            stdout = io.StringIO()
            with patch("sys.stdin", io.StringIO(payload)), redirect_stdout(stdout):
                self.assertEqual(0, main(arguments))
            outputs.append(json.loads(stdout.getvalue()))
        self.assertFalse(outputs[0]["already_recorded"])
        self.assertTrue(outputs[1]["already_recorded"])
        self.assertEqual(outputs[0]["event_id"], outputs[1]["event_id"])

    def test_status_json_is_machine_readable(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(0, main([
                "--root", self.root, "status", "--json",
            ]))
        status = json.loads(stdout.getvalue())
        self.assertEqual("sqlite", status["backend"])
        self.assertFalse(status["degraded"])

    def test_init_replay_verify_and_gc_handlers(self):
        payload = json.dumps(desired_payload())
        with patch("sys.stdin", io.StringIO(payload)), redirect_stdout(io.StringIO()):
            main([
                "--root", self.root, "record", "state.desired",
                "--node", "node-a", "--producer", "node-a:router",
                "--operation-key", "key-1",
            ])
        for arguments in (
            ["--root", self.root, "init"],
            ["--root", self.root, "replay"],
            ["--root", self.root, "verify", self.root],
            ["--root", self.root, "gc", "--archive-days", "90"],
        ):
            with redirect_stdout(io.StringIO()) as output:
                self.assertEqual(0, main(arguments))
                self.assertTrue(output.getvalue())

    def test_serve_arguments_forward_to_service_entrypoint(self):
        with patch("anvil_events.cli.serve", return_value=0) as serve:
            self.assertEqual(0, main([
                "--root", self.root, "serve", "--config", "node.toml",
                "--durable", "node-events", "--health-port", "9000",
                "--artifact-root", "artifacts",
                "--artifact-auth-env", "ANVIL_ARTIFACT_AUTH",
            ]))
        forwarded = serve.call_args.args[0]
        self.assertIn("node.toml", forwarded)
        self.assertIn("node-events", forwarded)
        self.assertIn("artifacts", forwarded)
        self.assertIn("ANVIL_ARTIFACT_AUTH", forwarded)

    def test_migration_command_reports_acked_role(self):
        legacy = Path(self.root) / "legacy"
        (legacy / "archive").mkdir(parents=True)
        event = desired_event()
        # Legacy migration accepts both envelope versions from managed JSONL.
        (legacy / "archive" / "2026-08-16.jsonl").write_text(
            json.dumps(event) + "\n", encoding="utf-8",
        )
        target = Path(self.root) / "target"
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(0, main([
                "--root", str(target), "migrate-legacy", str(legacy),
                "--offline-source",
            ]))
        self.assertIn("archive=1", stdout.getvalue())

    def test_broker_init_uses_explicit_stream_config(self):
        config = Path(self.root) / "stream.json"
        config.write_text(json.dumps({
            "name": "ANVIL_EVENTS", "subjects": ["anvil.events.v2.>"],
        }), encoding="utf-8")

        class Client:
            def connect(inner_self, timeout):
                return inner_self

            def configure_stream(inner_self, value, timeout):
                self.assertEqual("ANVIL_EVENTS", value["name"])
                return {"created": False}

            def close(inner_self):
                pass

        stdout = io.StringIO()
        with (
            patch("anvil_events.commands.broker.NATSClient.from_env",
                  return_value=Client()),
            redirect_stdout(stdout),
        ):
            self.assertEqual(0, main([
                "broker-init", str(config), "--wait", "1",
            ]))
        self.assertIn("verified", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
