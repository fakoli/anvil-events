"""Authenticated artifact publisher tests."""

import json
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from anvil_events.reconciliation.artifacts import HTTPSArtifactResolver
from anvil_events.runtime.artifact_http import ArtifactHTTPPublisher
from anvil_events.runtime.health import HealthServer


class ArtifactHTTPTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "artifacts"
        target = self.root / "routing" / "clients"
        target.mkdir(parents=True)
        (target / "rev-1").write_bytes(b'{"generation":1}\n')
        self.stop = threading.Event()
        publisher = ArtifactHTTPPublisher(self.root, "ANVIL_ARTIFACT_TEST_AUTH")
        self.health = HealthServer(
            ("127.0.0.1", 0),
            lambda: {"local_accepting": True, "fleet_delivery_ready": True},
            self.stop,
            route=publisher.route,
        )
        self.environment = patch.dict(
            os.environ, {"ANVIL_ARTIFACT_TEST_AUTH": "test-only-value"},
        )
        self.environment.start()
        self.health.start()
        host, port = self.health.address
        self.base = f"http://{host}:{port}"

    def tearDown(self):
        self.health.close()
        self.environment.stop()
        self.temporary.cleanup()

    def test_https_resolver_contract_reads_exact_authenticated_revision(self):
        resolver = HTTPSArtifactResolver(
            self.base + "/artifacts", mode="development",
            token_env="ANVIL_ARTIFACT_TEST_AUTH",
        )
        artifact = resolver.resolve("routing/clients", "rev-1")
        self.assertEqual("rev-1", artifact.revision)
        self.assertEqual(b'{"generation":1}\n', artifact.data)

    def test_missing_or_wrong_bearer_is_refused(self):
        for header in (None, "Bearer wrong"):
            request = Request(self.base + "/artifacts/routing%2Fclients/rev-1")
            if header:
                request.add_header("Authorization", header)
            with self.subTest(header=header), self.assertRaises(HTTPError) as raised:
                urlopen(request, timeout=2)
            self.assertEqual(401, raised.exception.code)

    def test_traversal_and_invalid_revision_are_not_resolved(self):
        for path in (
            "/artifacts/..%2Foutside/rev-1",
            "/artifacts/routing%2Fclients/..",
            "/artifacts/routing%2Fclients/%C3%A9",
        ):
            request = Request(
                self.base + path,
                headers={"Authorization": "Bearer test-only-value"},
            )
            with self.subTest(path=path), self.assertRaises(HTTPError) as raised:
                urlopen(request, timeout=2)
            self.assertEqual(404, raised.exception.code)

    def test_health_route_remains_available_without_artifact_auth(self):
        with urlopen(self.base + "/live", timeout=2) as response:
            self.assertEqual(200, response.status)
            self.assertTrue(json.load(response)["local_accepting"])

    def test_oversized_request_does_not_kill_server(self):
        client = socket.create_connection(self.health.address, timeout=2)
        client.sendall(b"GET /live HTTP/1.1\r\nX-Large: " + b"x" * 9000 + b"\r\n\r\n")
        client.recv(4096)
        client.close()
        with urlopen(self.base + "/live", timeout=2) as response:
            self.assertEqual(200, response.status)

    def test_fragmented_headers_are_read_to_the_bounded_terminator(self):
        client = socket.create_connection(self.health.address, timeout=2)
        client.sendall(b"GET /live HTTP/1.1\r\nHost:")
        client.sendall(b" localhost\r\n\r\n")
        response = client.recv(4096)
        client.close()
        self.assertIn(b"200 OK", response)


if __name__ == "__main__":
    unittest.main()
