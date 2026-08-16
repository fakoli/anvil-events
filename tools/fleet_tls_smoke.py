"""Exercise the sanitized mTLS identity map and negative fleet ACLs."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from anvil_events.domain_v2 import make_event_v2
from anvil_events.transport import NATSClient

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "nats:2.11-alpine"


def openssl_executable():
    discovered = shutil.which("openssl")
    if discovered:
        return discovered
    if os.name == "nt":
        candidate = Path(os.environ.get(
            "ProgramFiles", "C:/Program Files",
        )) / "Git" / "usr" / "bin" / "openssl.exe"
        if candidate.is_file():
            return str(candidate)
    raise RuntimeError("OpenSSL is required for the ephemeral TLS probe")


def run(command, *, check=True):
    return subprocess.run(
        command, check=check, text=True, capture_output=True,
    )


def openssl(*arguments):
    run([openssl_executable(), *map(str, arguments)])


def make_ca(directory):
    openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", directory / "ca.key", "-out", directory / "ca.pem",
        "-subj", "/CN=anvil-events-test-ca", "-days", "1", "-sha256",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        "-addext", "subjectKeyIdentifier=hash",
    )


def make_certificate(directory, name, serial, *, server=False):
    key = directory / f"{name}.key"
    request = directory / f"{name}.csr"
    certificate = directory / f"{name}.pem"
    extensions = directory / f"{name}.ext"
    usage = "serverAuth" if server else "clientAuth"
    san = "DNS:localhost,IP:127.0.0.1" if server else f"DNS:{name}"
    extensions.write_text(
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature,keyEncipherment\n"
        f"extendedKeyUsage={usage}\n"
        f"subjectAltName={san}\n",
        encoding="ascii",
    )
    openssl(
        "req", "-new", "-newkey", "rsa:2048", "-nodes",
        "-keyout", key, "-out", request, "-subj", f"/CN={name}",
    )
    openssl(
        "x509", "-req", "-in", request,
        "-CA", directory / "ca.pem", "-CAkey", directory / "ca.key",
        "-set_serial", str(serial), "-out", certificate,
        "-days", "1", "-sha256", "-extfile", extensions,
    )
    return certificate, key


def client(url, directory, identity):
    return NATSClient(
        url,
        mode="fleet",
        ca_file=str(directory / "ca.pem"),
        cert_file=str(directory / f"{identity}.pem"),
        key_file=str(directory / f"{identity}.key"),
        server_name="localhost",
        handshake_first=True,
    )


def connect_eventually(url, directory, identity, timeout=20):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        candidate = client(url, directory, identity)
        try:
            return candidate.connect(timeout=2)
        except Exception as exc:
            last_error = exc
            candidate.close()
            time.sleep(0.25)
    raise AssertionError(f"secured broker did not become ready: {last_error}")


def expect_denied(label, action):
    try:
        action()
    except (OSError, TimeoutError):
        return
    raise AssertionError(f"fleet ACL unexpectedly allowed {label}")


def desired_event():
    artifact = b"router = 'node-a'\n"
    return make_event_v2(
        "node-a:router", "state.desired", "node-a",
        {
            "resource": "routing/clients",
            "generation": 1,
            "revision": "rev-1",
            "content_sha256": hashlib.sha256(artifact).hexdigest(),
            "adapter": "router_config",
            "artifact": "routing/clients",
            "targets": ["node-b"],
        },
        producer_seq=1,
    )


def acceptance():
    container = "anvil-events-tls-" + secrets.token_hex(6)
    started = False
    with tempfile.TemporaryDirectory(prefix="anvil-events-tls-") as temporary:
        directory = Path(temporary)
        make_ca(directory)
        make_certificate(directory, "localhost", 2, server=True)
        for serial, identity in enumerate(
                ("node-a", "node-b", "stream-admin", "intruder"), 3):
            make_certificate(directory, identity, serial)
        config = ROOT / "deploy" / "nats-fleet.example.conf"
        command = [
            "docker", "run", "--name", container, "-d",
            "-p", "127.0.0.1::4222",
            "-v", f"{directory}:/tls:ro",
            "-v", f"{config}:/etc/nats/nats.conf:ro",
            "-e", "ANVIL_EVENTS_NATS_SERVER_CERT=/tls/localhost.pem",
            "-e", "ANVIL_EVENTS_NATS_SERVER_KEY=/tls/localhost.key",
            "-e", "ANVIL_EVENTS_NATS_CA=/tls/ca.pem",
            IMAGE, "-c", "/etc/nats/nats.conf",
        ]
        try:
            run(command)
            started = True
            port_output = run([
                "docker", "port", container, "4222/tcp",
            ]).stdout.strip()
            port = int(port_output.rsplit(":", 1)[1])
            url = f"tls://localhost:{port}"

            admin = connect_eventually(url, directory, "stream-admin")
            stream_config = json.loads(
                (ROOT / "deploy" / "nats-stream.json").read_text(),
            )
            created = admin.configure_stream(stream_config)
            if not created["created"]:
                raise AssertionError("ephemeral stream was not created")
            admin.close()

            event = desired_event()
            publisher = connect_eventually(url, directory, "node-a")
            ack = publisher.publish_js(
                event["subject"], event, msg_id=event["event_id"],
                wait_ack=True,
            )
            if ack.get("stream") != "ANVIL_EVENTS":
                raise AssertionError(f"unexpected PubAck: {ack}")
            publisher.close()

            subscriber = connect_eventually(url, directory, "node-b")
            delivery = subscriber.bind_durable_consumer(
                "ANVIL_EVENTS", "node-b-events", "anvil.events.v2.>",
            )
            messages = subscriber.receive(1, 10, subscription=delivery)
            if not messages or json.loads(messages[0]["body"]) != event:
                raise AssertionError("node-b did not receive node-a desired state")
            subscriber.ack(messages[0]["reply"])
            subscriber.close()

            forged = {**event, "subject": "anvil.events.v2.node-b.state.desired"}
            denied = connect_eventually(url, directory, "node-a")
            expect_denied(
                "node-a publishing node-b's prefix",
                lambda: denied.publish_js(
                    forged["subject"], forged,
                    msg_id="forged-prefix", wait_ack=True, timeout=2,
                ),
            )
            denied.abort()

            wrong_consumer = connect_eventually(url, directory, "node-a")
            expect_denied(
                "node-a creating node-b's durable consumer",
                lambda: wrong_consumer.bind_durable_consumer(
                    "ANVIL_EVENTS", "node-b-events", "anvil.events.v2.>",
                    timeout=2,
                ),
            )
            wrong_consumer.abort()

            expect_denied(
                "an unmapped certificate identity",
                lambda: client(url, directory, "intruder").connect(timeout=2),
            )
            print(json.dumps({
                "mapped_publish": True,
                "cross_node_delivery": True,
                "foreign_prefix_denied": True,
                "foreign_consumer_denied": True,
                "unmapped_identity_denied": True,
            }, sort_keys=True))
        except Exception:
            if started:
                logs = run([
                    "docker", "logs", container,
                ], check=False)
                print(logs.stdout + logs.stderr)
            raise
        finally:
            if started:
                run(["docker", "rm", "-f", container], check=False)


if __name__ == "__main__":
    acceptance()
