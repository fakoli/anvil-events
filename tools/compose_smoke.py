"""Run the synthetic broker, reconciliation, and outage-recovery acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "deploy" / "compose.yml"
EVENT_ROOT = "/var/lib/anvil/events"
ARTIFACT = b'router = "node-a"\ngeneration = 1\n'


def run(command, *, input_text=None, check=True, capture=True):
    return subprocess.run(
        command,
        cwd=ROOT,
        input=input_text,
        text=True,
        check=check,
        capture_output=capture,
    )


class Stack:
    def __init__(self, project):
        self.base = [
            "docker", "compose", "-p", project, "-f", str(COMPOSE_FILE),
        ]

    def compose(self, *arguments, **kwargs):
        return run([*self.base, *arguments], **kwargs)

    def events(self, *arguments, input_text=None):
        return self.compose(
            "exec", "-T", "events", *arguments, input_text=input_text,
        )

    def status(self):
        result = self.events(
            "anvil-events", "--root", EVENT_ROOT, "status", "--json",
        )
        return json.loads(result.stdout)

    def record(self, generation):
        payload = {
            "resource": "routing/clients",
            "generation": generation,
            "revision": "rev-1",
            "content_sha256": hashlib.sha256(ARTIFACT).hexdigest(),
            "adapter": "router_config",
            "artifact": "routing/clients",
            "targets": ["node-b"],
        }
        result = self.events(
            "anvil-events",
            "--root", EVENT_ROOT,
            "record", "state.desired",
            "--node", "node-a",
            "--producer", "node-a:router",
            "--operation-key", f"compose-smoke-generation-{generation}",
            input_text=json.dumps(payload),
        )
        accepted = json.loads(result.stdout)
        if not accepted.get("accepted"):
            raise AssertionError(f"desired state was not accepted: {accepted}")

    def logs(self):
        return self.compose(
            "logs", "--no-color", check=False,
        ).stdout


def eventually(description, predicate, timeout=45):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (json.JSONDecodeError, OSError, subprocess.SubprocessError) as exc:
            last_error = exc
        time.sleep(0.5)
    suffix = f"; last error: {last_error}" if last_error else ""
    raise AssertionError(f"timed out waiting for {description}{suffix}")


def ready(stack):
    result = stack.events(
        "python", "-c",
        "import urllib.request; "
        "print(urllib.request.urlopen("
        "'http://127.0.0.1:9877/ready', timeout=2).status)",
    )
    return result.stdout.strip() == "200"


def converged(stack, minimum):
    status = stack.status()
    if (
        status["pending"] == 0
        and status["archived"] >= minimum
        and status["journaled"] >= minimum
    ):
        return status
    return None


def acceptance(project):
    stack = Stack(project)
    stack.compose("down", "-v", "--remove-orphans", check=False)
    try:
        stack.compose("up", "--build", "-d")
        eventually("the event service to become ready", lambda: ready(stack))
        stream_check = stack.compose("run", "--rm", "stream-init").stdout
        if "stream: verified ANVIL_EVENTS" not in stream_check:
            raise AssertionError(
                f"existing stream was not exactly verified: {stream_check!r}"
            )

        stack.record(1)
        first = eventually(
            "generation 1 convergence", lambda: converged(stack, 2),
        )
        managed = stack.events(
            "python", "-c",
            "from pathlib import Path; "
            "print(Path('/var/lib/anvil/events/managed/router-client.toml')"
            ".read_text(), end='')",
        ).stdout.encode()
        if managed != ARTIFACT:
            raise AssertionError("managed configuration differs from artifact")

        stack.compose("stop", "nats")
        stack.record(2)
        offline = eventually(
            "an event to remain pending while the broker is down",
            lambda: stack.status() if stack.status()["pending"] >= 1 else None,
        )

        stack.compose("start", "nats")
        recovered = eventually(
            "generation 2 catch-up after broker recovery",
            lambda: converged(stack, 4),
            timeout=60,
        )
        print(json.dumps({
            "generation_1": first,
            "broker_offline": offline,
            "recovered": recovered,
        }, sort_keys=True))
    except Exception:
        print(stack.logs(), file=sys.stderr)
        raise
    finally:
        stack.compose("down", "-v", "--remove-orphans", check=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="anvil-events-smoke")
    args = parser.parse_args()
    acceptance(args.project)


if __name__ == "__main__":
    main()
