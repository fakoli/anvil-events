"""Explicit broker administration commands used during deployment."""

from __future__ import annotations

import json
import time
from pathlib import Path

from ..transport import NATSClient


def initialize(args):
    config = json.loads(Path(args.stream_config).read_text(encoding="utf-8"))
    deadline = time.monotonic() + args.wait
    last_error = None
    while time.monotonic() <= deadline:
        client = None
        try:
            client = NATSClient.from_env(args.url).connect(timeout=3)
            result = client.configure_stream(config, timeout=3)
            action = "created" if result["created"] else "verified"
            print(f"stream: {action} {config['name']}")
            return 0
        except Exception as exc:
            last_error = exc
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
    raise RuntimeError(f"broker initialization failed: {last_error}")
