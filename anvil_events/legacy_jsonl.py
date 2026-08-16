"""Read-only legacy JSONL discovery used by verification and migration."""

from __future__ import annotations

import json
import re
from pathlib import Path

DAILY_JSONL = re.compile(r"^\d{4}-\d{2}-\d{2}\.jsonl$")
ARCHIVE_JSONL = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:\.\d{9,11})?\.jsonl$"
)


def iter_managed_jsonl(directory, managed_pattern=DAILY_JSONL,
                       malformed="skip", managed_name=None):
    """Yield legacy rows without mutating, repairing, or following symlinks."""
    root = Path(directory)
    if not root.exists() or root.is_symlink() or not root.is_dir():
        return
    paths = [root / managed_name] if managed_name else sorted(root.iterdir())
    for path in paths:
        if not managed_pattern.fullmatch(path.name):
            continue
        if path.is_symlink() or not path.is_file():
            if malformed == "raise":
                raise ValueError(f"managed legacy path is not a regular file: {path}")
            continue
        data = path.read_bytes()
        if data and not data.endswith(b"\n"):
            if malformed == "raise":
                raise ValueError(f"managed JSONL file has a torn tail: {path}")
            lines = data.splitlines()[:-1]
        else:
            lines = data.splitlines()
        for line_number, line in enumerate(lines, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                if malformed == "raise":
                    raise ValueError(
                        f"invalid JSON in {path}:{line_number}"
                    ) from exc
                continue
            if not isinstance(value, dict):
                if malformed == "raise":
                    raise ValueError(
                        f"legacy row is not an object: {path}:{line_number}"
                    )
                continue
            yield value


class LegacyRuntimeRemoved(RuntimeError):
    pass


class Outbox:
    """Fail-closed compatibility name for the removed POSIX write backend."""

    def __init__(self, root):
        raise LegacyRuntimeRemoved(
            "the legacy JSONL runtime was removed; run `anvil-events "
            "--root <target> migrate-legacy <legacy-root>`"
        )
