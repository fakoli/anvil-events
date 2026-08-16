"""Secret-preserving JSON merge-patch adapter."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path

from .contracts import Artifact, Preview
from .file_adapter import ManagedFileAdapter

_SENSITIVE_KEY = re.compile(
    r"(?:^|_)(?:token|password|passwd|secret|credential|authorization|cookie|"
    r"api_key|private_key|access_key|client_secret|client_key|bearer)(?:$|_)",
    re.IGNORECASE,
)


def _object(data, label):
    try:
        value = json.loads(
            data,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON number {value}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must be a UTF-8 JSON object") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _merge(document, patch):
    """Apply RFC 7396 object semantics without mutating either input."""
    result = copy.deepcopy(document) if isinstance(document, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict):
            result[key] = _merge(result.get(key), value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _touches(path, patch):
    current = patch
    for index, segment in enumerate(path):
        if not isinstance(current, dict) or segment not in current:
            return False
        current = current[segment]
        if index < len(path) - 1 and not isinstance(current, dict):
            return True
    return True


def _encoded(value):
    return (json.dumps(
        value, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False,
    ) + "\n").encode()


def _contains_sensitive_key(value):
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, list):
            pending.extend(current)
            continue
        if not isinstance(current, dict):
            continue
        for key, child in current.items():
            normalized = re.sub(
                r"[^A-Za-z0-9]+", "_",
                re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(key)),
            ).strip("_").lower()
            if _SENSITIVE_KEY.search(normalized):
                return True
            if isinstance(child, dict | list):
                pending.append(child)
    return False


class JSONMergeAdapter:
    """Merge an authority patch into one JSON file while preserving other keys."""

    def __init__(self, name, destination, *, protected_paths=(), mode=None):
        self.name = name
        self.destination = str(destination)
        if not isinstance(protected_paths, list | tuple) or not all(
                isinstance(path, str) and path
                and all(path.split(".")) for path in protected_paths):
            raise ValueError("protected_paths must contain dotted JSON paths")
        self.protected_paths = tuple(tuple(path.split(".")) for path in protected_paths)
        self._file = ManagedFileAdapter(name, destination, mode=mode)
        self._preview_digest = None

    @staticmethod
    def _digest(data):
        return hashlib.sha256(data).hexdigest()

    def _patch(self, artifact):
        patch = _object(artifact.data, "JSON merge artifact")
        if _contains_sensitive_key(patch):
            raise ValueError("JSON merge artifact contains a credential-shaped key")
        touched = [".".join(path) for path in self.protected_paths if _touches(path, patch)]
        if touched:
            raise ValueError(
                "JSON merge artifact touches protected paths: " + ", ".join(touched)
            )
        return patch

    def _current(self):
        self._file._refuse_symlink()
        try:
            data = Path(self.destination).read_bytes()
        except FileNotFoundError:
            return b"{}\n", {}
        return data, _object(data, "managed JSON destination")

    def preview(self, desired, artifact):
        patch = self._patch(artifact)
        before, document = self._current()
        after = _encoded(_merge(document, patch))
        self._preview_digest = self._digest(before)
        return Preview(
            summary=f"merge managed JSON for {desired['payload']['resource']}",
            changes=(f"sha256:{self._digest(before)} -> {self._digest(after)}",),
        )

    def apply(self, desired, artifact: Artifact):
        patch = self._patch(artifact)
        before, document = self._current()
        if self._preview_digest is not None and self._digest(before) != self._preview_digest:
            raise RuntimeError("managed JSON changed after preview")
        merged = Artifact(_encoded(_merge(document, patch)), artifact.revision)
        self._file.apply(desired, merged)

    def verify(self, desired, artifact):
        patch = self._patch(artifact)
        _, document = self._current()
        return _merge(document, patch) == document

    def rollback(self, desired):
        self._file.rollback(desired)
