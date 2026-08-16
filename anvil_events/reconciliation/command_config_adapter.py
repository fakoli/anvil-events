"""Allowlisted argv-only adapter for applications with config get/set/unset."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess

from .contracts import Preview

_SENSITIVE_KEY = re.compile(
    r"(?:^|[._-])(?:token|password|passwd|secret|credential|authorization|"
    r"cookie|api[_-]?key|private[_-]?key|access[_-]?key|client[_-]?secret|"
    r"client[_-]?key|bearer)(?:$|[._-])",
    re.IGNORECASE,
)
_CONFIG_KEY = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z][A-Za-z0-9_-]*)*$")


def _argv(value, label):
    if not isinstance(value, list | tuple) or not value or not all(
            isinstance(item, str) and item and "\x00" not in item for item in value):
        raise ValueError(f"{label} must be a non-empty string array")
    return tuple(value)


def _plan(data, allowed):
    try:
        value = json.loads(
            data,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON number {value}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("command-config artifact must be a UTF-8 JSON object") from exc
    if not isinstance(value, dict) or not value:
        raise ValueError("command-config artifact must be a non-empty JSON object")
    unknown = sorted(set(value) - set(allowed))
    if unknown:
        raise ValueError("command-config artifact contains unapproved keys")
    for item in value.values():
        if item is not None and (
                isinstance(item, dict | list)
                or not isinstance(item, str | int | float | bool)):
            raise ValueError("command-config values must be JSON scalars or null")
        if item is not None:
            rendered = _text(item)
            if (len(rendered) > 4096 or rendered.startswith("-")
                    or any(character in rendered for character in ("\x00", "\r", "\n"))):
                raise ValueError("command-config value is not a safe bounded argument")
    return value


def _text(value):
    if value is True:
        return "true"
    if value is False:
        return "false"
    return str(value)


class CommandConfigAdapter:
    """Run one fixed executable; desired events can choose only allowlisted values."""

    def __init__(self, name, command, *, allowed_keys, get_args=("get",),
                 set_args=("set",), unset_args=("unset",), timeout=30,
                 missing_returncode=1, missing_stderr_prefix=None,
                 runner=subprocess.run):
        self.name = name
        self.command = _argv(command, "command")
        self.allowed_keys = _argv(allowed_keys, "allowed_keys")
        self.get_args = _argv(get_args, "get_args")
        self.set_args = _argv(set_args, "set_args")
        self.unset_args = _argv(unset_args, "unset_args")
        self.timeout = timeout
        self.missing_returncode = missing_returncode
        self.missing_stderr_prefix = missing_stderr_prefix
        self.runner = runner
        self._previous = None
        if len(set(self.allowed_keys)) != len(self.allowed_keys):
            raise ValueError("allowed_keys must not contain duplicates")
        if not all(_CONFIG_KEY.fullmatch(item) for item in self.allowed_keys):
            raise ValueError("allowed_keys must contain safe dotted config keys")
        normalized_keys = (
            re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", item).replace(".", "_")
            for item in self.allowed_keys
        )
        if any(_SENSITIVE_KEY.search(item) for item in normalized_keys):
            raise ValueError("allowed_keys must not contain credential-shaped keys")
        if (isinstance(timeout, bool) or not isinstance(timeout, int)
                or not 1 <= timeout <= 120):
            raise ValueError("command timeout must be an integer from 1 to 120")
        if (isinstance(missing_returncode, bool)
                or not isinstance(missing_returncode, int)
                or not 1 <= missing_returncode <= 255):
            raise ValueError("missing return code must be an integer from 1 to 255")
        if missing_stderr_prefix is not None and (
                not isinstance(missing_stderr_prefix, str)
                or not missing_stderr_prefix
                or len(missing_stderr_prefix) > 512
                or any(character in missing_stderr_prefix
                       for character in ("\x00", "\r", "\n"))):
            raise ValueError("missing stderr prefix must be one bounded line")

    @staticmethod
    def _digest(value):
        return hashlib.sha256(value.encode()).hexdigest()[:16]

    def _run(self, tail):
        try:
            result = self.runner(
                [*self.command, *tail], capture_output=True, text=True,
                timeout=self.timeout, shell=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("configured application command could not complete") from exc
        if len(result.stdout or "") > 65536 or len(result.stderr or "") > 65536:
            raise RuntimeError("configured application command output exceeded limit")
        return result

    def _get(self, key):
        result = self._run([*self.get_args, key])
        if result.returncode == 0:
            return True, result.stdout.rstrip("\r\n")
        missing = result.returncode == self.missing_returncode
        if self.missing_stderr_prefix is not None:
            missing = missing and (result.stderr or "").startswith(
                self.missing_stderr_prefix,
            )
        if missing:
            return False, None
        raise RuntimeError("configured application rejected a read")

    def _write(self, key, value):
        tail = [*self.unset_args, key] if value is None else [*self.set_args, key, _text(value)]
        if self._run(tail).returncode != 0:
            raise RuntimeError("configured application rejected an update")

    def preview(self, desired, artifact):
        plan = _plan(artifact.data, self.allowed_keys)
        previous = {key: self._get(key) for key in plan}
        self._previous = previous
        changes = tuple(
            f"{key}:{self._digest(old) if present else 'absent'} -> "
            f"{self._digest(_text(value)) if value is not None else 'absent'}"
            for key, value in sorted(plan.items())
            for present, old in (previous[key],)
            if (present, old) != (value is not None, None if value is None else _text(value))
        )
        return Preview(
            summary=f"update allowlisted application config for {desired['payload']['resource']}",
            changes=changes,
        )

    def apply(self, desired, artifact):
        plan = _plan(artifact.data, self.allowed_keys)
        if self._previous is None:
            self._previous = {key: self._get(key) for key in plan}
        applied = []
        try:
            for key, value in plan.items():
                self._write(key, value)
                applied.append(key)
        except Exception:
            for key in reversed(applied):
                present, old = self._previous[key]
                self._write(key, old if present else None)
            raise

    def verify(self, desired, artifact):
        plan = _plan(artifact.data, self.allowed_keys)
        for key, value in plan.items():
            present, current = self._get(key)
            if value is None:
                if present:
                    return False
            elif not present or current != _text(value):
                return False
        return True

    def rollback(self, desired):
        if self._previous is None:
            return
        for key, (present, old) in reversed(tuple(self._previous.items())):
            self._write(key, old if present else None)
