"""A narrow managed-file adapter for portable reference deployments."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from .contracts import Artifact, Preview


class ManagedFileAdapter:
    """Atomically replace one locally configured path; events cannot choose it."""

    def __init__(self, name, destination, validator=None, mode=None):
        self.name = name
        self.destination = os.path.abspath(os.fspath(destination))
        self.validator = validator or (lambda data: None)
        if mode is not None and (
                isinstance(mode, bool) or not isinstance(mode, int)
                or not 0 <= mode <= 0o777):
            raise ValueError("managed-file mode must be an integer from 0 to 0o777")
        self.mode = mode
        self._previous = None
        self._previous_mode = None

    @staticmethod
    def _digest(data):
        return hashlib.sha256(data).hexdigest()

    def preview(self, desired, artifact):
        self.validator(artifact.data)
        self._refuse_symlink()
        try:
            before = Path(self.destination).read_bytes()
        except FileNotFoundError:
            before = b""
        return Preview(
            summary=f"replace managed file for {desired['payload']['resource']}",
            changes=(f"sha256:{self._digest(before)} -> {self._digest(artifact.data)}",),
        )

    def apply(self, desired, artifact: Artifact):
        self.validator(artifact.data)
        self._refuse_symlink()
        directory = os.path.dirname(self.destination)
        os.makedirs(directory, exist_ok=True)
        try:
            self._previous = Path(self.destination).read_bytes()
            self._previous_mode = os.stat(self.destination).st_mode & 0o777
        except FileNotFoundError:
            self._previous = None
            self._previous_mode = None
        fd, temporary = tempfile.mkstemp(prefix=".anvil-events-", dir=directory)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(artifact.data)
                handle.flush()
                os.fsync(handle.fileno())
                os.chmod(
                    temporary,
                    self.mode if self.mode is not None
                    else (self._previous_mode or 0o600),
                )
            os.replace(temporary, self.destination)
            self._sync_directory(directory)
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def verify(self, desired, artifact):
        self._refuse_symlink()
        try:
            current = Path(self.destination).read_bytes()
        except FileNotFoundError:
            return False
        return self._digest(current) == desired["payload"]["content_sha256"]

    def rollback(self, desired):
        if self._previous is None:
            try:
                os.unlink(self.destination)
            except FileNotFoundError:
                pass
            return
        directory = os.path.dirname(self.destination)
        fd, temporary = tempfile.mkstemp(prefix=".anvil-events-rollback-", dir=directory)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(self._previous)
                handle.flush()
                os.fsync(handle.fileno())
                os.chmod(temporary, self._previous_mode or 0o600)
            os.replace(temporary, self.destination)
            self._sync_directory(directory)
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def _refuse_symlink(self):
        if os.path.lexists(self.destination) and os.path.islink(self.destination):
            raise OSError("managed-file destination must not be a symlink")

    @staticmethod
    def _sync_directory(directory):
        if os.name == "nt":
            return
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
