"""Crash-released cross-process lock for one managed resource."""

from __future__ import annotations

import hashlib
import os
import stat
from contextlib import contextmanager

if os.name == "nt":
    import msvcrt
else:
    import fcntl


class ResourceBusy(RuntimeError):
    """Another process is applying this resource."""


def _acquire(handle):
    handle.seek(0)
    if os.name == "nt":
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _release(handle):
    handle.seek(0)
    if os.name == "nt":
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def resource_lock(root, node, resource):
    lock_root = os.path.join(os.fspath(root), "reconcile-locks")
    if os.path.lexists(lock_root) and os.path.islink(lock_root):
        raise OSError("reconciliation lock directory must not be a symlink")
    os.makedirs(lock_root, exist_ok=True)
    identity = hashlib.sha256(f"{node}\0{resource}".encode()).hexdigest()
    shard = int(identity[:8], 16) % 64
    path = os.path.join(lock_root, f"{shard:02d}.lock")
    if os.path.lexists(path) and os.path.islink(path):
        raise OSError("reconciliation lock file must not be a symlink")
    flags = os.O_RDWR | os.O_CREAT | os.O_APPEND
    flags |= getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    handle = os.fdopen(descriptor, "a+b")
    try:
        details = os.fstat(handle.fileno())
        if not stat.S_ISREG(details.st_mode):
            raise OSError("reconciliation lock must be a regular file")
        if details.st_size == 0:
            handle.write(b"\0")
            handle.flush()
        try:
            _acquire(handle)
        except OSError as exc:
            raise ResourceBusy(
                f"resource {resource!r} is already being reconciled"
            ) from exc
        try:
            yield
        finally:
            _release(handle)
    finally:
        handle.close()
