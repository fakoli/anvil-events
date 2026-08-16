"""Storage backend selection and compatibility policy."""

from __future__ import annotations

import os

from .sqlite_store import DATABASE_NAME, SQLiteStore, legacy_state_exists

BACKENDS = frozenset(("auto", "sqlite", "legacy"))


def open_event_store(root, backend="auto"):
    """Open the requested store without silently converting existing state.

    ``auto`` selects an existing SQLite database first, then an existing legacy
    JSONL store, and uses SQLite for an empty/new root.  This makes new installs
    cross-platform while leaving deployed journals untouched until an explicit
    migration command is run.
    """
    if backend not in BACKENDS:
        raise ValueError(f"unknown storage backend {backend!r}")
    root = os.path.abspath(os.fspath(root))
    if backend == "sqlite":
        return SQLiteStore(root)
    if backend == "legacy":
        raise RuntimeError(
            "the mutable legacy JSONL runtime was removed; run "
            "`anvil-events migrate-legacy`"
        )
    if os.path.exists(os.path.join(root, DATABASE_NAME)):
        return SQLiteStore(root)
    if legacy_state_exists(root):
        raise RuntimeError(
            "legacy JSONL state requires explicit migration; run "
            "`anvil-events --root <target> migrate-legacy <legacy-root>`"
        )
    return SQLiteStore(root)


def backend_name(store):
    return getattr(store, "backend", "legacy-jsonl")
