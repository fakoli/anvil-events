"""Compatibility import for the composable storage package."""

from .storage import DATABASE_NAME, SQLiteStore, legacy_state_exists
from .storage.database import SCHEMA_VERSION as STORE_SCHEMA_VERSION

__all__ = [
    "DATABASE_NAME", "STORE_SCHEMA_VERSION", "SQLiteStore",
    "legacy_state_exists",
]
