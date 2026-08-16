"""V1 compatibility imports.

The mutable POSIX JSONL outbox and unused LogPlayer queue prototype were
removed in v2. Legacy files remain supported as an explicit read-only migration
source.
"""

from .dependency_graph import CausalChecker, DependencyGraphChecker
from .domain import (
    KINDS,
    PAYLOAD_ALLOWED,
    PAYLOAD_REQUIRED,
    SCHEMA,
    make_event,
    utcnow_iso,
    validate_payload,
)
from .legacy_jsonl import Outbox

__all__ = [
    "CausalChecker", "DependencyGraphChecker", "KINDS", "Outbox",
    "PAYLOAD_ALLOWED", "PAYLOAD_REQUIRED", "SCHEMA", "make_event",
    "utcnow_iso", "validate_payload",
]
