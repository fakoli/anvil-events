"""Compatibility imports for the decomposed transport package."""

from .transport.client import NATSClient
from .transport.protocol import encode_js_publish, validate_subject
from .transport.security import parse_endpoint, parse_url

__all__ = [
    "NATSClient",
    "encode_js_publish",
    "parse_endpoint",
    "parse_url",
    "validate_subject",
]
