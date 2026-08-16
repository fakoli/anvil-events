"""Exact JetStream configuration comparison helpers."""

from __future__ import annotations

from .protocol import VALID_TOKEN, validate_subject


def validate_stream_config(config):
    if not isinstance(config, dict):
        raise ValueError("stream config must be an object")
    name = config.get("name")
    subjects = config.get("subjects")
    if not isinstance(name, str) or not VALID_TOKEN.fullmatch(name):
        raise ValueError("stream config requires a safe name")
    if not isinstance(subjects, list) or not subjects:
        raise ValueError("stream config requires subjects")
    for subject in subjects:
        validate_subject(subject, allow_wildcards=True)
    return name


def verify_stream_config(requested, response, source):
    actual = response.get("config")
    if not isinstance(actual, dict):
        raise OSError(f"JetStream {source} omitted its config")
    mismatches = [
        key for key, value in requested.items() if actual.get(key) != value
    ]
    if mismatches:
        raise OSError(
            f"JetStream {source} differs in: " + ", ".join(mismatches)
        )
    return actual
