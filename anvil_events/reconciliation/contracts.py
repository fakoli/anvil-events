"""Interfaces and registries for narrow host-owned reconcilers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Artifact:
    data: bytes
    revision: str


@dataclass(frozen=True)
class Preview:
    summary: str
    changes: tuple[str, ...]


class ArtifactResolver(Protocol):
    def resolve(self, reference: str, revision: str) -> Artifact: ...


class ReconcileAdapter(Protocol):
    name: str

    def preview(self, desired: dict, artifact: Artifact) -> Preview: ...

    def apply(self, desired: dict, artifact: Artifact) -> None: ...

    def verify(self, desired: dict, artifact: Artifact) -> bool: ...

    def rollback(self, desired: dict) -> None: ...


class ApplyPolicy(Protocol):
    def allows(self, desired: dict, preview: Preview) -> bool: ...


class DenyByDefaultPolicy:
    def allows(self, desired, preview):
        return False


class AllowResourcesPolicy:
    """Explicit local allowlist; an absent resource always requires approval."""

    def __init__(self, resources):
        self.resources = frozenset(resources)

    def allows(self, desired, preview):
        return desired["payload"]["resource"] in self.resources


class AllowBindingsPolicy:
    """Allow exact authority-to-resource-to-adapter bindings only."""

    def __init__(self, bindings):
        normalized = []
        for binding in bindings:
            binding = tuple(binding)
            if len(binding) != 3 or not all(
                    isinstance(item, str) and item for item in binding):
                raise ValueError(
                    "policy bindings must be (producer, resource, adapter)"
                )
            normalized.append(binding)
        self.bindings = frozenset(normalized)

    def allows(self, desired, preview):
        payload = desired["payload"]
        return (
            desired["producer"], payload["resource"], payload["adapter"],
        ) in self.bindings


class AdapterRegistry:
    def __init__(self):
        self._adapters = {}

    def register(self, adapter):
        name = getattr(adapter, "name", None)
        if not isinstance(name, str) or not name:
            raise ValueError("adapter requires a non-empty name")
        if name in self._adapters:
            raise ValueError(f"adapter {name!r} is already registered")
        self._adapters[name] = adapter

    def get(self, name):
        try:
            return self._adapters[name]
        except KeyError as exc:
            raise ValueError(f"adapter {name!r} is not registered") from exc
