"""TOML composition root for a portable node reconciler."""

from __future__ import annotations

import json
import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .artifacts import DirectoryArtifactResolver, HTTPSArtifactResolver
from .command_config_adapter import CommandConfigAdapter
from .contracts import AdapterRegistry, AllowBindingsPolicy
from .engine import ReconcileEngine
from .file_adapter import ManagedFileAdapter
from .json_merge_adapter import JSONMergeAdapter
from .processor import DesiredStateProcessor


@dataclass(frozen=True)
class NodeRuntime:
    node: str
    producer: str
    allowed_producers: frozenset[str]
    processor: DesiredStateProcessor


def _validator(name):
    if name in (None, "none"):
        return lambda data: None
    if name == "json":
        def validate_json(data):
            json.loads(data)
        return validate_json
    if name == "toml":
        def validate_toml(data):
            tomllib.loads(data.decode())
        return validate_toml
    raise ValueError(f"unsupported managed-file validator {name!r}")


def _path(base, value):
    path = Path(os.path.expanduser(value))
    return path if path.is_absolute() else (base / path).resolve()


def load_node_runtime(path, store):
    config_path = Path(path).resolve()
    with config_path.open("rb") as handle:
        config = tomllib.load(handle)
    if config.get("version") != 1:
        raise ValueError("node config version must be 1")
    node = config.get("node")
    producer = config.get("producer")
    allowed = config.get("allowed_producers")
    if not isinstance(node, str) or not node:
        raise ValueError("node config requires node")
    if not isinstance(producer, str) or not producer:
        raise ValueError("node config requires producer")
    if not isinstance(allowed, list) or not all(
            isinstance(item, str) and item for item in allowed):
        raise ValueError("allowed_producers must be a string array")
    producer_pattern = re.compile(
        r"^[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*$",
    )
    if (not re.fullmatch(r"[A-Za-z0-9_-]+", node)
            or not producer_pattern.fullmatch(producer)
            or producer.split(":", 1)[0] != node):
        raise ValueError("node and producer identities are inconsistent")
    if not all(producer_pattern.fullmatch(item) for item in allowed):
        raise ValueError("allowed_producers contains an invalid identity")
    accepted_producers = frozenset([*allowed, producer])
    source = config.get("artifact_source", {})
    source_type = source.get("type")
    if source_type == "directory":
        resolver = DirectoryArtifactResolver(
            _path(config_path.parent, source["root"]),
        )
    elif source_type == "https":
        resolver = HTTPSArtifactResolver(
            source["url"], mode=source.get("mode", "fleet"),
            token_env=source.get("token_env"),
        )
    else:
        raise ValueError("artifact_source.type must be directory or https")
    registry = AdapterRegistry()
    bindings = []
    adapters = config.get("adapters")
    if not isinstance(adapters, list) or not adapters:
        raise ValueError("node config requires at least one [[adapters]] entry")
    for entry in adapters:
        adapter_type = entry.get("type")
        if adapter_type == "managed_file":
            adapter = ManagedFileAdapter(
                entry["name"],
                _path(config_path.parent, entry["destination"]),
                validator=_validator(entry.get("validator")),
                mode=entry.get("mode"),
            )
        elif adapter_type == "json_merge":
            adapter = JSONMergeAdapter(
                entry["name"],
                _path(config_path.parent, entry["destination"]),
                protected_paths=entry.get("protected_paths", []),
                mode=entry.get("mode"),
            )
        elif adapter_type == "command_config":
            adapter = CommandConfigAdapter(
                entry["name"], entry.get("command", []),
                allowed_keys=entry.get("allowed_keys", []),
                get_args=entry.get("get_args", ["get"]),
                set_args=entry.get("set_args", ["set"]),
                unset_args=entry.get("unset_args", ["unset"]),
                timeout=entry.get("timeout", 30),
                missing_returncode=entry.get("missing_returncode", 1),
                missing_stderr_prefix=entry.get("missing_stderr_prefix"),
            )
        else:
            raise ValueError(
                "adapter type must be managed_file, json_merge, or command_config"
            )
        registry.register(adapter)
        resources = entry.get("auto_apply_resources", [])
        if not isinstance(resources, list) or not all(
                isinstance(resource, str) and resource for resource in resources):
            raise ValueError("auto_apply_resources must be a string array")
        if len(resources) > 1:
            raise ValueError(
                "an adapter may auto-apply exactly one resource"
            )
        authorities = entry.get("authority_producers", [])
        if not isinstance(authorities, list) or not all(
                isinstance(item, str) and producer_pattern.fullmatch(item)
                for item in authorities):
            raise ValueError("authority_producers must be a producer string array")
        if resources and not authorities:
            raise ValueError(
                "auto_apply_resources requires authority_producers"
            )
        unknown = set(authorities) - accepted_producers
        if unknown:
            raise ValueError(
                "authority_producers must also appear in allowed_producers"
            )
        bindings.extend(
            (authority, resource, entry["name"])
            for authority in authorities
            for resource in resources
        )
    engine = ReconcileEngine(
        node, store, registry, resolver, policy=AllowBindingsPolicy(bindings),
    )
    processor = DesiredStateProcessor(
        engine, store, producer=producer, node=node,
    )
    return NodeRuntime(
        node=node,
        producer=producer,
        allowed_producers=accepted_producers,
        processor=processor,
    )
