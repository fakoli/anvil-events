"""Shared hermetic test fixtures."""

from __future__ import annotations

import hashlib

from anvil_events.domain_v2 import make_event_v2


def desired_payload(data=b"router = 'node-a'\n", *, generation=1,
                    revision="rev-1", resource="routing/clients",
                    adapter="router_config", artifact="routing/clients",
                    targets=None):
    payload = {
        "resource": resource,
        "generation": generation,
        "revision": revision,
        "content_sha256": hashlib.sha256(data).hexdigest(),
        "adapter": adapter,
        "artifact": artifact,
    }
    if targets is not None:
        payload["targets"] = targets
    return payload


def desired_event(sequence=1, **payload_options):
    return make_event_v2(
        "node-a:router",
        "state.desired",
        "node-a",
        desired_payload(**payload_options),
        producer_seq=sequence,
    )
