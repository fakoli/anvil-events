"""Dependency-DAG integrity checking.

This checks explicit dependencies and per-producer program order. It does not
claim conformance to a database causal-consistency model.
"""

from __future__ import annotations

import collections
import json


class DependencyGraphChecker:
    @staticmethod
    def check(events):
        unique = []
        by_id = {}
        canonical_by_id = {}
        for event in events:
            event_id = event.get("event_id")
            canonical = json.dumps(
                event, sort_keys=True, separators=(",", ":"), allow_nan=False,
            )
            if event_id in by_id:
                if canonical_by_id[event_id] != canonical:
                    return False, (
                        f"conflicting envelopes for event {event_id!r}"
                    )
                continue
            by_id[event_id] = len(unique)
            canonical_by_id[event_id] = canonical
            unique.append(event)
        adjacency = [set() for _ in unique]
        indegree = [0] * len(unique)
        by_producer = {}
        for index, event in enumerate(unique):
            by_producer.setdefault(event.get("producer"), []).append(
                (event.get("producer_seq", 0), index),
            )
            for cause in event.get("causes") or []:
                cause_index = by_id.get(cause)
                if cause_index is not None and index not in adjacency[cause_index]:
                    adjacency[cause_index].add(index)
                    indegree[index] += 1
        for producer_events in by_producer.values():
            producer_events.sort()
            for (_, before), (_, after) in zip(
                    producer_events, producer_events[1:], strict=False):
                if after not in adjacency[before]:
                    adjacency[before].add(after)
                    indegree[after] += 1
        queue = collections.deque(
            index for index, degree in enumerate(indegree) if degree == 0
        )
        ordered = []
        while queue:
            current = queue.popleft()
            ordered.append(current)
            for target in adjacency[current]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
        if len(ordered) != len(unique):
            completed = set(ordered)
            cycle = next(
                (index for index in range(len(unique)) if index not in completed),
                0,
            )
            return False, (
                f"dependency cycle involving {unique[cycle].get('event_id')}"
            )
        return True, None


# V1 compatibility name. New output and documentation use the honest name.
CausalChecker = DependencyGraphChecker
