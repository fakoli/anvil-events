# anvil-events TLA+-style invariants

> Formal invariant list for the outbox + LogPlayer-style delivery machinery.
> Written in the style of a TLA+ spec (invariants that must NEVER be falsified,
> plus the test that proves each one). Derived from the LogPlayer paper
> (arXiv:1911.11286 §2.4–2.5) and the causal-consistency paper (arXiv:2011.09753).
> These convert the reviewer charge "reliability is internally impossible"
> into concrete, testable guarantees.

## Notation

- `state` is the durable outbox journal (append-only JSONL, fsync'd) plus the
  acked archive + per-target cursors.
- `TargetQueue` = per-target LogPlayer state machine (S/RF/FC/N).
- A `degraded` event means pending > 0 (an event exists that has not been acked).
- "Delivered to a target" = the consumer's cursor passed that event.

---

## Invariants

### INV1 — No invented history (outbox atomicity)

```
Always: if event e was journaled with side effect s, then (e, s) were
written under ONE critical section (the per-producer flock). The journal
never records an event whose side effect did not happen, and never omits
a side effect that did.
```

**Test:** `test_*_order*` — emission holds the flock for read/compute/append;
`ack()` archives BEFORE removing from pending (crash → duplicate, never loss).

### INV2 — At-least-once, never lost

```
Always: if an event is journaled (pending) and later acked, the ack is
recorded in the archive FIRST; the pending entry is removed only after.
A crash between = duplicate archive entry, which consumers dedup by
event_id. No event is ever removed before it is durably archived.
```

**Test:** `test_ack_archive_first_then_remove_pending`.

### INV3 — Duplicate prevention after reconnect (LogPlayer term)

```
Always: for each target, entries pushed under an expired term are dropped.
A target's term increments on every reconnect; any entry with a stale term
cannot be delivered again after a reconnect.
```

**Test:** `test_reconnect_term_prevents_stale_duplicates` — old term push
returns False, expired-term entry never reaches the queue.

### INV4 — Per-target order preserved

```
Always: within a target's queue, entries are popped in push order
(FIFO). The catch-up queue is preferred while in RECOVERY_FETCHING /
FETCHING_COMPLETED; normal streaming resumes only after catch-up empties.
```
**Test:** `test_normal_stream`, `test_fetching_completed_transitions`.

### INV5 — No loss while enabled

```
Always: while publish is enabled (target not SUSPENDED), every emitted
event is either delivered or remains pending (visible as degraded).
An event is never silently dropped while enabled.
```
**Test:** `test_suspend_clears_and_drops_pushes` (suspended drops are
explicit, surfaced as degraded, not silent).

### INV6 — Causal consistency (no cycles)

```
Always: the happens-before graph built from explicit `causes` edges +
per-producer `producer_seq` chains is a DAG. `verify` fails loudly on a
cycle; the checker uses an iterative Kahn topological sort (no recursion
overflow on large journals).
```
**Test:** `TestCausalChecker` — explicit-edges-only construction, iterative
Kahn.

### INV7 — Degraded signal is truthful

```
Always: health reports `pending` (unacked events) and `degraded_events`
(event.degraded records) computed live. A non-zero pending is observable
-> "no event" is distinguishable from "delivery failed".
```
**Test:** `TestDaemonHealthObservability` — seeds a pending event + an
event.degraded, asserts both appear in the health response.

### INV8 — Retention never deletes non-rotated archives

```
Always: gc() hard-cap eviction removes ONLY rotated-overflow siblings
matching the epoch-suffix pattern `\d{4}-\d{2}-\d{2}\.\d{9,11}\.jsonl$`.
Ordinary day archives and odd named files are never candidates.
```
**Test:** `test_gc_enforces_hard_cap_evicts_oldest_rotated` + odd-suffix
survival regression.

---

## Model-check summary (what TLA+ would check)

For each invariant: the property is `[]INV` (always INV) over the state
machine. The unit tests are the executable proxy — a falsifying input
(term mismatch, un-flocked emit, archive-after-remove, odd-suffix file)
must be impossible to reach without the test failing.

## Gap-to-close note

The theory map's "TLA+-style invariant list" build item is this document's
reason to exist. A full TLA+ model (translated from the LogPlayer spec) is
a future stretch; the testable invariant list is the durable form.
