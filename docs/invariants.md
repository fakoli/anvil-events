# Executable invariants

These are implementation invariants, not a TLA+ model or proof. Each invariant
has a hermetic regression and a separate live gate where external systems are
involved.

## INV-1: immutable event identity

One `event_id` and one `(producer, producer_seq)` identify one canonical
envelope. A conflicting duplicate aborts the transaction.

Evidence: storage identity/sequence collision tests; migration conflict tests.

## INV-2: atomic local acceptance

The idempotency key, operation record, producer sequence, and pending event are
committed together. Repeating the same key and intent returns the same event;
changing intent fails.

Evidence: concurrent sequence and atomic-record tests.

## INV-3: PubAck gates delivery completion

An unknown event cannot be acknowledged. For a known pending event, PubAck
evidence, `acked` state, and cursor update commit in one SQLite transaction.
Conflicting evidence fails closed.

Evidence: PubAck/cursor, unknown event, and conflicting evidence tests. A real
broker PubAck/fault proof remains a Compose/live gate.

## INV-4: poison data cannot block valid state silently

Malformed pending data is quarantined, removed from the pending role, and
replaced by a visible journal-only local degradation audit event. That audit
never becomes broker work under an unrelated node identity. Invalid broker
messages are never journaled or reconciled.

Evidence: corrupt-row repair and subscriber validation tests.

## INV-5: resource generation cannot equivocate

For one node and logical resource, an applied or attempted generation has one
desired event. Lower generations are superseded; the same generation with
different revision/digest/adapter fails.

Evidence: stale-generation and generation-reuse tests.

## INV-6: events cannot select authority-bearing local state

An event supplies a logical resource, adapter name, artifact reference,
revision, and digest. Node config supplies artifact origin, local destination,
credential reference, validation, and exact auto-apply binding. Events cannot
execute shell, Git, or arbitrary paths.

The binding includes the exact authority producer, resource, and adapter; a
producer that is trusted for ingestion is not automatically trusted to apply
every resource.

Evidence: config composition, traversal, URL/credential rejection, and adapter
binding tests.

## INV-7: apply is verified or classified

Successful apply must pass adapter verification before `reconcile.applied`.
Verification failure attempts rollback and records failure. An exception during
apply is `INDETERMINATE` and is not automatically applied again.

Evidence: apply, verification rollback, and indeterminate replay tests.

## INV-8: broker ACK follows durable local processing

The subscriber journals and processes a desired event before ACK. Storage or
processor failure leaves the delivery unacknowledged. Awaiting approval also
remains unacknowledged so a later policy change can receive it again.

Evidence: subscriber processing and awaiting-approval tests. Real durable
redelivery remains a broker integration gate.

## INV-9: transport cannot silently downgrade

Plaintext is loopback-development only. Fleet mode requires verified TLS and
authentication. Standard INFO-then-TLS and explicit TLS-first are separate
paths. Actual and envelope subjects must match.

Evidence: endpoint policy, handshake-order, TLS-advertisement, authentication,
framing, system-reply, and subject-mismatch tests. Negative server ACL probes
remain a secured-broker gate.

## INV-10: legacy migration is non-destructive

Migration imports one stable, locked/offline, strict snapshot with provenance.
Torn tails, malformed rows, symlinks, pending/acked conflicts, source mutation,
or SQLite integrity failure roll back. The source is never deleted.

Evidence: migration success/idempotency/torn/conflict/mutation tests on native
platforms.

## INV-11: dependency claims remain narrow

`verify` checks only explicit-cause and producer-order DAG integrity. It never
labels that result causal-consistency conformance.

Evidence: acyclic, cyclic, identical duplicate, and conflicting duplicate
tests plus CLI wording.

## INV-12: recovery history outlives arbitrary node absence

The default broker stream does not expire desired-state history. Finite
retention is not valid until a tested snapshot or per-resource compaction path
can seed a new or long-offline node, and the exact referenced artifact remains
available.

Evidence: exact stream configuration verification and clean Compose replay.
