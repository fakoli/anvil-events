# anvil-events research library

Published theory mapped onto the anvil-events design. Each paper has a
**paper → design decision → build item** trace in
[`2026-08-12-theory-map.md`](2026-08-12-theory-map.md). Full texts live in
[`papers/`](papers/) (PDF + extracted `.txt`). Citations: [`references.bib`](references.bib).

## Index

| Paper | arXiv | Year | Builds in anvil-events |
|---|---|---|---|
| LogPlayer: Fault-tolerant Exactly-once Delivery | [1911.11286](https://arxiv.org/abs/1911.11286) | 2019 | `anvil_events/outbox.py` `TargetQueue` (S/RF/FC/N + terms), recovery-stream protocol, outbox cursors |
| Delivery, consistency, and determinism | [1907.06250](https://arxiv.org/abs/1907.06250) | 2019 | guarantees-by-axis contract (PRD "Reliability contract") |
| Lamport's Arrow of Time: The Category Mistake in Logical Clocks | [2602.21730](https://arxiv.org/abs/2602.21730) | 2026 | per-producer epistemic ordering (no global sequencer) |
| Checking Causal Consistency of Distributed Databases | [2011.09753](https://arxiv.org/abs/2011.09753) | 2020 | `anvil_events/outbox.py` `CausalChecker` + `anvil-events verify` |

## Reading order

1. `2026-08-12-theory-map.md` — the map (claims + why they matter + decisions).
2. `papers/*.txt` — full text where you want the algorithm detail
   (LogPlayer §2.4–2.5 is the protocol; Zennou et al. §3 is the cycle reduction).
3. `references.bib` — citations for any write-up.

## Status

Papers fetched 2026-08-12 via the arxiv skill (export.arxiv.org API) and
Semantic Scholar metadata lookups (rate-limited that day; arXiv metadata is
authoritative). All four are implemented or contract-mapped in the current
code; see `anvil_events/` and `tests/`.
