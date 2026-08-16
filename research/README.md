# anvil-events research library

Published theory mapped onto the anvil-events design. Each paper has a
**paper → design decision → build item** trace in
[`2026-08-12-theory-map.md`](2026-08-12-theory-map.md). Full texts live in
[`papers/`](papers/) (PDF + extracted `.txt`). Citations: [`references.bib`](references.bib).

## Index

| Paper | arXiv | Year | Bounded use in anvil-events |
|---|---|---|---|
| LogPlayer: Fault-tolerant Exactly-once Delivery | [1911.11286](https://arxiv.org/abs/1911.11286) | 2019 | Safety/liveness/target-state vocabulary; JetStream owns recovery and no LogPlayer implementation is claimed. |
| Delivery, consistency, and determinism | [1907.06250](https://arxiv.org/abs/1907.06250) | 2019 | guarantees-by-axis contract (PRD "Reliability contract") |
| Lamport's Arrow of Time: The Category Mistake in Logical Clocks | [2602.21730](https://arxiv.org/abs/2602.21730) | 2026 | per-producer epistemic ordering (no global sequencer) |
| Checking Causal Consistency of Distributed Databases | [2011.09753](https://arxiv.org/abs/2011.09753) | 2020 | Scope boundary: `DependencyGraphChecker` checks a DAG but does not implement the paper's database-history conformance model. |

## Reading order

1. `2026-08-12-theory-map.md` — the map (claims + why they matter + decisions).
2. `papers/*.txt` — full text where you want the algorithm detail
   (LogPlayer §2.4–2.5 is the protocol; Zennou et al. §3 is the cycle reduction).
3. `references.bib` — citations for any write-up.

## Status

Papers were fetched on 2026-08-12. The original implementation mapping was
audited and corrected on 2026-08-16. These papers inform contracts and scope;
none is presented as a formal verification of the current code.
