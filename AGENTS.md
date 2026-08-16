# AGENTS.md — anvil-events autonomous workflow

This file exists so any agent session (Hermes, a fresh context window, a
delegated subagent) can pick up exactly where the last one left off and run
the same loop. It is the operating manual for the repo's milestone pipeline.

---

## 1. The core loop (autonomous execution contract)

Work through the milestones **in order**. For EACH milestone:

1. **Implement** the milestone's scope (per `prd.md` Milestones + the ADRs).
   - Stdlib-only (`dependencies = []`). No real network in tests.
   - Every behavior change carries a hermetic test (unittest, `tests/`).
   - `ruff check .` must pass before any commit.
2. **Submit to GPT 5.6 for adversarial review** via `gpt-5.6-sol`
   (`--provider openai-codex`), structured as an **adversarial review**
   (skeptical senior infra architect; find the holes, do not rubber-stamp).
   - This is the gate: no next milestone while a review is open.
3. **Merge your changes when you ask for the review** — i.e., commit + push
   to `main` BEFORE dispatching the review, and open/merge a PR as part of
   the submission (branch → PR → merge → then review the merged result).
4. **Get the feedback**, fix everything real it flags (same discipline as the
   M1 review loop: accept valid findings, patch code + tests, commit).
5. **Only then move to the next milestone.**

Repeat until all milestones are complete.

## 2. Review boundary (operator-set, 2026-08-12)

**One review per milestone; only CODE correctness can REQUEST CHANGES.**
Small doc drift is NON-BLOCKING (the design docs evolve across milestones and
are reconciled at the end). The reviewer must judge the *diff of the
milestone*, not re-scan the whole repo for doc inconsistencies. A review may
only REQUEST CHANGES for a real code defect (correctness/security/durability/
race), not for prose. If the only findings are doc wording, the milestone
passes and doc drift is tracked as a follow-up, not a blocker.

## 3. Escalation: getting stuck
  produces a finding you don't understand: **ask GPT 5.6 (gpt-5.6-sol,
  --provider openai-codex) for advice** — not just as a reviewer, but as a
  consultant. Frame the question with the exact code/error/finding.
- Do not spin: after two failed attempts at a fix, escalate with full
  context (repo state, diff, error output).
- If the operator is present, an explicit question is always allowed.

## 3. Context-window safety (compaction protocol)

The session WILL run long. When context runs low:

- **Do a compaction** (Hermes compacts automatically; on manual trigger,
  place the pointer file `STATE.md` at the repo root with: current milestone,
  current commit, open review id, next action).
- **Do NOT lose where you are.** Before any potentially lossy operation
  (compaction, /stop, /new), the facts must live in:
  1. `AGENTS.md` (this file — workflow is static),
  2. `STATE.md` (repo root — mutable position pointer, updated as you go),
  3. git history (every milestone = committed + pushed, so state is
     reconstructible from `git log`).
- AGENTS.md + STATE.md + git history are the single source of truth for "what
  is done and what is next."

## 4. Where we are (keep this updated)

| Milestone | Scope (prd.md) | Status |
|---|---|---|
| R1 | audit + research correction + composable domain/storage | ✅ DONE — PRs #1/#2 merged; review blockers fixed; CI green |
| R2 | secure transport + reconciliation + portable runtime | ✅ DONE — PRs #1/#2 merged; review blockers fixed; CI/acceptance green |
| R3 | public anvil-serving lifecycle seam | ✅ DONE — public lifecycle seam and fixes merged; exact-head CI green |
| R4 | private manifests + canary | ✅ DONE — portable product support merged; private reference canary and rollback passed |
| R5 | staged rollout + live acceptance | ✅ DONE — v0.2.2 released; four-node reference fleet accepted |

**Last completed action:** the live duplicate-delivery defect found during R5
was fixed in PR #10 and released as v0.2.2 at
`c1a08bd82d7f43da7f166a43aab2818c84a79360`. Exact-head CI, staged
installation, broker-loss recovery, and private live acceptance pass.

## 5. Repo facts (for quick orientation)

- Public repo: `fakoli/anvil-events` on GitHub. Branch: `main`.
- Git identity: `Sekou Doumbouya <sdoumbouya81@gmail.com>`.
- Test command: `python -W error::ResourceWarning -m unittest discover -s tests -q`
  (currently 161 pass).
- Lint: `ruff check .`.
- v0.2.2 is installed on the private four-node reference topology. Live
  identities, routes, rollback paths, and raw evidence remain private.
- Real service, broker, topology, and route state belong in the private
  operator repository; do not copy them into this public file.
- Review model: `gpt-5.6-sol` through the authenticated Codex OpenAI provider.
- Public-content policy: never commit real operator identity (hosts, models,
  IPs, ports, revisions) — de-identify as `node-a`/`node-b`, synthetic names.
- The origin story lives in `docs/origin-story.md`; the deployment model is
  ADR-0002 (one artifact, two runtimes: launchd/systemd daemon OR container).
