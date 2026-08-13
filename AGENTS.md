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

## 2. Escalation: getting stuck

- If a design decision is unclear, the tests won't pass, or the review
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
| M1 | repo + schema + CI | ✅ DONE — public, 4-round review loop passed (final APPROVE) |
| M2 | Core CLI + outbox + **JetStream + degraded + gc** | 🔄 IN PROGRESS (partial: CLI/outbox/verify done; JetStream dedup publish + `event.degraded` + gc size guard pending) |
| M3 | anvil-serving `[events]` seam | ⬜ pending |
| M4 | private operator adapter (commit-push-on-promote + gateway subscriber) | ⬜ pending |
| M5 | rollout + observability | ⬜ pending |

**Last completed action / commit:** [UPDATE ME on every milestone boundary]

## 5. Repo facts (for quick orientation)

- Public repo: `fakoli/anvil-events` on GitHub. Branch: `main`.
- Git identity: `Sekou Doumbouya <sdoumbouya81@gmail.com>`.
- Test command: `python3 -m unittest discover -s tests -q` (currently 26 pass).
- Lint: `ruff check .`.
- CLI: `anvil-events` installed at `~/.local/bin/anvil-events` (uv tool).
- Daemon: launchd agent `com.fakoli.anvil-events` on Mini
  (`serve --root ~/.anvil/events --subject anvil.fleet.>`; health `:9877`).
- Broker: nats-server `127.0.0.1:4222` (JetStream) — NOT yet a persistent
  service (gap: make it brew services or a LaunchAgent).
- Review model: `gpt-5.6-sol` via `--provider openai-codex` (ChatGPT sub,
  authenticated; `codex` model name returns 400 — must use `gpt-5.6-sol`).
- Public-content policy: never commit real operator identity (hosts, models,
  IPs, ports, revisions) — de-identify as `node-a`/`node-b`, synthetic names.
- The origin story lives in `docs/origin-story.md`; the deployment model is
  ADR-0002 (one artifact, two runtimes: launchd/systemd daemon OR container).
