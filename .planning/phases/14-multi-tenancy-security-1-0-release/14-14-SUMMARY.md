---
phase: 14-multi-tenancy-security-1-0-release
plan: 14
subsystem: docs
tags: [docs, migrations, DIST-03]
requires: ["14-01b"]
provides: ["docs/migrations/index.md", "docs/migrations/from-cron-and-queue-tables.md", "docs/migrations/from-temporal.md", "docs/migrations/from-restate.md"]
affects: ["14-25 (docs-check, docs index)"]
tech-stack:
  added: []
  patterns: ["D-P14-27 ostro:snippet / ostro:illustrative markers"]
key-files:
  created:
    - docs/migrations/index.md
    - docs/migrations/from-cron-and-queue-tables.md
    - docs/migrations/from-temporal.md
    - docs/migrations/from-restate.md
  modified: []
decisions:
  - "Every Ostro code block is a byte-exact ostro:snippet of an existing guest/example; no Ostro block is illustrative. Only foreign-system code (crontab, SQL, Temporal TS, Restate TS) is ostro:illustrative."
  - "Virtual objects are mapped to two honest shapes (entity run per key; run per call with concurrency_key + key_concurrency=1), with the explicit caveat that key_concurrency counts leased activations and is not a per-key mutex."
  - "Competitor descriptions stay general and point readers to their own docs for version-specific details."
metrics:
  duration: "~1h"
  completed: 2026-11-27
---

# Phase 14 Plan 14: Migration guides Summary

Three migration guides (cron/queue tables, Temporal, Restate) plus an index under
`docs/migrations/`. Each guide has a concept mapping table that cites the shipped Ostro
doc section for every row, an honest "no equivalent" list, a worked conversion with
byte-exact snippets from CI-built guests/examples, and a cut-over plan.

## Commits

| Task | Commit | Files |
|---|---|---|
| 1: cron/queue tables + index | 8ffe1a3 | docs/migrations/index.md, docs/migrations/from-cron-and-queue-tables.md |
| 2: Temporal + Restate | 7eac527 | docs/migrations/from-temporal.md, docs/migrations/from-restate.md |

## Snippet markers

All snippet blocks are `rust`; the bytes were inserted mechanically from the files and
re-compared by an offline checker (marker presence, byte equality, relative links and
`#anchors` with GitHub slugs): 0 problems.

| Guide | path | lines | what |
|---|---|---|---|
| from-cron-and-queue-tables | examples/order-saga/src/lib.rs | 230-232 | workflow entry point |
| from-cron-and-queue-tables | guests/fx-http/src/lib.rs | 77-91 | semantic HTTP idempotency key in body + header |
| from-cron-and-queue-tables | examples/order-saga/src/lib.rs | 104-123 | step with retry policy |
| from-temporal | examples/order-saga/src/lib.rs | 104-123 | step with retry policy |
| from-temporal | examples/order-saga/src/lib.rs | 199-222 | durable sleep + signal raced with cancellation |
| from-temporal | examples/order-saga/src/lib.rs | 178-192 | shielded compensations |
| from-temporal | guests/tokens/src/lib.rs | 22-32 | await token + set_status |
| from-temporal | guests/child-parent/src/lib.rs | 62-68 | attached child |
| from-temporal | guests/child-parent/src/lib.rs | 76-82 | detached child |
| from-temporal | guests/can-chain/src/lib.rs | 17-26 | continue-as-new |
| from-temporal | guests/patch-v2/src/lib.rs | 27-31 | patched gate |
| from-restate | guests/signals/src/lib.rs | 36-42 | signal wait loop (FIFO) |
| from-restate | guests/can-chain/src/lib.rs | 17-26 | continue-as-new |
| from-restate | guests/tokens/src/lib.rs | 22-32 | await token |

Illustrative blocks (foreign systems only): from-cron-and-queue-tables `crontab` and `sql`
(the job table + SKIP LOCKED poll); from-temporal one `typescript` (Temporal saga);
from-restate one `typescript` (Restate cart object). Shell and JSON blocks carry no marker
(not in the D-P14-27 language set).

**Illustrative for lack of an example:** none of the Ostro code. Two places use a shorter
existing snippet instead of a dedicated example and say so in the text: the nightly-report
workflow (only the order-saga entry point is shown) and the Restate cart (described in
prose from the entity-run snippets; "there is no cart example in this repository").

## Verification

- Task 1 and Task 2 `<automated>` verify commands: PASS (run verbatim in the worktree).
- Plan gate (docs-only, marker check on all four files): PASS.
- Offline snippet-bytes/link/anchor check (own script, same rules as the planned
  docs-check): 0 problems.
- No build needed (no code changed). No CI gate in this plan; `scripts/docs-check.sh`
  does not exist yet on main (14-25 adds it and will re-verify these files).

## Deviations from Plan

None in scope. Facts were checked against main's code where docs were ambiguous:

- `ostro schedules create` takes `--name` (CLI `CreateArgs`), so the guides use
  `--name`; docs/cron.md's opening example (`ostro schedules create nightly ...`,
  positional) is wrong. **Doc bug for 14-25**, not fixed here (not in files_modified).
- `concurrency_key` and `priority` exist only on the REST/gRPC start body (no `ostro run`
  flag); the guides say so.
- `GET /v1/runs/{id}` returns `published_status` (from `ctx.set_status`), used as the
  replacement for Temporal queries and Restate shared handlers.
- Await-token completion has no failure/reject variant (`CompleteTokenBody` = token,
  payload, dry_run); the Restate guide says to encode errors in the payload.
- Audit actions named in the guides were checked against the action strings in
  ostro-server (module.upload, schedule.*, run.cancel/terminate, token/tenant/secret,
  auth.failure); signed-module commands are `ostro deploy --signature` and
  `ostro security trust add`.

## Notes for the orchestrator / later plans

- 14-25: docs/cron.md CLI example fix (`--name`); docs/connectors.md §2.1 has an unmarked
  `rust` block (and other pre-existing docs likely do too) that docs-check will flag.
- 14-25: once docs/tenancy-and-auth.md, docs/audit.md and docs/signed-modules.md exist,
  the "Operational notes" in from-cron-and-queue-tables.md and the index's "Before you
  start" could link to them (they reference CLI commands only today, because those files
  do not exist on main yet and docs-check would reject dead links).
- Flow control docs still say per-node concurrency counters "until Phase 13 adds
  cluster-wide quotas"; the Restate guide repeats only "counters are kept per node". If
  cluster-wide queue limits exist, 14-25 should update both.

## Self-Check: PASSED

- FOUND: docs/migrations/index.md, from-cron-and-queue-tables.md, from-temporal.md,
  from-restate.md (on main at 7eac527)
- FOUND: 8ffe1a3, 7eac527 in `git log main`
