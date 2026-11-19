---
phase: 05-workflow-semantics-core-hardening
plan: 09
subsystem: partition-apply
tags: [lifecycle, terminate, retry, resume-in-place, run-timeout, schedule-to-start, continue-as-new, retention, expiry, d1, d2, d3, d9, d10, pf-f2, ops-06]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 contract (TerminateRun/RetryRun, reserved ops, RunStatus 6..8, kind 71, outbox StartRun); 05-02 seams (lifecycle.rs/retention.rs stubs, terminal hook, reserved-timer routing, RunRecord Phase 5 fields); 05-22 attempt_failures + status_seq; 05-08 CancelRun forwarding along continued_to"
provides:
  - "lifecycle.rs: terminate_run, retry_run (resume in place), on_start (RUN_TIMEOUT_OP + SCHEDULE_TO_START_OP/s2s_due_ms), on_attempt_granted, on_terminal (timeout rows out, EXPIRE_OP in), on_reserved_timer, inbound_start_run, complete_continued_as_new"
  - "retention.rs: on_expire deleting every run-scoped row incl. the dedupe row (guarded)"
  - "pub(crate) lifecycle::expire_due: the single expiry-due formula"
  - "tests/apply_lifecycle.rs (18), tests/apply_retention.rs (5), common/gen_lifecycle.rs generator"
affects: [05-11, 05-12, 05-13, 05-15, 05-16, 05-17, 05-20]

tech-stack:
  added: []
  patterns:
    - "Reserved timers are matched against the record (due == formula) before acting; any mismatched/stale row only loses its row"
    - "Whole-batch CAN validation on a copy of the intents without the successor StartRun and the final CompleteRun, then run::check_intents"

key-files:
  created:
    - crates/ostro-partition/tests/apply_lifecycle.rs
    - crates/ostro-partition/tests/apply_retention.rs
  modified:
    - crates/ostro-partition/src/apply/lifecycle.rs
    - crates/ostro-partition/src/apply/retention.rs
    - crates/ostro-partition/tests/common/gen_lifecycle.rs
    - crates/ostro-partition/tests/apply_contract_p5.rs (tripwire retirement, outside files_modified)
    - crates/ostro-partition/tests/apply_seams.rs (tripwire retirement, outside files_modified)

key-decisions:
  - "S2S clock starts at max(ready_at_ms, requested_at_ms): a start with ready_at 0/past would otherwise time out immediately"
  - "Reserved timers act only when due matches the record: RUN_TIMEOUT needs created_at + run_timeout_ms and resumed_count == 0; S2S needs s2s_due_ms == due; EXPIRE needs completed_at + retention_ms. Keeps the 05-02 routing tripwires green and makes stale rows harmless"
  - "Hard-stop ChildCompleted carries result None and event None (no executor-built parent event exists); the parent side must build its journal entry from the status"
  - "RetryRun: non-terminal and non-resumable terminal both Rejected(Invalid(BadIntent)); resumable cleared, cancel_requested and current_attempt kept; enqueue at requested_at_ms"
  - "CAN batch shape: events + SendMessage* (exactly one StartRun) + ScheduleTimer/CancelTimer + final CompleteRun{CAN}; EnqueueReady/Suspend/RegisterWait/CancelWait/extra CompleteRun -> BadIntent. Apply fills successor continued_from = run and parent = predecessor's parent; a different continued_from/parent -> BadIntent; successor == self -> BadIntent"
  - "CAN is never resumable (resumable forced false) and stores the CompleteRun.result payload"
  - "Inbound successor StartRun claims no dedupe row (the key belongs to the chain's first run); exactly-once via inbound watermark + run-id check"
  - "Expiry: events deleted blind over 0..next_seq; guest timer rows and cron dedupe rows (__cron/<id>) are not run-prefixed and stay (documented)"

requirements-completed: []

duration: ~2h40m
completed: 2026-11-19
---

# Phase 5 Plan 09: Lifecycle and Retention Apply Summary

**Apply now enforces run timeouts and schedule-to-start (D2), terminates with a RunTerminated marker only after seq 0 (D3), resumes retryable terminal runs in place with `resumed_count`/`attempt_failures` bookkeeping and no new run timeout (D1/F9), completes continue-as-new with the successor start in the outbox, `continued_to` and no ChildCompleted (D10/PF-F2), and expires terminal runs by deleting every run-scoped row including the dedupe row, so an idempotency key can be reused (D9/OPS-06).**

## Performance

- **Duration:** about 2 h 40 min, much of it test time on a shared box (load 4–12).
- **Tasks:** 3 of 3.
- **Files:** 2 created, 3 owned files modified, 2 shared tripwire files edited (see Deviations).

## Task Commits

1. **Task 1 + CAN/retry parts of Task 2, lifecycle apply:** `bbea9cc` (feat). lifecycle.rs, apply_lifecycle.rs, tripwire retirement.
2. **Task 2, retention expiry:** `73f357f` (feat). retention.rs, apply_retention.rs.
3. **Task 3, rebuild_digest generator:** `23559e1` (test). gen_lifecycle.rs.

Tasks 1 and 2 share `lifecycle.rs` and `apply_lifecycle.rs`, so the lifecycle half of Task 2 (RetryRun, continue-as-new, inbound start) landed in commit 1 and the retention half in commit 2. Tests were written alongside the code (no separate RED commit: the stubs had no behaviour to fail against beyond the tripwires).

## What landed

**Timeouts (D2)**
- `on_start`: `RUN_TIMEOUT_OP` at `created_at_ms + run_timeout_ms`; `SCHEDULE_TO_START_OP` at `max(ready_at_ms, requested_at_ms) + schedule_to_start_ms`, stored in `s2s_due_ms`. Each armed row emits `Action::ScheduleTimer`, each deleted one `Action::CancelTimer`.
- `on_attempt_granted`: while `s2s_due_ms != 0` (only before the first grant) the S2S row is deleted by that key and zeroed. This works after the ready row is gone (W13, tested with a delayed start woken by START_DELAY then granted).
- `on_reserved_timer`: run timeout on a live run whose deadline matches → hard stop `TimedOut`/`run_timeout`; S2S on a live never-granted run → `TimedOut`/`schedule_to_start`; otherwise the row removal is the whole effect.

**Hard stop (D3)**, shared by TerminateRun and both timeouts: status, `terminal_reason`, `completed_at_ms`, `resumable = true`; ready/running rows removed; `RunTerminated{reason}` journaled via `journal_input` only when `next_seq > 0` and after the status is terminal (so no wake); `terminal::on_terminal`; `Action::RunCompleted`; ChildCompleted to the parent; graceful CancelRun to each child (scan only when `children > 0`). TerminateRun: RunNotFound / WrongPartition / FieldTooLong (> MAX_REASON_LEN) / RunTerminal.

**RetryRun (D1)**: Completed → `RunCompleted` (code 27); live or non-resumable → `Invalid(BadIntent)`; otherwise expiry row deleted by key, `terminal_reason`/`completed_at_ms` cleared, `resumable = false`, `resumed_count += 1`, `attempt_failures = 0`, ready at `requested_at_ms` (`EnqueueReady`, `first_activation` = never granted). No run timeout is re-armed: `run_timeout_due` returns `None` once `resumed_count > 0` (doc comment on `retry_run` for 05-17).

**on_terminal (lifecycle part)**: deletes the run-timeout row (if any, not after a resume) and the S2S row (`s2s_due_ms`), then arms `EXPIRE_OP` at `expire_due(rec) = completed_at_ms.unwrap_or(0) + retention_ms` when `retention_ms > 0`.

**Continue-as-new (D10, PF-F2)**: `complete_continued_as_new` validates the whole batch (shape in key-decisions; `run::check_intents` on the remaining intents; `run::any_oversize`), then writes events (with the 05-22 F10 `status_seq` update and the B1 seq-0 hook, mirroring `apply_events_appended`), the intents, the successor StartRun with lineage filled in, `continued_to`, status ContinuedAsNew (not resumable), the result payload, `terminal::on_terminal`, `Action::RunCompleted{ContinuedAsNew}`. No ChildCompleted is emitted. `inbound_start_run` = `start_run(.., None, ..)`; the successor stores `continued_from` and `parent` and gets a fresh run-timeout deadline from its own StartRun.

**Retention (D9, OPS-06)**: on a terminal run with a matching due time, one batch deletes runs, running, the ready row, events `0..next_seq` (blind), both payload rows, and prefix scans of inbox (both shapes), waits (both shapes), children, effects, signal_seen and dlq; the dedupe row is deleted only if it still points at the run. Non-terminal/stale → timer row only; unknown run → Duplicate.

**Read counts** (asserted): TerminateRun 1 (+1 children scan, +2 waits scans when non-zero); RetryRun 1; timeout fire 2 (+ hard-stop scans); CAN 1 (EventsAppended's record read); expiry 10 (timer, record, 7 scans, dedupe); on_start/on_attempt_granted/on_terminal 0.

## Measurements

- **50k-event expiry** in one apply chunk: **252 ms** apply + commit, debug build (`a_50k_event_run_expires_in_one_apply`). Below 1 s, so no bounded-delete continuation is needed now; 05-16/05-20 may re-measure at larger journals (the journal cap) in release.
- **rebuild_digest** with the lifecycle generator: 153 s and 190 s (load 7–10), against 215 s for the stub generator in the same session (load 6–12). The difference is inside load noise; no measurable increase. Final run on HEAD with 05-08's generator: 184 s.

## Decisions Made

See `key-decisions` in the frontmatter. STATE.md is not edited by this plan (orchestrator rule); STATE notes are below.

### STATE notes (for the orchestrator to fold in)

- 05-09 complete: lifecycle + retention apply (D1/D2/D3/D9/D10/PF-F2), commits `bbea9cc`, `73f357f`, `23559e1`.
- Decision: S2S clock starts at `max(ready_at_ms, requested_at_ms)`.
- Decision: reserved timers act only when the due time matches the record (stale rows harmless).
- Decision: CAN batch shape and apply-filled successor lineage (`continued_from`, inherited `parent`).
- Decision: hard-stop ChildCompleted carries no result and no event.
- Open item: guest timer rows of terminal runs and cron dedupe rows survive expiry (not run-prefixed).

## Deviations from Plan

### Plan conflicts resolved

**1. [Rule 3: Blocking] Tripwire tests outside files_modified had to be retired**
- **Found during:** Task 1 (full partition suite).
- **Issue:** `apply_contract_p5.rs` (`phase5_commands_are_rejected_until_implemented`, `inbound_start_run_is_consumed_without_effect_until_implemented`) and `apply_seams.rs` (`phase5_commands_reach_their_stubs_and_change_nothing`, `start_run_stores_phase5_fields` asserting `s2s_due_ms == 0`) assert the 05-02 stub behaviour and name 05-09 as their replacer, but neither file is in any wave-3 plan's files_modified. 05-08 hit the same thing and edited the same files (`aee02e2`).
- **Fix:** minimal hunks built on the current HEAD blobs (after 05-08): both "commands rejected" tripwires retired with a comment pointing at `apply_lifecycle.rs` (only TerminateRun/RetryRun were left in them), the inbound tripwire renamed `inbound_start_run_starts_the_successor` asserting the run exists, the start test asserting `s2s_due_ms == 30_100`, unused imports dropped. Committed via a private index; no other hunk in those files changed.
- **Commit:** `bbea9cc`.

**2. [Rule 2: Correctness] Reserved timers are matched against the record**
- The plan says "RUN_TIMEOUT on a non-terminal run → hard stop". A stray row (manual, or any future bug) would then kill a run with no timeout. The routing tripwire `reserved_timer_ops_are_routed_and_journal_nothing` fires RUN_TIMEOUT on a run without a timeout and expects no change; it stays green unmodified.

**3. [Rule 2] S2S clock starts at `max(ready_at_ms, requested_at_ms)`** instead of `ready_at_ms` (a 0 ready time would make the deadline already past).

**4. [Rule 2] CAN validation rejects a non-final `CompleteRun`**: stripping the final intent before `check_intents` would otherwise have let `[CompleteRun{Failed}, StartRun, CompleteRun{CAN}]` through. Test case added.

**5. 05-22 coordination:** RetryRun resets `attempt_failures`; the CAN path copies the F10 `status_seq` update (test asserts `status_seq == Some(1)`) and deliberately does not forward `parent_event` (no ChildCompleted, PF-F2).

**6. Local duplicates (cleanup candidates):** `cascade_cancel` mirrors the children scan of `inbound::cancel_run` (private, 05-08's file); the CAN write loop mirrors the ScheduleTimer/CancelTimer/SendMessage arms of `run::apply_events_appended`.

**7. Generator scope:** inbound StartRun batches are exercised through `ContinueAsNew` (outbox → shuffler → InboundBatch), not forged batches, which would race the shuffler's `inbound_seen` watermark. `StartTimed` uses ids derived from the op index (byte 2 = 0x09, clear of v7 ids) on partitions 0..2 (`P = 2` in rebuild_digest).

## Verification

- `cargo test -p ostro-partition --locked --no-fail-fast` on 1070fe0 + 05-09 (identical to 23559e1): all binaries green (apply_lifecycle 18, apply_retention 5, apply_seams, apply_contract_p5, apply_waits, rebuild_digest 3, ...).
- `cargo clippy -p ostro-partition --all-targets [--features failpoints] --locked -- -D warnings`: clean. `cargo fmt --check`: clean.
- `bash scripts/ci-local.sh --no-musl` at `23559e1` (scratch worktree, `CARGO_TARGET_DIR=target/wt-05-09`): see the Gate section below.

### Gate (ci-local --no-musl at 23559e1): 1 step FAILED, caused by 05-09, fix not committed

- PASS: toolchain, fmt, clippy -D warnings, warning-free build, SDK fmt/clippy/test/wasm, guests, order-saga, **failpoint suites** (588 s), deny, vendored, deny-fixtures, chronik grep, workflow validation. musl steps skipped (`--no-musl`).
- **FAIL: `cargo test`** at `ostro-server --test sink` (15/15 fail; not a flake). `cargo test --workspace --no-fail-fast --exclude ostro-partition` confirms it is the **only** failing target; ostro-partition was green separately.
- **Cause (05-09 semantics meeting an old fixture):** `crates/ostro-server/tests/sink.rs::start` submits `run_timeout_ms: 60_000` with `requested_at_ms: 1_700_000_000_000` (2023). Apply now arms `RUN_TIMEOUT_OP` at 2023 + 60 s, the node's live timer service fires it immediately, the run becomes `TimedOut`, and every sink append maps `RunTerminal` to `Fenced` (`left: Err(Fenced), right: Ok(1)` at sink.rs:124). `f.timers()` would also count the new reserved row.
- **Not fixed in place:** `tests/sink.rs` is outside 05-09's files_modified and owned by no remaining plan (05-11 owns `src/sink.rs` only); the orchestrator rule says stop and report on a break in another plan's file.
- **Validated fix (scratch worktree only, 15/15 green, clippy clean):** `requested_at_ms: 4_102_444_800_000` (far future) in `start`, and `Fixture::timers()` counts only non-reserved ops (`decode_timer_key` + `is_reserved_op`). Patch: 10 insertions, 2 deletions in `crates/ostro-server/tests/sink.rs` (in the report).
- No production path is affected: `control/runs.rs` sets `requested_at_ms = now`, so a run timeout fires only after the real deadline.

## Hand-off

- **05-11 (server seam / derive / executor):**
  - CAN batch shape apply accepts: events, any `SendMessage` (exactly one `StartRun` = successor), optional `ScheduleTimer`/`CancelTimer`, final `CompleteRun{ContinuedAsNew}`. Do not emit EnqueueReady/Suspend/RegisterWait/CancelWait in it. You may leave `StartRun.continued_from` empty and `parent` unset: apply fills `continued_from = predecessor` and copies the predecessor's parent; a different value is rejected BadIntent. Workflow qualification (D23) and tenant/tags/traceparent copying (F11) stay with derive; the successor needs its own `run_timeout_ms`/`retention_ms` (fresh deadline) and run id (any partition).
  - `Action::RunCompleted{TimedOut|Terminated}` can arrive for a run the executor holds (hard stop while Running): revoke the lease; the next append gets `RunTerminal`.
  - Resume: `RunRecord.resumed_count > 0` and `ReadyValue.first == false` on the first activation after RetryRun; no reserved timers are re-armed by apply, and wait-timeout rows were deleted at the terminal transition, so re-arm unresolved ops from the journal (B2). `attempt_failures` is 0 after a retry.
  - Policy resolution must put the final values in `StartRun.run_timeout_ms`, `schedule_to_start_ms`, `retention_ms` (0 = none). `ready_at_ms` may be 0/past: S2S starts at request time then.
  - Hard-stop ChildCompleted has `event: None` and `result: None`; the parent-side journal entry (kind 67) must be synthesized from status when `event` is None.
  - CAN chain under a parent: the parent's `children` row stays keyed by the **first** child id; the chain end's ChildCompleted names the successor id, so inbound `child_completed` removes no row (the `children` counter still decrements). Parent cancel cascades reach the chain through 05-08's `continued_to` forwarding; the stale row goes with the parent's expiry. If exact children rows matter, re-key on CAN (inbound.rs owner).
- **05-12 (corpus):** RunTerminated (71) appears as the last event of a terminated/timed-out run that had started; never at seq 0. A resumed run's journal continues after the marker at the next seq (current_attempt kept, fencing still increasing).
- **05-13 (ControlPlane):** RetryRun outcomes: `RunCompleted` → 409 `run_completed`; `Invalid(BadIntent)` → not terminal or not resumable (choose restart-as-new from `RunRecord.resumable` before submitting); `RunNotFound`. TerminateRun: `RunTerminal` for already-terminal, `FieldTooLong` for reasons > 1024 bytes. Long-poll follow: `RunRecord.continued_to` is set atomically with the ContinuedAsNew status.
- **05-15 (e2e):** cover resume after timeout (no second run timeout fires; `resumed_count` 1), S2S with a delayed start, terminate before start (empty journal) and mid-run (RunTerminated last), CAN child (parent receives exactly one ChildCompleted from the chain end), expiry then same idempotency key → new run.
- **05-16/05-20:** 50k-event expiry is 252 ms (debug); re-measure at the journal cap in release before deciding on a bounded-delete continuation. Retention never trims the WAL (`snapshot_floor` untouched).
- **05-17 (docs):** a resumed run has no run timeout; schedule-to-start applies to the initial start only; cron dedupe rows and pending guest timer rows outlive expiry.

## Self-Check: PASSED (code); gate blocked by one out-of-scope fixture

- All five owned files exist: `lifecycle.rs` (618 lines, min 200), `retention.rs`, `apply_lifecycle.rs`, `apply_retention.rs`, `common/gen_lifecycle.rs`.
- Commits `bbea9cc`, `73f357f`, `23559e1` are on main.
- Artifact strings: `EXPIRE_OP` in lifecycle.rs, `idempotency_key` in retention.rs.
- ci-local: `cargo test` fails only at `ostro-server --test sink` (see Gate); the fix is validated but not committed (outside files_modified).
