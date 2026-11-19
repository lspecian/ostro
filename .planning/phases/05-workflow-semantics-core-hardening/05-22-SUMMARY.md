---
phase: 05-workflow-semantics-core-hardening
plan: 22
subsystem: partition-apply
tags: [ready-entries, dispatcher, attempt-failures, status-seq, parent-event, d21, pre-freeze]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-02 RunRecord.attempt_failures/status_seq fields, ReadyValue Default, parent_event W5 oversize check"
  - phase: 04-single-node-server-rust-mvp
    provides: "04-14 dispatcher (Entry, rebuild from ready_entries + running_runs)"
provides:
  - "ReadyValue {concurrency_key, workflow, tenant, first}; first = current_attempt.is_none() at enqueue (Phase 7 F3/F7)"
  - "query::ReadyEntryRow + Action::EnqueueReady carry workflow, tenant, first_activation; ReadyEntryRow: Default"
  - "Dispatcher Entry carries workflow/tenant/first_activation; rebuild builds ready entries from ready rows only"
  - "AttemptGranted{after_failure} -> RunRecord.attempt_failures += 1 (F9)"
  - "EventsAppended -> RunRecord.status_seq = last StatusPublished (kind 25) seq of the batch (F10)"
  - "CompleteRun.parent_event forwarded verbatim into outbox ChildCompleted.event (PF-F1, D21)"
affects: [05-09, 05-11, 05-12, 05-16, 05-20, phase-7, phase-9]

tech-stack:
  added: []
  patterns:
    - "Ready rows are self-sufficient for placement: a rebuild never point-reads run records for ready runs"

key-files:
  created:
    - crates/ostro-partition/tests/apply_ready_attempts.rs
  modified:
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/outcome.rs
    - crates/ostro-partition/src/query.rs
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/tests/apply_phase4.rs
    - crates/ostro-partition/tests/apply_core.rs
    - crates/ostro-partition/tests/apply_cross.rs
    - crates/ostro-partition/tests/store.rs

key-decisions:
  - "first is computed before enqueue_ready touches the record (current_attempt is never cleared, so any granted run enqueues with first = false)"
  - "ReadyEntryRow::default is a manual impl (RunId has no Default and ostro-types is outside this plan); default run id = 18 zero bytes"
  - "status_seq is only set on the plain EventsAppended path; batches ending in CompleteRun{ContinuedAsNew} are owned whole by 05-09's lifecycle::complete_continued_as_new"
  - "Dispatcher Job is unchanged; the new fields live on the internal Entry only (Phase 7 consumes them)"
  - "attempt_failures uses saturating_add"

requirements-completed: []

duration: ~1h45m (mostly gate time under load ~20)
completed: 2026-11-19
---

# Phase 5 Plan 22: Ready Entries, Attempt Counters and parent_event Forwarding Summary

**Summary.** This plan lands four pre-freeze apply changes:
- Ready rows, `ReadyEntryRow` and `Action::EnqueueReady` carry the run's workflow, tenant and first-activation flag (`current_attempt.is_none()`), so a dispatcher rebuild needs no run-record reads.
- `AttemptGranted` with `after_failure` maintains a durable `attempt_failures` counter.
- `EventsAppended` maintains `status_seq`, an O(1) pointer to the last StatusPublished event, read from the event kind only.
- `CompleteRun.parent_event` is forwarded verbatim into the outbox `ChildCompleted.event`.

## Performance

- **Duration:** about 1 h 45 min, most of it CI time under load average about 20.
- **Tasks:** 2 of 2.
- **Files:** 1 created, 9 modified.

## Task Commits

1. **Task 1, ReadyValue workflow/tenant/first through the ready rows, EnqueueReady and the dispatcher rebuild (plus the approved literal fixes):** `de78f86` (feat).
2. **Task 2, attempt_failures, status_seq, parent_event forwarding, and the `apply_ready_attempts` suite:** `8246115` (feat).

TDD note: the tests were written with the implementation in one scratch-worktree pass, so there are no separate RED commits. The work was split into the two task commits afterwards. Task 1's `run.rs` is the final file minus the three Task 2 hunks.

## What landed

- **`values.rs`:** `ReadyValue{concurrency_key, workflow, tenant: Option<String>, first: bool}`. It is a state value changed before the freeze, so there is no VALUE_FORMAT bump.
- **`run.rs`:**
  - `enqueue_ready` fills `workflow` and `tenant` from the RunRecord and sets `first = rec.current_attempt.is_none()`.
  - `apply_attempt_granted` increments `attempt_failures` when `after_failure` is set, in the same batch as the grant.
  - The event write loop sets `status_seq` whenever `kind == EventKind::StatusPublished.code()`, so the batch's last such event wins.
  - `ChildCompleted.event = c.parent_event.clone()`.
  - No reads were added. The tests assert 1 read for AttemptGranted and 1 for EventsAppended.
- **`outcome.rs`:** `Action::EnqueueReady` gains `workflow`, `tenant` and `first_activation`.
- **`query.rs`:** `ReadyEntryRow` gains the same three fields and has a `Default` impl; `ready_entries` fills them from the value.
- **`dispatcher.rs`:**
  - `Entry` gains the three fields. `Entry::from_ready_row` is used by `apply_scan`, which gets no store handle. `Entry::from_record`, used only for running runs, sets `first = current_attempt.is_none()`.
  - The `EnqueueReady` handler copies the new fields.
  - New unit test: `ready_rows_and_actions_carry_workflow_tenant_and_first_activation`.
- **`tests/apply_ready_attempts.rs` (9 tests):**
  - Kind code 25 is pinned.
  - First activation:
    - a fresh start gives first = true, with tenant `Some` or `None`;
    - a delayed-start timer wake gives first = true;
    - a requeue after a grant gives first = false;
    - a guest timer waking a run after Suspend gives first = false.
  - Record-free ready scan: `ready_entries` still returns every field after both run records are deleted and the store is reopened.
  - `attempt_failures`: three after-failure grants give 3, which survives a reopen. Normal grants and fenced grants leave it unchanged.
  - `status_seq`:
    - starts as None and stays None without a StatusPublished event;
    - two in one batch: the later seq (4) is kept;
    - a batch without one, or a rejected batch, leaves the value unchanged;
    - the value survives a reopen.
  - PF-F1:
    - a 1 MiB blob ref is byte-identical in the outbox row;
    - arbitrary inline bytes are forwarded verbatim;
    - None stays None.

## Deviations from Plan

### Approved deviation (orchestrator, 2026-11-19)

**1. [Rule 3, blocking] Literal fixes in three test files outside `files_modified`**
- **Found during:** Task 1.
- **Issue:** these test files build `ReadyValue` / `Action::EnqueueReady` as full struct literals, so they stop compiling once the plan's required fields are added:
  - `tests/store.rs` lines 601 and 637;
  - `tests/apply_core.rs` lines 411 and 538;
  - `tests/apply_cross.rs` line 816.
- **Process:** I stopped and reported before committing. The orchestrator confirmed that no running or later Phase 5 plan lists these files and approved the change.
- **Fix:**
  - `store.rs`: `..Default::default()`.
  - `apply_core.rs` and `apply_cross.rs`: explicit `workflow: "wf", tenant: None`, with `first_activation` set to `true` for fresh or delayed starts and `false` after a grant.
- **Size:** mechanical only, 16 lines added and 3 changed.
- **Commit:** `de78f86`.

### Follow-up for 05-20 (not done here; file outside this plan)
- `docs/storage-format.md:130` still says `postcard ReadyValue { concurrency_key }`. It should read `ReadyValue { concurrency_key, workflow String, tenant Option<String>, first bool }`. Fix it before the FROZEN v0.1 mark.

## Verification

- Verification ran in the detached scratch worktree at `b046598`, with `CARGO_TARGET_DIR=target/wt-05-22`.
- No partition file changed between `b046598` and the commit parent `b4478f9`. Every committed partition file is byte-identical to the verified tree; I checked with `cmp` against the HEAD blobs.
- The intermediate Task 1 commit was not built on its own.
- Targeted suites:

  | Suite | Passed |
  |---|---|
  | apply_ready_attempts | 9 |
  | dispatcher | 12 |
  | apply_phase4 | 19 |
  | apply_core | 19 |
  | apply_cross | 16 |
  | store | 19 |
  | lib | 18 |

- `cargo clippy -p ostro-partition --all-targets [--features failpoints] -- -D warnings`: clean.
- The workspace builds with no warnings. `cargo fmt` is clean.
- **`bash scripts/ci-local.sh --no-musl`:**
  - Every step passed except `cargo test`, which failed on the known load flake `dev_startup::warm_restart_under_1s` (`dev_startup.rs:136`).
  - The failpoint suites passed (1264 s, including `ostro-partition` rebuild_digest).
  - fmt, clippy, the warning-free build, SDK, guests, deny, vendored, deny-fixtures, chronik grep and workflow validation all passed.
- **Flake follow-up:**
  - `cargo test -p ostro --test dev_startup` alone: 4 passed, 1 ignored.
  - `cargo test --workspace --no-fail-fast`: exit 0, 125 suites ok.
- The target dir was deleted afterwards.

## STATE notes (no gsd-tools mutators run)

- 05-22 is complete. Phase 5 is at 11 of 24 committed SUMMARYs.
- Decisions: see `key-decisions` above.
- No blockers.

## Hand-off

- **05-11 (server seam):**
  - `RunRecord.attempt_failures` is durable and counts every `AttemptGranted{after_failure: true}`. Base the retry limit on it, not on the in-memory `Job::failures`, and set `after_failure` on the re-grant after an `ExecOutcome::Retry`.
  - `status_seq` points at the last StatusPublished event.
  - Inbound `ChildCompleted.event` now carries the child's submitter-encoded event (Phase 9 journals it).
  - Dispatcher `Entry` has workflow, tenant and first_activation if the pause hook needs them.
- **05-12 (corpora):** add these entries:
  - a ReadyValue with workflow, tenant `Some`/`None` and first `true`/`false`;
  - a RunRecord with nonzero `attempt_failures` and a `Some` `status_seq`;
  - an outbox ChildCompleted with an inline and a blob `event`.
- **05-16:** `first` is `current_attempt.is_none()`, so writing RunStarted at StartRun does not change it. Re-assert first = true in your StartRun/RunStarted test. The test `requeue_after_grant_and_wake_after_suspend_are_not_first` covers false.
- **05-09:**
  - RetryRun must reset `attempt_failures` to 0.
  - `complete_continued_as_new` owns its whole batch. Set `status_seq` there and forward `parent_event` there if a CAN child must notify its parent.
- **05-20:** the `storage-format.md:130` ReadyValue line (above).
- **Phase 7:** the dispatcher carries but does not use workflow, tenant and first_activation.

## Self-Check: PASSED

- `crates/ostro-partition/tests/apply_ready_attempts.rs` exists on HEAD.
- Commits `de78f86` and `8246115` are on main.
- The artifact patterns `first_activation` and `current_attempt` are present in `run.rs`.
