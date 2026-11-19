---
phase: 05-workflow-semantics-core-hardening
plan: 02
subsystem: partition-store-apply
tags: [fjall, keyspaces, waits, signals, inbox, run-record, apply-seams, d21, b1, storage-format]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 ostro-types Phase 5 contract (Cmd 8..12, StartRun 19..23, Intent 7..8, kinds 70/71/96/97, reserved ops, RunStatus 6..8) and temporary BadIntent arms"
  - phase: 04-single-node-server-rust-mvp
    provides: "04-12 Running keyspace / RunRecord Phase 4 fields; 04-13 TimerFired journaled at next_seq + wake"
provides:
  - "Keyspaces waits (14) and signal_seen (15); waits key shapes 0x01 run‖op -> WaitRow and 0x02 run‖name‖op FIFO index"
  - "Inbox re-key: signal rows run‖name‖n -> SignalSlot{Journaled, Pending}; legacy InboxMsg rows run‖0xFFFF‖n"
  - "RunRecord Phase 5 trailing fields (incl. attempt_failures, status_seq, continued_to, pause); RunRecord/ReadyValue/PauseMark/RunStatus derive Default"
  - "apply/inputs.rs journal_input + journal_encoded (D21 verbatim, blob-safe, zero reads)"
  - "apply/terminal.rs on_terminal (waits + wait-timer cleanup only when open_waits > 0)"
  - "Stub files waits.rs (05-08), lifecycle.rs + retention.rs (05-09) with every call site wired in mod.rs/run.rs/timer.rs/inbound.rs"
  - "First-seq-0 hook (B1) waits::on_first_events; reserved-timer routing; CompleteRun.parent_event oversize (W5)"
  - "RejectReason::RunCompleted (index 11, code 27)"
  - "rebuild_digest generator hooks common::gen_waits / gen_lifecycle (+ GenRun driver view)"
  - "docs/storage-format.md §13 (keyspaces, SignalSlot, RunRecord fields, read-budget exceptions, amended D18, BlobRef invariant, node.toml rule)"
affects: [05-08, 05-09, 05-11, 05-12, 05-16, 05-20, 05-22, phase-7, phase-8, phase-9]

tech-stack:
  added: []
  patterns:
    - "Seam files: call sites live in mod.rs/run.rs/timer.rs/inbound.rs; owners fill waits.rs/lifecycle.rs/retention.rs only"
    - "Intent seams split into check_* (pre-write validation, Result<(), RejectReason>) and an apply fn (write loop, Result<(), StoreError>) to keep validate-then-write"
    - "Generator hooks: strategy() -> BoxedStrategy<Option<Op>> + commands(op, &[GenRun], n) so owners never edit rebuild_digest.rs"
    - "serde `with` adapters for prost PayloadRef (prost bytes) and Option<RunId> ([u8;18]) inside postcard state values"

key-files:
  created:
    - crates/ostro-partition/src/apply/inputs.rs
    - crates/ostro-partition/src/apply/terminal.rs
    - crates/ostro-partition/src/apply/waits.rs
    - crates/ostro-partition/src/apply/lifecycle.rs
    - crates/ostro-partition/src/apply/retention.rs
    - crates/ostro-partition/tests/apply_seams.rs
    - crates/ostro-partition/tests/common/gen_waits.rs
    - crates/ostro-partition/tests/common/gen_lifecycle.rs
  modified:
    - crates/ostro-partition/src/store/{mod,keys,values}.rs
    - crates/ostro-partition/src/apply/{mod,run,timer,inbound,outcome}.rs
    - crates/ostro-partition/tests/{store,apply_cross,rebuild_digest}.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-server/src/run_started.rs
    - docs/storage-format.md

key-decisions:
  - "Inbound outbox Signal implements the no-wait branch now (Pending slot while next_seq == 0, else SignalPayload journaled verbatim + Journaled slot) instead of a consumed no-op: the no-op would break the unowned Phase 2 shuffler exactly-once suites; 05-08's plan already says 'keep the 05-02 behaviour of inbound_signal as the no-wait branch'"
  - "Legacy InboxMsg rows re-keyed run‖0xFFFF‖n so signal_inbox_prefix scans (any name, even empty) never see them"
  - "InboxMsg::Signal removed; CancelRequested kept (inbound CancelRun still writes it until 05-08); ChildCompleted kept (live until Phase 9); TimerFired kept (never written, but tests/timers.rs - not in this plan's files - matches on it)"
  - "completed_at_ms already existed (04-12), so it was not re-added"
  - "CompleteRun{ContinuedAsNew} as last intent routes the whole EventsAppended to lifecycle::complete_continued_as_new after token/seq/contiguity checks; check_intents and any_oversize are pub(crate) for 05-09"
  - "on_reserved_timer / on_expire receive Option<RunRecord> after timer.rs removed the row and read the record (2 reads, unchanged budget); routed before the terminal check"
  - "CompleteRun sets resumable from the intent and clears terminal_reason (executor completions carry no reason)"
  - "journal_encoded wakes a Scheduled run at rec.created_at_ms (apply cannot read the ts inside the event); journal_input at ts_ms (unchanged 04-13 behaviour)"
  - "PF-F1 parent_event forwarding left to 05-22 (DECISIONS); only the W5 oversize check landed here"

patterns-established:
  - "Tripwire: apply_seams.rs inbound_signal_buffers_without_waits_until_implemented (05-08 extends it with wait matching)"

requirements-completed: []

duration: ~2h
completed: 2026-11-19
---

# Phase 5 Plan 02: Partition Store Schema and Apply Seams Summary

**This plan adds the Phase 5 partition state schema and the apply seams.**
- Schema: the `waits` and `signal_seen` keyspaces, a name-keyed signal inbox holding `SignalSlot{Journaled, Pending}`, and every Phase 5 `RunRecord` field (with `Default`).
- D21 journaling helpers: `journal_input` and `journal_encoded`.
- The terminal hook clears waits and their timers.
- Reserved-timer routing and the B1 first-seq-0 hook.
- Every new command and intent goes to a stub in `waits.rs`, `lifecycle.rs` or `retention.rs`, so 05-08, 05-09 and 05-22 can work on disjoint files.

## Performance

- **Duration:** about 2 h. A large share was test time under heavy parallel load (load average about 15).
- **Tasks:** 3 of 3.
- **Files:** 8 created, 15 modified.

## Task Commits

1. **Task 1, store schema + storage-format doc:** `811c89d` (feat)
2. **Task 2, journaling helpers, terminal hook, lifecycle/first-events hooks, reserved-timer routing:** `8dee579` (feat)
3. **Task 3, dispatch stubs, inbound routing, RunCompleted, generator hooks:** `0471e1a` (feat)
4. **Follow-up, Node::open harness timeout of 30 s (load, not a regression):** `f079dd7` (test)

TDD note: each task wrote its tests together with the code. There were no separate RED commits: the stubs have no behaviour to fail against, and the store tests were written first and then run.

## What landed

**Store (Task 1)**
- `Table::Waits` is ordinal 14 (`waits`) and `Table::SignalSeen` is ordinal 15 (`signal_seen`), taken from RESERVATIONS §6. `TABLE_COUNT` is now 16.
- Key helpers:
  - `wait_key`, `wait_name_key`, `wait_prefixes`, `wait_name_prefix` and their decoders;
  - `signal_inbox_key`, `signal_inbox_prefix`, `decode_signal_inbox_key`;
  - `signal_seen_key`, `run_prefix`;
  - `INBOX_LEGACY_TAG = 0xFFFF`.
- Value types:
  - `WaitRow{kind: WaitKind, name, timeout_due_ms}`;
  - `SignalSlot::{Journaled{payload_seq, sent_at_ms}, Pending{event: PayloadRef, sent_at_ms}}`. `Pending.event` is stored as a prost byte string, which keeps it under the BlobRef invariant;
  - `PauseMark`.
- `RunRecord` gained these trailing fields, in order: `schedule_to_start_ms`, `retention_ms`, `idempotency_key`, `terminal_reason`, `resumable`, `retry_of`, `continued_from`, `cancel_reason`, `resumed_count`, `open_waits`, `s2s_due_ms`, `attempt_failures`, `status_seq`, `continued_to`, `pause`.
- StartRun stores fields 19..22. A malformed `retry_of` or `continued_from` is rejected with `Invalid(BadRunId)`.
- Tests:
  - FIFO order of ops for one name;
  - arrival order of the signal inbox;
  - legacy rows are excluded from every signal prefix, including the empty name;
  - round trips, including `pause: Some` and both `SignalSlot` variants with an inline and a blob event.

**Seams (Task 2)**
- `journal_encoded` writes `JournalEvent{seq: next_seq, kind, payload}` verbatim with zero reads. A unit test covers a 1 MiB blob ref and an inline event.
- Waking a run:
  - Scheduled → Ready, plus `EnqueueReady`;
  - Running → `InputAppended`;
  - Ready → nothing.
- The 04-13 timer path now goes through `journal_input`. `timers.rs` and `apply_timer_input.rs` pass unchanged.
- `terminal::on_terminal`:
  - with two waits, one carrying a timer, completion leaves zero waits rows and no timer row, emits `Action::CancelTimer`, and costs exactly 3 reads;
  - with `open_waits == 0` it costs exactly 1 read.
- `waits::on_first_events` runs once per run, at the batch that writes seq 0 (unit test with a thread-local counter). An empty batch at seq 0 or a rejected batch does not call it.
- Timer routing:
  - `RUN_TIMEOUT_OP` and `SCHEDULE_TO_START_OP` go to `lifecycle::on_reserved_timer`;
  - `EXPIRE_OP` goes to `retention::on_expire`, which also runs on terminal runs;
  - other reserved ops have no effect;
  - guest ops call `waits::try_wait_timeout` first.
- W5: an inline `parent_event` over 256 KiB is rejected with `PayloadTooLarge` and writes nothing. Exactly 256 KiB inline and any blob ref are accepted.

**Dispatch (Task 3)**
- The five new commands and both intents reach their stubs. Each returns `BadIntent`, emits no actions, and leaves the data tables unchanged. The 05-01 tripwires in `apply_contract_p5.rs` stay green and unmodified.
- `RejectReason::RunCompleted` is index 11 (code 27). The unit test `outcome_codes_are_pinned` pins every outcome code and the `InvalidCode` order. `NotImplemented` was reused at index 6, not re-added.

## Decisions Made

See `key-decisions` in the frontmatter. These are recorded here instead of STATE.md, because 05-07 owns STATE.md in this wave.

## Deviations from Plan

### Plan conflicts resolved

**1. [Rule 1/3: Blocking] Inbound `Kind::Signal` is not a consumed no-op**
- **Found during:** Task 3.
- **Issue:** The plan asked for "consumed with no effect". `tests/shuffler.rs` and `tests/shuffler_exactly_once.rs` (not in any Phase 5 plan's file list) assert exactly-once signal delivery through `common::signal_names`, so a no-op would fail them.
- **Fix:** `waits::inbound_signal` implements the no-wait branch that 05-08 keeps:
  - `next_seq == 0`: a `SignalSlot::Pending` with nothing journaled; a Scheduled run is still woken (Phase 2 behaviour);
  - otherwise: `SignalPayload` is journaled verbatim and a `Journaled` slot is written;
  - `sent_at_ms = 0`, because the outbox Signal carries no time;
  - a missing event or an over-long name is dropped.
  `common::signal_names` now reads `SignalSlot` rows ordered by `n`. This works for both 05-02 and 05-08 behaviour, and the shuffler files were not touched. The tripwire is renamed `inbound_signal_buffers_without_waits_until_implemented` (in `apply_seams.rs`). `apply_cross.rs` signal assertions now check `Pending` slots.
- **Commit:** `0471e1a`.

**2. [Rule 3: Blocking] Dead-variant removal was partial**
- `InboxMsg::Signal` was removed. `TimerFired` stays because `tests/timers.rs` matches on it; that file is not in this plan, and the plan requires that suite to pass unchanged.
- `CancelRequested` is still live: inbound CancelRun writes it, and the plan says CancelRun "keeps its current behaviour".
- `ChildCompleted` is still live (inbound child completions).

**3. [Rule 2: Correctness] Legacy inbox key gained a `0xFFFF` length sentinel**
- **Issue:** The old `run ‖ n` key prefix-matched `signal_inbox_prefix(run, "")`.
- **Fix:** Legacy rows are now keyed `run ‖ 0xFFFF ‖ n`. Signal names are at most 256 bytes, and the key builders assert the length is below `0xFFFF`.

**4. [Rule 2] `rebuild_digest` driver re-reads live `next_seq`**
- **Issue:** Inbound signals now journal on started runs, which would otherwise turn the driver's later Events into `SeqMismatch`.
- **Fix:** `live_next_seq` re-reads the value before Events, Suspend and hook ops. The proptest case count is unchanged.

**5. [Rule 3] Node::open harness timeout raised to 30 s** (`f079dd7`). See the investigation under Verification.

**6. Scope notes**
- `completed_at_ms` already existed (04-12).
- PF-F1 forwarding is 05-22's, per DECISIONS. The 05-01 hand-off said 05-02, but this plan's objective excludes it.
- `apply/mod.rs` gained the five mod declarations and the dispatch arms.
- `ApplyResult` lives in `outcome.rs`.

## Verification

- `cargo test -p ostro-partition --locked`: all suites green, including `apply_seams` (11), `apply_cross` (16), `store` (19), `apply_contract_p5` (5), `timers` (7), `apply_timer_input` (8) and lib (17).
- `rebuild_digest` took 240 s under load average about 15. Earlier loaded runs of the same suite took 219–229 s (05-03/05-07 logs) and quiet runs about 120 s. The stub hooks add no commands, so I attribute the difference to load.
- `cargo test -p ostro-partition --features failpoints -- --test-threads=1`: green. `startup_order` hit the known `Node::open timed out` flake under load (4 tests failed at `common::open_node`). Rerun alone it passed 9/9, and the binaries after it (`store`, `store_digest`, `timers`, `trim_safety`, `fatal_io_state`, `ack_after_apply`) passed.
- `cargo clippy -p ostro-partition --all-targets [--features failpoints] -- -D warnings`: clean.
- `cargo build --workspace --all-targets`: warning-free.
- `cargo fmt --check`: clean.
- **`CARGO_TARGET_DIR=target/wt-05-02 bash scripts/ci-local.sh --no-musl`** at code HEAD `e37c76e`: every step passed except two, both caused by load:
  - **`cargo test`:** failed on the known flake `dev_startup::warm_restart_under_1s`. Rerun alone it passed (4 passed, 1 ignored). `cargo test --workspace --no-fail-fast` showed one other failure, `ostro-api openapi_document`. That is an insta snapshot from 05-04's in-flight commit `e37c76e` (lifecycle protos), not from this plan's files.
  - **Failpoint suites:** failed in `rebuild_digest` with `node open timed out` (load average about 18). Rerunning `ostro-partition` and `ostro-crashtest` with `--features failpoints --no-fail-fast -- --test-threads=1` was green. The `ostro-wal`, `ostro-blob` and `ostro-log` failpoint suites passed in the gate run itself.
  - fmt, clippy, the warning-free build, SDK, guests, deny, vendored, deny-fixtures, chronik grep and workflow validation all passed.

### Node::open investigation (orchestrator request)

Other executors reported `node open timed out` at the harness after `811c89d`/`8dee579`, so I compared Node::open time before and after this plan.
- **Probe:** a temporary, uncommitted test that opens a fresh 16-partition node and reopens it, 5 rounds each, at `80ca356` (before 05-02) and at HEAD.
- **Before 05-02:** fresh opens took 1.22–2.64 s and reopens 99–174 ms.
- **HEAD:** fresh opens took 1.27–1.31 s and reopens 94–142 ms.
- **Repeats:** alternating repeats showed the same overlap. They are less clean, because the two worktrees shared one target dir and cargo reused a library across them.
- **Open path:** this plan adds no work there beyond creating two more (empty) keyspaces per partition. No rebuild scans or per-run reads were added. The apply hooks run only on terminal or seq-0 batches and read nothing when `open_waits == 0`.
- **Conclusion:** the timeouts come from load (load average 15–18 with 4+ parallel executors). The fix is commit `f079dd7`: `common::OPEN_T = 30 s` for `open_node`, `open_node_cfg` and `rebuild_digest`'s own open. `T` stays at 10 s, and the reason is in a doc comment. This is a minimal, append-only edit to the shared `tests/common/mod.rs`.

## Hand-off to wave 3

- **05-08 (waits.rs, inbound.rs, apply_waits.rs, gen_waits.rs):**
  - Fill the stubs: `signal_run`, `request_cancel`, `complete_token`, `check_register_wait`/`register_wait`, `check_cancel_wait`/`cancel_wait`, `try_wait_timeout` (gets `&mut rec` and must `store_run` itself when it returns `Some`), and `on_first_events`.
  - Remove the cfg(test) `FIRST_EVENTS_CALLS` counter, or keep incrementing it: the `run.rs` unit test `first_events_hook_runs_once_per_run_at_the_seq0_batch` reads it.
  - `inbound_signal` already has the no-wait branch. Put the waits-by-name match in front of it, and add W5 if you want a whole-batch reject (`check_msg` already rejects oversize inline payloads).
  - Keep `open_waits` exact: `terminal::on_terminal` relies on it to skip the scans.
  - `common::cancel_count` counts legacy `CancelRequested` rows plus journaled kind-69 events, and counts 1 for a `cancel_requested` run with `next_seq == 0` and neither. That matches your "flag only before seq 0" rule.
  - After you remove the legacy push, no shuffler change is needed.
  - Fill `gen_waits::WaitOp`, `strategy()` and `commands()`. `rebuild_digest` already merges them and re-reads live `next_seq` into `GenRun`.
- **05-09 (lifecycle.rs, retention.rs, gen_lifecycle.rs):**
  - Stubs to fill: `terminate_run`, `retry_run`, `on_start` (after the record is built, before the store; arm `RUN_TIMEOUT_OP` and set `s2s_due_ms`), `on_attempt_granted`, `on_terminal` (called from `terminal::on_terminal` after the waits cleanup; arm `EXPIRE_OP`), `on_reserved_timer(rec: Option<RunRecord>)`, `complete_continued_as_new` (owns the whole EventsAppended; use `run::check_intents` and `run::any_oversize`; `SendMessage(StartRun)` is still rejected by `check_intents` for non-CAN batches) and `inbound_start_run`.
  - In `retention::on_expire`, the timer row is already removed when it is called.
  - Hard stops must call `terminal::on_terminal`.
  - `RejectReason::RunCompleted` exists for RetryRun on Completed runs.
  - Cron starts store their own (usually empty) `idempotency_key`; their dedupe row is under `__cron/<id>`.
- **05-22 (run.rs, query.rs, dispatcher.rs):** `RunRecord.attempt_failures` and `status_seq` exist (0/None). `ReadyValue` derives `Default` and has no new fields. The `parent_event` oversize check is in `any_oversize`; forward `c.parent_event` into `ChildCompleted.event`, which is still `None`.
- **05-16:** call `waits::on_first_events` after writing `StartRun.run_started` at seq 0.
- **05-11:** inbound signals to a started run journal `SignalPayload` (96) and emit `InputAppended`. The engine skips 96 (05-03).
- **05-20:** storage-format §13 is ready for the FROZEN v0.1 mark. RESERVATIONS rows waits/signal_seen (§6) and RunCompleted (§7) can flip to `exists`.

## Self-Check: PASSED

- All eight created files exist: `inputs.rs`, `terminal.rs`, `waits.rs`, `lifecycle.rs`, `retention.rs`, `apply_seams.rs`, `gen_waits.rs`, `gen_lifecycle.rs`.
- All four commits were found on main: `811c89d`, `8dee579`, `0471e1a`, `f079dd7`.
- The artifact strings were found: `pub(crate) fn journal_encoded` in `inputs.rs`, `Waits` in `docs/storage-format.md`, `on_first_events` in `run.rs`, `is_reserved_op` in `timer.rs`, and `gen_waits` in `rebuild_digest.rs`.
