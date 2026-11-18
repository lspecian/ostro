---
phase: 02-durable-partition-log-state-machine
plan: 06
subsystem: storage
tags: [ostro-types, prost, command-set, fencing, outbox, timers, golden-vectors]
requires:
  - phase: 02-03
    provides: "ids (RunId, PartitionId, AttemptToken, IdError), PayloadRef"
provides:
  - "ostro_types::command: Command + Cmd oneof (tags 1..=7; 8..=63 reserved) and all message types"
  - "Opaque JournalEvent + Phase-2-owned Derived intents (ScheduleTimer, CancelTimer, SendMessage, EnqueueReady, CompleteRun)"
  - "OutboxMsg kinds (StartChild, ChildCompleted, Signal, CancelRun, CronStart) + target_partition()"
  - "encode_command / decode_command, per-variant ctors, kind_name"
  - "validation helpers: StartRun/ParentRef/TimerFired/EventsAppended::run_id, EventsAppended::{token, events_contiguous_from}, MissingField"
  - "tests/golden/commands.txt (one frozen encoding per Cmd variant)"
affects: [02-08, 02-09, 02-10, 02-11, phase-3, phase-4]
tech-stack:
  added: []
  patterns:
    - "Hand-written prost messages; oneof enums at module level (`command::Cmd`, `command::derived::Intent`, `command::outbox_msg::Kind`)"
    - "Golden vectors: committed file (OSTRO_BLESS=1 regenerates) + one first-principles byte check"
key-files:
  created:
    - crates/ostro-types/src/command.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/golden/commands.txt
  modified:
    - crates/ostro-types/src/lib.rs
decisions:
  - "EventsAppended carries opaque journal events plus submitter-computed Derived intents (Phase 2 storage contract); apply validates fencing and applies both atomically; Phase 4 may move derivation into apply additively"
  - "`Cmd` lives at ostro_types::command::Cmd (re-exported as ostro_types::Cmd), not in a nested `command::command` module"
  - "A missing outbox kind or nested StartRun counts as an empty run id -> IdError::BadLength(0); a missing EventsAppended token -> command::MissingField(\"token\") (ids.rs untouched)"
  - "derived::Intent allows clippy::large_enum_variant (SendMessage ~280 B), so the interface's unboxed field types stay as specified"
metrics:
  duration: 12min
  completed: 2026-11-18
---

# Phase 2 Plan 06: Partition Command Set Summary

This plan defines the partition log command set as hand-written prost messages. A `Command` holds a `Cmd` oneof with seven frozen tags. Journal events are opaque, and the storage-level consequences of those events travel as explicit Phase-2-owned `Derived` intents. Every outbox message can name its target partition without a lookup. The encodings are pinned by a committed golden file.

## What was built

- **`command.rs`:**
  - `Command { cmd: Option<Cmd> }`. The variants are LeaderAnnounced=1, StartRun=2, AttemptGranted=3, EventsAppended=4, TimerFired=5, InboundBatch=6 and OutboxTruncated=7. Tags 8..=63 are reserved, as the doc comment states.
  - The message types match the interface block exactly. That covers `StartRun` (13 fields, including `rng_seed` and `parent: ParentRef`), `AttemptTokenMsg` (with `From` in both directions to `AttemptToken`), `JournalEvent`, `EventsAppended{.., events, derived}`, `Derived` with oneof `Intent` (tags 1..=5), `TimerFired`, `OutboxMsg` with oneof `Kind` (tags 1..=5), `InboundMsg`, `InboundBatch` and `OutboxTruncated`.
  - Enums: `ConflictPolicy {ReturnExisting=0, Reject=1}` and `TerminalStatus {Completed=0, Failed=1, Cancelled=2}`, both declared with `prost::Enumeration`.
  - Functions: `encode_command` (returns `Bytes`), `decode_command` (returns `Result<_, prost::DecodeError>`), one constructor per variant, `kind_name()` (returns `"None"` for an empty command) and `OutboxMsg::start_child`.
  - Helpers (Task 2):
    - `target_partition()`: StartChild and CronStart route to the new run's partition, ChildCompleted to the parent's, Signal and CancelRun to the target run's.
    - `run_id()` on StartRun, ParentRef, TimerFired and EventsAppended.
    - `EventsAppended::token()`, which returns `Err(MissingField)` when the token is absent.
    - `events_contiguous_from(from_seq)`, which uses checked arithmetic, so a sequence never wraps around. It returns true for an empty list.
  - The module doc records the freeze policy, the rule that field numbers are never reused, the opaque-events ownership split and the Derived-intent design.
- **`lib.rs`:** additive only: `pub mod command;` and `pub use command::{Cmd, Command};`. Phase 3's `journal` module and re-exports are untouched.
- **Golden file:** `tests/golden/commands.txt` holds 7 `name<TAB>hex` lines. A separate check computes the `LeaderAnnounced` bytes from first principles (`0a070807100118e807`).

## Design decision for Phase 3 and Phase 4 (Derived intents)

Apply cannot interpret Phase 3's journal events, because their semantics are not Phase 2's.
- `EventsAppended` carries the events opaquely as `{seq, kind: u32, payload: PayloadRef}`, plus a list of `Derived` intents computed by the submitter (the engine host in Phase 4): ScheduleTimer, CancelTimer, SendMessage(OutboxMsg), EnqueueReady and CompleteRun(status, result, completed_at_ms).
- Apply (02-09/02-10) validates fencing with the `(epoch, n)` token, checks `events_contiguous_from(from_seq)`, then applies the events and the intents in one atomic step. Both are in the log, so replay is deterministic.
- Phase 4 may later move derivation into apply. That change must be additive: new fields or variants, with no renumbering.
- `StartRun.rng_seed` (32 bytes, generated by the submitter) is stored so that Phase 3's `RunStarted.rng_seed` has a home. Phase 3 owns its meaning.

## Verification

- `cargo test -p ostro-types`: all suites pass. The `command` suite has 16 tests:
  - a round trip for each of the 7 variants, with nested fields populated (3 events, all 5 Derived kinds, all 5 OutboxMsg kinds);
  - the golden encodings;
  - an empty slice decoding to `cmd: None`;
  - an unknown tag-60 varint being skipped;
  - malformed bytes returning an error;
  - the `AttemptToken` conversion round trip;
  - two 1024-case proptests showing that decoding random or mutated bytes never panics;
  - 6 `routing*` tests.
- `cargo test -p ostro-types --test command -- routing` selects exactly the 6 routing tests.
- `cargo clippy -p ostro-types --all-targets -- -D warnings`: clean.
- `cargo build --workspace`: no warnings.
- `cargo fmt --all --check`: no diffs in ostro-types. The remaining diffs are in `crates/ostro-log/tests/*`, which is plan 02-07's in-progress, uncommitted work and outside this plan's scope.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] clippy `large_enum_variant` on `derived::Intent`**
- **Found during:** Task 1
- **Issue:** `SendMessage` embeds an `OutboxMsg` of about 280 bytes, so `-D warnings` failed.
- **Fix:** A targeted `#[allow]` with a reason. Boxing would change the public field type the interface specifies. The wire bytes are the same either way.
- **Commit:** 862b270

**2. [Interface clarification] `Cmd` location and the missing-field error type**
- The interface block wrote `pub mod command { enum Cmd }`. Inside `command.rs` that path would become `command::command::Cmd`, so `Cmd` sits at module level and is re-exported as `ostro_types::Cmd`.
- `EventsAppended::token()` returns `command::MissingField` instead of changing `ids::IdError`, because `ids.rs` is not in this plan's files.
- **Commits:** 862b270, 25d9dd7

## Requirements

STOR-01, STOR-10, STOR-12 and STOR-13 are **partially** delivered. This plan supplies the command format and helpers only. They stay Pending until the WAL journal, apply/fencing, outbox shuffler and timer wheel plans land.

## Commits

- bca6c40 test(02-06): add failing partition command set tests
- 862b270 feat(02-06): implement partition command set with golden encodings
- 4538b5f test(02-06): add failing outbox routing and validation helper tests
- 25d9dd7 feat(02-06): add outbox routing and command validation helpers

## Self-Check: PASSED

All created files exist; commits bca6c40, 862b270, 4538b5f, 25d9dd7 present.
