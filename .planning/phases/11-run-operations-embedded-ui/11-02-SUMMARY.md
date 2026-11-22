---
phase: 11-run-operations-embedded-ui
plan: 02
subsystem: types / partition apply / engine (contract)
tags: [contract, ordinals, golden, stubs, run-limits, literal-fixups]
requires:
  - 11-01 PHASE-11 GATE (RESERVATIONS P11 block)
provides:
  - StartRun 40 rerun_prefix / 41 rerun_from_seq, JournalEvent.4 attempt, Cmd 45 DlqUpdate
  - InvalidCode 29 BadRerunPrefix / 30 BadDlqUpdate (+ placeholders 24..28)
  - apply/{dlq,index,rerun}.rs stubs (D-P11-23)
  - ostro_engine::RunLimits as EngineConfig.run_limits (D-P11-13)
affects: [11-05, 11-06, 11-07, 11-08, 11-10, every later plan building StartRun/JournalEvent/EngineConfig literals]
tech-stack:
  added: []
  patterns: ["..Default::default() on JournalEvent/StartRun/EngineConfig literals"]
key-files:
  created:
    - crates/ostro-engine/src/run_limits.rs
    - crates/ostro-partition/src/apply/dlq.rs
    - crates/ostro-partition/src/apply/index.rs
    - crates/ostro-partition/src/apply/rerun.rs
    - crates/ostro-partition/tests/p11_format.rs
  modified:
    - crates/ostro-types/src/command.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/golden/commands.txt
    - crates/ostro-partition/src/apply/{mod,outcome}.rs
    - crates/ostro-engine/src/{config,lib}.rs
    - 44 files with literal fix-ups (list below)
    - .planning/RESERVATIONS.md (P11 rows flipped to exists)
decisions:
  - "InvalidCode placeholders ReservedP8_24, ReservedP9_25..ReservedP9_28 declared (P-15 canonical names) so BadRerunPrefix sits at 29 and BadDlqUpdate at 30"
  - "DlqUpdate requeue modes exposed as dlq_update::REQUEUE_RESUME/RESTART/RERUN (0/1/2); DlqUpdate::run_id() and check_limits() (Dismissed.reason <= 1024) helpers added"
  - "RunLimits carries #[serde(default)] (EngineConfig itself is not serde); no enforcement until 11-06"
metrics:
  duration: "~2h (mostly the affected-crates test run under shared load)"
  completed: 2026-11-22
---

# Phase 11 Plan 02: Contract a Summary

The Phase 11 storage contract is in: StartRun 40/41, JournalEvent.4, Cmd 45 `DlqUpdate` (with the failure episode `episode_completed_at_ms`), InvalidCode 29/30, the three apply stubs and `RunLimits` in `EngineConfig`, with every struct literal the new fields broke fixed in the same green commit. All new fields default-omit in proto3, so every pre-Phase-11 command re-encodes byte-identically and the v0.1 format corpus rebuilds unchanged.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1+2 | 42bfc30 | `feat(11-02)`: contract, stubs, RunLimits, literal fix-ups, goldens and regression tests (one commit; Task 2's re-encode test was added in it, so no separate `test(11-02)` commit) |

## Contract (as built)

- `command.rs`: `StartRun.rerun_prefix` (40, repeated JournalEvent), `StartRun.rerun_from_seq` (41), `JournalEvent.attempt` (fixed64, 4), `Cmd::DlqUpdate` (45), `DlqUpdate {run_id 1, op oneof {HookStarted{hook_run_id} 2, Dismissed{reason, at_ms} 3, Requeued{mode, new_run_id, at_ms} 4}, episode_completed_at_ms 5}`, constants `DLQ_DISMISS_REASON_MAX = 1024`, `SYSTEM_TAG_PREFIX`, `RERUN_FROM_TAG`, `ON_FAILURE_OF_TAG`; `Command::dlq_update`, `kind_name` "DlqUpdate". Module doc "Phase 11 additions" section; the reserved-tag doc lines now list 45 as used.
- `apply/mod.rs`: `Cmd::DlqUpdate(d) => dlq::apply_dlq_update(..)` -> `Rejected(Invalid(NotImplemented))`, 0 reads, 0 writes. `index.rs` (`on_start/on_terminal/on_resume/on_expire` no-ops) and `rerun.rs` (`journal_prefix` -> `Err(Invalid(NotImplemented))`, not called) carry `#![allow(dead_code)]` naming 11-05 / 11-07.
- A Cmd 45 in an old binary decoded to `cmd: None` (`Invalid(Empty)`); it now yields `Invalid(NotImplemented)`. Both have outcome code 26 (the InvalidCode detail is not hashed), so the rolling hash is unchanged.
- `ostro_engine::run_limits::RunLimits {max_history_events 50_000, max_children_per_run 10_000, max_payload_bytes 4 MiB}`, re-exported as `ostro_engine::RunLimits`; `EngineConfig.run_limits` defaults to it. `limits.rs` untouched.

## Literal-site grep (step 1)

`git grep -nE '\b(JournalEvent|StartRun|EngineConfig) \{' -- '*.rs'` at the final base: 239 matching lines in 86 files (most already used `..`, or are type/impl headers). The compiler (`cargo check --workspace --all-targets --keep-going`, plus `cargo clippy -p <crate> --features failpoints --all-targets` for every FAILPOINT crate) located the literals that became non-exhaustive: **65 literals in 44 files**, each fixed by appending `..Default::default()` (pure additions, no other line changed):

crates/ostro-crashtest/{src/bin/crash-target.rs, tests/apply_throughput.rs}; crates/ostro-engine/{examples/record_corpus.rs, tests/replay_corpus.rs}; crates/ostro-partition/examples/gen_format_corpus.rs (2); crates/ostro-partition/src/apply/{inputs.rs (4), run.rs}; crates/ostro-partition/tests/{apply_contract_p5, apply_core, apply_effects, apply_lifecycle, apply_phase4 (2), apply_ready_attempts, apply_rearm, apply_retention, apply_run_started (2), apply_seams (2), apply_timer_input, apply_waits, blobrefs (3), children_apply, children_chain, children_common/mod, common/gen_effects, common/gen_lifecycle, common/gen_pause, common/gen_waits, dispatcher, dispatcher_failpoints, fencing, pause, rebuild_basic, rebuild_digest (2), snapshot, timers}.rs; crates/ostro-server/src/{control/lifecycle.rs, control/runs.rs (2), control/schedules.rs, derive.rs, sink.rs}; crates/ostro-server/tests/{children_derive.rs, children_fx/mod.rs, common/p7_workers.rs, sink.rs (3)}; crates/ostro-types/tests/command.rs (8).

No EngineConfig literal broke (all already used `..Default::default()`).

## Deviations from Plan

### Recorded (allowed) deviations

**1. Literal fix-ups in files outside `files_modified`** (mechanical only): `crates/ostro-server/src/control/schedules.rs`, `crates/ostro-server/tests/{children_derive.rs, children_fx/mod.rs, common/p7_workers.rs}`, `crates/ostro-partition/tests/dispatcher_failpoints.rs` (failpoints-gated). Files listed in the plan that needed no change: `ostro-server/src/config.rs`, `ostro-server/tests/{common/mod, derive_phase5, executor, executor_phase5, resume_rearm}.rs`, `ostro-crashtest/tests/crash_loop.rs`, `ostro-engine/src/engine.rs`, the other listed engine tests, `ostro-types/src/journal.rs`.

**2. Workflow per the executor brief** (overrides the plan's `/tmp/ostro-wt-11-02` + private index): scratch worktree detached at main, build slot `target/slot-2`, main moved by CAS `update-ref`. The worktree was created at main, so every edited file was clean at its base; it was rebased twice (onto 11-03/12-01 and onto 09-17/09-18) and `cargo check --workspace --all-targets` re-run on each rebased tree (no new literals appeared).

**3. Digest assertion in `p11_format.rs`.** The store digest includes the cursor/state_hash meta rows, which every command moves; the test instead proves "nothing changed" with 0 overlay writes, 0 reads, unchanged run row and the expected `fold_hash` step.

## Tests

- `cargo test -p ostro-types --test command`: 51 passed (new: `phase11_vectors_round_trip`, `phase11_tags_from_first_principles`, `phase11_attempt_defaults_and_is_omitted_at_zero`, `phase11_dlq_update_run_id_and_limits`, `phase11_golden_lines_follow_phase7`, `old_bytes_decode_unchanged_after_phase11`).
- Golden file: 5 lines appended, 0 removed (`git diff -U0` has no `-` lines).
- `cargo test -p ostro-partition --test format_corpus`: 2 passed (v0.1 corpus rebuilds unchanged); `--test rebuild_digest` green; `--test p11_format`: 2 passed; `--lib` 39 passed (outcome order test extended to 29/30).
- `cargo test -p ostro-engine --lib run_limits`: 1 passed.
- Full `cargo test -p ostro-types -p ostro-partition -p ostro-engine -p ostro-server -p ostro-crashtest --locked --no-fail-fast`: exit 0, 150 test binaries `ok`, 1347 tests passed, 0 failed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. `cargo clippy -p <c> --features failpoints --all-targets -- -D warnings` for ostro-partition, ostro-crashtest, ostro-engine, ostro-server, ostro, ostro-worker, ostro-meta, ostro-backup, ostro-bridge, ostro-log, ostro-blob: clean. `cargo fmt --check`: clean.
- Not run: full `bash scripts/ci-local.sh` (5 agents share the machine; the 11-01 baseline run took 2h22m). The workspace-wide check/clippy and the affected-crate test suites cover this change.

## Self-Check: PASSED
- Created files exist (run_limits.rs, dlq.rs, index.rs, rerun.rs, p11_format.rs); feat commit present on main.
