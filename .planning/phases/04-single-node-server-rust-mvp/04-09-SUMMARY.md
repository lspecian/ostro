---
phase: 04-single-node-server-rust-mvp
plan: 09
subsystem: storage-contract
tags: [phase-gate, command-set, prost, golden, api-03, suspend]
requires:
  - phase: 02-15
    provides: rebuild-digest proptest + ostro-crashtest crash loops (Phase 2 work in ostro-partition/ostro-crashtest committed)
  - phase: 02-16
    provides: tail-only WAL recovery fsync, fjall measurements, docs/storage-format.md
  - phase: 02-06
    provides: hand-written prost command set + tests/golden/commands.txt
  - phase: 04-02
    provides: settled root manifest / Cargo.lock for the baseline ci-local run
provides:
  - Phase 4 contract-gap gate passed (baseline HEAD 8ffdc01, ci-local --no-musl green)
  - StartRun fields module_sha (14), tags (15), tenant (16), traceparent (17), run_timeout_ms (18)
  - derived::Intent::Suspend(Suspend {}) at Derived oneof tag 6; Derived::new / Derived::suspend
  - limit constants MODULE_SHA_LEN=32, MAX_RUN_TAGS=64, MAX_RUN_TAG_LEN=128, MAX_TENANT_LEN=128, MAX_TRACEPARENT_LEN=256
  - golden vectors start_run_v4 and events_appended_suspend appended to commands.txt
affects: [04-12, 04-13, 04-15, 04-16, 04-18, 05-01]
tech-stack:
  added: []
  patterns:
    - "Golden files: original vectors stay in golden_commands(); additive vectors go in golden_appended(); all_golden() = file order"
    - "StartRun literals outside ostro-types end with ..Default::default() so later additive fields don't break them"
key-files:
  created:
    - .planning/phases/04-single-node-server-rust-mvp/04-09-SUMMARY.md
  modified:
    - crates/ostro-types/src/command.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/golden/commands.txt
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/tests/apply_core.rs
    - crates/ostro-partition/tests/apply_cross.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-crashtest/src/bin/crash-target.rs
    - crates/ostro-crashtest/tests/apply_throughput.rs
    - crates/ostro-crashtest/tests/crash_loop.rs
key-decisions:
  - "Field limits live in ostro-types as pub consts (MODULE_SHA_LEN 32, MAX_RUN_TAGS 64, MAX_RUN_TAG_LEN 128, MAX_TENANT_LEN 128, MAX_TRACEPARENT_LEN 256). Only the maximum is fixed; empty = none. Apply enforces them in 04-12. traceparent is checked by length only, not W3C syntax"
  - "Suspend is an empty message (Copy, no fields); future fields are additive. Until 04-12, apply rejects it with Invalid(BadIntent) in check_intents"
  - "Cmd gets #[allow(clippy::large_enum_variant)]: StartRun is now >= 352 bytes. Boxing would change the variant type every submitter and apply match uses. Same trade-off as derived::Intent"
requirements-completed: []
duration: 45min
completed: 2026-11-19
---

# Phase 4 Plan 09: Phase-4 gate + StartRun 14..18 + Derived::Suspend Summary

**Phase 2 was confirmed complete at HEAD `8ffdc01`, with a green ci-local baseline. The partition command set gained StartRun fields 14..18 (module_sha, tags, tenant, traceparent, run_timeout_ms) and a `Suspend` intent at Derived oneof tag 6. These are additive changes: the 7 original golden vectors are byte-identical, and two new vectors were appended.**

## Performance

- **Duration:** about 45 min, of which about 14 min was the baseline ci-local run
- **Tasks:** 2 (gate check plus TDD command-set change)
- **Files:** 10 modified

## Task 1: Phase 2 completion gate

| Check | Result |
| ----- | ------ |
| `02-15-SUMMARY.md` and `02-16-SUMMARY.md` exist | PASS |
| `git status --porcelain crates/ostro-partition crates/ostro-log crates/ostro-types crates/ostro-crashtest docs scripts` empty | PASS (empty) |
| `docs/storage-format.md` exists | PASS |
| `bash scripts/ci-local.sh --no-musl` on the current tree | PASS (all steps passed) |

**Phase 4 contract-gap baseline:** `8ffdc0103878870ef8bc2f87236ad3dcec007ccf` (`8ffdc01 docs(05): list the 19 Phase 5 plans in ROADMAP`). While this plan ran, the parallel Phase 2 verifier committed `663c6b9 docs(02): mark Phase 2 complete (verification passed 5/5)`. It changed only planning docs.

Baseline ci-local step timings (`--no-musl`; the build was warm, so fmt/clippy/build took about 1 s each):

| Step | Result | Time |
| ---- | ------ | ---- |
| toolchain 1.100.0 | PASS | 0s |
| cargo fmt --check | PASS | 0s |
| cargo clippy -D warnings | PASS | 1s |
| cargo build (warning-free) | PASS | 0s |
| cargo test | PASS | 453s |
| failpoint suites | PASS | 376s |
| cargo deny check | PASS | 1s |
| check-vendored.sh | PASS | 1s |
| check-deny-fixtures.sh | PASS | 0s |
| no chronik/tokio-uring/prometheus | PASS | 1s |
| workflow validation | PASS | 0s |
| musl legs (3 steps) | SKIP (--no-musl) | - |

## Accomplishments

- `command.rs`:
  - New StartRun fields: `module_sha: Vec<u8>` (bytes 14), `tags: Vec<String>` (repeated string 15), `tenant: String` (16), `traceparent: String` (17) and `run_timeout_ms: u64` (18, 0 = none). Each has a doc comment.
  - New intent `derived::Intent::Suspend(Suspend {})` at tag 6, plus `Derived::new(intent)` and `Derived::suspend()`.
  - Limit constants, and a "Phase 4 additions" section in the module docs (the Suspend CAS semantics).
- Golden file: `start_run_v4` (every new field set) and `events_appended_suspend` (one event plus a `Suspend`) are appended. `git diff` of commands.txt against the baseline shows 2 added lines and no removed lines.
- New tests in `tests/command.rs`:
  - `appended_vectors_round_trip`
  - `old_start_run_bytes_decode_with_defaults`: the 02-06 StartRun line decodes with empty defaults, equals `sample_start(5)`, and re-encodes to the same bytes.
  - `old_events_appended_bytes_decode_unchanged`
  - `start_run_new_fields_bytes_from_first_principles`: keys 0x72 / 0x7a / 0x82 01 / 0x8a 01 / 0x90 01.
  - `suspend_intent_bytes_from_first_principles`: `32 00`.
  - `start_run_field_limits_are_documented_constants`
  - The forward-compatibility and mutation proptests now cover all 9 vectors.
- `apply/run.rs`: a temporary `Intent::Suspend(_) => Err(Invalid(BadIntent))` arm in `check_intents`, plus a no-op arm in the write loop (it is unreachable after validation).

## Task Commits

1. **Task 1: Phase 2 completion gate**: no commit (no files are modified; the findings are recorded above)
2. **Task 2 RED: failing tests for StartRun 14..18 and Derived::Suspend**: `13a8620` (test)
3. **Task 2 GREEN: StartRun fields 14..18 and Derived::Suspend intent**: `20575fc` (feat)

## Verification

- `cargo test -p ostro-types --locked`: 7 + 22 (command) + 11 + 17 + 10 tests, all green.
- `cargo test -p ostro-types -p ostro-partition -p ostro-crashtest --locked`: 180 passed, 0 failed. This includes rebuild_digest and the apply suites. Ignored crash loops and measurements were not run.
- `cargo build --workspace --locked`: 0 warnings. `cargo build --workspace --all-targets --locked`: clean.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. Failpoint clippy for ostro-blob, ostro-log, ostro-partition and ostro-crashtest: clean.
- `cargo fmt --all --check`: clean.
- `git diff 8ffdc01 -- crates/ostro-types/tests/golden/commands.txt | grep '^-[^-]'` returns nothing.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] StartRun struct literals outside ostro-types stopped compiling**
- **Found during:** Task 2 (`cargo build --workspace --all-targets`)
- **Issue:** Six exhaustive `StartRun { .. }` literals broke when the new fields were added: crash-target.rs, crash_loop.rs, apply_throughput.rs, apply_core.rs, apply_cross.rs and tests/common/mod.rs.
- **Fix:** Added `..Default::default()` to each literal. The encoded bytes are unchanged, because the new fields keep proto3 defaults and are not emitted.
- **Files modified:** the 6 files above
- **Commit:** 20575fc

**2. [Rule 3 - Blocking] clippy `large_enum_variant` on `Cmd`**
- **Found during:** Task 2 (workspace clippy -D warnings)
- **Issue:** StartRun grew to at least 352 bytes, against 96 for the next-largest variant.
- **Fix:** Added `#[allow(clippy::large_enum_variant)]` on `Cmd` with a justification comment, matching the existing allow on `derived::Intent`. Boxing would change the public variant type.
- **Commit:** 20575fc

**3. The temporary apply arm was required.** As the plan anticipated, both `match i` blocks in `apply/run.rs` are exhaustive. `check_intents` now rejects Suspend as `Invalid(BadIntent)`. No test pins this behaviour, so 04-12 can replace it freely.

**4. Constants added rather than updated.** command.rs had no limit constants, so the plan's "update the constants if command.rs defines limits" became "add documented constants". The values match 04-12's checks: tags 64 x 128 bytes, tenant 128, traceparent 256. module_sha must be 0 or 32 bytes. A minimum traceparent length (55) is not enforced, because empty means none.

## Notes for later plans

- **04-12:**
  - Replace the temporary Suspend arms in `apply/run.rs`: `check_intents` (rejects) and the write loop (no-op).
  - Use the `ostro_types::command::{MODULE_SHA_LEN, MAX_RUN_TAGS, MAX_RUN_TAG_LEN, MAX_TENANT_LEN, MAX_TRACEPARENT_LEN}` constants for `FieldTooLong` / module_sha length validation instead of re-declaring them.
  - docs/storage-format.md §4 was not touched here. The "Phase 4 additions (pre-freeze)" section still needs StartRun 14..18 and Derived 6 = Suspend.
  - In tests, build Suspend with `Derived::suspend()`. New StartRun fixtures can spread `..Default::default()` or `..common::start_run(&id)`.
  - The rebuild_digest generator can add the new fields without touching the existing generators.
- **Hot path:** `Cmd` is now larger in memory (StartRun at least 352 bytes inline). No apply-path reads were added, and encoded sizes are unchanged for commands that leave the new fields empty. If 04-12 or 05-15 re-measures RMW throughput (80.8k cmds/s at a 16 MiB cache), check whether the larger `Command` value has any effect. Boxing StartRun is a breaking API change, so leave it until it is shown to matter.
- **Golden test layout:** append future vectors to `golden_appended()` in tests/command.rs (file order). The `golden_encodings` test compares every line in order, and `OSTRO_BLESS=1` rewrites the whole file, so diff it for removed lines before committing. The blessing run itself fails once, because `include_str!` is stale; rerun without bless.
- **05-01:** plans StartRun 19..23 and Cmd 8..12. This plan used 14..18 and Derived tag 6 only, so the next free Derived oneof tag is 7.
- **API-03** stays Pending. The fields exist, but storage and validation (04-12) and the API (04-18) deliver the requirement.

## Self-Check: PASSED

- FOUND: crates/ostro-types/src/command.rs (contains `tag = "18"`, `Suspend`)
- FOUND: crates/ostro-types/tests/golden/commands.txt (start_run_v4, events_appended_suspend)
- FOUND: commit 13a8620
- FOUND: commit 20575fc
