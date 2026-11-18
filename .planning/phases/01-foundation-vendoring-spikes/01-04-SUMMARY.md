---
phase: 01-foundation-vendoring-spikes
plan: 04
subsystem: infra
tags: [vendoring, raft, raft-rs, prost, protox, chronik, ostro-meta]

requires:
  - phase: 01-01
    provides: "Virtual workspace, workspace.dependencies pins, cargo-deny policy"
provides:
  - "vendor/ostro-raft-proto: eraftpb on prost 0.14.6, codegen via protox (no protoc)"
  - "vendor/ostro-raft-rs: tikv raft-rs v0.7.0 (Chronik-modified), lib name raft, modernised deps"
  - "vendor/ostro-raft-driver: Chronik RaftDriver + durable RaftLogStore, no Chronik crates"
  - "ostro-meta: re-exports the driver API plus NullOutbox; single-voter smoke test"
  - "VENDORED.md: raft-side provenance rows, excluded list, vendor/ostro-wal placeholder"
affects: [01-06, 01-07, phase-04]

tech-stack:
  added: [ostro-raft-rs 0.7.0 (vendored), ostro-raft-proto 0.7.0 (vendored), ostro-raft-driver 0.1.0 (vendored), rustc-hash 2, rand 0.9, getset 0.1, slog 2, slog-stdlog 4, crc32fast 1.5, lazy_static 1]
  patterns:
    - "Vendored crates: renamed package plus original [lib] name, path deps, no [patch], no [workspace], edition 2021"
    - "Vendored manifests carry an empty [lints.rust] table (silences cargo 1.100's missing-[lints] warning without inheriting workspace lints)"
    - "Vendored crate roots carry #![allow(clippy::all)], so workspace clippy -D warnings stays green"
    - "Ostro modification header goes below the original licence header"

key-files:
  created:
    - vendor/ostro-raft-proto/build.rs
    - vendor/ostro-raft-proto/Cargo.toml
    - vendor/ostro-raft-rs/Cargo.toml
    - vendor/ostro-raft-driver/Cargo.toml
    - vendor/ostro-raft-driver/src/lib.rs
    - vendor/ostro-raft-driver/src/driver.rs
    - vendor/ostro-raft-driver/src/log_store.rs
    - crates/ostro-meta/tests/single_voter.rs
    - VENDORED.md
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-meta/Cargo.toml
    - crates/ostro-meta/src/lib.rs

key-decisions:
  - "The failpoints cfg blocks in raft.rs were deleted rather than declaring an empty `failpoints` feature. An empty feature would break compilation if enabled, because `fail` is gone"
  - "Vendored crate roots use #![allow(clippy::all)]. Upstream raft-rs had #![deny(clippy::all)], and neither command-line -D warnings nor manifest [lints.clippy] allows nor --exclude stop clippy from failing on path-dep workspace members"
  - "Test-only quorum helpers (describe, new_joint_from_majorities), orphaned by the removed datadriven tests, get #[allow(dead_code)] instead of being deleted"
  - "On restart the driver replays its durable log into a fresh state machine. The smoke test asserts count 2 after restart"

patterns-established:
  - "Vendored crate manifest template: explicit metadata, publish = false, empty [lints.rust], rustfmt.toml disable_all_formatting"
  - "VENDORED.md rows use bare paths (`| vendor/... |`) so scripts can grep them"

requirements-completed: [FOUND-01]
requirements-partial: [FOUND-02, FOUND-03]  # raft half done; WAL half (metrics facade, tokio-uring removal, ostro-wal) lands in 01-06

duration: 8min
completed: 2026-11-18
---

# Phase 1 Plan 04: Vendor Raft Stack and Wire ostro-meta Summary

**Vendored three crates from chronik-stream@08715f4 as workspace members, with no `[patch]` and no Chronik crates:**
- **ostro-raft-proto:** tikv raft-proto, now on prost 0.14.6 with protox codegen
- **ostro-raft-rs:** tikv raft-rs v0.7.0, moved to rustc-hash, thiserror 2, rand 0.9 and slog-stdlog
- **ostro-raft-driver:** Chronik's RaftDriver and durable RaftLogStore

**ostro-meta now runs a single-voter raft group in a test. It elects itself leader, applies a proposal, and replays its log after a restart.**

## Performance

- **Duration:** about 8 min
- **Started:** 2026-11-18T17:23Z
- **Completed:** 2026-11-18T17:31Z
- **Tasks:** 3 (Task 2 was TDD, so it has two commits)
- **Files modified:** 41

## Accomplishments
- Tests pass: raft has 45 unit tests and 14 doc tests, the driver has 26 tests (log_store plus driver), and the ostro-meta smoke test passes.
- `cargo build --workspace`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check` all pass.
- `cargo deny check` passes: advisories, bans, licences and sources are all ok.
- The dependency graph has no git sources, no `[patch]` and no `[workspace]` tables in the vendored manifests.
- The raft dependency tree has no fxhash, slog-term, chrono or bincode. The driver tree has no chronik, tonic, bincode or chrono.
- Every file listed as unchanged in VENDORED.md was checked byte-for-byte against upstream with `cmp`.
- VENDORED.md has a row for each of the 35 raft-side files under vendor/, the excluded-file list, and a `## vendor/ostro-wal` placeholder for plan 01-06.

## Task Commits

1. **Task 1: Vendor and modernise ostro-raft-proto and ostro-raft-rs** - `2c58b50` (feat)
2. **Task 2: Vendor ostro-raft-driver, wire ostro-meta** (TDD)
   - RED: `a01005d` (test)
   - GREEN: `6f5ebad` (feat)
3. **Task 3: VENDORED.md and modification headers** - `6016c54` (docs)

## Files Created/Modified
- `vendor/ostro-raft-proto/*`: unchanged proto and src. The manifest and build.rs were rewritten to use protox.
- `vendor/ostro-raft-rs/*`: 21 src files, 6 of them modified. The manifest was rewritten.
- `vendor/ostro-raft-driver/*`: driver.rs (`is_empty_snap` fix) and an unchanged log_store.rs. lib.rs and the manifest were rewritten.
- `vendor/*/rustfmt.toml`: `disable_all_formatting = true`
- `Cargo.toml`: `members = ["crates/*", "vendor/*"]`, plus the workspace deps `raft`, `raft-proto` and `ostro-raft-driver`
- `crates/ostro-meta/src/lib.rs`: re-exports the driver API and adds `NullOutbox`
- `crates/ostro-meta/tests/single_voter.rs`: `single_voter_applies_proposal`
- `VENDORED.md`: the provenance manifest

## Decisions Made
See key-decisions in the frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Workspace clippy broken by vendored code**
- **Found during:** Task 2
- **Issue:** Upstream raft-rs has `#![deny(clippy::all)]`, and the driver trips several newer clippy lints. That made `cargo clippy --workspace -- -D warnings` fail, even though plan 01-01 had left it green. Three fixes did not work: `--exclude`, manifest `[lints.clippy] all = "allow"`, and a command-line `-D warnings` override.
- **Fix:** Added a crate-level `#![allow(clippy::all)]` in `vendor/ostro-raft-rs/src/lib.rs` (replacing the `deny`) and in `vendor/ostro-raft-driver/src/lib.rs`. Both are recorded in VENDORED.md.
- **Commit:** 6f5ebad

**2. [Rule 3 - Blocking] Cargo 1.100 "missing `[lints]` to inherit `[workspace.lints]`" warning**
- **Found during:** Task 1
- **Fix:** Added an empty `[lints.rust]` table to each vendored manifest. `workspace = false` is rejected by cargo.
- **Commit:** 2c58b50

**3. [Rule 1 - Bug] Dead-code warnings in raft lib tests**
- **Issue:** Removing the datadriven tests orphaned three `#[cfg(test)]` helpers in quorum/joint.rs and quorum/majority.rs.
- **Fix:** Added `#[allow(dead_code)]` with an Ostro comment.
- **Commit:** 2c58b50

**Total deviations:** 3 auto-fixed. All are small and recorded in VENDORED.md. There was no scope creep.

## Issues Encountered
- The plan's Task 1 check `cargo build -p ostro-raft-rs | grep -c '^warning' == 0` cannot literally pass. Cargo prints workspace-level `unused workspace dependency` warnings for metrics, thiserror, tracing and prost-build (accepted in 01-01), and now also raft and raft-proto. No crate consumes those two directly, because ostro-meta reaches raft through `ostro_raft_driver::raft`. The vendored crates themselves build with zero warnings.
- The single-voter smoke test takes about 6 s, because of two elections with the default 100 ms tick × 20 election ticks.

- FOUND-02 and FOUND-03 were left Pending in REQUIREMENTS.md. Both also cover the WAL (WAL vendoring, `metrics` facade, tokio-uring removal), which plan 01-06 delivers. The tooling marked them complete, and that change was reverted.

## User Setup Required
None.

## Next Phase Readiness
- Plan 01-06 can append WAL rows under `## vendor/ostro-wal` in VENDORED.md and reuse the vendored-manifest pattern (empty `[lints.rust]`, rustfmt.toml, and `#![allow(clippy::all)]` if needed).
- Phase 4 (META-01) supplies a real `Outbox` in place of `NullOutbox`.

---
*Phase: 01-foundation-vendoring-spikes*
*Completed: 2026-11-18*

## Self-Check: PASSED

Key files exist; commits 2c58b50, a01005d, 6f5ebad, 6016c54 found in git log.
