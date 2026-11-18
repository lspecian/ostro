---
phase: 01-foundation-vendoring-spikes
plan: 06
subsystem: infra
tags: [vendoring, wal, group-commit, metrics, chronik, ostro-log, provenance]

requires:
  - phase: 01-04
    provides: "Vendored-crate manifest pattern, VENDORED.md with raft rows and a vendor/ostro-wal placeholder"
provides:
  - "vendor/ostro-wal: trimmed Chronik WAL (GroupCommitWal, WalManager, segment index, suffix truncation), metrics facade, no io_uring/bincode/chrono/env reads"
  - "ostro-log: re-exports ostro_wal; append/read_tail smoke test"
  - "VENDORED.md: complete (raft + WAL rows, full excluded list, canonical_data gap answered)"
  - "scripts/check-vendored.sh: CI provenance / isolation check (5 checks)"
affects: [01-07, phase-02]

tech-stack:
  added: [ostro-wal 0.1.0 (vendored), dashmap 6, byteorder 1.5, libc 0.2, metrics 0.24.6 (first consumer)]
  patterns:
    - "Library code reads no env vars: upstream CHRONIK_* knobs became GroupCommitConfig fields/constants"
    - "Vendored integration-test crates also need #![allow(clippy::all)] (each tests/*.rs is its own crate)"
    - "check-vendored.sh ignores comment lines when looking for upstream-repo references, so manifest provenance comments stay allowed"

key-files:
  created:
    - vendor/ostro-wal/Cargo.toml
    - vendor/ostro-wal/src/lib.rs
    - vendor/ostro-wal/src/group_commit.rs
    - vendor/ostro-wal/src/manager.rs
    - vendor/ostro-wal/tests/suffix_truncation.rs
    - vendor/ostro-wal/tests/idle_seal_rotation.rs
    - crates/ostro-log/tests/wal_smoke.rs
    - scripts/check-vendored.sh
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-log/Cargo.toml
    - crates/ostro-log/src/lib.rs
    - VENDORED.md

key-decisions:
  - "CHRONIK_WAL_TAIL_CACHE_BYTES became a new GroupCommitConfig::tail_cache_bytes field (default 2 MiB). CHRONIK_WAL_PROFILE is gone (Default = low_resource()), and CHRONIK_WAL_ROTATION_SIZE became the DEFAULT_ROTATION_SIZE_BYTES constant (250 MB)"
  - "is_raft_topic keeps its backpressure exemption, with the reserved prefix changed from __chronik_metadata to __ostro_ (the __raft prefix is unchanged)"
  - "GroupCommitConfig::use_io_uring, config::AsyncIoConfig and CompressionType::Zstd (behind the never-enabled wal-compression feature) were removed, so no io_uring code or knobs remain"
  - "io_priority::set_wal_priority() is kept (it uses Linux ioprio_set via libc and is a no-op elsewhere)"
  - "The rand dev-dependency is dropped because no vendored test uses it"

patterns-established:
  - "scripts/check-vendored.sh is the provenance gate. CI can run it as-is (it needs jq and cargo)"

requirements-completed: [FOUND-01, FOUND-02, FOUND-03]

duration: 8min
completed: 2026-11-18
---

# Phase 1 Plan 06: Vendor WAL, Wire ostro-log, check-vendored.sh Summary

**Vendored the Chronik group-commit WAL as `ostro-wal` (edition 2021) from chronik-stream@08715f4.**
- Metrics now go through the `metrics` facade (`ostro_wal_batches_total`, `ostro_wal_batch_records`).
- All 26 io_uring cfg sites are gone, and so are bincode, chrono and every `CHRONIK_*` env read.
- `ostro-log` is wired to the new crate with a round-trip smoke test.
- `scripts/check-vendored.sh` enforces provenance and keeps Chronik references out of the build.

## Performance

- **Duration:** about 8 min
- **Completed:** 2026-11-18T17:40Z
- **Tasks:** 2 (Task 2 was TDD, so it has RED, GREEN and docs commits)
- **Files modified:** 21

## Accomplishments
- ostro-wal tests: 73 unit, 17 integration (3 idle_seal_rotation + 14 suffix_truncation) and 1 doc test all pass.
- The ostro-log test `wal_append_then_read_tail` passes. It appends a V2 record with acks=1, then reads it back through `read_tail` and compares offsets, topic/partition and payload.
- `cargo build/test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all --check` and `cargo deny check` (advisories, bans, licences and sources) all pass.
- `cargo tree -e normal` has no chronik, tokio-uring or prometheus. The workspace has 15 packages: 11 ostro-* crates plus ostro-raft-proto, ostro-raft-rs, ostro-raft-driver and ostro-wal.
- VENDORED.md has a row for every file under vendor/, raft and WAL alike. buffer_pool.rs, segment_index.rs and truncate.rs are marked unchanged because they are byte-identical to upstream (checked with `cmp`).
- check-vendored.sh passes on the clean tree. Negative tests caught each kind of failure: an unlisted vendored file (check 1), a tokio_uring reference (check 2), a non-comment upstream reference in a manifest (check 4) and a `[workspace]` table (check 5).

## Task Commits

1. **Task 1: Vendor and modernise ostro-wal** - `50f0f5d` (feat)
2. **Task 2: Wire ostro-log, VENDORED.md, check-vendored.sh** (TDD)
   - RED: `6153287` (test)
   - GREEN: `f558fc2` (feat)
   - Docs and script: `de05950` (docs)

## Files Created/Modified
- `vendor/ostro-wal/*`: 10 src files (3 unchanged, 7 modified or rewritten), 2 integration tests, a rewritten manifest and rustfmt.toml
- `Cargo.toml`: workspace dependency `ostro-wal = { path = "vendor/ostro-wal" }`
- `crates/ostro-log/*`: depends on ostro-wal and tokio (dev-dependency tempfile); `pub use ostro_wal;`; `tests/wal_smoke.rs`
- `VENDORED.md`: `## vendor/ostro-wal` table, the excluded chronik-wal files, a metrics note and the answered research gap
- `scripts/check-vendored.sh`: the 5-check provenance gate

## Decisions Made
See key-decisions in the frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Workspace clippy failed on the vendored integration tests**
- **Found during:** Task 1
- **Issue:** `clippy::field_reassign_with_default` fired in tests/idle_seal_rotation.rs. Each integration test is its own crate, so the crate-level allow in lib.rs does not reach it.
- **Fix:** Added `#![allow(clippy::all)]` to both vendored test files (recorded in VENDORED.md).
- **Commit:** 50f0f5d

**2. [Rule 3 - Blocking] The literal grep gate matched comments and config**
- **Found during:** Task 1
- **Issue:** The Task 1 grep (`io_uring|bincode|MetricsRecorder|...` over src/) matched three things: config.rs `AsyncIoConfig::use_io_uring`, a comment in record.rs about bincode, and my own provenance comments.
- **Fix:** Removed `AsyncIoConfig` (the plan allowed this as optional). Reworded the record.rs comment, which moved record.rs from "copy" to "modified, comment only". Reworded the Ostro comments.
- **Commit:** 50f0f5d

**3. [Rule 1 - Bug] Unexpected `cfg(feature = "wal-compression")` warning**
- **Issue:** `CompressionType::Zstd` was gated on a feature that ostro-wal no longer declares.
- **Fix:** Removed the variant. Nothing referenced it.
- **Commit:** 50f0f5d

**4. Ordering:** the `// Vendored from ...; modified by Ostro` headers on the WAL `.rs` files went in with Task 1 (commit 50f0f5d), not Task 2, because they were added while editing.

**5. The `rand` dev-dependency was dropped** (the plan listed rand 0.9). No vendored test uses it.

**Total deviations:** 3 auto-fixed, plus 2 minor ones. Everything is recorded in VENDORED.md. There was no scope creep.

## Issues Encountered
- As in 01-04, `cargo build | grep -c '^warning'` cannot be 0, because cargo still prints workspace-level "unused workspace dependency" warnings (metrics, thiserror, tracing, prost-build, raft, raft-proto). The ostro-wal warning cleared once ostro-log consumed it. The vendored crate itself builds with zero warnings.
- check-vendored.sh check 4 skips comment lines. The vendored manifests and the raft-proto build.rs carry `chronik-stream@08715f4` provenance comments, which the plan explicitly allows.

## User Setup Required
None.

## Next Phase Readiness
- Phase 2 (storage) can build the CRC-verified partition journal on `ostro_log::ostro_wal::GroupCommitWal`. `WalRecord::verify_checksum` is a no-op for V2 records, so the Ostro envelope must carry its own CRC32C.
- Plan 01-07 (CI) can add a `bash scripts/check-vendored.sh` step. It needs jq on the runner.

---
*Phase: 01-foundation-vendoring-spikes*
*Completed: 2026-11-18*

## Self-Check: PASSED

Key files exist; commits 50f0f5d, 6153287, f558fc2, de05950 found in git log.
