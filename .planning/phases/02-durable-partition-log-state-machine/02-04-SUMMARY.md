---
phase: 02-durable-partition-log-state-machine
plan: 04
subsystem: storage
tags: [blob, sha256, fsync, failpoints, content-addressed]
requires:
  - phase: 02-01
    provides: "ostro-blob crate wiring (sha2, thiserror, tracing, optional fail)"
  - phase: 02-02
    provides: "FAILPOINT_CRATES suite in scripts/ci-local.sh + CI failpoint step"
provides:
  - "ostro_blob::{BlobStore {open, put, get, contains, path_of}, BlobRef, BlobError, hex}"
  - "fail points blob::fsync (EIO) and blob::before_rename"
affects: [02-11]
tech-stack:
  added: []
  patterns:
    - "Durable publish: tmp/ write + sync_all, rename into blobs/<hh>/<hex>, fsync shard dir; created dirs fsync their parent"
    - "Visible file == complete file; idempotent put short-circuits on existence"
key-files:
  created:
    - crates/ostro-blob/tests/blob_store.rs
    - crates/ostro-blob/tests/blob_failpoints.rs
  modified:
    - crates/ostro-blob/src/lib.rs
    - crates/ostro-blob/Cargo.toml
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
key-decisions:
  - "A failed put leaves its tmp file in place (BlobStore::open removes it on restart); put never deletes on error"
  - "contains() checks visibility only (is_file); integrity is verified by get() (len then sha256)"
requirements-completed: []
duration: 8min
completed: 2026-11-18
---

# Phase 2 Plan 04: Content-Addressed Blob Store Summary

`ostro-blob` now holds a durable sha256-addressed local blob store. `put` writes to `tmp/`, calls fsync, renames into `blobs/<hh>/<hex>` and then fsyncs the shard dir. Calling `put` again with the same bytes does nothing. `get` checks the length and the sha256 before returning bytes. Failpoint tests show that a crash before the rename never leaves a visible blob.

## What was built

- **`BlobStore::open(data_dir)`:** creates `blobs/` and `tmp/` and fsyncs the parent dir when it creates one. It deletes leftover `tmp/*` entries, fsyncs `tmp/` and logs how many it removed.
- **`put(bytes)`:**
  - Computes `Sha256::digest` and returns at once if the final path already exists.
  - Otherwise writes `tmp/<hex>.<pid>.<counter>`, calls `write_all` then `sync_all`, creates the shard dir if needed (fsyncing `blobs/` when it does), renames the file into place and fsyncs the shard dir.
  - Every io error becomes `BlobError::Io`.
- **`get(r)`:** returns `NotFound` if the file is missing. Otherwise it checks `LenMismatch` first, then `ShaMismatch` (which carries the actual hex).
- **`contains`, `path_of` and `hex`:** lowercase, 64 characters.
- **Fail points:**
  - `blob::fsync` sits before the tmp `sync_all` and returns `EIO` (raw os error 5).
  - `blob::before_rename` sits between the tmp fsync and the rename.
  - The `failpoints` feature is now `["dep:fail", "fail/failpoints"]`, and `[[test]] blob_failpoints` requires it.
- **CI:** `ostro-blob` is appended to `FAILPOINT_CRATES` in `scripts/ci-local.sh` and to the "Failpoint suites" step in `.github/workflows/ci.yml`.
- The `fail` placeholder anchor was removed. The `sha2`, `thiserror` and `tracing` anchors were dropped because the crate now uses them. The `#[cfg(test)] tempfile` anchor is kept for the lib unit-test target.

## Verification

- `cargo test -p ostro-blob`: 8/8 tests in `blob_store` pass. They cover the 300 KiB put/layout check, idempotence (one file, mtime unchanged), the round trip including an empty blob, a flipped byte giving `ShaMismatch`, truncation giving `LenMismatch`, a missing blob giving `NotFound`, open cleaning up tmp files, and hex.
- `cargo test -p ostro-blob --features failpoints -- --test-threads=1`: 8 + 2 pass.
- `cargo clippy -p ostro-blob --all-targets -D warnings` is clean both with and without `--features failpoints`. `cargo fmt --check` is clean.
- `cargo build --workspace` produces no warnings or errors right now.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical] CI workflow failpoint step kept in sync**
- **Found during:** Task 2
- **Issue:** `.github/workflows/ci.yml` says the step must be kept in sync with `FAILPOINT_CRATES`. The plan only listed `scripts/ci-local.sh`.
- **Fix:** Added `cargo test -p ostro-blob --features failpoints --locked -- --test-threads=1` to the step. The YAML parses.
- **Commit:** 52d62a9

## Requirements

STOR-14 is **not** marked complete. This plan delivers the blob store. Referencing blobs from the journal (`PayloadRef` and `prepare_payload`) is plan 02-11.

## Commits

- ce6968c test(02-04): add failing tests for BlobStore put/get
- 8100701 feat(02-04): implement durable content-addressed BlobStore
- 52d62a9 test(02-04): blob crash-window failpoints and CI registration

## Self-Check: PASSED

The created files exist, and commits ce6968c, 8100701 and 52d62a9 are present.
