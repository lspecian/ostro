---
phase: 02-durable-partition-log-state-machine
plan: 01
subsystem: storage
tags: [fjall, cargo-deny, nextest, testkit, fault-injection, clippy]
requires: []
provides:
  - "fjall =3.1.13 workspace pin (redb removed from the graph)"
  - "every Phase 2 workspace dependency pinned once and wired into its consuming crate"
  - "ostro-partition fjall smoke (open_smoke) + determinism clippy.toml"
  - "ostro-testkit: corrupt::{flip_bit, zero_range, zero_sector, truncate_at, append_garbage}, fs::{tempdir, file_len, list_files_sorted}, proptest_cases"
  - ".config/nextest.toml [profile.crash]"
affects: [02-03, 02-04, 02-05, 02-06, 02-07, 02-08, phase-3-ostro-types]
tech-stack:
  added: [fjall 3.1.13 (lsm-tree 3.1.10, lz4_flex 0.13.2, byteview, flume, dashmap 6, quick_cache, sfa, varint-rs), bytes, crc32c 0.6.9, blake3 1.8.7 (pure), xxhash-rust 0.8.21, uuid 1 (v7), fail 0.5.1, proptest-state-machine 0.9.0]
  patterns:
    - "`use <dep> as _;` anchors (incl. #[cfg(test)] dev-dep anchors) keep the per-crate unused_dependencies lint quiet until the consuming plan lands"
    - "Offline corruption on closed files (kill -9 never tears writes)"
key-files:
  created:
    - crates/ostro-partition/clippy.toml
    - crates/ostro-testkit/Cargo.toml
    - crates/ostro-testkit/src/lib.rs
    - crates/ostro-testkit/src/fs.rs
    - crates/ostro-testkit/src/corrupt.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - deny.toml
    - .config/nextest.toml
    - crates/ostro-types/Cargo.toml
    - crates/ostro-types/src/lib.rs
    - crates/ostro-log/Cargo.toml
    - crates/ostro-log/src/lib.rs
    - crates/ostro-partition/Cargo.toml
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-blob/Cargo.toml
    - crates/ostro-blob/src/lib.rs
decisions:
  - "deny.toml licence allow list gains 0BSD (varint-rs via lsm-tree) and BSL-1.0 (xxhash-rust: fjall, lsm-tree, sfa, ostro-types)"
  - "blake3 keeps features [std, pure] (blake3 1.8.7 has a `std` feature, default-on)"
  - "Phase 2 crates anchor not-yet-used deps with `use x as _;` (same pattern as Phase 3); consuming plans may drop the anchors"
metrics:
  duration: 10min
  completed: 2026-11-18
---

# Phase 2 Plan 01: Wave 0 Infrastructure Summary

fjall 3.1.13 now replaces redb as the partition state store. All Phase 2 workspace pins are declared once and consumed. cargo-deny passes with fjall's dependency tree. The new `ostro-testkit` crate supplies tested offline corruption helpers, and nextest has a `crash` profile.

## What was built

- **Root `Cargo.toml`:**
  - Removed `redb`.
  - Added `fjall = "=3.1.13"`, `bytes`, `crc32c 0.6.9`, `blake3 1.8.7` (no default features, `std` + `pure`), `xxhash-rust 0.8.21` (xxh3), `uuid` (v7), `fail 0.5.1`, `proptest-state-machine 0.9.0` and `ostro-testkit` (path).
  - Reused Phase 3's existing serde, postcard, sha2 0.11, thiserror, tracing, metrics and proptest pins.
- **Crate wiring:**
  - `ostro-types`: bytes, prost, crc32c, xxhash-rust, uuid, thiserror.
  - `ostro-log`: ostro-types, bytes, thiserror, tracing, metrics. Dev: proptest, ostro-testkit.
  - `ostro-partition`: fjall, ostro-types, ostro-log, ostro-blob, serde, postcard, blake3, bytes, tokio, thiserror, tracing, metrics, optional fail. Feature `failpoints = ["dep:fail"]`. Dev: tempfile, proptest, proptest-state-machine, ostro-testkit.
  - `ostro-blob`: sha2, thiserror, tracing, optional fail. Feature `failpoints`. Dev: tempfile.
- **`ostro-partition/src/lib.rs`:** a fjall skeleton. `open_smoke(path)` sets 1 worker and a 64 MiB journal. The test writes one batch across the `meta` and `runs` keyspaces, calls `persist(SyncAll)`, reopens the database and reads both keys back.
- **`ostro-partition/clippy.toml`:** disallows `SystemTime::now`, `Instant::now`, `HashMap` and `HashSet`. Clippy loads it without unresolved-path warnings.
- **`ostro-testkit`:**
  - `corrupt::{flip_bit, zero_range, zero_sector, truncate_at, append_garbage}`. Each one opens the file read+write and calls `sync_all`. Out-of-range arguments return `InvalidInput`.
  - `fs::{tempdir, file_len, list_files_sorted}`. `tempdir` asserts on Linux that the path is not under `/mnt/`.
  - `proptest_cases(default)`.
  - 9 unit tests.
- **`.config/nextest.toml`:** appended `[profile.crash]` (120 s × 10, 1 thread, no retries). `[profile.default]` and `[profile.ci]` are unchanged. cargo-nextest 0.9.152 was already installed.

## fjall dependency tree (now in the cargo-deny/licence check)

```
├── fjall v3.1.13
│   ├── byteview v0.10.2
│   ├── dashmap v6.2.1
│   ├── flume v0.12.0
│   ├── lsm-tree v3.1.10
│   │   ├── byteview v0.10.2
│   │   ├── enum_dispatch v0.3.13 (proc-macro)
│   │   ├── interval-heap v0.0.5
│   │   │   └── compare v0.0.6
│   │   ├── lz4_flex v0.13.2
│   │   ├── quick_cache v0.6.24
│   │   ├── self_cell v1.3.0
│   │   ├── sfa v1.0.0
│   │   │   └── xxhash-rust v0.8.21
│   │   ├── tempfile v3.27.0
│   │   ├── varint-rs v2.2.1
│   │   └── xxhash-rust v0.8.21
│   ├── lz4_flex v0.13.2 (*)
│   ├── tempfile v3.27.0 (*)
│   └── xxhash-rust v0.8.21
```

**Licences added to `deny.toml` `[licenses] allow`:**
- `0BSD`: varint-rs, via lsm-tree.
- `BSL-1.0`: xxhash-rust.

No ban or skip entries were added. Duplicate versions remain warnings only.

## Verification

- `cargo build --workspace --locked`: my crates build with no warnings. The workspace-level warnings come from Phase 3's in-progress `ostro-engine` code (replay dead code, deprecated `fetch_update`).
- `cargo test -p ostro-partition` passes (fjall smoke, 1 test). `cargo test` and `cargo nextest run -p ostro-testkit` both pass 9/9.
- `cargo clippy -D warnings` is clean for ostro-log, ostro-partition, ostro-blob and ostro-testkit (all targets), for ostro-partition and ostro-blob with `--features failpoints`, and for the ostro-types lib.
- `cargo deny check` exits 0. `cargo tree --workspace -e normal | grep -c redb` = 0. `grep -c '^redb' Cargo.toml` = 0.
- `scripts/ci-local.sh --no-musl` was run in a clean worktree at f423834. It passes toolchain, warning-free build, failpoint suites, deny, check-vendored, deny fixtures, forbidden crates and workflow validation. It fails fmt, clippy and test, all because of Phase 3 code in `ostro-engine`: `cache.rs` fmt diffs and the RED test `tests/cache.rs` importing `AotCache`, which is not implemented yet. In the same worktree, `cargo test`, `cargo clippy` and `cargo fmt` over the workspace minus ostro-engine are all green.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Per-crate `unused_dependencies` lint**
- **Found during:** Task 1
- **Issue:** The plan assumed unused *crate* deps do not warn. On this toolchain they do (the same finding as in 03-01).
- **Fix:** Added `use x as _;` anchors in the ostro-types, ostro-log, ostro-partition and ostro-blob lib.rs files. Dev-deps get anchors inside `#[cfg(test)]` and `fail` gets one behind `#[cfg(feature = "failpoints")]`.
- **Commits:** 26df26d, 12e6f57

**2. [Rule 3 - Blocking] `ostro-testkit` pin moved to Task 2**
- **Issue:** The path dep would point at a crate that doesn't exist yet. If declared without consumers, it would trigger the unused workspace dependency warning.
- **Fix:** The root pin and the ostro-log/ostro-partition dev-dep wiring for ostro-testkit were added in Task 2's GREEN commit, together with the crate.

**3. [Rule 3] deny.toml licences:** added 0BSD and BSL-1.0, as the plan anticipated.

### Concurrency notes
- Phase 3 executors (03-02..03-06) had uncommitted work and RED commits in `ostro-engine` and `ostro-types/tests` while this plan ran. I didn't touch any of it. I scoped verification to my crates and used a clean worktree.
- `ostro-log` uses `ostro-types.workspace = true` (the root pin added by 03-01) instead of a relative path dep. Both resolve to the same crate.

## Known Stubs

- `ostro_partition::open_smoke` is a skeleton. Plan 02-05 replaces it with the real store.
- `ostro-blob` is still a placeholder that only has dependency anchors.

## Commits

- 26df26d feat(02-01): swap redb for fjall 3.1.13, pin Phase 2 workspace deps
- adaaf19 test(02-01): add failing tests for ostro-testkit corruption and fs helpers
- 12e6f57 feat(02-01): implement ostro-testkit helpers and nextest crash profile

## Self-Check: PASSED

All key files exist; commits 26df26d, adaaf19, 12e6f57 present.
