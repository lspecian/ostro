---
phase: 08-triggers-bottomless-observability
plan: 19
subsystem: storage
tags: [blob-gc, bottomless, ledger, mark-sweep, grace, back-01]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-02 blob_refs_snapshot visitor; 08-08 BlobStore put observer; 08-09 snapshot service + .blobrefs sidecars + Node::local_snapshots; 08-13 uploader (blob_key, manifests, LOCK)
provides:
  - "ostro_blob::gc: GcLock, BlobStore::sweep(mark, older_than) -> SweepReport, BlobStore::list, parse_hex32; put takes the GC read lock and touches the mtime of an existing blob; failpoint blobgc::before_unlink"
  - "ostro_backup::remote_gc: Ledger (open/record/due/run/adopt_orphans/compact), ReputGuard (on_put/on_lagged), retention_window, LEDGER_PATH"
  - "ostro-server services::blob_gc: BlobGc (mark/pass), spawn (hourly when [blob_gc] enabled), put watcher, metrics ostro_blob_gc_{deleted_total,bytes_freed_total,duration_seconds}"
affects: [08-20 end-to-end GC check, 08-22 gate (clock grep, failpoint suites), Phase 13 module GC]

tech-stack:
  added: []
  patterns:
    - "Scan without the lock, re-stat + unlink under the exclusive GC lock in batches of 1024"
    - "Remote deletion only after deleted_at + window < now, window = age of the oldest retained manifest, and only while LOCK names this incarnation"

key-files:
  created:
    - crates/ostro-blob/src/gc.rs
    - crates/ostro-blob/tests/gc.rs
    - crates/ostro-backup/tests/remote_gc.rs
  modified:
    - crates/ostro-blob/src/lib.rs
    - crates/ostro-blob/tests/blob_failpoints.rs
    - crates/ostro-blob/tests/blob_store.rs
    - crates/ostro-backup/src/remote_gc.rs
    - crates/ostro-server/src/services/blob_gc.rs

key-decisions:
  - "The sweep holds the exclusive GC lock only per unlink batch (1024 candidates), re-reading each candidate's mtime under the lock; the scan runs unlocked"
  - "Window = now - cut_at_ms of the oldest manifest still in the prefix (oldest decodable manifest/gen/<g>, min with manifest/current); due when deleted_at + window < now (strict); only the newest deletion time of a sha counts"
  - "The remote pass runs only when LOCK names this node's incarnation (never deletes in a prefix taken over by another node); the ledger is still written"
  - "ReputGuard + put watcher re-upload a blob re-put after its remote deletion, because the uploader caches uploaded shas and would skip it (no uploader.rs change)"
  - "Ledger::adopt_orphans records remote blobs that are neither local nor in the ledger (crash between unlink and ledger append, or blobs a restore did not download)"
  - "The mtime touch reads SystemTime::now() (clock-exempt: filesystem mtime domain, same clock the kernel stamps new files with); the sweep cut-off comes from the injected clock"

requirements-completed: [BACK-01]

duration: ~1h30m
completed: 2026-11-20
---

# Phase 8 Plan 19: Blob GC Summary

**Hourly blob GC: `put` touches the mtime of an existing blob under a shared GC lock. A mark of partition state (one fjall snapshot each), meta modules and retained snapshot `.blobrefs` drives a sweep that deletes unmarked blobs older than `gc_start - grace` under the exclusive lock. With bottomless on, deleted shas go into a fsynced ledger, and their remote copies are deleted only after the oldest retained restore point was cut after the local deletion, and only if the blob was not re-put.**

## Race-test design (put vs. sweep)

`crates/ostro-blob/tests/gc.rs::concurrent_put_never_loses_a_blob`, 1,000 iterations. Each iteration:

- the blob exists, is unmarked, and has an mtime 2 h in the past;
- a sweeper thread (`older_than = now - 1 h`) and a putter thread (same bytes) start on one `Barrier`, and the putter yields on every third iteration to vary the interleaving;
- the putter asserts `contains` right after its own `put` returns, while the sweep may still be running;
- after both join, the test asserts the blob exists and `get` verifies its content.

Both orders occur. In the recorded run the sweep deleted first and the put rewrote the blob in **925** iterations, and the put's touch won and the blob was kept in **75**. The test counts both.

Why it holds: `put` holds the read lock for its whole body (existence check, touch or tmp+rename). The sweep re-stats each candidate under the write lock. Every put therefore either completed before the batch lock, so its touch makes the blob young and it is kept, or it runs after the unlink and writes the blob again.

`put_between_mark_and_sweep_survives` makes the plan's direct assertion: a put that completes after the mark and before the sweep never ends with the blob missing.

The failpoint test panics at `blobgc::before_unlink` on the second candidate (`1*off->panic`). Exactly one unlink has happened by then, the referenced blob is intact, the poisoned lock still works for `put`, the store reopens, and a rerun deletes the remaining three.

## Window computation

- `retention_window(store, now)` lists `manifest/gen/`, takes the oldest gen that still decodes, and combines its `cut_at_ms` with `manifest/current` by taking the minimum. The window is `now - oldest_cut`. `None` means there is no manifest yet, and then nothing is deleted remotely.
- A ledger entry is due when `deleted_at + window < now`, i.e. `deleted_at < oldest_cut`. Every retained restore point was cut after the local deletion. At that moment the blob was unreferenced, so no retained restore point references it, unless it was re-referenced, and re-referencing goes through `put`:
  - re-put and still local → the entry is dropped and the blob is never deleted remotely;
  - re-put and deleted again → a newer ledger entry, and the newest time wins.
- Retention deleting old gens shrinks the window (tested: gens at cut 400/600/900 at now = 1000 give 600 ms, and 400 ms after gen 4 is deleted).
- Undecodable gens are skipped. A dead gen that retention has not deleted yet only makes the window larger, which is the conservative direction.

## Timing: one sweep over 100k blobs

From `sweep_100k_timing` (`#[ignore]`, `--ignored --nocapture`, WSL2 ext4 tempdir, machine shared with 3 other executors and a ci-local run). The mix was 100,000 blobs: 50k referenced, 25k young, 25k old and unreferenced.

- **Debug build: sweep 1.32 s** (scanned 100,000, deleted 25,000, kept_marked 50,000, kept_young 25,000).
- **Release build: 1.59 s.** The time is dominated by readdir, stat and unlink syscalls, so both builds land in the same range.

Puts stall for at most one 1,024-unlink batch at a time.

The service-level pass on a real server (one partition, mark from fjall + meta + snapshots, sweep, ledger) took well under a second in the lib tests.

## Task Commits

1. **Task 1: GC lock, mtime touch, sweep, failpoint, tests**: `62f4519`
2. **Task 2: remote ledger, service, tests** (also contains the rustfmt fix-up of two Task 1 test files and the 100k timing test): `b65095d`

## Verification

- `cargo test -p ostro-blob --locked`: blob_store 8, gc 7 (+1 ignored timing), observer 5. All ok.
- `cargo test -p ostro-blob --features failpoints --locked -- --test-threads=1`: blob_failpoints 3 passed (incl. `gc_panic_before_unlink_is_safe_and_rerun_completes`), blob_store 8, gc 7, observer 5. All ok.
- `cargo test -p ostro-backup --test remote_gc --locked`: ok, 6 passed. The same passes with `--features failpoints -- --test-threads=1`.
- `cargo test -p ostro-backup --lib`: 10 passed. Regression check of the touched BlobStore: `--test uploader` 7, `--test fencing` 5 and `--test chain_start` 4 passed.
- `cargo test -p ostro-server --lib --locked -- blob_gc`: ok, 3 passed:
  - `blobrefs_sidecar_parses`;
  - `blob_gc_reclaims_expired_run_blobs_and_keeps_modules_and_snapshot_refs`;
  - `remote_copies_follow_the_ledger_window_and_the_lock`.
- `cargo clippy -p ostro-blob -p ostro-backup -p ostro-server --all-targets --locked -- -D warnings`: clean, both with and without `ostro-blob/failpoints,ostro-backup/failpoints,ostro-server/failpoints`.
- `cargo check --workspace --all-targets --locked` on main b65095d: no warnings. rustfmt `--check` is clean on every touched file.
- `bash scripts/ci-local.sh --no-musl` was **not run**: a full ci-local and three other executors were already running. Gate it at the wave boundary (08-22).

## Deviations from Plan

1. **[Rule 1] `crates/ostro-blob/tests/blob_store.rs` `put_is_idempotent`** asserted that a second put leaves the mtime unchanged, which contradicts this plan's must-have. It now checks the same inode, the same content and `mtime2 >= mtime`. The file is not in `files_modified`.
2. **`Ledger::run` takes `&BlobStore` instead of a `still_local` closure.** It also needs the local bytes to re-upload a blob that reappeared during the delete. `still_local` is `BlobStore::contains`.
3. **Added beyond the plan, for correctness (Rule 2):**
   - `ReputGuard` plus the put watcher. The uploader's in-session `remote_blobs` cache would otherwise never re-upload a re-put blob whose remote copy GC deleted.
   - The LOCK ownership check before any remote deletion.
   - `adopt_orphans`, covering the crash leak between the unlink and the ledger append.
4. **Service tests start a real in-process server** (`server::start`) inside the `#[cfg(test)]` module, building the `guests/` workspace once, the same way the integration tests do. `ServerHandle` does not expose `Arc<Node>`/`Arc<MetaHandle>`, so `BlobGc::pass` takes `&Node` + `&MetaView` instead of a `ServiceCtx`.
5. **The interval wait uses `tokio::time::sleep(interval)`**, like the 08-13 uploader. There is no clock-aware sleep helper in the tree yet. The GC cut-off and the ledger times come from the injected clock.

## Known limitations (documented, not fixed)

- **Mark consistency.** The mark is one fjall snapshot per partition, taken one partition after another, as the plan specifies. A blob ref that moves between partitions between two of those snapshots without a `put` (for example a child's input carried over the outbox) could be missed if the blob is also older than the grace. Today payloads always go through `put` at the API or sink, and the parent journal keeps the ref while the child runs, so this needs an unusual flow. Worth revisiting if cross-partition ref passing becomes common.
- **The ReputGuard set lives only in memory and grows** by 32 bytes per remotely deleted sha in this process. After a restart, the uploader's first round reconciles against `LIST blobs/`.
- **A tiny window remains.** Between a remote delete and the re-check re-upload, a manifest may be written whose state references the re-put blob. The blob is back remotely within the same pass, before any restore could need it in practice.

## Self-Check: PASSED

- Commits 62f4519 and b65095d are on main.
- `crates/ostro-blob/src/gc.rs` contains `before_unlink`, `remote_gc.rs` contains `ledger`, `services/blob_gc.rs` contains `blob_refs_snapshot`, and lib.rs contains `set_modified`.
