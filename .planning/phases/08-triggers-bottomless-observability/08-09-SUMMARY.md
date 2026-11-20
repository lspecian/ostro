---
phase: 08-triggers-bottomless-observability
plan: 09
subsystem: storage
tags: [snapshot, trim, retention, wal, bottomless, sc4, crash-safety]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-02 OSNP v1 export/install/recover_install; 08-08 node primitives; 08-01 SnapshotConfig contract
provides:
  - per-partition SnapshotService (cadence, keep=2, durable floor publication, SnapshotTaken events with sha256)
  - dynamic trim floors (FloorWatches local/upload) replacing RetentionConfig.snapshot_floor; WAL trimming enabled
  - Node::open snapshot + tail rebuild (missing / unreadable / below-log_start state)
  - Node handles for 08-13 (floor_handles, snapshot_dir, snapshot_events) plus snapshot_now / retention_pass
  - SC4 property tests, failpoints, snapshot_tail equivalence, crash_loop_snapshots
affects: [08-13 uploader, 08-18 restore, 08-19 blob GC, Phase 12 follower install]

tech-stack:
  added: [sha2 (existing workspace pin) as an ostro-partition dependency]
  patterns:
    - "Floor published only after tmp -> sync_all -> sidecar -> rename -> dir fsync; old snapshots deleted after the floor moves"
    - "Floors are watch channels read by each retention pass after force_checkpoint"

key-files:
  created:
    - crates/ostro-partition/tests/snapshot_service.rs
    - crates/ostro-partition/tests/snapshot_tail.rs
  modified:
    - crates/ostro-partition/src/leader/snapshot.rs
    - crates/ostro-partition/src/leader/retention.rs
    - crates/ostro-partition/src/leader/mod.rs
    - crates/ostro-partition/src/node.rs
    - crates/ostro-partition/src/startup.rs
    - crates/ostro-partition/tests/trim_safety.rs
    - crates/ostro-crashtest/src/lib.rs
    - crates/ostro-crashtest/src/bin/crash-target.rs
    - crates/ostro-crashtest/tests/crash_loop.rs
    - crates/ostro-partition/Cargo.toml
    - Cargo.lock

key-decisions:
  - "Local floor = applied_offset of the oldest .osnap on disk (by name, conservative); 0 when there is none or [snapshots] enabled = false, so stores without snapshots behave exactly as before"
  - "Bytes cadence estimates envelope bytes as (WAL segment bytes / (LEO - log_start)) x offsets since the last snapshot; no WAL change"
  - "Snapshot sha256 needs sha2 in ostro-partition (Rule 3; workspace pin already in the lock, one Cargo.lock line)"
  - "Install-on-open info is exposed as Node::snapshot_installed(p) instead of a new StartupEvent variant (startup_order.rs matches the enum exhaustively)"
  - "Missing state over an untrimmed log with no usable snapshot still rebuilds by full replay; an unreadable state with no usable snapshot keeps its original error"

requirements-completed: [BACK-04]

duration: ~1h15m
completed: 2026-11-20
---

# Phase 8 Plan 09: Snapshot Service and Safe WAL Trimming Summary

**WAL trimming is now on. A per-partition snapshot service publishes a local floor only after a snapshot is durable. Retention trims to `min(ckpt, local, upload?, retention?)`, so `log_start` never passes the oldest retained snapshot. `Node::open` rebuilds a missing, unreadable or behind state from the newest usable snapshot plus the log tail.**

## Node handle API (for 08-13 / 08-18)

```rust
pub struct FloorHandles { pub upload: watch::Sender<Option<Offset>> } // None = unbounded
impl Node {
    pub fn floor_handles(&self, p: PartitionId) -> Option<FloorHandles>;
    pub fn snapshot_dir(&self, p: PartitionId) -> PathBuf;              // <data>/snapshots/pNNNN
    pub fn snapshot_events(&self) -> broadcast::Receiver<SnapshotTaken>;
    pub fn local_floor(&self, p: PartitionId) -> Option<Offset>;
    pub fn snapshot_installed(&self, p: PartitionId) -> Option<Offset>;
    pub async fn local_snapshots(&self, p: PartitionId) -> Vec<LocalSnapshot>;
    pub async fn snapshot_now(&self, p: PartitionId) -> Result<Option<SnapshotTaken>, String>;
    pub async fn retention_pass(&self, p: PartitionId) -> Result<Offset, String>;
}
pub struct SnapshotTaken { partition, applied_offset, path, sha256: [u8; 32], info: SnapshotInfo }
// leader::snapshot: SnapshotService, SnapshotShared, LocalSnapshot, scan_dir, quick_check,
//   snapshot_dir, snapshot_file_name, SNAPSHOT_EXT, BLOBREFS_EXT, TMP_PREFIX
// leader::retention: FloorWatches { local, upload }, trim_floor(ckpt, local, upload, retention),
//   run_pass, PassError
// LeaderCtx gains `floors: FloorWatches` and `snapshots: Option<Arc<SnapshotService>>`.
```

With `NodeConfig::backup_enabled`, the upload floor is `Some(log_start)` from `Node::open` on, before any leader service starts (B4). Without backup it is `None`. Only raise it.

`.blobrefs` sidecar = the BlobRefs frame payload (`u32 LE n`, then n sorted 32-byte digests), written and renamed before the `.osnap` rename.

## Measurements

- **Apply throughput during export** (Pitfall 6, reported only): release build, load average about 6, single partition, 500-command submits over a 100k-run store. Apply alone ran at **28.9k cmds/s**. During back-to-back exports it ran at **15.8k cmds/s**, so continuous exporting costs about 45 %. With the production cadence (1 h or 256 MiB, one partition at a time) the cost lands in occasional windows of under a second.
- **Snapshot timings** (same run): 43.8–57.1 MB, 0.73–0.96M rows, **356–550 ms** per snapshot. That covers export, sha256, fsync, sidecar, rename and dir fsync.
- **crash_loop** (`cargo nextest run -P crash -p ostro-crashtest --run-ignored only -E 'test(crash_loop)'`, debug, 20 iterations each): **4/4 passed**. `crash_loop_snapshots` took 479.6 s, kill9 72.3 s, torn_tail 100.1 s and corruption_detected 77.4 s. nextest swallowed the per-iteration output of the passing run. An earlier 5-iteration `--nocapture` run printed: 1514 ACKs verified, 211 of them below `log_start` and checked through the state; max `log_start` 383; 4 interrupted snapshot writes (`.tmp-*`) found after kills; up to 9 `.osnap` files on 4 partitions (a kill between the rename and the old-snapshot delete leaves keep+1 files). No lost ACKs, and the rebuild digest matched every iteration.

## Task Commits

1. **Task 1: snapshot service + dynamic floors + Node handles** (also holds the open-path install and the rewritten trim_safety, because the API change broke the old file): `13265ac`
2. **Task 2: snapshot_tail, crashtest rebuild via snapshots, crash_loop_snapshots**: `5a970df`

## Verification

- `cargo test -p ostro-partition -p ostro-crashtest --locked`: every binary ok, 0 failed. trim_safety 6 passed in 981.8 s, with 64 SC4 cases × 30 steps. snapshot_tail 1 passed (64 cases) in 528.2 s. snapshot_service 4 passed. snapshot 15, rebuild_digest 4, startup_order 9 and node_incarnation 5 all passed.
- `cargo test -p ostro-partition --features failpoints --test trim_safety --locked -- --test-threads=1`: 8 passed. That includes crash_before_rename, crash_before_dir_fsync and crash_after_trim_delete.
- `cargo test -p ostro-crashtest --features failpoints --locked -- --test-threads=1`: green.
- `cargo clippy -p ostro-partition -p ostro-crashtest --all-targets --locked -- -D warnings`, with and without failpoints: clean. `cargo check --workspace --all-targets --locked`: no warnings after rebasing onto current main.
- `bash scripts/ci-local.sh` was not run, because other executors were building at the same time. Gate it at the wave boundary.

## Deviations from Plan

1. **[Rule 3] sha2 dependency.** `SnapshotTaken.sha256` needs SHA-256 and ostro-partition had none. Added `sha2.workspace = true`, which is one Cargo.lock line and no new crate.
2. **[Rule 3] crash-target flags.** `crash_loop`'s tiny-threshold mode needs `--snapshot-ms`, `--snapshot-bytes`, `--retention-ms` and `--segment-bytes` in `src/bin/crash-target.rs`. That file is not in `files_modified`.
3. **"Kill" in the in-process proptest** is a node stop that leaves files exactly as the interrupted operation left them. Dropping a live Node and reopening the same directory would race the old instance's WAL and fjall threads. Real `kill -9` coverage comes from `crash_loop_snapshots`. A kill without state deletion checks reopen + SC4. A kill with state deletion also checks the rebuilt digest against the pre-kill live digest (the model) in Inspect mode. snapshot_tail compares against an explicit full replay.
4. **The SC4 random-sequence proptest is cfg(not(feature = "failpoints")).** The single-threaded failpoint run covers the three failpoint cases and does not repeat the roughly 16-minute proptest. `OSTRO_SC4_CASES` and `OSTRO_SNAPSHOT_TAIL_CASES` override the 64 cases.
5. **Commit split.** The open-path install (Task 2 behaviour) landed in the Task 1 commit together with node.rs.
6. **No new StartupEvent variant** (see decisions). The install is visible through `Node::snapshot_installed` and the log.
7. `crash_loop` asserts no lost ACKs through `verify_acks`. ACKs below `log_start` count as `AckCheck::trimmed` and are verified via `applied_offset` and run existence.

## Issues Encountered

- `scan_dir` deletes `.tmp-*`, so it must run only before the services start. It runs in `Node::open`. A test monitor that used it while snapshots were running deleted in-flight exports. The tests now list files read-only.
- `cargo fmt -p ostro-partition` reformatted 06-03 files on main that were not fmt-clean (apply/terminal.rs and four test files). I reverted that locally and did not commit it. Whoever owns 06-03 should run fmt.

## Self-Check: PASSED

- Commits 13265ac and 5a970df are on main. leader/snapshot.rs contains `before_dir_fsync`, retention.rs contains `upload_floor`, and node.rs calls `install_snapshot`.
