---
phase: 12-replicated-partition-logs
plan: 12
subsystem: replication-snapshot-bootstrap
tags: [replication, snapshot, osnp, install, crash-safety, d-p12-8, clus-04, failpoints, mtls]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 InstallGuard/PartitionControl + cluster.proto FetchSnapshot; 12-05 TcpTransport/require_node; 12-08 PartitionLog::reset_to + F3 anchoring; 12-11 fetcher calling install_from_leader on SNAPSHOT_REQUIRED; Phase 8 OSNP v1 + install_snapshot + SnapshotService"
provides:
  - "ostro_cluster::snapshot::{SnapshotCtx, Exporter, SnapshotServeConfig, SnapshotServeStats, PinnedSnapshot, stream_snapshot, serve_snapshot, frame_crc_ok, SNAPSHOT_MESSAGE_LIMIT}"
  - "ostro_cluster::install::{InstallCtx::new/with_config, InstallConfig, InstallOutcome::Installed{applied, latest_epoch, logical_digest, bytes}, InstallError, install_from_leader, INCOMING_PREFIX}"
  - "failpoints install::{after_log_reset, before_rename, after_rename} (names reserved by 12-01)"
affects: [12-13, 12-14, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "the leader serves OSNP frames verbatim (frame 0 = header), CRC-checked through Phase 8's RawFrame::encode (no crc32c dependency in ostro-cluster)"
    - "per-replica pins hold an open file descriptor: retention may unlink, the stream and its resumes continue"
    - "the critical install section (quiesce -> reset -> install -> keep -> reopen) runs in a spawned task, so a cancelled fetcher never abandons a quiesced partition"
key-files:
  created:
    - crates/ostro-cluster/tests/snapshot_stream.rs
    - crates/ostro-cluster/tests/install.rs
    - crates/ostro-cluster/tests/install_crash.rs
    - crates/ostro-cluster/tests/support_install/mod.rs
  modified:
    - crates/ostro-cluster/src/snapshot.rs
    - crates/ostro-cluster/src/install.rs
key-decisions:
  - "Frame i>=1 of FetchSnapshot is the i-th stored OSNP frame byte for byte; concatenation == the file. One format (OSNP v1), no re-encoding"
  - "A resume without a pin is FAILED_PRECONDITION (restart at 0): frames of two snapshots are never spliced. Pins live pin_ttl (10 min, injected clock) after the last request"
  - "On-demand export: SnapshotService::take when the partition has one (durable local snapshot), else an ephemeral store export unlinked right after open; single-flight per partition, reused for export_reuse (60 s) while it covers log_start"
  - "The whole-file sha256 check compares the re-read staged file with the received bytes (the proto carries no leader-side sha256); verify_snapshot covers header/frame CRCs, structure and the trailer digest"
  - "The installed snapshot is kept as snapshots/pNNNN/<S>.osnap (+ .blobrefs) BEFORE the reopen, so a reopen that rebuilds the snapshot service counts it"
  - "In-process failure of reset/install: recover_install, then re-anchor the log at the LIVE state's (applied, epoch history), then reopen; same result as a crash at that point"
duration: ~3h
completed: 2026-11-23
---

# Phase 12 Plan 12: Snapshot bootstrap Summary

**`Replication.FetchSnapshot` now streams the leader's newest usable OSNP v1 snapshot as resumable frames, byte-identical to the file and pinned against retention. `install_from_leader` stages, verifies and installs it in the fixed D-P12-8 order (quiesce, reset the log, Phase 8 `install_snapshot`, keep, reopen) and leaves the follower fetching from `S` with `last_fetched_epoch = latest_epoch`. A crash at each of the three install failpoints (20 iterations each, with random frame-level stream cuts) reopens through `Node::open` into the old state at `A` or the new one at `S`, never "state ahead of log". The retry then reaches the leader's digest.**

## Commits

| Commit | Message |
| --- | --- |
| d717dc2 | feat(12-12): FetchSnapshot streaming of OSNP v1 with resume |
| e95c298 | feat(12-12): crash-safe follower snapshot install (reset log first) |

## Shapes (for 12-13 / 12-14)

```rust
// install.rs
pub struct InstallCtx { /* private */ }            // Default = unconfigured: NotConfigured
impl InstallCtx {
    pub fn new(node_id: u64, data_dir: PathBuf, count: PartitionCount,
               transport: Arc<dyn ClusterTransport>,          // reaches the leader's cluster listener
               control: Arc<dyn PartitionControl>) -> Self;   // 12-13: Node's quiesce/reopen
    pub fn with_config(self, cfg: InstallConfig) -> Self;     // frame_timeout 30 s, max_resumes 5
}
pub enum InstallOutcome { Installed { applied: Offset, latest_epoch: Epoch, logical_digest: [u8; 32], bytes: u64 } }
pub enum InstallError { NotConfigured, Stream{leader, reason}, Corrupt(_), Mismatch(_), Io(_),
                        Quiesce(_), Log(_), State(_), Reopen(_), Aborted(_), Injected(_) }
pub async fn install_from_leader(ctx: &InstallCtx, p: PartitionId, leader: u64) -> Result<InstallOutcome, InstallError>;

// snapshot.rs (one SnapshotCtx per partition, both roles may serve)
pub enum Exporter { Service(Arc<SnapshotService>), Store(Arc<PartitionStore>), None }
pub struct SnapshotServeConfig { pub pin_ttl /*10 min*/, pub export_reuse /*60 s*/, pub channel_frames /*4*/ }
impl SnapshotCtx {
    pub fn new(log: Arc<PartitionLogHandle>, count: PartitionCount, data_dir: &Path,
               assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
               exporter: Exporter, clock: Arc<dyn Clock>, cfg: SnapshotServeConfig) -> Self;
    pub fn stats(&self) -> Arc<SnapshotServeStats>;    // exports, streams, resumes, frames
    pub fn pinned(&self, replica: u64) -> Option<Arc<PinnedSnapshot>>;
}
pub async fn serve_snapshot(ctx: &SnapshotCtx, req: Request<FetchSnapshotRequest>) -> Result<Response<BoxStream<SnapshotFrame>>, Status>; // require_node + stream_snapshot
pub async fn stream_snapshot(ctx: &SnapshotCtx, peer: u64, req: FetchSnapshotRequest) -> Result<BoxStream<SnapshotFrame>, Status>;
pub const SNAPSHOT_MESSAGE_LIMIT: usize = MAX_FRAME_LEN + 9 + 64 KiB;
```

Wiring notes:
- **12-14:** the `Replication` server's encode/decode limit must be `max(fetch_decode_limit(..), SNAPSHOT_MESSAGE_LIMIT)`. One limit covers every method, and OSNP frames may reach 64 MiB plus framing. `tests/support_install::server_limit()` does this. The install client sets `SNAPSHOT_MESSAGE_LIMIT` itself.
- **12-13:** the `InstallGuard` reopen closure must call `PartitionStore::start_run_index_rebuild()` (the fake control here does, and `tests/install.rs` checks the resulting `x_run_index` digest against the leader's). It must also accept that the log is already anchored at `S`: `reset_to` seeded the epochs, so the reopen does not need to seed them. `quiesce_for_install` is called by the fetcher task itself (install runs inside the fetcher round), so it must not wait for that fetcher to stop. Cancelling the fetcher while the install is running is safe: steps 3-7 run in a spawned task.
- **12-13:** `snapshots/pNNNN/.incoming-*` files are left behind only by a crash during staging. The next install attempt deletes them. Phase 8's `scan_dir` ignores them (it only removes `.tmp-*`).

## Install order (as built)

1. Stage: delete stale `.incoming-*`. Stream into `.incoming-<applied 20d>`. Each frame is checked on arrival: the index, the header CRC/partition/partition_count for frame 0, the frame CRC for the others, and `last` only on the trailer. A broken stream resumes at the next frame (`resume_frame`, from the leader's pin), at most 5 times.
2. Verify: fsync the file and the directory. Re-read the file through `verify_snapshot`, which checks every CRC, the structure, the row order and the trailer digest/counts. The re-read sha256 must equal the sha256 of the received bytes, and the size must match. Read the cursor row from the meta table: `cursor.epoch == header.latest_epoch`, and `epoch_history` is the anchor history.
3. `quiesce_for_install(p)`.
4. `log.reset_to(S, history)`. Failpoint `install::after_log_reset`.
5. Failpoint `install::before_rename`, then Phase 8 `install_snapshot` (staged dir, digest + applied check, rename, fsync), then a digest equality check, then failpoint `install::after_rename`.
6. Rename the file to `snapshots/pNNNN/<S>.osnap` and write the `.blobrefs` sidecar.
7. `guard.reopen()`.

Failure without a crash in step 4 or 5: run `recover_install` (Phase 8's interrupted-swap rules), re-anchor the log at the live state's `(applied, epoch_history)`, then reopen. A failure after the rename reopens on `S`.

## Crash-loop results (`tests/install_crash.rs`, `--features failpoints --test-threads=1`)

Each iteration: a follower with an old state `A` (random, a prefix of the leader's history) and an old log `[0, L)` (`L >= A`); 0-2 random frame-level cuts; the failpoint `panic`s the install task; the follower's processes are dropped; `Node::open` (Inspect) on its data dir; then a second install with new random cuts.

| Failpoint | Iterations | After the crash (`Node::open`) | Second attempt |
| --- | --- | --- | --- |
| `install::after_log_reset` | 20/20 | opens Ready; applied = log_start = LEO = `A`; digest = old | Installed, digest = leader's, log `[S, S)` |
| `install::before_rename` | 20/20 | opens Ready; applied = log_start = LEO = `A`; digest = old | same |
| `install::after_rename` | 20/20 | opens Ready; applied = log_start = LEO = `S`; digest = leader's | same |

In-process variant (`return`): `after_log_reset`, `before_rename` and Phase 8's `snapshot::install_after_old_rename` (the state dir is renamed aside) reopen at once on the old state. The log is re-anchored at `A` with the old cursor's epochs, `Node::open` agrees, and the retry installs. `after_rename` reopens on `S`.

Duration: about 140 s per crash loop (about 7 s per iteration, dominated by roughly ten fjall opens: old-state build, staging open/reopen, reopen, `Node::open`, second install). The whole binary takes about 6.5 min.

## Snapshot stream throughput (debug build, WSL2, loopback mTLS)

17.1 MB snapshot (4,000 runs with 4 KiB inputs, 56 frames), 3 runs: 0.058-0.075 s, **217-283 MiB/s**.

## Phase 8 hook gap

`install_snapshot` has no pre-rename hook callable from outside, only its own `snapshot::install_*` failpoints. So `install::before_rename` sits immediately before the call, as the plan's fallback allows. A crash inside the call (`.install-pNNNN` staged, or `pNNNN` renamed aside to `.old-pNNNN`) is covered by Phase 8's `recover_install`, which `Node::open` runs. The in-process path also runs it before re-anchoring. That window is exercised by the `snapshot::install_after_old_rename` case above.

## Tests

| Suite | Result |
| --- | --- |
| `--test snapshot_stream --test-threads=2` | 9 passed: byte-identical stream (frame 0 = 64-byte header, consecutive indices, `last` only on the trailer, `verify_snapshot` ok), newest snapshot covering `log_start` chosen (below-start and corrupt files skipped), resume at every k gives the identical suffix, 4 concurrent requesters share one store export (no file left behind, reused later), `Exporter::Service` writes one durable local snapshot, non-replica / wrong replica_id / other partition refused (in-process and over mTLS with a node-9 certificate), retention delete mid-stream (stream completes, resume continues from the pin, fresh request without a snapshot is UNAVAILABLE), resume without a pin / after the pin TTL is FAILED_PRECONDITION, throughput |
| `--test install --test-threads=1` | 6 passed: an empty follower bootstraps through the real `FetcherDriver` (SNAPSHOT_REQUIRED → 1 install → fetches 25 records from S; byte-identical log from S, equal epoch history, digest equal, `x_run_index` digest equal (W13), snapshot kept, reopen from disk at S); an old log behind log_start installs (old segment files deleted, at most the open one left, anchored at S with epoch 1 seeded, reopen ok); a digest-only corruption (valid frame CRC) and a frame-CRC corruption change nothing (no quiesce, state/log unchanged, staged file removed); a partition-count mismatch is refused unchanged; 3 stream cuts resume (3 resumes); too many cuts fail without changes; a stale `.incoming-*` is removed |
| `--features failpoints --test install_crash --test-threads=1` | 4 passed (3 × 20 crash iterations + the in-process rollback case) |
| lib unit tests (snapshot, install) | frame CRC check, local listing never deletes `.tmp-*`, default ctx NotConfigured, stale incoming removal |
| `cargo test -p ostro-cluster --no-fail-fast` / `--features failpoints --no-fail-fast -- --test-threads=1` (the ci-local failpoint step) | all binaries green. Without failpoints: lib 17, controller 8, fetch_service 7, fetcher 9, install 6, isr_publisher 5, liveness 8, meta_peer 3, snapshot_stream 9. With failpoints: lib 17, controller 9, fetch_mutants 2, fetch_service 7, fetcher 9, install 6, install_crash 4 (353.8 s), isr_publisher 5, liveness 8, meta_peer 3, snapshot_stream 9 |
| `cargo check --workspace --all-targets --locked` after rebasing onto cf9e801; `cargo fmt --all --check` | clean |
| clippy `-D warnings` on ostro-cluster, all targets, with and without failpoints; `cargo fmt --check` | clean |

## Deviations from Plan

1. **[Rule 1 - Bug] The in-process rollback must run `recover_install` and anchor at the live state.** The first draft read the old position before the reset and re-anchored there. If Phase 8's `install_snapshot` failed after renaming `pNNNN` aside, the reopen would have created an empty store at `pNNNN`. A later `recover_install` would then have deleted `.old-pNNNN` and lost the old state. If it failed after the swap, the state would have been `S` with the log at `A` (state ahead of log). Fix: `live_position` runs `recover_install`, then reads the live state, and the log is anchored there. This was found while designing the crash suite and is covered by the `snapshot::install_after_old_rename` case.
2. **Step 6 (keep the file as a local snapshot) runs before the reopen**, not after it. This way the reopened partition's snapshot service (rebuilt by 12-13's reopen) counts the file toward retention and the local floor. A crash between the two only loses the extra copy.
3. **Whole-file sha256.** `FetchSnapshotRequest` / `SnapshotFrame` carry no leader-side sha256, and the proto is frozen. The check compares the sha256 of the received bytes with the sha256 of the re-read staged file, which catches local corruption between receive and install. Transfer integrity comes from the per-frame CRCs plus the trailer digest.
4. **`InstallCtx::default()` stays** as an unconfigured context, because 12-11's fetcher tests construct it. Every install with it fails `NotConfigured`.
5. **Crash failpoints use `panic`** (the spawned install task dies; nothing after the point runs). `return` is exercised separately for the in-process rollback.
6. TDD order: tests and code were written together per task, with no separate RED commits (same as 12-08/12-11).

## Open items

- **Crash-suite runtime:** about 6.5 min in the failpoint step (20 iterations × 3 points as planned; fjall opens dominate). If the CI budget needs it, 12-18 can lower the iteration count. That would need an env name registered in RESERVATIONS §11, so none was added here.
- **D-P12-6** unchanged. A follower that 12-11 sends SNAPSHOT_REQUIRED now actually installs instead of retrying.
- `scripts/ci-local.sh --no-musl` was not run here (parallel executors and the disk budget). The ostro-cluster suites with and without failpoints, clippy, fmt and the workspace check ran (table above). The phase gate (12-18) covers the rest.

## Self-Check: PASSED

- Files: crates/ostro-cluster/src/{snapshot,install}.rs and tests/{snapshot_stream,install,install_crash}.rs plus tests/support_install/mod.rs exist at e95c298.
- Commits d717dc2 and e95c298 are on main.
