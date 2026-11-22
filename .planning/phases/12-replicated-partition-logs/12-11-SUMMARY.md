---
phase: 12-replicated-partition-logs
plan: 11
subsystem: replication-data-path
tags: [replication, fetch, follower, isr, kip-320, kip-595, kip-279, kip-497, f11, rp-12, mutants, mtls]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-04 divergence/isr/tracker rules; 12-05 TcpTransport/PeerIdentity/require_node/MetaHandle multi-voter; 12-08 PartitionLog replica API; 12-10 bootstrap + SuspectSink"
provides:
  - "ostro_cluster::fetch::{FetchCtx, FetchConfig, handle_fetch, serve_fetch, fetch_decode_limit, FETCH_DECODE_SLACK, DEFAULT_FETCH_*}"
  - "ostro_cluster::fetcher::{FetcherDriver (impl FollowerDriver), FetcherConfig, FetcherHooks, FetcherStats}"
  - "ostro_cluster::blob::{fetch_blob, fetch_blob_into, push_blob, push_to, has_blob, command_blob_refs, blob_refs_of, BlobStream, BlobTransferError, BLOB_CHUNK, BLOB_MESSAGE_LIMIT, MAX_BLOB_BYTES}"
  - "ostro_cluster::isr_publisher::{IsrPublisher::run, IsrMeta (impl MetaHandle), IsrPublisherConfig, IsrPublisherExit}"
  - "ostro_log::PartitionLog::{isr_view, propose_isr_change, clear_isr_change} + ostro_log::IsrView"
  - "mutant sites: repl::mutant::skip_epoch_check (fetch.rs), no_truncate + count_nondurable_leo follower half (fetcher.rs); failpoints fetch::before_append, fetch::before_durable, isr::before_alter"
affects: [12-12, 12-13, 12-14, 12-17]
tech-stack:
  added: []
  patterns:
    - "per-partition FetchCtx over the meta assignment watch; the service layer (12-14) does require_node then delegates"
    - "a pending ISR change lives in the log (KIP-497 maximal ISR); the publisher only proposes, meta's publication is the only writer of the published ISR"
key-files:
  created:
    - crates/ostro-cluster/tests/fetch_service.rs
    - crates/ostro-cluster/tests/fetcher.rs
    - crates/ostro-cluster/tests/isr_publisher.rs
    - crates/ostro-cluster/tests/fetch_mutants.rs
    - crates/ostro-cluster/tests/support_fetch/mod.rs
  modified:
    - crates/ostro-cluster/src/{fetch,fetcher,blob,isr_publisher}.rs
    - crates/ostro-log/src/partition_log.rs (ISR hooks; follower truncate epoch reset)
    - crates/ostro-log/src/lib.rs (IsrView re-export)
key-decisions:
  - "Fetch fencing compares against max(assignment epoch, local role epoch); NOT_LEADER unless the log leads exactly that epoch"
  - "The response is cut at the durable end sampled before the read, so leader_durable_end bounds every envelope it carries (W3); progress is recorded with the pre-wait sample"
  - "fetch_decode_limit = max(max_envelope_bytes, fetch_max_bytes) + 64 KiB (= max_envelope_bytes + 64 KiB when fetch_max_bytes <= max_envelope_bytes)"
  - "Worker certificates and foreign-cluster node certificates never reach the handler: require_node answers PERMISSION_DENIED (12-05 matrix); NOT_REPLICA is for nodes of this cluster outside the replica set or claiming another replica_id"
  - "The follower reports suspects through the existing liveness::SuspectSink trait (no new SuspectReporter type)"
  - "OutcomeUnknown keeps the pending change and re-proposes the identical CAS; a StaleVersion answer to such a re-proposal waits for meta's view instead of dropping the change (the first proposal may have applied)"
duration: ~2h
completed: 2026-11-22
---

# Phase 12 Plan 11: Follower-pull replication Summary

**The leader's `Fetch` handler, the follower fetch loop, `FetchBlob`/`PushBlob` and the leader-side ISR publisher are in `ostro-cluster`. They replicate between real `PartitionLog`s over the mTLS transport. Fencing, divergence truncation (including two-epoch KIP-279), separate wait and read timeouts, blobs before durability and the meta-only ISR are each covered by a test. Both 12-11 mutants bite.**

## Commits

| Commit | Message |
| --- | --- |
| af9adb9 | feat(12-11): leader Fetch handler |
| 68a578c | feat(12-11): follower fetch loop with divergence truncation and blobs-before-durable |
| 285c3ef | feat(12-11): leader ISR publisher via AlterIsr + mutant bites |

## Constructor signatures (for 12-14)

```rust
// fetch.rs
pub struct FetchConfig { pub max_wait: Duration /*500 ms*/, pub read_timeout: Duration /*5 s*/, pub max_bytes: usize /*8 MiB*/ }
impl FetchCtx {
    pub fn new(log: Arc<PartitionLogHandle>,
               assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>, // AssignmentSource::subscribe()
               clock: Arc<dyn Clock>,   // the node clock (same one behind LogNodeConfig.clock)
               cfg: FetchConfig) -> Self;
    pub fn partition(&self) -> PartitionId;
}
pub async fn serve_fetch(ctx: &FetchCtx, req: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status>; // require_node + handle_fetch
pub async fn handle_fetch(ctx: &FetchCtx, peer: u64, req: FetchRequest) -> Result<FetchResponse, Status>;
pub fn fetch_decode_limit(max_envelope_bytes: usize, fetch_max_bytes: usize) -> usize; // set on ReplicationServer AND clients

// fetcher.rs
pub struct FetcherConfig { pub max_wait, pub max_bytes, pub read_timeout, pub max_envelope_bytes, pub backoff_min /*50 ms*/, pub backoff_max /*1 s*/ }
impl FetcherDriver {
    pub fn new(node_id: u64, transport: Arc<dyn ClusterTransport>, blobs: Arc<BlobStore>,
               suspects: Arc<dyn liveness::SuspectSink>,   // 12-14: sends MetaPeer.ReportSuspect
               install: Arc<InstallCtx>, cfg: FetcherConfig) -> Self;
    pub fn stats(&self) -> Arc<FetcherStats>;
}
impl FollowerDriver for FetcherDriver { fn start(&self, p, epoch, leader, log) -> Box<dyn FollowerTask> }

// blob.rs (service layer runs require_node first)
pub async fn fetch_blob(store: Arc<BlobStore>, req: FetchBlobRequest) -> Result<BlobStream, Status>;
pub async fn push_blob<S: Stream<Item = Result<BlobChunk, Status>> + Unpin>(store: Arc<BlobStore>, stream: S) -> Result<PushBlobAck, Status>;
pub async fn push_to(transport: &dyn ClusterTransport, peers: &[u64], store: &Arc<BlobStore>, sha: [u8; 32]) -> Vec<(u64, Result<bool, BlobTransferError>)>;
pub async fn fetch_blob_into(channel: Channel, store: &Arc<BlobStore>, sha: [u8; 32]) -> Result<(), BlobTransferError>;

// isr_publisher.rs
pub trait IsrMeta: Send + Sync + 'static { fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, MetaError>>; } // impl for MetaHandle
pub struct IsrPublisherConfig { pub replica_lag_max: Duration /*10 s*/, pub resolve_retry: Duration /*500 ms*/ }
impl IsrPublisher {
    pub async fn run(p: PartitionId, log: Arc<PartitionLogHandle>, meta: Arc<dyn IsrMeta>,
                     assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
                     clock: Arc<dyn Clock>, cfg: IsrPublisherConfig,
                     shutdown: CancellationToken) -> IsrPublisherExit; // Shutdown | LostLeadership
}
```

Wiring notes for 12-13/12-14:
- One `FetchCtx` per led partition. The `Replication` service maps `req.partition` to its ctx and calls `serve_fetch`. `tests/support_fetch::TestReplication` is a working reference.
- `ReplicationServer::max_{de,en}coding_message_size(fetch_decode_limit(..))` on the server. The fetcher sets the same limit on its client. Blob streams use `BLOB_MESSAGE_LIMIT`.
- Start `IsrPublisher::run` after `become_leader` and stop it on demotion. It also exits by itself (`LostLeadership`) once meta shows a newer epoch or another leader. It adopts every newer `isr_version` of its epoch through `set_published_isr`, so the lifecycle does not need to.
- Start the fetcher after `become_follower(e, leader)`. It never exits on FENCED/NOT_LEADER: it backs off (≤ 1 s) until the lifecycle stops it.

## Fetch evaluation (as built)

1. Identity: peer == `replica_id` and peer ∈ assignment `replicas`, else NOT_REPLICA.
2. Fencing against `current = max(assignment epoch, local role epoch)`: `<` gives FENCED_LEADER_EPOCH, `>` gives UNKNOWN_LEADER_EPOCH (`skip_epoch_check` bypasses this step).
3. The log must lead exactly `current`, else NOT_LEADER.
4. `divergence::check`: `diverging{epoch,end}` with error NONE, or SNAPSHOT_REQUIRED. Both carry HW, log_start and durable end, and **record nothing**.
5. `record_progress{durable_leo: min(durable_leo, fetch_offset), fetch_offset, now_ms, leader_durable_end: durable end sampled now}`.
6. If `fetch_offset < snapshot`, the read starts at once. Otherwise the handler waits up to `min(req.max_wait_ms, fetch_max_wait)` on the durable watch, re-checks leadership and re-samples. The read runs under `timeout(fetch_read_timeout)`; a timeout returns `DEADLINE_EXCEEDED`, never an empty answer. Envelopes are cut at the snapshot, whole and verbatim, at least one, up to about `max_bytes`.

## Measured rates (debug build, WSL2, loopback mTLS)

- **Fetch round trip**, one single-command batch per request (`max_bytes = 1`), 200 sequential fetches, 3 runs: 0.69-0.73 ms each (1,370-1,450 fetches/s).
- **Catch-up**, an empty follower replicating 2,000 single-command batches, 3 runs: 85-99 ms (20,000-23,600 batches/s). All 2,000 envelopes fit in one 8 MiB response (1 fetch), then fsync and append.

## Mutant results (`tests/fetch_mutants.rs`, `--features failpoints --test-threads=1`)

| Mutant | Site | Bite (property off → violated on → holds after `fail::remove`) |
| --- | --- | --- |
| `repl::mutant::skip_epoch_check` | `fetch::handle_fetch` step 2 (and the NOT_LEADER epoch match) | stale-epoch fetch: FENCED → gets 1 envelope → FENCED |
| `repl::mutant::no_truncate` | `fetcher` diverging branch | divergent follower: prefix below its HW equals the leader's → its records x, y sit below HW 4 → equal again |
| `repl::mutant::count_nondurable_leo` (follower half) | `fetcher::request` + no durable wait after `append_replica` | placed; proven in tier C by 12-17 (as planned) |

## Tests

| Suite | Result |
| --- | --- |
| `--test fetch_service --test-threads=2` | 7 passed. Every error code, the order (identity before fencing before NOT_LEADER), no progress on diverging/snapshot, durable_leo capped, a long-poll of about max_wait with progress recorded before the wait, an early return on a write, a 500 ms read under a 100 ms max_wait still returning data, a read past the read timeout returning DEADLINE_EXCEEDED, the W3 snapshot hook, a max-size (4 MiB) envelope through the `+64 KiB` limit (and failing without the slack), worker/foreign certs PERMISSION_DENIED, the round-trip rate |
| `--test fetcher --test-threads=1` | 9 passed. Byte-identical convergence with the HW-bound watcher (follower HW ≤ leader HW and ≤ own durable end), the catch-up rate, one-epoch divergence (1 truncation), two-epoch KIP-279 (2 truncations, identical epoch histories), the F11 1 MiB blob ordering (no durable end past the blob command before the blob is local, with the blob fetch delayed 400 ms), Push/FetchBlob RPCs (3-chunk blob, `stored` false on repeat, NOT_FOUND), a leader restart (suspect `(2, 1, Some(0))` reported, reconnects), a corrupted envelope (rejected once, nothing appended, retried), the BlobRef walk |
| `--test isr_publisher --test-threads=1` | 5 passed. A never-fetching follower is not added. A fetching but empty follower is not added and not even pending. A caught-up follower is added. A stalled follower is removed, and while the leader's published ISR held 3 the HW never passed 3's durable end. OutcomeUnknown not applied (pending Remove kept, meta and the log still hold 3, HW ≤ d3, then resolved by the identical re-proposal). OutcomeUnknown applied (adopted from meta). The KIP-497 race (ElectLeader epoch 2 lands first, AlterIsr gets StaleVersion, the publisher exits LostLeadership, HW ≤ d3) |
| `--features failpoints --test fetch_mutants` | 2 passed |
| `cargo test -p ostro-cluster --features failpoints -- --test-threads=1` (the ci-local failpoint step) | all binaries green (lib 13, controller 9, fetch_mutants 2, fetch_service 7, fetcher 9, isr_publisher 5, liveness 8, meta_peer 3) |
| `cargo test -p ostro-log` and `-p ostro-log --features failpoints --test-threads=1` | green (after the truncate and ISR-hook changes) |
| `cargo test -p ostro-repl-conformance --no-fail-fast` | green (fault_layer 8, harness_smoke 2, oracle_selftest 8, runner_selftest 3, skeleton 2). One earlier run under load failed `fault_layer::cut_fails_fast_and_heal_restores` on a timing bound; 3 reruns plus this run were green. It uses the in-memory transport and does not touch the log, so it is unrelated to this plan |
| clippy `-D warnings`, ostro-cluster + ostro-log, all targets, with and without failpoints | clean |
| `cargo fmt --all --check`; `cargo check --workspace --all-targets --locked` after rebasing onto cb7afeb | clean |

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] Follower `truncate_to` kept the dropped epochs' fence**
- **Found during:** Task 2 (two-epoch KIP-279 test)
- **Issue:** After truncating away its epoch-3 records, the follower's sequencer still had `current_epoch = 3`. `append_replica` then refused the leader's epoch-2 records forever.
- **Fix:** On a follower, `truncate_to` now resets `current_epoch` to the newest epoch still in the log. Leaders and the RF=1 repair path keep their fence.
- **Files:** crates/ostro-log/src/partition_log.rs (outside files_modified; 12-08's file, no in-flight owner). **Commit:** 68a578c

**2. [Rule 2 - Correctness] ISR hooks on `PartitionLog`**
- **Issue:** The publisher needs the tracker, the ISR state, the HW and the epoch start for `isr::next_change`. It also needs a pending **add** to count in the maximal ISR before meta commits it (KIP-497: otherwise the HW could pass the joiner's LEO and the joiner would enter the ISR without data below the HW). The trait has no hook for either.
- **Fix:** Added the inherent `PartitionLog::{isr_view, propose_isr_change, clear_isr_change}` and `IsrView`, re-exported from `ostro_log`. No trait change, no format change.
- **Commit:** 285c3ef

**3. Commit granularity / TDD order.** Code and tests were written together and committed per task. There are no separate RED commits. `blob.rs` landed in the Task 1 commit because the test service in `support_fetch` serves `FetchBlob`. Every commit builds.

**4. Signature notes.**
- `IsrPublisher::run` takes an extra `shutdown: CancellationToken` and returns `IsrPublisherExit`.
- `meta` is `Arc<dyn IsrMeta>`, a thin trait implemented for `MetaHandle`, so tests can inject outcomes.
- The plan's "SuspectReporter" is the existing `liveness::SuspectSink`.
- `FetchCtx` is per partition.
- `ReplicationService` (`service.rs`, 12-14's) is untouched. The tests serve their own `TestReplication`.

## Open items

- **Leader tracker after a diverging answer** (12-08's note): the trait has no `on_truncate` hook, so after a diverging fetch the leader's tracker keeps the follower's old durable progress until its next Ok fetch. This is harmless for the HW: a follower never truncates below the HW, and `durable_leo` only rises. It can delay a shrink decision by one fetch at most. If a hook is wanted, `ReplicaTracker::on_truncate` already exists.
- **D-P12-6** is unchanged: SnapshotRequired for an untrimmed follower holding only an older epoch. Snapshot installs stay on the 12-12 `install_from_leader` stub, so for now such a follower retries with backoff.
- **Progress-time target in the long-poll case:** the tracker's caught-up target is the durable end sampled at progress time (before the wait). A response after the wait carries the later pre-read sample. Kafka uses the request-time LEO, so this is the same leniency.
- **The full `scripts/ci-local.sh --no-musl` was not run.** Five parallel executors were running and the disk budget is limited. The ostro-cluster failpoint step, the ostro-log suites, workspace check and fmt ran (table above). The phase gate (12-18) covers the rest.

## Self-Check: PASSED

- Files: fetch.rs, fetcher.rs, blob.rs, isr_publisher.rs and tests/{fetch_service, fetcher, isr_publisher, fetch_mutants}.rs plus tests/support_fetch/mod.rs exist at 285c3ef.
- Commits af9adb9, 68a578c and 285c3ef are in the history of the docs commit.
