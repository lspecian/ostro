---
phase: 08-triggers-bottomless-observability
plan: 08
subsystem: storage
tags: [bottomless, hw-cut, raw-envelopes, blob-observer, node-incarnation, backup]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 contract (node.toml `incarnation` name reserved, P8 RESERVATIONS §11)
provides:
  - HwVector + LogNode::cut(): causally closed per-partition HW cut, updated at the single HW publication point publish_hw (before the watch is published)
  - ReplicatedLog::read_envelopes -> Vec<RawBatch>: HW-capped stored OSTR envelope bytes, per-epoch grouping, batch boundaries kept
  - BlobStore::subscribe_puts(): broadcast of every successful put, Lagged on overflow
  - node.toml `incarnation` key (128-bit, 32 hex), Node::incarnation()
  - Node::log_cut(), Node::read_envelopes(), Node::applied_state() (atomic applied_offset + state_hash)
affects: [08-13 uploader/fencing, 08-18 restore, Phase 12 12-04 tracker-driven HW]

tech-stack:
  added: []
  patterns:
    - "Single HW publication point: vector update under its lock, release, then send_if_modified (W7 / F12a), grep-tested"
    - "Atomic meta pair: one StoreReader prefix scan over the meta keyspace = one fjall snapshot"

key-files:
  created:
    - crates/ostro-log/src/cut.rs
    - crates/ostro-log/tests/hw_cut.rs
    - crates/ostro-log/tests/read_envelopes.rs
    - crates/ostro-blob/tests/observer.rs
    - crates/ostro-partition/tests/node_incarnation.rs
  modified:
    - crates/ostro-log/src/lib.rs
    - crates/ostro-log/src/node.rs
    - crates/ostro-log/src/replicated.rs
    - crates/ostro-log/src/rf1.rs
    - crates/ostro-blob/src/lib.rs
    - crates/ostro-blob/Cargo.toml
    - crates/ostro-partition/src/node.rs
    - Cargo.lock

key-decisions:
  - "retract_hw is the second (truncation-only) HW watch writer next to publish_hw; the grep test pins exactly these two in cut.rs"
  - "Node::applied_state reads both meta rows from one fjall snapshot (StoreReader::prefix over Table::Meta), no store/* change"
  - "Incarnation entropy without a new dependency: BLAKE3 over /dev/urandom (when readable) + std RandomState (OS-seeded) + pid + counter; no clock read"
  - "Node::read_envelopes returns LogError::Failed for a partition that did not start (no new NodeError variant)"
  - "ostro-blob takes the tokio workspace dependency for the broadcast channel (one-line Cargo.lock change)"

requirements-completed: []  # BACK-01 completes with 08-13 (uploader)

duration: ~1h (Task 1 in an earlier run, Task 2 + summary in this run)
completed: 2026-11-20
---

# Phase 8 Plan 08: Bottomless Primitives Summary

**The uploader (08-13) and restore (08-18) now have their low-level pieces: a causally closed HW cut fed by one publication point, raw OSTR envelope reads, a blob put observer with lag detection, a stable per-data-dir `incarnation` in node.toml, and Node pass-throughs including an atomic `(applied_offset, state_hash)` read.**

## Tasks

1. **Task 1: HwVector consistent cut + raw envelope reads**: `5937dff` (earlier run; re-verified here: hw_cut 4 passed, read_envelopes 2 passed).
2. **Task 2: BlobStore put observer + node.toml incarnation + Node pass-throughs**: `92a7802`.

## Signatures for 08-13 / 08-18

```rust
// ostro-log
pub struct HwVector { /* Mutex<Vec<Offset>> + per-partition HW watch senders */ }
impl HwVector {
    pub fn publish_hw(&self, p: PartitionId, hw: Offset); // sole raising writer: vector, then watch
    pub fn cut(&self) -> Vec<Offset>;
    pub fn subscribe(&self, p: PartitionId) -> Option<watch::Receiver<Offset>>;
}
impl LogNode { pub fn cut(&self) -> Vec<Offset>; pub fn hw_vector(&self) -> &Arc<HwVector>; }
pub struct RawBatch { pub base: Offset, pub end: Offset, pub epoch: u32, pub envelopes: Vec<Bytes> }
// trait ReplicatedLog
fn read_envelopes(&self, from: Offset, max_bytes: usize)
    -> impl Future<Output = Result<Vec<RawBatch>, LogError>> + Send; // HW-capped

// ostro-blob
pub const PUT_EVENTS_CAPACITY: usize = 4096;
impl BlobStore { pub fn subscribe_puts(&self) -> tokio::sync::broadcast::Receiver<[u8; 32]>; }

// ostro-partition
impl Node {
    pub fn incarnation(&self) -> [u8; 16];
    pub fn log_cut(&self) -> Vec<Offset>;
    pub async fn read_envelopes(&self, p: PartitionId, from: Offset, max_bytes: usize)
        -> Result<Vec<RawBatch>, LogError>;           // LogError::Failed if p did not start
    pub fn applied_state(&self, p: PartitionId) -> Option<(Offset, [u8; 32])>;
}
pub fn ostro_partition::node::new_incarnation() -> [u8; 16]; // restore writes a fresh one
pub fn ostro_partition::node::hex16(&[u8; 16]) -> String;
```

node.toml line format: `incarnation = "<32 lowercase hex>"`. A file without it gains the line through an append-only rewrite (existing bytes kept, tmp + fsync + rename + dir fsync). A malformed value is a `NodeError::NodeFile`. Restore (08-18) has no helper to rewrite an existing key yet: it writes node.toml fresh with `new_incarnation()`.

## Measurements

- **HwVector overhead** (`publish_hw_cost_report`, release, single thread, 1M iterations): 45.8 ns per `publish_hw` vs 42.4 ns for the bare `send_if_modified`, so **about 3.4 ns per group commit**. `cut()` costs 15.6 ns.
- **Causal closure** (`causal_closure`, release): 256 cases, 148,266,925 concurrent cuts checked, no violation.
- **Negative control** (publish first, then a 50 ms delay before the vector update on partition A): the violation showed up on **iteration 1** (1 of 524,290 cuts had `cut[B] >= y` with `cut[A] < x`). The control is deterministic, not skipped.

## Verification

- `cargo test -p ostro-log -p ostro-blob -p ostro-partition --locked`: every binary reported `test result: ok`, 0 failed (largest: 26, 23 and 22 passed; slowest 417.69 s and 110.19 s under load average around 15).
- Failpoint suites: ostro-log `fatal_io` 3 passed, ostro-blob `blob_failpoints` 2 passed, ostro-partition `fatal_io_state` 4 passed and `dispatcher_failpoints` 3 passed.
- New tests: `observer` 5 passed, `node_incarnation` 5 passed, `node::tests::node_file_parse` passed.
- `cargo clippy -p ostro-log -p ostro-blob -p ostro-partition --all-targets --locked -- -D warnings` was clean, both with and without the failpoints features. `cargo build --workspace --all-targets --locked` produced no warnings after rebasing onto current main.
- `bash scripts/ci-local.sh --no-musl` was **not run**: four other executors were building at the same time, so it would have competed for the machine. Gate it at the wave/phase boundary.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] ostro-blob needed the tokio dependency**
- **Found during:** Task 2
- **Issue:** The plan's interface returns `tokio::sync::broadcast::Receiver`, but ostro-blob did not depend on tokio, and the phase rule says "no Cargo.toml/Cargo.lock changes".
- **Fix:** Added `tokio.workspace = true` to `crates/ostro-blob/Cargo.toml`. tokio was already in the graph, so Cargo.lock changed by one line (`"tokio"` in ostro-blob's dependency list). No new crate.
- **Files modified:** crates/ostro-blob/Cargo.toml, Cargo.lock
- **Commit:** 92a7802

**2. Concurrency in the applied_state test**
- The plan wanted "every returned pair matches a replay to that offset". The test replays the node's log into a fresh `PartitionStore` with `apply_command` and records `(next, hash)` after each command. It then checks that every pair the poller saw is in that map, and that the poller saw more than one distinct offset.

## Next Phase Readiness

- 08-13 can use `Node::log_cut`, `Node::read_envelopes`, `Node::applied_state`, `Node::incarnation` and `BlobStore::subscribe_puts` (reconcile with a listing on `Lagged`).
- 08-18 uses `node::new_incarnation()` when writing the restored node.toml.
- Phase 12 12-04 publishes tracker-driven HW through `HwVector::publish_hw`.

## Self-Check: PASSED

- Files: cut.rs, hw_cut.rs, read_envelopes.rs, observer.rs, node_incarnation.rs present on main.
- Commits: 5937dff and 92a7802 present on main.
