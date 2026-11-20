---
phase: 08-triggers-bottomless-observability
plan: 13
subsystem: storage
tags: [bottomless, oseg, manifest, fencing, uploader, retention, upload-floor, back-01]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-03 ostro-objstore (frozen trait, LocalFs, S3Store); 08-08 HwVector cut, read_envelopes, applied_state, blob put observer, incarnation; 08-09 snapshot service, floor_handles, local_snapshots, snapshot_now, retention_pass; 08-10 tests/common/phase8.rs
provides:
  - "ostro_backup::chunk: OSEG v1 encode/decode/peek_header, verify_chain(ChainStart), walk_back(store, head_key, head_sha, stop), chunk_key/parse_chunk_key"
  - "ostro_backup::manifest: Manifest v1 (postcard + sha256), Manifest::validate, encode/decode (decode validates), gen_key/parse_gen_key, CURRENT_KEY"
  - "ostro_backup::fence: Ident, acquire(takeover), check_before_manifest (LOCK re-read fallback), put_immutable (W9), store_options(BackupConfig)"
  - "ostro_backup::uploader: Uploader::new/round(store)/run, UploaderConfig, RoundReport, key helpers, metric names, failpoint upload::before_manifest"
  - "ostro-server services::uploader (spawned when [backup] url is set)"
  - "docs/bottomless.md"
affects: [08-18 restore + online backup, 08-19 remote blob GC, 08-20 restore_rpo / bottomless_s3, 08-22 gate, Phase 12 chain repair]

tech-stack:
  added: []
  patterns:
    - "Applied pairs read before the HW cut (W2); chunks capped at the cut; snapshots uploaded eagerly but referenced only once a cut covers them (B3)"
    - "Per-store session state rebuilt from manifest/current; reset after any failed round (idempotent re-puts)"
    - "Retention moves chain_start up in the same manifest, deletes only after it is durable"

key-files:
  created:
    - crates/ostro-backup/tests/common/mod.rs
    - crates/ostro-backup/tests/fencing.rs
    - crates/ostro-backup/tests/uploader.rs
    - crates/ostro-backup/tests/chain_start.rs
    - crates/ostro-backup/tests/cut_consistency.rs
    - crates/ostro-server/tests/bottomless_service.rs
    - docs/bottomless.md
  modified:
    - crates/ostro-backup/src/{chunk,manifest,fence,uploader,lib}.rs
    - crates/ostro-backup/Cargo.toml
    - Cargo.lock
    - crates/ostro-server/src/services/uploader.rs

key-decisions:
  - "Manifest field is `generation`, not `gen` (gen is a reserved keyword in edition 2024)"
  - "A partition skipped by the chain start rule (no usable snapshot) blocks the manifest for that round and requests a snapshot (node.snapshot_now), so no manifest is ever cross-partition inconsistent"
  - "Chunk identity = sha256 of the whole object (header, body, trailer); prev_chunk_sha256 and last_chunk_sha256 use it"
  - "Snapshots are uploaded with one put_if_absent (immutability needs a conditional PUT; S3 multipart cannot be conditional), not put_multipart"
  - "walk_back stops at the first chunk with base <= stop: restore passes chain_start for current, snapshot_offset for older gens (retention may have removed chunks before an older gen's chain_start)"
  - "Meta snapshot = ostro_meta::encode_snapshot under the view's read lock, index = view.applied() read inside the lock (the state may be at most one entry ahead of the index name)"

requirements-completed: [BACK-01]

duration: ~2h
completed: 2026-11-20
---

# Phase 8 Plan 13: Bottomless Uploader Summary

**A node with `[backup] url` now streams OSEG v1 chunks (raw OSTR envelopes, hash-chained, epoch-split, at most 8 MiB), snapshots, a meta snapshot and blobs to object storage under one causally consistent cut per tick, with a validated manifest written through `If-Match`, LOCK/incarnation fencing, immutable puts, the B4 chain start rule, B3 snapshot hold-back, the upload floor feeding WAL trimming, and remote retention.**

## APIs for 08-18 (restore / online backup)

```rust
// manifest
pub struct Manifest { format, cluster_id, node_id, incarnation: [u8;16], generation, prev_manifest_sha256,
                      cut_at_ms, cut: Vec<PartitionCut>, meta_snapshot_key, meta_snapshot_sha256, ostro_version }
pub struct PartitionCut { p, cut_offset, chain_start, last_chunk_key, last_chunk_sha256, snapshot_key,
                          snapshot_sha256, snapshot_offset, verify_offset, verify_state_hash }
impl Manifest { pub fn validate(&self) -> Result<(), ManifestError>; pub fn partition(&self, p) -> Option<&PartitionCut> }
pub fn decode(bytes) -> Result<Manifest, ManifestError>;   // sha trailer + format + validate()
pub fn encode(&Manifest) -> Vec<u8>; pub fn object_sha(bytes) -> [u8;32];
pub const CURRENT_KEY: &str = "manifest/current"; pub fn gen_key(g) -> String; pub fn parse_gen_key(k) -> Option<u64>;
// chunk
pub async fn walk_back(store: &Arc<dyn ObjectStore>, last_chunk_key: &str, last_chunk_sha256: [u8;32], stop: u64)
    -> Result<Vec<ChunkRef{key, chunk: Chunk{header, envelopes, sha256}}>, ChunkError>;  // forward order, chain-verified
pub fn decode(bytes) -> Result<Chunk, ChunkError>; pub fn verify_chain(ChainStart{base, prev_sha}, &[Chunk]);
// uploader (online backup: publish_floor = false, retention = false, a temp LocalFs store)
pub struct UploaderConfig { seal_interval, remote_snapshots_keep, cluster_id, node_id, max_chunk_bytes, publish_floor, retention }
impl Uploader {
    pub fn new(node: Arc<Node>, meta: Option<MetaView>, clock: Arc<dyn Clock>, cfg: UploaderConfig) -> Self;
    pub async fn round(&self, store: &Arc<dyn ObjectStore>) -> Result<RoundReport, UploadError>;
    pub async fn run(self: Arc<Self>, store, shutdown: watch::Receiver<bool>);
}
// keys: snap_key/parse_snap_key ("p/<pppp>/snap/<applied 20d>-<sha hex>.osnap"), meta_key ("meta/<index 20d>.msnap"), blob_key ("blobs/<hex>")
```

Restore notes: `simulate_restore` in `crates/ostro-backup/tests/common/mod.rs` is a working reference: install the snapshot (sha checked), `walk_back(.., stop = snapshot_offset.unwrap_or(chain_start))`, skip envelopes below the snapshot offset, apply the rest with `apply_command`, check `state_hash` at `verify_offset`, end exactly at `cut_offset`. Old chunks of a restarted chain stay unreachable from the head and are ignored.

## Upload round timings (P=16, LocalFs, debug build, loaded machine)

`cut_lag_per_tick_at_p16` (seal 250 ms, all 16 partitions busy, ~1600 runs/s offered by the test writer, load average 10–18 from parallel executors):

- 12 rounds, 19,200 runs started, 192 chunks, 1.60 MB of chunk bytes.
- **Upload latency** (`ostro_backup_upload_seconds`, round duration): p50 **1.02 s**, max **9.73 s**. LocalFs fsyncs each object, its parent directory and the rename (about 17 objects per round at P=16), so the latency is fsync-bound under this load.
- **Cut lag**: the gap between consecutive cut reads minus the previous round's duration stayed within seal + 500 ms slack on every tick, and the cut age at manifest time minus the round duration stayed within the seal interval (asserted; the max cut age at manifest time was 9.73 s, which is all upload latency).
- So, per P8-D23, RPO ≈ seal_interval + upload latency. Uploads are sequential per partition. Parallelising them across partitions is the obvious next lever if the 08-20 RPO numbers need it.

## Fencing

- **Conditional PUT (LocalFs emulated; S3/SeaweedFS):** LOCK `If-None-Match: *`, takeover via `If-Match` on the LOCK etag, manifest `If-Match` on the previous etag (`If-None-Match` for the first). A second uploader that resumed the prefix makes the first one's next manifest PUT fail: `UploadError::Fenced("...precondition failed")`, `ostro_backup_fenced = 1`, and every later round refuses (test `second_uploader_is_fenced_by_manifest_if_match`).
- **Fallback (`StoreOptions::conditional_put = false`):** `check_before_manifest` re-reads LOCK and compares the identity, and `put_immutable` HEADs before an unconditional PUT. A replaced LOCK is detected (test `fallback_without_conditional_put_detects_lock_change`). Documented as weaker.
- **Immutable objects (W9):** the same sha is `AlreadyPresent`; a different sha is `Fenced`.
- **S3 leg:** `s3_fencing` is env-gated on `OSTRO_TEST_S3_*` (SeaweedFS in CI). Locally it prints `SKIP (human_needed)`.

## Chain start cases (B4, tests/chain_start.rs)

| Case | Setup | Result |
|------|-------|--------|
| (a) | fresh prefix, log_start 0, no snapshots | chain_start 0, no snapshot_key, validates, restore digest == live |
| (b) | backup enabled on a node trimmed while backup was off | both local snapshots uploaded, chain_start = newest snapshot, first chunk prev = zero, restore digest == live |
| (c) | retention pass before the first upload | no trim (floor Some(0)); after the manifest the floor = snapshot offset and trimming resumes up to it |
| (d) | uploaded_end < log_start after an outage | generation continues, chain restarts at the newest snapshot, old chain chunks deleted by retention, restore ok |

## Task Commits

1. **Task 1 (+ uploader core, test common, fencing tests):** `29938fb`
2. **Task 2 (uploader/chain_start/cut_consistency suites, session reset on failure):** `6450244`
3. **Task 3 (server service, bottomless_service test, docs):** `c50e23b`

## Verification

- `cargo test -p ostro-backup --lib`: ok, 10 passed.
- `--test fencing`: ok, 5 passed (s3_fencing SKIP, human_needed).
- `--test uploader`: ok, 7 passed. The failpoint test passed with `--features failpoints`.
- `--test chain_start`: ok, 4 passed.
- `--test cut_consistency`: ok, 2 passed (24 proptest cases plus the B3 case; 566 s under load).
- `cargo test -p ostro-server --test bottomless_service`: ok, 1 passed.
- `cargo clippy -p ostro-backup --all-targets -D warnings`, with and without failpoints, and `-p ostro-server --all-targets`: clean. `cargo fmt` clean on the touched files.
- Re-run after rebasing onto main c50e23b (06-14 and 08-14 had landed): `cargo build -p ostro-server -p ostro-backup --all-targets` produced no warnings, and clippy `-D warnings` on both was clean. `bottomless_service` passed 1. `cargo test -p ostro-backup --features failpoints --locked -- --test-threads=1`: lib 10, chain_start 4, cut_consistency 2 (498 s), fencing 5 and uploader 8 passed, all ok.
- `bash scripts/ci-local.sh --no-musl` was **not run** (a full ci-local run and other executors were already running). Gate it at the wave boundary.

## Deviations from Plan

1. **[Rule 3] dev-dependencies.** The tests need `tempfile`, `proptest` and `bytes` (all workspace pins already in the lock). The plan said "no Cargo.toml/Cargo.lock changes"; the lock diff is three lines in ostro-backup's dependency list, with no new crates.
2. **[Process] Task 1's commit also holds the uploader core and `tests/common`.** The fencing suite's manifest-If-Match test needs two real uploaders.
3. **`gen` → `generation`** (reserved keyword in Rust 2024).
4. **Snapshots use a single `put_if_absent`, not multipart** (see key-decisions). S3 single PUT allows up to 5 GiB.
5. **Workload in cut_consistency is signals only** (cross-partition outbox traffic). The outbox seq / inbound_seen mechanics are the same for StartChild, which needs the parent journal op wiring.
6. **Skipped partition → no manifest that round** plus a snapshot request (the plan only said "skipped"). This keeps every manifest cross-partition consistent. It is reachable only in edge cases, because a trimmed log always keeps a retained snapshot.
7. **Blob ordering test** compares LocalFs mtimes (blob ≤ first chunk) plus the code order; there is no recording store, because implementing `ObjectStore` in a test needs async-trait.
8. **Meta snapshot test** uses an empty `MetaSm` view. Driving the meta index needs Raft. The server test checks that a real meta snapshot is uploaded and named.

## Self-Check: PASSED
