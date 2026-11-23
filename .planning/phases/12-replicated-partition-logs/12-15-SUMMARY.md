---
phase: 12-replicated-partition-logs
plan: 15
subsystem: bottomless
tags: [replication, bottomless, backup, fencing, takeover, w4, d-p12-20, d-p12-3, clus-02]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 UploaderClusterDeps / Uploader::with_cluster, mutant name UPLOAD_ABOVE_HW, key p/<pppp>/head and metric ostro_repl_upload_fenced reserved; 12-08 PartitionLog epochs()/read_replica/durable_end/set_published_isr; 12-13 Node::roles() (PartitionRoles::led()), replica services in both roles"
provides:
  - "ostro_backup::cluster: Head {epoch, last_chunk_key, last_chunk_sha256, chain_start, end, snapshot, node_id, members} as JSON at p/<pppp>/head; encode_head/decode_head/read_head/put_head (If-Match / If-None-Match); reconcile_on_takeover(Takeover{store, partition, epoch, node_id, members, log, snapshots}) -> TakeoverOutcome::{Ready(Box<Reconciled>), Fenced{head_epoch}, NoSnapshot{log_start, tail_end}, Interrupted}; METRIC_UPLOAD_FENCED"
  - "Uploader cluster mode: leader-only uploads gated by PartitionRoles::led() each round, takeover reconcile before the first upload of a (p, e), per-partition head fencing, Uploader::partition_fenced(p)"
  - "fence::acquire_if_single_member(store, ident, takeover, members) -> bool"
  - "RestoreError::ClusterBucketUnsupported"
  - "chunk::{envelope_span, recut_prefix}"
  - "failpoint site repl::mutant::upload_above_hw in uploader.rs"
affects: [12-14, 12-16, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "one writer per partition chain: the current leader, fenced by its epoch through the head's etag; a stale epoch never replaces a newer head"
    - "repair = re-cut the straddling chunk under a new key + head PUT, deletes strictly after the head PUT returned"
    - "Phase 8 path untouched when cluster deps are None; cluster mode is a branch inside the same round"
key-files:
  created:
    - crates/ostro-backup/tests/cluster_head.rs
    - crates/ostro-backup/tests/cluster_repair.rs
    - crates/ostro-backup/tests/cluster_mutants.rs
  modified:
    - crates/ostro-backup/src/{cluster,uploader,fence,chunk,restore}.rs
    - crates/ostro-repl-conformance/src/oracle/objstore.rs (decode_head/find_head only, as the coordinator allowed)
key-decisions:
  - "Heads are written only when cluster deps are set (single- or multi-member). With cluster = None the uploader is byte-for-byte Phase 8: no head objects, so every 08 suite runs unchanged"
  - "A consistent takeover writes nothing (as the plan says). A new leader with no new data therefore does not bump the head epoch at once: the old leader is fenced at the new leader's first head write. Until then the old leader can only append committed data, which the new leader also holds"
  - "Divergence uses the new leader's committed log (HW). Valid end per chunk epoch: HW for the latest local epoch, end_offset_for_epoch for an earlier known one, HW for one older than the retained history, and 0 for an epoch the local log never had. The byte check covers envelopes at or above log_start"
  - "Unreachable chunks are deleted by the reconcile right after its head PUT returned (no separate queue). Leftover forks are removed by Phase 8 remote retention, which in cluster mode is restricted to led partitions whose head is current"
  - "In cluster mode a manifest/current precondition failure is retryable (the session re-reads it), not a global fence. The heads fence per partition, and a node that becomes the only member must be able to continue another node's manifest"
  - "The meta snapshot is uploaded only when a manifest can reference it (single member)"
requirements-completed: [CLUS-02]
duration: ~3 h
completed: 2026-11-23
---

# Phase 12 Plan 15: Bottomless in cluster mode Summary

**Only a partition's current leader uploads its chunks now, still HW-capped. Each partition has its own epoch-fenced pointer, `p/<pppp>/head`. A stale leader is fenced for that partition only, and a stale epoch can never move a head. A new leader repairs the remote chain before its first upload. It drops wholly divergent chunks, re-cuts a straddling chunk to its committed prefix, and re-chains from its own snapshot when its `log_start` lies above the remote tail (W4). A single-member cluster still writes `manifest/current` (plus heads), and the 08 restore helper reproduces the live digest from it. Restore refuses a bucket that has heads but no manifest. The `upload_above_hw` mutant bites.**

## Commits

| Commit | Message |
| --- | --- |
| f528824 | feat(12-15): leader-only per-partition uploads with epoch-fenced heads |
| fdb8bb8 | feat(12-15): takeover reconcile of divergent remote tails; restore refuses cluster buckets |

## Head object

`p/<pppp>/head` (JSON, `cluster::encode_head`):

```json
{"format":1,"epoch":3,"last_chunk_key":"p/0000/seg/0000000002-…-….oseg","last_chunk_sha256":"<64 hex>",
 "chain_start":0,"end":7,"snapshot_key":null,"snapshot_offset":null,"snapshot_sha256":null,
 "snapshot_state_hash":null,"node_id":2,"members":2}
```

`last_chunk_key`/`last_chunk_sha256` are `null` for an empty chain at `chain_start` (right after a re-chain, before the tail is uploaded). The snapshot is then the chain base. The 12-03 oracle's `decode_head` now reads `chain_start` as its walk-back stop, and treats a `null` key as nothing to compare. Its old three-field shape still decodes.

## Fencing sequence (per round, cluster deps set)

1. `led = roles.led()`, keeping only partitions that run here with an open barrier and are not fenced at an epoch ≥ the led one. With no led partition the round does nothing (no blobs, no manifest).
2. A led `(p, e)` whose state was reconciled for another epoch (or never) runs `reconcile_on_takeover` first. `Fenced` → the partition is fenced at `e`. `NoSnapshot` → a snapshot is requested and the partition is skipped this round.
3. Chunks `[uploaded_end, HW)` are uploaded as in Phase 8 (`put_immutable`).
4. For each changed partition (new chunks, a new referenced snapshot, or a retention-moved chain start), the head moves with `If-Match: <etag>` (`If-None-Match` when absent). If the precondition fails, the head is re-read. `head.epoch > e` → fenced for p: `ostro_repl_upload_fenced{partition}=1`, the partition state is dropped, and uploads of p stop until this node leads p at a higher epoch. Otherwise the next round reconciles again.
5. Single member: the Phase 8 manifest follows (fenced, excluded and non-led partitions keep their last entry), then floors and retention. Several members: floors (`referenced or chain_start`, capped at the uploaded end) and retention right after the heads, both limited to led, non-excluded partitions.

## Repair sequence (`reconcile_on_takeover`)

1. Read the head (fallback: the `manifest/current` entry of a Phase 8 bucket, at epoch 0). If `head.epoch > e` → `Fenced`.
2. `walk_back` to `chain_start`. A chain that does not verify or does not reach `chain_start` gets re-chained.
3. Scan oldest-first for the first envelope that ends above `valid_end(chunk.epoch)` or whose bytes differ from the local committed log.
4. Gap check: tail end `< log_start` (or `> HW`, or the chain was unusable) → re-chain. Upload the newest local snapshot `S ∈ [log_start, HW]`, set head = empty chain at `S` with the snapshot, then delete the old chain (the whole `seg/` of p if it was not walkable). With no such snapshot: `log_start == 0` restarts at 0, otherwise `NoSnapshot` and nothing is written.
5. Divergence: re-cut the straddling chunk (`chunk::recut_prefix`, same epoch and `prev`, new key), set head = repaired tail (If-Match), and delete every chunk from the divergent one on **after** the head PUT returned. A head conflict at an equal or lower epoch retries (3 attempts); a higher one → `Fenced`.
6. Nothing divergent → `Ready` with the walked chain and no write.

A crash between the re-cut PUT and the head PUT leaves the old head and chain intact. The re-run finds the same re-cut object (`put_immutable` → AlreadyPresent) and converges.

## Members watch semantics

`UploaderClusterDeps.members: watch::Receiver<usize>` is read every round. 0 or 1 means single member: heads + `manifest/current` (+ gen manifests, meta snapshot) are written. 2 or more means heads only: no manifest, no meta snapshot. A manifest precondition failure in cluster mode is retryable. `fence::acquire_if_single_member` skips LOCK for more than one member. The server calls it (12-14), because the server owns LOCK acquisition (`ostro-server/src/services/uploader.rs`, outside this plan).

## Tests

| Suite | Result |
| --- | --- |
| `--test cluster_head --test-threads=1` | 3 passed, 15.6 s (7.0 s in the full run). Leader-only uploads: p1 is not led, so it has no head and no seg/. An idle round leaves the head bytes unchanged. Losing p0 stops its uploads. B takes p0 at epoch 3 (its takeover deletes A's divergent chunks, and its chain equals its committed log). A's next round leaves `partition_fenced(p0)=Some(2)` and p1 still uploading, with `is_fenced()` false. More A rounds and a fresh A uploader at epoch 2 leave B's head bytes untouched. A at epoch 4 lifts the fence and wins, and then B at 3 is fenced. Single member: manifest entries equal the heads (key, sha, chain_start, snapshot), `simulate_restore` reproduces both live digests. With members=2 there is no manifest and heads keep moving. Back to 1, the manifest continues at generation+1 |
| `--test cluster_repair --test-threads=1` | 9 passed, 6.3 s. Whole divergent chunk dropped (head → [5,7)@2, [7,10) deleted). Straddling [5,10)@2 re-cut to [5,7)@2 and linked to sha([0,5)). Crash before the head PUT, then re-run, then a third run that writes nothing. A consistent chain keeps the same head bytes and etag and the same seg/. Same-epoch byte divergence at offset 2 re-cuts [0,2). A higher-epoch head gives `Fenced{5}`. Gap without a snapshot gives `NoSnapshot{20, 10}` and no write. W4 end to end with Nodes: B's log_start is above A's tail, so the head moves to chain_start = B's newest snapshot with the snapshot uploaded and A's chain deleted. The first chunk has prev zero, and the chain reaches B's HW and equals B's committed log. Restore refusal with the exact message, and nothing written |
| `--features failpoints --test cluster_mutants --test-threads=1` | 1 passed, 3.2 s. ISR {1,2}/min_insync 2 holds the HW while a submit becomes durable. Without the mutant the remote end equals HW. With `repl::mutant::upload_above_hw` the remote end equals the durable end, which is above HW |
| `cargo test -p ostro-backup --locked` (every 08 suite unmodified) | lib 13, chain_start 4, cluster_head 3, cut_consistency 2 (383 s under load), fencing 5, remote_gc 6, restore 5 (+1 ignored), uploader 9: all green |
| `cargo test -p ostro-backup --features failpoints --locked -- --test-threads=1` (the Task 2 gate) | 59 passed, 0 failed, 1 ignored: lib 13, chain_start 4, cluster_head 3, cluster_mutants 1, cluster_repair 9, cut_consistency 2 (372 s), fencing 5, remote_gc 6, restore 6, uploader 10 |
| `cargo clippy -p ostro-backup --all-targets` with and without `failpoints`, plus `-p ostro-repl-conformance`, `-D warnings` | clean |
| `cargo test -p ostro-repl-conformance --test oracle_selftest` | 8 passed (old head shape still decodes) |
| `cargo check --workspace --all-targets --locked` | clean |

Not run: the full `scripts/ci-local.sh --no-musl`. Several other executors were building (load 10–13), so the phase gate (12-18) covers it, as for 12-08 and 12-11 to 12-13. FAILPOINT_CRATES and ci.yml are unchanged, and no dependency was added.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] The 12-03 oracle's `decode_head`/`find_head` were adjusted** (`ostro-repl-conformance/src/oracle/objstore.rs`, outside files_modified, which the coordinator allowed). The head needs `chain_start` (retention and re-chains move it, so a walk to `prev == 0` would fail), and an empty chain after a re-chain has no key.

**2. [Rule 1 - Correctness] Manifest precondition failure is not a global fence in cluster mode.** In Phase 8 it fences the whole uploader. With heads doing per-partition fencing, a node that becomes the only member must continue a manifest that another node wrote earlier.

**3. [Rule 2 - Correctness] Retention is restricted to led partitions whose head is current.** `plan_retention` runs over every partition state. A stale state of a partition led elsewhere would otherwise delete that leader's chunks.

**4. Commit granularity.** `cluster.rs` (including `reconcile_on_takeover`) and the `chunk.rs` helpers landed in the Task 1 commit, because the uploader's takeover step needs them. Task 2 holds restore, the oracle tweak and the repair and mutant suites. Tests and code were written together, with no separate RED commit (as in 12-08 and 12-11 to 12-13).

**5. `TakeoverOutcome::Ready` boxes `Reconciled`** (clippy `large_enum_variant`).

## Handed to later plans

- **12-14 (server wiring):** build `UploaderClusterDeps { roles: node.roles(), members: <watch fed by meta Member records> }`, pass it through `Uploader::with_cluster`, and replace `fence::acquire` with `fence::acquire_if_single_member(.., members)` in `services/uploader.rs`.
- **docs/replication.md**: the restore refusal message points at it. Whichever plan owns the docs must describe the cluster bucket layout and the deferred cluster-wide restore (Phase 13/14).
- **Residual:** a stale `manifest/current` from a single-member period stays restorable after the cluster grows (restore checks heads only when the manifest is missing). A cluster-wide restore (Phase 13/14) must treat heads with `members > 1` as authoritative.
- The old leader can still PUT orphan chunks (its own committed data) between its last read and the fencing conflict. They are unreachable, and remote retention removes them.

## Self-Check: PASSED

- Files: crates/ostro-backup/src/cluster.rs (contains `pub async fn reconcile_on_takeover`, calls `chunk::walk_back`), tests/cluster_head.rs, tests/cluster_repair.rs (contains "straddling"), tests/cluster_mutants.rs exist; uploader.rs gates on `roles.led()`.
- Commits e31a8ae and 3fa080e exist on main.
