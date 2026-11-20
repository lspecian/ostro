---
phase: 08-triggers-bottomless-observability
plan: 18
subsystem: storage
tags: [bottomless, restore, backup, tar, meta-bootstrap, wal-import, cli, back-02, back-03]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-02 install_snapshot/verify_snapshot; 08-08 read_envelopes, applied_state, new_incarnation/hex16; 08-09 snapshot_now, local snapshots, "snapshot required" rule; 08-10 AdminApi/BackupStream + ControlCtx; 08-12 CLI conventions; 08-13 chunk walk_back/verify_chain, Manifest validate/decode, fence, Uploader::round; 08-16 (wave order)
provides:
  - "MetaHandle::bootstrap_from_snapshot(dir, node_id, index, bytes): native raft snapshot install into an empty metadata log"
  - "ostro_log::import::write_partition_log(wal_root, p, batches, upto, ImportMode::{Create,Append}) -> ImportReport"
  - "ostro_backup::restore::{restore, RestoreSource::{Url,Tar,Store}, RestoreOptions, At::{Generation,TimeMs}, RestoreReport, PartitionReport, RestoreDurations, RestoreError}"
  - "ostro_backup::archive::{pack_dir, unpack, backup_node_to_tar, backup_offline, node_file_partitions, BackupReport, BackupError}"
  - "Uploader::for_inspect_node() (offline backup over a closed-barrier Inspect node)"
  - "GET /v1/admin/backup implemented (control/admin.rs)"
  - "ostro backup --to f.tar [--server | --data-dir]; ostro restore --from <s3://|file://|f.tar> --data-dir [--at] [--takeover] [--json]"
  - "docs/bottomless.md Restore runbook"
affects: [08-20 restore_rpo / bottomless_s3 / SC3 timing, 08-22 gate, Phase 12 follower install (empty-log anchor + native meta install)]

tech-stack:
  added: []
  patterns:
    - "Two-step verified replay in files: import to verify_offset, Inspect open, compare state_hash; append to the cut, Inspect again (no HW-capped open needed)"
    - "Backup file = tar of the bottomless LocalFs layout; restore from a tar = unpack + the LocalFs code path"
    - "Offline tools never write the source: meta started on a temp copy, node in Inspect mode, LOCK try_lock refuses a running server"

key-files:
  created:
    - crates/ostro-log/src/import.rs
    - crates/ostro-log/tests/import.rs
    - crates/ostro-meta/tests/bootstrap_snapshot.rs
    - crates/ostro-backup/tests/restore.rs
    - crates/ostro-cli/src/commands/backup.rs
    - crates/ostro/tests/backup_cli.rs
  modified:
    - crates/ostro-meta/src/handle.rs
    - crates/ostro-log/src/lib.rs
    - crates/ostro-backup/src/{restore,archive,uploader}.rs
    - crates/ostro-server/src/control/{admin,mod}.rs
    - crates/ostro-server/tests/phase8_smoke.rs
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro/src/main.rs
    - docs/bottomless.md

key-decisions:
  - "Meta bootstrap spike: the native path works. The snapshot is written into an empty RaftLogStore with apply_snapshot (index from the meta/<index>.msnap key, term 1, voters {node_id}) and RaftDriver::start restores it; MetaState is byte-identical (updated_index included), proposals continue after the index. No fallback (synthetic Put stream) needed"
  - "Restore starts each partition log exactly at the snapshot offset S (envelopes below S skipped, a batch straddling S is an error); walk_back stop = snapshot_offset.unwrap_or(chain_start), valid for current and older generations"
  - "verify_offset must be a batch boundary (checked before import); write_partition_log rejects a mid-batch cut"
  - "Inspect mode runs no snapshot service, so the offline backup stages each partition's newest verified local snapshot under its snap/ key before the round, and the uploader gains for_inspect_node() to skip the barrier check"
  - "Offline backup starts the metadata group on a temporary copy of meta/ (starting it appends a term entry); the source data directory is never written"
  - "restore's node_id defaults to the manifest's node_id (the metadata voter id must match the server's [node] node_id)"
  - "CLI: restore and offline backup run in the ostro binary (they need ostro-backup); ostro-cli holds the clap args, the online download and the report printer, so ostro-cli gains no dependency"
  - "No --tls-roots webpki in the CLI: the webpki bundle is excluded (D-FX2), only the platform store or --tls-roots-file"

requirements-completed: [BACK-02, BACK-03]

duration: ~1h45m
completed: 2026-11-20
---

# Phase 8 Plan 18: Restore + backup files Summary

**`ostro restore` rebuilds a data directory from a bucket prefix or a tar backup file: manifest validated before any write, new incarnation, native metadata snapshot install, sha-verified blobs and snapshots, chunk chains walked back from the manifest head, a two-step in-files replay that checks every partition's state hash at its verify offset, and a cross-partition consistency check. `ostro backup` writes the same layout as a tar, online (`GET /v1/admin/backup`) or offline (Inspect mode), and restores through the same LocalFs code path.**

## Meta bootstrap spike (Open Question 5)

**Outcome: native path.** `MetaHandle::bootstrap_from_snapshot` decodes the snapshot (a bad one fails before anything is written), opens an empty `RaftLogStore` (an existing log or snapshot is refused), and calls `apply_snapshot` with `index` (from `meta/<index>.msnap`), term 1 and `conf_state.voters = [node_id]`, exactly as a follower installs a leader's snapshot. It then starts normally: `RaftDriver::start` sees a non-blank store and restores the state machine, and the lone voter campaigns. Results:

- the restored `encode_snapshot` bytes equal the source's (byte-identical state, `updated_index` of every record included);
- the applied index is at least the snapshot index; the next proposal lands after it;
- `InitCluster` with the same partition count stays idempotent (the server start path), and a restart replays snapshot + new entries.

No synthetic-Put fallback was needed, so no index fidelity is lost.

## RestoreReport format

`ostro restore --json` prints the serialized `RestoreReport`:

```json
{
  "data_dir": "/var/lib/ostro-restored", "source": "s3://bucket/prefix",
  "generation": 812, "cluster_id": "c-…", "node_id": 1,
  "source_incarnation": "<32 hex>", "incarnation": "<32 hex, new>",
  "cut_at_ms": 1763648400123, "meta_index": 2210, "blobs": 41, "takeover": false,
  "partitions": [
    {"p": 0, "snapshot_offset": 10240, "chain_start": 10240, "verify_offset": 10377,
     "verify_state_hash": "<64 hex>", "cut_offset": 10377, "digest": "<64 hex>",
     "chunks": 3, "envelopes": 12}
  ],
  "durations": {"manifest_ms": 12, "meta_ms": 40, "blobs_ms": 210, "partitions_ms": 950,
                "replay_ms": 610, "total_ms": 1830}
}
```

Without `--json`: one summary line, a per-partition table (`P SNAPSHOT VERIFY CUT CHUNKS STATE_HASH DIGEST`, hashes shortened to 16 hex chars), the verification line, durations, and the takeover / new-prefix hint. `node.toml` gains `restored_from = "<source>"` (additive key, ignored by the reader).

## Restore timings (P=16, ~10k runs, LocalFs)

`restore_timing_p16_10k_runs` (ignored test; debug build; load average 5–9 from parallel executors):

- 16 partitions, 9,920 runs plus LeaderAnnounced records (9,936 commands); a snapshot per partition after 9k runs, 1k runs in the tail;
- upload round into a fresh LocalFs prefix: 0.93 s;
- restore with `--parallelism 4`: **total 9.0 s** = partitions 8.2 s (16 snapshot installs: fjall store creation plus staged install fsyncs dominate) + replay 0.78 s (two Inspect opens, tail append, consistency check); manifest, meta and blobs about 0 ms (no meta view, no blobs in this run).

The spawned-binary round trip (P=4, 6 runs, meta, blobs) restores in under a second per run. SC3 timing/RPO proof belongs to 08-20.

## Restore pipeline (as built)

1. Non-empty data dir → `RestoreError::NotEmpty` ("delete the directory and retry").
2. Manifest: `manifest/current`, `--at <gen>` (`manifest/gen/<g>` or current), `--at <time>` (newest cut at or before). `manifest::decode` checks the sha trailer, format and `validate()`; failures are `BadManifest` and nothing is written (the data dir is not even created).
3. Partition count: from the meta snapshot's `ClusterInfo`, cross-checked against the cut (every partition needs an entry).
4. `node.toml` (format 1, P, new incarnation, restored_from), durably.
5. Meta bootstrap (sha verified); failpoint `restore::after_meta`.
6. Blobs: every `blobs/<sha>`, content sha checked before the local put.
7. Partitions in parallel: snapshot sha → `SnapshotSha{partition}`; `install_snapshot` (applied offset must equal `snapshot_offset`); `walk_back` from the head + `verify_chain` → `Chain{partition}`; chain must reach the restore start and end at the cut; `write_partition_log(.., verify_offset, Create)`; failpoint `restore::mid_partition`.
8. Inspect open: `applied_state` must be `(verify_offset, verify_state_hash)` → `HashMismatch{partition}`.
9. `write_partition_log(.., cut, Append)`, Inspect open again: applied = cut, digests, and Pitfall 1 (`outbox_truncated[A→B] <= inbound_seen_B[A] < outbox_next_seq[A→B]`) → `Inconsistent`.
10. A Leader-mode open of the restored directory works (tested); an idle partition opens through the empty-log anchor.

## Task Commits

1. **Task 1: meta bootstrap (native) + WAL import**: `0e1dc71`
2. **Task 2: restore pipeline, archive, online backup endpoint**: `aeacc41`
3. **Task 3: CLI verbs, spawned-binary tests, restore runbook**: `8a86117`

## Verification

All runs in the worktree with `CARGO_TARGET_DIR=target/wt-08-18`.

- `cargo test -p ostro-meta --test bootstrap_snapshot`: ok, 2 passed. `cargo test -p ostro-log --test import`: ok, 5 passed. `cargo test -p ostro-meta -p ostro-log --locked`: every binary ok.
- `cargo test -p ostro-backup --test restore --locked`: ok, 5 passed (+1 ignored timing test). With `--features failpoints -- --test-threads=1`: ok, 6 passed (adds the interrupted-restore test for both failpoints).
- `cargo test -p ostro-server --test phase8_smoke`: ok, 1 passed (the admin/backup row now checks a real `application/x-tar` answer holding `manifest/current` and `LOCK`).
- `cargo test -p ostro-cli -p ostro --locked`: every binary ok, including `backup_cli` 2 passed (bucket restore after SIGKILL with the runs served by a server on the restored dir, online + offline tar round trips with equal digests across all three restores, refusals, help/usage) and ostro-cli lib 41.
- `cargo clippy -p ostro-meta -p ostro-log -p ostro-backup -p ostro-server -p ostro-cli -p ostro --all-targets --locked -- -D warnings`: clean; also with the ostro-meta/ostro-log/ostro-backup failpoints features. `cargo fmt` clean on the touched crates. No Cargo.toml / Cargo.lock change.
- Full suites after the last commit: `cargo test -p ostro-backup --features failpoints --locked -- --test-threads=1`: lib 10, chain_start 4, cut_consistency 2 (326 s), fencing 5, remote_gc 6, restore 6 (+1 ignored), uploader 8, all ok. `cargo test -p ostro-backup -p ostro-server --locked`: every binary ok (ostro-server lib 78, bootstrap 5, bottomless_service 1, phase8_smoke 1, sdk_e2e 9, semantics_e2e 18, sink 19, …; perf and semantics_sleepers ignored by design). `cargo clippy -p ostro --features failpoints --all-targets -D warnings`: clean.
- `bash scripts/ci-local.sh --no-musl` was **not run** (other executors were building; the brief caps concurrent builds). Gate it at the wave boundary.

## Deviations from Plan

1. **[Rule 3] `Uploader::for_inspect_node()` in `crates/ostro-backup/src/uploader.rs`** (08-13's file, not in files_modified). `Uploader::round` returns `barrier_closed` for a node whose barriers are closed, which is always the case in Inspect mode, so the offline backup could not run "the same round". A builder flag skips only that check (13 lines).
2. **[Rule 3] `not_yet()` removed from `crates/ostro-server/src/control/mod.rs`** and the controller table updated: admin was the last stub, so the helper became dead code (a warning). Not in files_modified.
3. **`crates/ostro-server/tests/phase8_smoke.rs`** updated as the brief asked (not in files_modified).
4. **`sm.rs` unchanged**: the native install needed no state-machine change.
5. **Bootstrap signature** takes `node_id` and `index` besides the snapshot bytes (the plan's `bootstrap_from_snapshot(data_dir, snapshot_bytes)`): the voter id and the raft index are needed for a faithful native install.
6. **Restore digests are compared with 08-13's `simulate_restore`** (the reference restore of the same manifest) and, where the source is stopped, with an Inspect open of the source; the plan's "Inspect open of the source truncated at cut" is equivalent and would need a WAL truncation tool.
7. **CLI split**: the verbs are top-level subcommands of the binary (`Command::Backup/Restore`) with their args in ostro-cli, not `ClientCommand` variants (dispatch there always builds a REST client, and restore needs ostro-backup).
8. **Online backup streams from a temp file** (packed first, then `ReaderStream`), not a tar writer over the response: simpler error handling (a failed round answers 503 instead of a truncated 200).
9. **`--tls-roots webpki` not offered** (D-FX2); `--tls-roots-file` only.

## Follow-ups

- 08-20: restore_rpo / bottomless_s3 can call `ostro_backup::restore::restore` directly (`RestoreSource::Url` with `fence::store_options`, or `Store`); the S3 leg SKIPs when `OSTRO_TEST_S3_URL` is unset.
- Restored nodes have no local `.osnap` files until the snapshot service takes one; copying the installed remote snapshot into `snapshots/pNNNN/` would shorten that window.
- Partition restore time is dominated by fjall install fsyncs; raising `--parallelism` helps on fast disks.

## Self-Check: PASSED
