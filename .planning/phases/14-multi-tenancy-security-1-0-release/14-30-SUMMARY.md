---
phase: 14-multi-tenancy-security-1-0-release
plan: 30
subsystem: bottomless backup (whole-cluster restore) + tier-C conformance
tags: [backup, restore, cluster, consistent-cut, seed, join, BACK-05]
requires:
  - phase: 14-29
    provides: "meta/head + cluster-mode meta snapshots, replay observer (CursorRecorders), cluster_cut::search"
  - phase: 08-18
    provides: "restore.rs (verified chain walk, write_partition_log, Inspect replay, check_consistency, blobs)"
  - phase: 13-09/13-10
    provides: "ostro cluster init/join, MemberState tombstones, placement controller RF raise"
provides:
  - "ostro_backup::cluster_restore::{restore_cluster, ClusterRestoreReport, ClusterPartitionReport}; detection inside restore::restore"
  - "RestoreError::NoConsistentCut; RestoreReport.cluster (absent on the Phase 8 path)"
  - "mutant repl::mutant::restore_skip_fixpoint (const local to cluster_restore.rs)"
  - "tier-C process_cluster_restore (file_bucket + s3_bucket), docs/cluster-restore.md"
affects: [14-27, 14-24, 14-25]
tech-stack:
  added: []
  patterns:
    - "two independent replays (scan from the floor, final from the newest snapshot below the cut) must agree on the cursors at the cut"
    - "cut search over step functions sampled at envelope boundaries, results rounded down to a boundary (values constant between boundaries)"
    - "metadata rewrite through MetaState::apply (Delete + Put for assignments, CAS Put for members) at the snapshot index: existing records and ext tags only"
key-files:
  created:
    - crates/ostro-backup/src/cluster_restore.rs
    - crates/ostro-backup/tests/cluster_restore.rs
    - crates/ostro-repl-conformance/tests/process_cluster_restore.rs
    - docs/cluster-restore.md
  modified:
    - crates/ostro-backup/src/restore.rs
    - crates/ostro-backup/src/lib.rs
    - crates/ostro-backup/tests/cluster_repair.rs
    - docs/bottomless.md
    - docs/replication.md
key-decisions:
  - "Dispatch: cluster bucket = meta/head newer than any manifest/current (by at_ms vs cut_at_ms), or p/<pppp>/head objects without a decodable manifest/current; --at on it -> BadManifest(\"--at is not supported for cluster buckets in 1.0 (no common time base)\") before anything is written"
  - "Floor = lowest restorable start: 0 when chain_start == 0, else the oldest remote snapshot (listed p/<pppp>/snap/ keys or head.snapshot) inside [chain_start, end]"
  - "Restore at c_p from the newest restorable start <= c_p; the recorded snapshot state hash (head.snapshot.state_hash) is compared after the install when present"
  - "Metadata choice: newest candidate (meta/head sha-verified, then older meta/<index>.msnap) whose workflow (name, version) exists and whose pinned module (run.module_sha, else the workflow's module_sha256) has a ModuleRecord and a blobs/<sha> object, for every non-terminal run at the cut"
  - "Seed id = --node-id, else partition 0's head writer; a tombstoned id is refused"
  - "The cluster CA stored in metadata (__ostro/cluster-ca) is written to <data>/tls/ca.{crt,key} (key created 0600); the operator must not import a different CA on the seed (runbook)"
requirements-completed: [BACK-05]
metrics:
  duration: ~4h
  completed: 2026-11-27
---

# Phase 14 Plan 30: Whole-cluster restore Summary

`ostro restore` now rebuilds a lost multi-node cluster from its bottomless bucket: it computes the greatest cross-partition consistent cut, restores every partition there into one seed data directory with metadata rewritten to that single member, and the cluster grows back to RF 3 with `ostro cluster join`. Proven in-process (skewed synthetic bucket, mutant bites) and on a real 3-process cluster against `file://` and SeaweedFS.

## Dispatch rule (restore.rs)

After the empty-dir check and the source open, `cluster_restore::detect`:

- `meta/head` exists and (no decodable `manifest/current` or its `cut_at_ms < meta/head.at_ms`) -> cluster;
- else a decodable `manifest/current` exists -> Phase 8 path (byte-for-byte unchanged, `cluster: None`);
- else `p/<pppp>/head` objects exist -> cluster;
- else Phase 8 path (which reports the missing manifest as before).

On the cluster path `--at` is refused first. A cluster bucket without `meta/head` and without any `meta/*.msnap` keeps `RestoreError::ClusterBucketUnsupported`, message now `cluster bucket cannot be restored: no metadata snapshot in the bucket; the metadata leader uploads one in 1.0+ (N per-partition head object(s), e.g. p/0000/head); see docs/cluster-restore.md`.

## Restore steps as landed (cluster_restore.rs)

1. Per partition (parallel, `--parallelism`): head, `walk_back` to `chain_start` + checks (first base <= chain_start, last end = head end), restorable starts, floor, envelope boundaries, highest envelope/chunk epoch.
2. Scan dir `<data_dir>/.cluster-restore-scan`: every partition `[floor, end)` imported, one Inspect open with `CursorRecorders::factory()`, `steps_from_replay`; the dir is removed afterwards.
3. Step functions sampled at envelope boundaries, `cluster_cut::search` (or the raw head ends under the mutant), results rounded down to a boundary.
4. Final restore into the data dir: newest start <= `c_p` (snapshot sha-verified, applied offset checked, recorded state hash compared), import `[S, c_p)` (one `RawBatch` per epoch run), Inspect open: applied must equal `c_p`; cursors must equal the scan's at `c_p`; Phase 8 `check_consistency` (Pitfall 1) on the finals; non-terminal runs read from the `runs` table (their pins).
5. Metadata choice (see key decisions), rewrite, `MetaHandle::bootstrap_from_snapshot(meta, seed, index, rewritten)`.
6. Cluster CA to `tls/` when stored, blobs (sha verified), `node.toml` (`format_version`, `partitions`, new incarnation, `restored_from`, `cluster_id`).

## Metadata rewrite as landed

Applied through `MetaState::apply` at the snapshot index (no new records, ext tags, reason codes or member states):

- every member other than the seed that is not already Removed: CAS `Put` with ext 97 `MemberState{state: Removed, changed_at_ms: now}`;
- the seed: existing record -> addresses cleared (`addr = ""`, `cluster_addr = repl_addr = None`) + ext 97 Active; absent -> new `Member` with ext 97 Active (the server re-upserts its addresses at start; `ostro cluster init` is idempotent on it);
- every partition: `Delete` (expected = current version) then `Put` with `leader 0`, `replicas = isr = [seed]`, `min_insync 1`, `isr_version = old + 1`, ext 100 (in-flight reassignment) removed, other ext kept, `epoch = max(snapshot assignment epoch, head.epoch, highest epoch in the chain's envelopes/chunks, restored snapshot latest_epoch)` (record version = epoch);
- `ClusterInfo` (cluster id, ext 98 ClusterVersion, ext 99 ReplicationPolicy), secrets (incl. the CA), workflows, modules and everything else unchanged.

In tier C the controller then elected the seed for every partition at epoch 2 (rewritten epoch 1).

## Report shape

`RestoreReport` (Phase 8 fields; for a cluster restore `generation = 0`, `source_incarnation = ""`, `cut_at_ms = meta/head.at_ms`, `partitions[]` with `verify_offset` = the start used and `verify_state_hash` = the recorded snapshot hash or `""`) plus `cluster` (serialised only when present; illustrative values):

```json
"cluster": {"meta_index": 59, "meta_term": 3, "meta_from_head": true, "seed": 1,
            "removed_members": [2, 3], "open_runs": 3, "ca_kept": true,
            "partitions": [{"p": 2, "head_end": 131, "head_epoch": 1, "floor": 0, "cut": 127,
                            "dropped": 4, "lowered_reason": "saw a message partition 0 had not sent at its cut",
                            "epoch": 1}, ...]}
```

(`meta_term` is `null` when an older kept snapshot was chosen.)

## Evidence

**Task 1 verify** (local, slot 5): `cluster_restore` 6 passed; `--features failpoints` 7 passed (incl. `restore_skip_fixpoint_mutant_bites`: raw heads -> `Inconsistent`); `restore` 5 (+1 ignored), `cut_consistency` 2, `cluster_head` 3, `cluster_repair` 9; `grep fn restore_cluster` ok. The skewed bucket (heads p2 15 < p0 25 < p1 35) restored to cuts p0 16 (rule 2: had truncated its outbox to p2 beyond what p2 saw), p1 17 (rule 1: saw a message p2 had not sent), p2 15; floor 15 (the chain started at the round-A snapshot). Other tests: older covering snapshot chosen (index 10, epoch 0) with rewritten epoch = max(head, log) > 0 and epoch + 1 above every restored epoch; floor crossed -> `NoConsistentCut{[1]}`; no metadata -> refusal; non-covering metadata -> "no metadata snapshot covers the module pins of 6 non-terminal runs"; single-member manifest bucket -> Phase 8 path, no `cluster` key in the JSON.

**Task 2 verify** (local, the plan's exact command with `OSTRO_FAILPOINT_BIN=$CARGO_TARGET_DIR/debug/ostro`, SeaweedFS 4.54 from `scripts/test-services.sh`): `test result: ok. 2 passed`, no `SKIP:`; doc greps ok; no "minio". Run summaries:

| | file_bucket | s3_bucket (SeaweedFS) |
|---|---|---|
| original | 3 nodes RF 3, 52 acked runs, SIGKILL x3 mid-load | same, 52 acked runs |
| restore (wall / report) | 349 ms / 321 ms, meta index 59, 6 blobs | 364 ms / 321 ms, meta index 59, 6 blobs |
| cut / head end | p0 93/93, p1 128/128, p2 132/132, p3 55/55 (0 dropped) | p0 93/93, p1 142/142, **p2 127/131**, p3 28/28 (4 dropped) |
| acked runs | 48 below the cut present (3 open at the cut), 4 above absent (RPO) | 45 present (0 open), 7 absent (RPO) |
| seed ready | 181 ms | 153 ms |
| joins | node 4 395 ms, node 5 408 ms | node 4 399 ms, node 5 385 ms |
| RF 3 / full ISR | 1.84 s after the 2nd join (2.64 s from the 1st) | 1.83 s (2.61 s) |
| restored runs | 48 completed, 0 lost, 0 duplicate journal entries | 45 completed, 0 lost, 0 duplicates |
| new runs | 12 completed | 12 completed |
| final checks | replica identity, acked on disk (60 acks, 3 nodes), LA rule, ISR-only elections pass | same (57 acks) |
| uploads | old prefix unchanged (44 objects), new prefix 32 objects, 4 heads | old unchanged (42), new 31, 4 heads |

The RPO beyond `dropped` is the acknowledged commands after each partition's last upload (normal bottomless lag). No D-P14-36 fallback was taken (RF 3 reached without an explicit reassign).

**Other local checks:** `cargo clippy -p ostro-backup -p ostro-repl-conformance --all-targets -D warnings` and `-p ostro-backup --features failpoints` clean; `cargo fmt --all -- --check` clean; `scripts/p12-grep-gates.sh` rc 0 (backup clock reads are `clock-exempt` durations; the rewrite's `changed_at_ms` uses `SystemClock` via the `Clock` trait in `restore()`, marked `clock-exempt`); Phase 8 `ostro` suites `backup_cli` / `bottomless_s3` (SeaweedFS) green (see "Phase 8 suites").

## Plan gate

**Gate incomplete — cluster withdrawn; consolidated gate pending on dell-4.**

Neither `ostro-k8s-ci --parallel` nor the `ostro-k8s-ci --cmd` runs of `process_cluster_restore` were run (the user withdrew the microk8s cluster; no kubectl, no runner). Unverified until the dell-4 consolidated gate:

- every ci-local group (lint incl. mutant matrices, test1/test2 shards, fp1-3 failpoint suites, musl1/musl2, poly, obuild, Phase 13 tier C) at 0612de6;
- `process_cluster_restore` (both variants) on a CI machine (only run locally, twice for `file_bucket` and once for both via the verify; release + `--features failpoints` build as the proposed CI step uses was not run);
- ostro-repl-conformance tests other than `process_cluster_restore` (not run locally: the crate's tests were only compiled and linted);
- the ostro crate's suites other than `backup_cli` / `bottomless_s3`.

## Deviations from Plan

1. **[Rule 1] `tests/cluster_repair.rs` assertion updated** (file not in `files_modified`): `restore_refuses_a_multi_member_cluster_bucket` matched the old message "cluster-wide restore is deferred (Phase 13/14); see docs/replication.md", which is wrong in 1.0. The variant and the refusal stay; the assertion now matches the new detail text. One line.
2. **[Rule 2] The seed keeps the cluster CA** (467c613). The first tier-C run showed joiners timing out ("metadata raft did not elect a leader / catch up in time"): the seed issues joiner certificates from `__ostro/cluster-ca` in the metadata (the old cluster's CA, stored by 13-09) while its own certificate came from a different local CA. Restore now writes the stored CA to `tls/ca.{crt,key}`; `ClusterRestoreReport.ca_kept`. The runbook says to start the seed without a different `[cluster.tls]` CA import. The tier-C test removes the harness's CA import from the seed's config file when `ca_kept` (the harness generates a fresh test CA per `ProcCluster`). **Orchestrator note:** a seed (or any P13 node) started with a configured CA that differs from the stored one fails joins silently with a 35 s timeout; a boot-time check in ostro-server (not in this plan's files) would make it a clear error.
3. **Seed id in tier C is 1 (an old member id), not a new id.** `ProcCluster` numbers nodes from 1 and a restored data dir can only be started as an existing node of a fresh single-node `ProcCluster`; harness files are outside this plan. Restore supports both (`--node-id` or the head writer). Removal of 2 and 3 (tombstones in `/v1/cluster/status`, joiners got fresh ids 4 and 5) is checked; starting an old node process against the restored cluster ((f) "join refused") is not exercised. The refusal path is the existing 13-09 `upsert_self` / voter check on `Removed`.
4. **Acked-run offsets come from the old nodes' logs** (`tools::scan` of nodes 1-3 after the kill, `StartRun` offsets) instead of an ack log with offsets: `Load` acks carry no offsets. The history check is the existing journal oracle of `outcomes` (strictly increasing seqs, exactly `STEPS` distinct steps), not a byte comparison with the old history.
5. **Non-terminal `signals` runs at the cut are re-signalled** on the restored cluster (the original signal may lie above the cut); a 409 `run_terminal` is accepted.
6. **Metadata coverage is checked after the partitions are restored** (it needs the open runs at the cut), so metadata bootstrap happens after the final replay (Phase 8 bootstraps metadata first). `node.toml` is written before the final Inspect open and rewritten with `cluster_id` at the end.
7. `docs/bottomless.md` also had the 12-15 sentence "no meta snapshot" for multi-member clusters; it now names the 14-29 metadata uploads.

## Phase 8 suites

Local, SeaweedFS from `scripts/test-services.sh`, at 0612de6's code: ostro `backup_cli` 2 passed (`help_lists_backup_and_restore`, `bucket_online_and_offline_round_trips`), `bottomless_s3` 1 passed (`bottomless_s3_restore_round_trip`, no SKIP). ostro-backup `restore` 5 passed (+1 ignored).

## CI hunk for 14-27 (SeaweedFS `process_cluster_restore` leg)

`.github/workflows/ci.yml`, in the `linux-x86_64-gnu` leg after "bottomless restore leg against S3 (SeaweedFS)" (same job, so the `OSTRO_TEST_S3_*` env from "Start SeaweedFS" is present):

```yaml
      - if: matrix.name == 'linux-x86_64-gnu'
        name: whole-cluster restore (tier C, file:// + SeaweedFS)
        # 14-30 BACK-05: 3 nodes -> SIGKILL -> ostro restore (seed) -> init + 2 joins -> RF 3,
        # acked runs below the cut intact, above it absent. The s3_bucket variant uses this
        # leg's SeaweedFS and must not SKIP. Keep in sync with check_cluster_restore in
        # scripts/ci-local.sh.
        env:
          OSTRO_P13_REPORT_DIR: ${{ runner.temp }}/ostro-p13-reports
        run: |
          test -n "${OSTRO_TEST_S3_URL:-}"
          cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
          out="$(cargo test --release -p ostro-repl-conformance --features failpoints --locked --test process_cluster_restore -- --ignored --test-threads=1 --nocapture 2>&1)" || { echo "$out"; exit 1; }
          echo "$out"
          grep -Eq 'test result: ok\. 2 passed' <<<"$out"
          if grep -q 'SKIP:' <<<"$out"; then echo "process_cluster_restore skipped"; exit 1; fi
```

`scripts/ci-local.sh` (14-27 owns it):

```bash
# 14-30 (BACK-05): whole-cluster restore, tier C, file:// + SeaweedFS (test-services.sh).
# Keep in sync with the "whole-cluster restore" step of .github/workflows/ci.yml.
check_cluster_restore() {
  local out rc
  cargo build -p ostro --features failpoints --target-dir target/failpoints --locked || return 1
  bash scripts/fetch-tools.sh weed >/dev/null || return 1
  bash scripts/test-services.sh start s3 || return 1
  out="$(eval "$(bash scripts/test-services.sh env)"; \
    OSTRO_P13_REPORT_DIR="${OSTRO_P13_REPORT_DIR:-$ROOT/target/p13-reports}" \
    cargo test --release -p ostro-repl-conformance --features failpoints --locked \
      --test process_cluster_restore -- --ignored --test-threads=1 --nocapture 2>&1)"; rc=$?
  bash scripts/test-services.sh stop
  echo "$out"
  [ "$rc" -eq 0 ] || return 1
  grep -Eq 'test result: ok\. 2 passed' <<<"$out" || return 1
  if grep -q 'SKIP:' <<<"$out"; then echo "process_cluster_restore skipped"; return 1; fi
}
step "whole-cluster restore (tier C)" check_cluster_restore
```

## Commits

- 4014156 feat(14-30): restore a whole cluster bucket to a consistent cut as a seed
- 467c613 fix(14-30): the restored seed keeps the cluster CA stored in metadata
- 0612de6 test(14-30): whole-cluster restore end to end; runbook

## Self-Check

PASSED: the 4 created files are on main (`git cat-file -e main:<path>`); commits 4014156, 467c613, 0612de6 are ancestors of main. Gate: incomplete (see "Plan gate").
