# Restoring a whole cluster from bottomless backups

Ostro 1.0 can rebuild an entire multi-node cluster (metadata and every partition) from the
bottomless backups in its object store: `ostro restore` builds **one seed node** at a
cross-partition consistent point, and the cluster grows back to its replication factor with
`ostro cluster join` (BACK-05, D-P14-45). This page is the operator runbook.

For a single node (or a cluster that never had more than one member) the Phase 8 restore in
[`docs/bottomless.md`](bottomless.md) § "Restore" applies unchanged: the bucket has a
`manifest/current` and `ostro restore` uses it.

## When to use it

Use it when the cluster is **lost**: every node (or a majority of the metadata voters
together with the data of some partitions) is gone, and the bucket is the only copy left.
A cluster that lost a minority of its nodes heals itself: replace the nodes with `ostro
cluster join` and remove the dead ones (`docs/replication.md` § 9), no restore needed.

The restored cluster **keeps the cluster identity**: the same `cluster_id`, and the same
cluster CA when the metadata holds one. So **the old cluster must stay down**. Running the
restored cluster next to a surviving node of the old one on the same network is unsupported:
both claim the same identity. Old member ids (other than the seed's) are tombstoned `Removed`
in the restored metadata, so an old node that comes back is refused ("node N was removed
from the cluster ... wipe the data dir and join again as a new node").

## What "consistent cut" means

A multi-member cluster has no single backup point: every partition's leader uploads its own
chain (`p/<pppp>/head`), at its own pace. Restoring every partition at its newest uploaded
offset can break cross-partition causality. For example, partition B may already hold a
signal that partition A sent, while A's backup ends before the send. Or A may have dropped
outbox rows that B had not yet received.

`ostro restore` therefore looks for the greatest **consistent cut** `{c_p}`. For every pair
of partitions A, B it must satisfy:

```text
outbox_truncated[A->B](c_A)  <=  inbound_seen_B[A](c_B)  <  outbox_next_seq[A->B](c_A)
```

This is the same rule a single-node restore checks (Phase 8 "Pitfall 1"). To find the cut,
restore replays every partition's chain once and records these cursors at every offset. It
starts at the head ends and lowers only the partitions that must go down. The result never
goes below a partition's **floor**, the oldest remote snapshot (or offset 0) its chain can be
restored from.

What a restore loses, per partition, is `head_end - cut` offsets (the `dropped` field of the
report). On top of that come the commands acknowledged after the partition's last upload
(the normal bottomless lag, bounded by `[backup] seal_interval`). Runs started at or above
the cut are gone; runs below it are restored with their history, and the non-terminal ones
continue after the restore.

## Prerequisites

- Bottomless backup was enabled (`[backup] url`) on every node of the old cluster, all with
  the same prefix.
- The bucket holds a **metadata snapshot**. Since 1.0 the metadata leader uploads one with a
  `meta/head` pointer (14-29). A bucket written by a pre-1.0 multi-member cluster has
  partition heads but no metadata snapshot, and restore refuses it.
- The modules of the runs that are still open at the cut are in the bucket (`blobs/`). The
  uploader puts them there.
- The old cluster is down (see above).
- An empty data directory for the seed, on a machine that will run `ostro server`.

## Commands

1. **Restore the seed.** Pick the seed's node id. It must not be one of the old nodes that
   could come back. Old ids that are not the seed become `Removed`. Without `--node-id`, the
   writer id of partition 0's head is used.

   ```bash
   ostro restore --from s3://my-bucket/ostro/prod --data-dir /var/lib/ostro \
     --node-id 7 --json > restore-report.json
   # S3 credentials come from the environment (OSTRO_BACKUP_ACCESS_KEY_ID /
   # OSTRO_BACKUP_SECRET_ACCESS_KEY, else AWS_*); --endpoint/--region/--allow-http as needed.
   ```

   The report holds the usual Phase 8 fields and a `cluster` object: `meta_index`,
   `meta_term`, `meta_from_head`, `seed`, `removed_members`, `open_runs`, `ca_kept` and
   `partitions[]` with `p`, `head_end`, `head_epoch`, `floor`, `cut`, `dropped`,
   `lowered_reason` and the assignment `epoch` written to the seed's metadata.

2. **Configure and start the seed** as a server-profile node with `[node] node_id` = the
   seed id, the same `[node] partitions`, the same `[cluster] cluster_id` (or none: the
   restored `node.toml` names it), and `[cluster] advertise`. Point `[backup] url` at a
   **new prefix**. To keep writing to the old prefix instead, start with
   `[backup] takeover = true` (heads are epoch-fenced, and the seed's epochs are above every
   epoch in the restored logs).

   **CA:** when the report says `"ca_kept": true`, the data directory already holds the
   cluster CA from the metadata (`tls/ca.crt`, `tls/ca.key`). Start the seed **without**
   `[cluster.tls] ca_cert_file` / `ca_key_file`, or with the original cluster's CA files.
   Do not use a different CA. The certificates the seed issues to joining nodes always come
   from the CA stored in the metadata. With a different local CA, the seed and the joined
   nodes do not trust each other, and the joiners time out with "metadata raft did not
   elect a leader / catch up in time".

   ```bash
   ostro server -c /etc/ostro/seed.toml
   ostro cluster init --advertise-cluster 10.0.0.11:7701   # idempotent: a kept policy stays
   ```

   On start, the placement controller elects the seed for every partition at an epoch above
   every restored epoch.

3. **Join fresh nodes** to grow back to the replication factor. Each node gets a new id;
   removed ids are never reused.

   ```bash
   ostro cluster token create --node                 # against the seed; prints the join line
   ostro cluster join https://10.0.0.11:7700 --token njt_... --ca-hash sha256:<hex> \
     --data /var/lib/ostro -c /etc/ostro/node.toml   # on each blank new node
   ```

   (`docs/cluster-operations.md` § 1.2.)

4. **Wait for RF.** `ostro cluster status` (or `GET /v1/cluster/status`) lists the members
   (the old ones as `removed`). `GET /v1/admin/replication` shows every partition back at the
   policy's RF with a full ISR. The controller raises the RF automatically once enough nodes
   are active. If a partition stays below it, reassign it explicitly
   (`docs/replication.md` § 9).

In the tier-C test (`crates/ostro-repl-conformance/tests/process_cluster_restore.rs`), a
3-node cluster killed with SIGKILL under load was restored in about 0.35 s. The seed was
ready 0.15 to 0.2 s later, each join took about 0.4 s, and RF 3 with a full ISR came about
2.6 s after the first join. The test ran on loopback, on `file://` and on SeaweedFS.

## Restrictions

- **`--at` is not supported for cluster buckets** in 1.0: partitions have no common time
  base. `ostro restore --at ...` on a cluster bucket fails with "--at is not supported for
  cluster buckets in 1.0 (no common time base)". The restore point is always the greatest
  consistent cut below the newest heads.
- **Audit logs are node-local and not in the backups** (D-P14-11): the restored cluster
  starts with empty audit logs. Keep the old nodes' audit files if you still have them.
- The seed holds every partition at first: size its disk for the whole cluster's state.

## Failure messages

| Message | Meaning | What to do |
|---------|---------|------------|
| `no consistent cut: partitions [..] would have to be restored below their oldest remote snapshot` | A partition must be lowered below its floor. Its chain was re-based on a newer snapshot (a leader re-chains when its log start passed the remote tail), so older offsets are gone. | Check that the old cluster really is down and that no writer is still moving heads. A cut below the floor cannot be restored. Raise `[backup] remote_snapshots_keep` so that older snapshots stay reachable in the future. |
| `metadata: no metadata snapshot covers the module pins of N non-terminal runs (first: run … on … vN (module …))` | No kept metadata snapshot has the workflow version, the module record or the module blob of an open run at the cut. | Make sure `blobs/` was not pruned by hand. Restore an older copy of the bucket if you have one. |
| `cluster bucket cannot be restored: no metadata snapshot in the bucket; the metadata leader uploads one in 1.0+` | A pre-1.0 multi-member bucket (no `meta/head`, no meta snapshot). | Run 1.0 on the cluster before you need the backup. A pre-1.0 cluster bucket cannot be restored as a cluster. |
| `node N is a removed member of the source cluster` | The `--node-id` you chose is a tombstone in the source metadata. | Pick another id. |
| `partition P: chunk chain: …` / `snapshot … sha256 mismatch` / `state hash …` | The bucket is damaged (missing or tampered objects). | As for a single-node restore: nothing was started, delete the data directory, fix the bucket or use another copy. |

Restore writes only into the empty target directory. A failed or interrupted restore leaves
it partly written: delete it and run the restore again. The source bucket is never modified.

## How it works (reference)

`crates/ostro-backup/src/cluster_restore.rs`, called from `ostro_backup::restore::restore`
when the bucket has a `meta/head` newer than any `manifest/current`, or partition heads
without a usable `manifest/current`:

1. Every partition's head and chain are read and verified (`chunk::walk_back`). Its
   restorable starts (offset 0 when the chain starts there, every remote snapshot inside the
   chain) give its floor.
2. `[floor, end)` of every partition is replayed in Inspect mode with the cursor observer
   (`ostro_partition::replay_observer`). The cursor histories, sampled at envelope boundaries,
   go to `cluster_cut::search` (greatest fixpoint).
3. Every partition is restored at its cut from the newest snapshot below it (sha verified,
   recorded state hash compared). The cursors must equal the scan's, and Pitfall 1 is checked
   on the final state.
4. The newest metadata snapshot that covers the open runs is rewritten to a single member
   with existing records and ext tags only: the seed Active (ext 97), the others `Removed`,
   every assignment `replicas = isr = [seed]`, `leader = 0`, `min_insync = 1`,
   `epoch = max(snapshot epoch, head epoch, highest restored log epoch)`; ReplicationPolicy
   (ext 99) kept. The rewritten snapshot is bootstrapped as a single-voter metadata group.
5. The cluster CA from the metadata goes to `tls/`, the blobs are restored (sha verified),
   and `node.toml` records the new incarnation, `restored_from` and the kept `cluster_id`.

The mutant `repl::mutant::restore_skip_fixpoint` (feature `failpoints`) restores the raw head
ends instead; `crates/ostro-backup/tests/cluster_restore.rs` shows that the final Pitfall 1
check catches it.
