# Bottomless storage (BACK-01)

A single Ostro node can stream everything it needs for a restore to an object store: the
partition logs as uploader-sealed chunks, partition snapshots, the metadata snapshot and the
blob store. Restore (`ostro restore`, plan 08-18) rebuilds a node from that prefix at any
retained manifest generation.

Implementation: `crates/ostro-backup` (`chunk`, `manifest`, `fence`, `uploader`, and the
08-18 `restore` / `archive` modules behind `ostro restore` / `ostro backup`) and the
server service `crates/ostro-server/src/services/uploader.rs`. Decisions: P8-D3, P8-D4,
P8-D5, P8-D23 in `.planning/phases/08-triggers-bottomless-observability/08-DECISIONS.md`.

## Config

```toml
[backup]
url = "s3://my-bucket/ostro/prod-1"   # or "file:///var/backups/ostro"; unset = off
seal_interval = "1s"                  # upload tick (default 1s); 5-10s is cheaper, see below
region = "eu-central-1"
endpoint = "https://s3.eu-central-1.amazonaws.com"   # override for SeaweedFS, R2, GCS
url_style = "path"                    # or "virtual"
access_key_id = "env:OSTRO_BACKUP_ACCESS_KEY_ID"     # env: references; never logged
secret_access_key = "env:OSTRO_BACKUP_SECRET_ACCESS_KEY"
# session_token = "env:AWS_SESSION_TOKEN"
allow_http = false                    # true only for a local test store
tls_roots = "platform"                # or a PEM bundle path (see TLS roots)
remote_snapshots_keep = 3
takeover = false                      # --takeover: replace another incarnation's LOCK
```

Setting `url` turns bottomless on. The node then holds local WAL trimming at the **upload
floor** (it starts at the current `log_start` when the node opens), so nothing that is not
yet safely uploaded is ever trimmed. Credentials go in the dedicated fields, never in the
URL: `open_url` rejects userinfo, and the startup log prints the URL only.

Two related sections control what bottomless uploads and what the node may delete locally
(keys and defaults checked against `crates/ostro-server/src/config.rs` at the Phase 8 gate,
08-22; durations take h/m/s/ms, `d` is rejected):

```toml
[snapshots]              # local OSNP snapshots (08-09); the uploader uploads the same files
enabled = true
interval = "1h"          # snapshot at least this often when something was applied
min_interval = "60s"     # never more often than this
bytes = 268435456        # ... or when this many envelope bytes were appended since the last one
keep = 2                 # local snapshots kept; the oldest kept one is the local trim floor

[blob_gc]                # local mark/sweep (08-19) + remote delayed deletion with bottomless on
enabled = true
interval = "1h"
grace = "1h"             # only unreferenced blobs older than this are deleted
```

Local WAL trimming needs a retained snapshot: with `[snapshots] enabled = false` the local
floor stays 0 and nothing is trimmed (storage-format §15.1).

## Remote layout

Everything lives under the configured prefix.

| Key | Contents | Written |
|-----|----------|---------|
| `LOCK` | JSON `{cluster_id, node_id, incarnation}` of the owning node | `If-None-Match: *` when backup starts; `If-Match` on takeover |
| `manifest/current` | the newest manifest (postcard + sha256 trailer) | every round that changed something, `If-Match` on the previous etag |
| `manifest/gen/<gen 20d>` | immutable copy of a manifest | rounds that changed a referenced snapshot (point-in-time anchors) |
| `meta/<index 20d>.msnap` | metadata snapshot (`[META_FORMAT] ‖ postcard(MetaState)`), captured after the cut | when the meta applied index moved |
| `blobs/<sha256 hex>` | content-addressed blobs | before any chunk that can reference them |
| `p/<pppp>/snap/<applied 20d>-<sha256>.osnap` | OSNP v1 partition snapshot (`docs/snapshot-format.md`) | as soon as a local snapshot appears |
| `p/<pppp>/seg/<epoch 10d>-<base 20d>-<end 20d>.oseg` | OSEG v1 chunk: raw OSTR envelopes of `[base, end)` under one epoch | every round, per busy partition |

OSEG v1:

```text
magic "OSEG" | ver u16 | partition u16 | epoch u32 | base u64 | end u64 | count u32
| prev_chunk_sha256 [32] | body: (u32 LE len ‖ OSTR envelope)* | trailer sha256(header ‖ body)
```

The envelopes are stored byte for byte (the frozen envelope format, each with its CRC32C).
`prev_chunk_sha256` is the sha256 of the whole previous chunk object of the same partition;
the first chunk of a chain has zeros. Chunks are split at epoch boundaries and capped at
8 MiB.

## Seal semantics

BACK-01 asks for "sealed segments". Here a chunk is **sealed by the uploader**: at each tick
it closes the chunk covering `[uploaded_end, cut)` and never appends to it afterwards. Local
WAL segments are not uploaded as files; the chunk is the unit.

Every `seg/`, `snap/`, `meta/`, `blobs/` and `manifest/gen/` object is immutable and written
with `put_if_absent`. If the key already exists with the same sha256, the write counts as
done (an idempotent retry after a crash). If it exists with different bytes, another writer
produced it, and the uploader treats that as fenced.

## Consistent cut and snapshot hold-back

Partitions exchange messages through outboxes with `(src, seq)` dedupe, so partitions
restored to independently chosen points can lose or silently drop messages. Every round
therefore uses one **causally consistent cut**:

1. read each partition's atomic `(applied_offset, state_hash)`, then the HW vector
   (`Node::log_cut`, updated at the single HW publication point) and the cut time;
2. upload blobs;
3. per partition, upload chunks for `[uploaded_end, cut_p)`, never beyond the cut (which is
   always below the HW);
4. upload the meta snapshot (captured after the cut, so every module and schedule a run
   below the cut needs is in it);
5. write the manifest.

A snapshot can be taken at any time, including between reading the cut and writing the
manifest. Such a snapshot is uploaded immediately but **held back**: a manifest only
references a snapshot with `snapshot_offset <= cut_offset`, and a later round references it
once its cut covers it. `Manifest::validate` enforces, per partition:

- `snapshot_offset <= verify_offset <= cut_offset`;
- `chain_start <= snapshot_offset` (or `chain_start == 0` when no snapshot is referenced);
- the head chunk ends exactly at `cut_offset` when `cut_offset > chain_start`.

Restore truncates every partition at its `cut_offset`, replays from the snapshot, and checks
the state hash at `verify_offset`.

## Chain start rule

Each partition's chunks form a hash chain from a start offset:

- **Fresh prefix**, or **uploaded end below `log_start`** (backup was off while the node
  trimmed): the uploader uploads the newest local snapshot `S` with
  `log_start <= S <= cut` first and starts a new chain at `S.applied_offset` with a zero
  previous link.
- The chain starts at offset 0 only when there is no such snapshot and `log_start == 0`.
- Otherwise (trimmed log, no usable snapshot) the partition is skipped, a snapshot is
  requested, `ostro_backup_failures_total{kind="no_snapshot"}` counts it, and **no manifest
  is written that round**, so every manifest stays consistent across partitions.

A manifest may omit `snapshot_key` only for a chain that starts at 0.

## Upload floor

After a manifest is durable, each partition's upload floor is raised (never lowered) to
`min(referenced snapshot offset (or chain start), uploaded chunk end)`. Local retention trims
the WAL to `min(checkpoint, local snapshot floor, upload floor, retention floor)`, so it
never removes data a restore from the bucket would still need from this node.

## Fencing and `--takeover`

- `LOCK` is created with `If-None-Match: *`. A LOCK that names this node's incarnation is
  ours (restart). A LOCK that names another incarnation stops the uploader unless `takeover`
  is set (`--takeover`), which replaces it with `If-Match` on its etag.
- The manifest PUT uses `If-Match` on the etag of the previous manifest. A failed
  precondition means another writer owns the prefix: uploads stop for good, the error is
  logged and `ostro_backup_fenced` becomes 1.
- A restored node has a new incarnation (restore writes a fresh one into `node.toml`), so it
  must pass `--takeover` to keep uploading to the prefix it was restored from. The usual
  alternative is a new prefix.
- **Fallback for stores without conditional PUT** (`StoreOptions::conditional_put = false`;
  not a `[backup]` key yet): before every manifest PUT the uploader re-reads `LOCK` and compares the identity,
  and immutable objects are checked with HEAD before an unconditional PUT. This is weaker:
  a second writer can race between the read and the write.

### Single-node scope

`LOCK` and the single `manifest/current` writer are single-node constructs. Cluster-wide
bottomless restore is deferred. Phase 12 does per-partition chain repair only:
divergent-tail truncation and per-partition writer fencing by leader epoch. Restore walks
each chain back from the manifest head (`chunk::walk_back`), following `prev_chunk_sha256`,
so forked chunks left by an old writer are ignored.

## Remote retention

After each manifest that changed a referenced snapshot (and on the first manifest after
start), per partition:

- keep the newest `remote_snapshots_keep` (default 3) referenced snapshots, plus any newer
  snapshot still held back;
- keep the chunks that end after the oldest kept snapshot **and** are on the chain walked
  back from the manifest head; delete older chunks and any unreachable (forked) chunk;
- move the manifest's `chain_start` up to the first kept chunk (written in the same
  manifest, before anything is deleted);
- delete `manifest/gen/*` copies that reference a deleted object.

Remote blob deletion is the delayed-deletion GC of plan 08-19.

## RPO measurement

Per P8-D23 the RPO has two measured parts:

1. **Cut lag**: per tick, the time between consecutive cut reads, and the cut age when the
   manifest becomes durable minus that round's upload duration, are both at most
   `seal_interval` (asserted in the 08-13 tests and in 08-20 `restore_rpo`).
2. **Upload latency**: the round duration, `ostro_backup_upload_seconds`, reported
   separately.

So RPO ≈ `seal_interval` + upload latency. After a crash, the data-loss window is the newest
lost acknowledgement minus the manifest's `cut_at_ms`.

Measured (08-20 `restore_rpo`, P=16, seal 1 s, 10 kill/restore iterations, release binary on a
loaded WSL2 machine, `file://` bucket): cut lag p50 1,044 ms / max 1,203 ms (asserted ≤ seal +
500 ms), upload latency p50 681 ms / max 849 ms for the restored rounds (all rounds: p50 733 ms,
max 2,030 ms), data-loss window p50 1,680 ms / max 2,449 ms. Every acknowledgement older than
the cut was restored (22,379 acks). Uploads are sequential across partitions inside a round
(08-13); at P=16 under load the round is fsync-bound on LocalFs (08-13: p50 1.02 s, max 9.73 s
at seal 250 ms). Parallel per-partition uploads are the next lever if a deployment needs a
smaller window. Real S3/R2 numbers are a human_needed item of the Phase 8 gate.

## Cost

S3 Standard PUTs cost about $0.005 per 1,000. Idle partitions upload nothing, and an idle
round writes no manifest.

| Busy partitions | `seal_interval` | PUT/s (chunks + manifest) | PUT/day | ≈ $/day |
|---|---|---|---|---|
| 16 | 1 s | ~17 | ≈ 1.47M PUT/day | ≈ $7 |
| 16 | 5 s | ~3.4 | ≈ 294k | ≈ $1.5 |
| 16 | 10 s | ~1.7 | ≈ 147k | ≈ $0.7 |
| 1 | 1 s | ~2 | ≈ 173k | ≈ $0.9 |

Snapshot, meta and blob objects are extra but rare. **Recommendation:** use 5–10 s when the
RPO allows it.

## Alerting: stuck upload floor

A stuck uploader (store down, credentials expired, fenced) also stops local WAL trimming,
because the upload floor no longer moves, so disk use grows. Alert on:

- `ostro_backup_upload_floor_lag{partition}` (applied offset minus upload floor) growing
  steadily;
- `ostro_backup_cut_age_seconds > 10 × seal_interval`;
- `ostro_backup_fenced == 1`;
- a rising `ostro_backup_failures_total{kind}`.

Other metrics: `ostro_backup_upload_bytes_total{kind}` (`chunk`, `snapshot`, `meta`, `blob`,
`manifest`) and `ostro_backup_upload_seconds`.

## Providers

| Provider | Notes |
|----------|-------|
| AWS S3 | conditional PUT (`If-None-Match`, `If-Match`) supported since 2024; `url_style = "virtual"` or path |
| SeaweedFS | the CI test backend (`weed mini`, S3 on :8333); conditional operations supported (412 on a failed condition) |
| Cloudflare R2 | conditional PUT supported; `endpoint = "https://<account>.r2.cloudflarestorage.com"`, `region = "auto"` |
| Google Cloud Storage | through the S3 interoperability API with HMAC keys (`endpoint = "https://storage.googleapis.com"`) |
| Local filesystem | `file:///abs/path`: durable writes (tmp + fsync + rename + dir fsync), emulated conditional PUT |
| Azure Blob | deferred |

## TLS roots for scratch images

`tls_roots = "platform"` (the default) uses the system store. A `FROM scratch` image has no
`/etc/ssl/certs`, so either mount a CA bundle and set `tls_roots = "/etc/ostro/ca.pem"`, or
copy one into the image. The bundled webpki roots are not shipped (their licence, CDLA, is
excluded by D-FX2), so `tls_roots = "webpki"` is a configuration error that points at the
file option.

## Restore runbook

`ostro restore` (plan 08-18, P8-D6) rebuilds a node's data directory from a bucket prefix or
a backup file and **verifies** it on the way. It never runs on its own: a node whose state is
missing and whose log was trimmed refuses to start with "snapshot required: run 'ostro
restore'".

### Prerequisites

- The `ostro` binary of the same or a newer version (the manifest records `ostro_version`).
- An **empty or absent** target directory. Restore refuses anything else. If a restore was
  interrupted (crash, Ctrl-C, a verification error), delete the directory and run it again.
- Read access to the source. For S3, credentials come only from the environment:
  `OSTRO_BACKUP_ACCESS_KEY_ID`, `OSTRO_BACKUP_SECRET_ACCESS_KEY` and optionally
  `OSTRO_BACKUP_SESSION_TOKEN`, else the standard `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`. No flag takes secret material, and the
  report never prints credentials. Endpoint and region: `--endpoint` / `--region`, else
  `OSTRO_BACKUP_ENDPOINT` / `AWS_ENDPOINT_URL` and `OSTRO_BACKUP_REGION` / `AWS_REGION`.

### Commands

```sh
# From a bucket (current manifest):
ostro restore --from s3://my-bucket/ostro/prod-1 --data-dir /var/lib/ostro-restored \
  --endpoint http://127.0.0.1:8333 --allow-http          # SeaweedFS / local store only

# From a local prefix or a backup file:
ostro restore --from file:///var/backups/ostro --data-dir ./restored
ostro restore --from ./ostro-backup.tar --data-dir ./restored

# A backup file: online from a running server, or offline from a stopped data directory.
ostro backup --to ./ostro-backup.tar --server http://127.0.0.1:7700
ostro backup --to ./ostro-backup.tar --data-dir /var/lib/ostro      # server stopped
```

A backup file is a tar of the same layout as a bucket prefix (`manifest/`, `p/<pppp>/seg/`,
`p/<pppp>/snap/`, `meta/`, `blobs/`, `LOCK`), so a file restore uses the same code path. The
online backup (`GET /v1/admin/backup`) takes a fresh snapshot per partition, then runs one
upload round into a temporary directory (a fresh fence identity; the server's own bucket,
upload floor and remote retention are untouched) and streams the tar. The offline backup
opens the stopped directory in Inspect mode (no append), starts the metadata group on a
temporary copy (the directory is never written), and refuses a directory whose `LOCK` is
held by a running server. Pass `--node-id` when the server's `[node] node_id` is not 1.

Then start the server on the restored directory with the same partition count
(`[node] partitions`); `node.toml` records it, together with the new `incarnation` and
`restored_from`.

### Choosing the restore point: `--at`

- `--at 42`: manifest generation 42 (`manifest/gen/00000000000000000042`, or the current
  manifest if it is generation 42).
- `--at 2026-11-20T14:00:00Z` (RFC 3339): the newest retained manifest cut at or before that
  time.

Generation copies exist for every manifest that changed a referenced snapshot, and remote
retention deletes those that reference deleted objects, so `--at` reaches back about
`remote_snapshots_keep` snapshot cadences.

### What restore does and checks

1. Loads the manifest; its sha256 trailer, format and ordering rules (`snapshot_offset <=
   verify_offset <= cut_offset`, chain start) are checked before anything is written.
2. Writes `node.toml` with a **new incarnation**.
3. Installs the metadata snapshot natively into the metadata Raft log (identical state,
   applied index preserved).
4. Downloads every blob, checking each sha256.
5. Per partition, in parallel (`--parallelism`, default 4): downloads and installs the
   snapshot (sha256 checked), walks the chunk chain back from the manifest head (forked
   chunks are ignored) and verifies it forward.
6. Writes the partition log up to `verify_offset`, replays it in Inspect mode and compares
   the state hash with the manifest. A mismatch fails the restore and names the partition.
   Then it appends the log up to `cut_offset` and replays again.
7. Checks cross-partition consistency: no partition has seen an outbox message its sender
   does not have.

### Verifying the report

`ostro restore` prints one row per partition (`--json` prints the full report):

```text
restored ./restored from s3://… (generation 812, cut at 1763648400123 ms, cluster c-…, 41 blobs, meta index 2210)
   P      SNAPSHOT        VERIFY           CUT   CHUNKS  STATE_HASH        DIGEST
   0         10240         10377         10377        3  5c1f0e…           9a7d22…
   …
verified: every partition's state hash matched at its verify offset; new incarnation 3f…
took 1830 ms (manifest 12, meta 40, blobs 210, partitions 950, replay 610)
```

`--json` fields: `data_dir`, `source`, `generation`, `cluster_id`, `node_id`,
`source_incarnation`, `incarnation`, `cut_at_ms`, `meta_index`, `blobs`, `takeover`,
`partitions[]` (`p`, `snapshot_offset`, `chain_start`, `verify_offset`,
`verify_state_hash`, `cut_offset`, `digest`, `chunks`, `envelopes`) and `durations`
(`manifest_ms`, `meta_ms`, `blobs_ms`, `partitions_ms`, `replay_ms`, `total_ms`). The data
lost by the restore is everything acknowledged after `cut_at_ms`. Restoring the same
source twice yields the same digests.

### Takeover or a new prefix

The restored node has a new incarnation, so the source prefix's `LOCK` names another node:

- **New prefix (default):** set `[backup] url` to a new prefix. The old prefix stays a
  read-only restore source.
- **Takeover:** `ostro restore --takeover` records the intent and prints the reminder; start
  the server with the same `[backup] url` and `takeover = true`. Make sure the old node is
  gone for good: after the takeover the old writer is fenced at its next manifest PUT.

### RPO expectations

RPO ≈ `seal_interval` + upload latency (see "RPO measurement"). With the default 1 s seal
interval a crash loses at most the last couple of seconds of acknowledged work; the restore
report's `cut_at_ms` says exactly where the restored history ends. Backup files have the RPO
of the moment they were taken. Measured numbers: "RPO measurement" above. Restore time:
P=16 with ~10k runs restores in about 9 s (debug build, LocalFs, `--parallelism 4`; partition
installs dominate); the 08-20 backup-file round trip (16 partitions, 2,045 runs) took 13.4 s.

### Fresh-machine checklist

1. Install the `ostro` binary.
2. Export the S3 credentials (`OSTRO_BACKUP_*` or `AWS_*`), and the endpoint/region for a
   non-AWS store.
3. On a `FROM scratch` image (no `/etc/ssl/certs`), mount a CA bundle and pass
   `--tls-roots-file /etc/ostro/ca.pem` (see "TLS roots for scratch images").
4. `ostro restore --from s3://… --data-dir /var/lib/ostro` and read the report.
5. Configure the server: same `[node] partitions` and `node_id`, and either a new
   `[backup] url` or the old one with `takeover = true`.
6. `ostro server --config …`; check `/readyz` and a few known runs.
