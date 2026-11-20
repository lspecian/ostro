# Bottomless storage (BACK-01)

A single Ostro node can stream everything it needs for a restore to an object store: the
partition logs as uploader-sealed chunks, partition snapshots, the metadata snapshot and the
blob store. Restore (`ostro restore`, plan 08-18) rebuilds a node from that prefix at any
retained manifest generation.

Implementation: `crates/ostro-backup` (`chunk`, `manifest`, `fence`, `uploader`) and the
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
