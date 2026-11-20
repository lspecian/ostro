# Phase 8: Triggers, Bottomless & Observability - Research

**Researched:** 2026-11-19
**Domain:** Partition snapshots and WAL trimming, object-storage backup/restore (bottomless), blob GC, cron and webhook triggers, OpenTelemetry traces, logs and metrics, and Prometheus exposition
**Confidence:** MEDIUM overall.
- HIGH for crate versions and dependency compatibility, all verified against crates.io on 2026-11-19.
- HIGH for fjall snapshot and batch semantics, verified in the fjall 3.1.13 source.
- MEDIUM for the new designs (consistent cut, ScheduleFire, deterministic span ids). These are proposals the planner must adopt explicitly.

## Summary

Phase 8 turns on several things that Phases 2–5 left switched off:
- WAL trimming (`snapshot_floor = 0` today, so `trim_prefix` never deletes);
- blob GC (Phase 5 TTL deletion leaves blob bytes behind);
- cron firing (the `Schedule` meta record exists, but nothing drives it);
- object-storage backup.

It adds webhooks, OTLP traces and logs, and a `/metrics` endpoint. The hard part is one snapshot format that:
- covers restore now and follower install in Phase 12;
- lets trimming proceed safely;
- gives a restore that is consistent across partitions.

The research found **three conflicts with what the orchestrator assumed**. The planner must not ignore them.

1. **`object_store` 0.14.2 cannot be used as is.** It has a *required* dependency on `chrono` (`chrono ^0.4.34`, not optional). `deny.toml` bans chrono (RUSTSEC-2026-0543, unmaintained, issued 2026-11-17). It also requires `humantime`, which is unmaintained too (RUSTSEC-2026-0542). Upstream issues #909 and #908, opened 2026-11-17, are still open. Recommendation: a small `ostro-objstore` crate with:
   - a `LocalFs` backend;
   - an S3-compatible backend built on **`rusty-s3` 0.12.0**: sans-IO SigV4, released 2026-11-18, on **jiff ^1.0**, BSD-2-Clause, which `deny.toml` allows;
   - the existing `reqwest` 0.13.5 with `rustls`.
   This also affects Phase 6 FX-05, whose requirement text names `object_store`.
2. **`croner` 4.0.3 (the cron parser named in STACK.md) supports jiff only at `^0.2.35`.** The workspace pins `jiff =1.0.1`. All four jiff-based cron crates (croner, jiff-cron, cronexpr, cronp) are still on jiff 0.2, because jiff 1.0 shipped on 2026-11-15. Recommendation: use croner behind an adapter crate that contains jiff 0.2, building both jiff majors' `TimeZone` from the same TZif bytes. Do not hand-roll cron evaluation: croner 4.0.3 fixed a DST bug two weeks ago.
3. **The `ci-local.sh` forbidden-crate grep (`prometheus `) matches `metrics-exporter-prometheus v0.18.3`.** This was verified by a simulated match. Adding the recommended exporter fails the gate until the regex is anchored. The `deny.toml` `prometheus` ban is exact-name, so it is fine.

The requirements name **only webhooks and cron** (TRIG-01..03). There is **no Kafka or Chronik topic-consumer trigger** in REQUIREMENTS.md, so it is out of scope (see "Out of scope (verified)").

**Primary recommendation:**
- **Snapshots.** Build a *logical* snapshot (OSNP v1) exported from one fjall cross-keyspace `Snapshot`. Name it by its own `applied_offset`, write it with frame CRCs and a trailer logical digest, and install it through a staged directory rename.
- **Trim floor.** Drive the trim floor from durable local snapshots and, when bottomless is on, from uploaded data.
- **Upload.** Upload *uploader-sealed* envelope chunks, keyed `p/<pppp>/seg/<epoch>-<base>-<end>`, every ≤ 1 s against a node-level **consistent cut** of HW values.
- **Restore.** Restore = meta snapshot → blobs → per-partition snapshot ≤ cut → chunks up to the cut → verified replay.
- **Cron.** Fire cron through a new partition command applied in the schedule's owner partition. It is watermarked, which gives exactly-once independently of dedupe-row retention.
- **Telemetry.** Wire OTel 0.33.1 over the existing tonic 0.14.9 / prost 0.14.6.

---

<user_constraints>
## User Constraints

No `08-CONTEXT.md` exists (no `/gsd:discuss-phase` run for Phase 8). The constraints below come from the orchestrator brief, STATE.md decisions, PROJECT.md and the Phase 5 decisions (05-DECISIONS.md D1–D18). Treat them as locked.

### Locked decisions (project-level)
- **Single static binary, no C/C++ deps beyond the accepted `aws-lc-sys`** (PROJECT.md constraint D1). Not allowed: rdkafka/librdkafka, zstd-sys (no wasmtime `cache` feature, no `zstd-*` OTLP features), RocksDB, or OpenSSL/native-tls.
- **cargo-deny bans:**
  - no duplicate `tonic`, `tonic-prost`, `prost`, `prost-types`, `hyper`, `http`, `axum`, `opentelemetry`, `opentelemetry_sdk` or `wasmtime`;
  - no `openssl-sys`, `native-tls`, `tokio-uring`, `prometheus`, `chrono` or `bincode`;
  - `ci-local.sh` also greps `cargo tree` for `chronik|tokio-uring|prometheus `.
- **Metrics go through the `metrics` 0.24.6 facade.** It is already used: `ostro_wal_*`, `ostro_apply_*`, `ostro_activation_*`, `ostro_step_retries_total`, and others.
- **Versions in Cargo.lock:** tonic 0.14.9, prost 0.14.6, hyper 1.12.0, http 1.5.0, axum 0.8.9, tower 0.5.3, tracing-subscriber 0.3.23, reqwest 0.13.5, rustls 0.23.46 + aws-lc-rs 1.19.0, jiff 1.0.1 (exact pin, chrono banned), fjall =3.1.13 / lsm-tree 3.1.10, hmac 0.13.0, sha2 0.11.1, blake3 1.8.7, lz4_flex 0.13.2 (via lsm-tree).
- **Storage model:**
  - one fjall DB per partition (`state/pNNNN`, 13 keyspaces in Phase 2, plus `Running`, `Waits` and `SignalSeen` from Phases 4 and 5), one shared group-commit WAL (`wal/p/<id>/`), one shared `BlobStore` (`blobs/<hh>/<sha>`);
  - recovery replays from the state's own `applied_offset`;
  - the trim floor is `min(checkpoint, snapshot_floor, retention_floor)`, applied only with a `TrimPermit` from the `ReadyBarrier`;
  - `snapshot_floor` stays 0 until Phase 8.
- **D18 (Phase 5 freeze scope):**
  - Frozen: the log (envelope, `Command` prost, journal codec, command hash), `node.toml`, the blob layout and the meta Raft log/snapshot encoding.
  - Partition state values are *derived*.
  - The freeze is at the end of Phase 5. After it, changes are additive only (new fields, oneof variants, keyspaces, meta keys, outcome codes appended).
- **D9:** TTL deletion (EXPIRE_OP) deletes state rows. WAL bytes and blobs remain until Phase 8.
- **Roadmap:** the snapshot format is designed once, in Phase 8, and serves both restore and Phase 12 follower install.
- **Phase 8 depends on Phase 5 and runs in parallel with Phases 6–7.**
- **Standing rules:**
  - the build is warning-free and clippy runs with `-D warnings`; `bash scripts/ci-local.sh` is the gate; `FAILPOINT_CRATES` stays in sync with ci.yml;
  - never reference a Chronik clone path; no git remotes or pushes;
  - commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`;
  - executors do not run gsd-tools state/roadmap mutation commands.

### Claude's discretion (researched and recommended here)
Snapshot format, cadence and install mechanism. Bottomless object layout, sealing, manifest and integrity chain. Blob GC algorithm. Cron ownership and command design. Webhook mapping model. Crate choices for object storage, cron, OTel and Prometheus. Span model. Guest-log pipeline. Plan breakdown.

### Deferred / out of scope
- Kafka or Chronik topic-consumer triggers (not in TRIG-01..03).
- Azure Blob and native GCS JSON-API backends. GCS works through its S3-interoperable XML API with HMAC keys.
- Follower snapshot install (Phase 12). The format must support it, but the transport is Phase 12.
- Module GC (Phase 13, CLUS-07): Phase 8 blob GC must never delete module blobs.
- DLQ semantics (Phase 11). Phase 8 only exposes the `ostro_dlq_total` series.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| TRIG-01 | `POST /hooks/{name}` → workflow, with signature verification (Stripe, GitHub, Slack, generic HMAC) and idempotency-key extraction | "Webhooks": verifier specs, the `Webhook` meta record (pre-freeze finding F3), `start` via StartRun `idempotency_key` + ReturnExisting, `signal` via `SignalRun.request_id` (D13 SignalSeen), raw-body handling, Slack `url_verification` |
| TRIG-02 | Timezone-aware cron; each `(schedule, fire_time)` starts exactly once | "Cron": owner-partition `ScheduleFire` command with a durable per-schedule watermark, plus the existing `__cron/` dedupe row as a second guard; croner + jiff 0.2 adapter; bundled tzdb |
| TRIG-03 | Overlap policy, catch-up after downtime, pause, trigger-now | "Cron": overlap evaluated **in apply** (the schedule's runs live in its owner partition); catch-up policy evaluated by the leader service; pause is meta CAS; trigger-now is `ScheduleFire{manual}` |
| BACK-01 | Sealed segments (leader-only, below HW, keyed partition/epoch/base), state snapshots, meta snapshots and blobs stream to object storage | "Bottomless": uploader-sealed envelope chunks below HW on the leader; object layout; blob observer; meta snapshot after the cut |
| BACK-02 | `ostro restore --from <bucket>` with RPO ≤ seal interval; state hash matches | "Restore": consistent cut (HwVector), manifest, chain verification, two-step replay that checks `state_hash` at the manifest-recorded offset |
| BACK-03 | `ostro backup` / `restore` to and from a local file | "Backup file": the same layout on `LocalFs`, packed as a tar (`tar` 0.4.46, pure Rust). One restore code path |
| BACK-04 | One snapshot format for restore and follower install; journal trim coordinates with snapshots | "Snapshot format (OSNP v1)", "Enabling WAL trimming" |
| OBS-01 | Run → activation → step/effect spans, `traceparent` in and out, replayed spans not re-emitted | "Traces": deterministic ids through a custom `IdGenerator`, live-only emission, run span emitted from post-apply actions (never during catch-up), `TraceContextPropagator` |
| OBS-02 | `/metrics` via the `metrics` facade (queues, lag, steps, replay, fuel, memory, DLQ, HW lag, ISR, Raft) | "Metrics": metrics-exporter-prometheus 0.18.3 `install_recorder` + axum route + upkeep task + buckets; series table |
| OBS-03 | Guest logs structured, rate-limited, bounded per run, forwarded to node logs and OTLP | "Guest logs": the existing per-activation bound (1000 lines / 1 MiB) is extended with a per-run token bucket and lifetime cap, a ring buffer, JSON node logs, and an OTLP LogRecord bridge |
</phase_requirements>

---

## Pre-freeze findings (Phase 5 must act before 05-19)

These are things Phase 5's freeze (D18, plan 05-19) would lock in a way Phase 8 cannot fix additively. Severity: **BLOCKING** means it must be fixed before the freeze. **SHOULD** means it is cheap now and expensive later.

| # | Severity | Finding | Evidence | Recommended action in Phase 5 |
|---|----------|---------|----------|-------------------------------|
| F1 | **BLOCKING (policy text)** | D18 says "state values are derived; a STATE_FORMAT bump rebuilds from the log (after Phase 8, from snapshot + tail)". Once Phase 8 trims the log, **snapshot rows become a source of truth**. A STATE_FORMAT bump can no longer rebuild from the log, because the log prefix is gone and the snapshot holds old-format rows. The same applies to Phase 12 follower install and to the Phase 13 N/N-1 rolling upgrade (an N-1 follower installing an N leader's snapshot). | `docs/storage-format.md` §6 values are positional postcard (`VALUE_FORMAT 0x01`). 05-02-PLAN line 173 contains the D18 text. | Amend the D18 text in 05-02 / 05-19: "State values are derived **while the full log is retained**. After Phase 8 enables trimming, every STATE_FORMAT / VALUE_FORMAT change must ship a row migrator (vN → vN+1) applied at snapshot install and store open. The snapshot carries `state_format`. Readers must accept every older snapshot format." No code change. |
| F2 | **BLOCKING** | Meta encoding is positional postcard. `MetaState` (the snapshot) is a struct of one `BTreeMap` per kind. `Schedule` is a positional struct, and so are the `MetaCommand::Put(MetaRecord::Schedule(..))` Raft entries. Phase 8 needs `Schedule` fields that do not exist: timezone, overlap policy, catch-up policy/window, and optionally start/end bounds and jitter. Adding fields to a positional postcard struct after the freeze breaks decoding of old Raft entries and snapshots. | `crates/ostro-meta/src/records.rs:105` (`Schedule{name, workflow, version, cron, input, paused}`), `state.rs:19` (`MetaState`), `sm.rs:58` (`[META_FORMAT] ‖ postcard(MetaState)`) | Before 05-19, either **(a)** extend `Schedule` now with `timezone: String`, `overlap: u8`, `catchup: u8`, `catchup_window_ms: u64`, `start_at_ms: Option<u64>`, `end_at_ms: Option<u64>`, `created_at_ms: u64`, plus a trailing `ext: Vec<u8>` reserved for postcard-encoded future options, **or (b)** document that post-freeze meta shape changes use a **new `MetaRecord` variant appended last** (`ScheduleV2`) plus a `META_FORMAT` bump whose decoder reads every older format. **Recommend (a) for `Schedule` and (b) as the general policy**, because Phases 6, 7, 11 and 14 will also add meta kinds. |
| F3 | **BLOCKING (same mechanism as F2)** | Phase 8 needs a new meta kind `Webhook` (name, action start/signal/complete-token, workflow, verifier kind + secret ref, idempotency extractor, tolerance). Appending a `MetaRecord` variant is decode-compatible for Raft entries. Adding a `webhooks` map to `MetaState` **changes the snapshot struct**. | `state.rs:19-34` | Add `MetaKind::Webhook` + `MetaRecord::Webhook` + `MetaState.webhooks` **before** the freeze (an empty map costs nothing). Otherwise adopt F2(b), the META_FORMAT bump with a v1 decoder. |
| F4 | **BLOCKING (coordination)** | Phases 6, 7 and 8 run **in parallel** after the freeze. All three append to ordinal-encoded spaces: `Cmd` oneof tags, `TABLES` order (**the digest order**), journal `EventBody` kinds, outcome codes, `InvalidCode`, `MetaRecord` variants (postcard variant index) and `MetaState` fields. Parallel appends will collide on numbers and on digest order. | 05-DECISIONS "Contract tags"; storage-format.md §6 ("declaration order is the digest order") | In 05-19, add a **reservation table** to `docs/storage-format.md` that pre-allocates blocks per phase. Example: Cmd tags 13–19 Phase 6, 20–26 Phase 7, 27–33 Phase 8. Keyspace ordinals in the same style. Meta variants: one fixed order list. Phase 8 needs: **1 Cmd tag** (`ScheduleFire`), **1 keyspace** (`ScheduleState`), **1–2 InvalidCode values**, **0 journal kinds**, **1 MetaRecord variant** (`Webhook`, unless done pre-freeze per F3). |
| F5 | SHOULD | Blob GC and the bottomless blob uploader need "every `PayloadRef` inside a state row or a command". Today refs sit at prost `PayloadRef` fields and in a few postcard values. Nothing stops a later phase from embedding a `BlobRef` inside *opaque* payload bytes, where GC cannot see it. | `ostro-types/src/payload.rs`; `store/values.rs:81` | Freeze the invariant now in storage-format.md: "A `BlobRef` appears only in a `PayloadRef` field of a prost message or a documented postcard value, **never inside opaque payload bytes**." Optionally add `fn blob_refs(&self, out: &mut Vec<BlobRef>)` on `Command` with an exhaustive match, so every new variant must say where its refs are. |
| F6 | SHOULD | `node.toml` will need `node_id` / `incarnation` (bottomless prefix ownership) and later `cluster_id`. | `node.rs:542 parse_node_file` already ignores unknown keys (its test includes `other = x`). | Nothing blocking. Document in storage-format.md §1 that unknown `node.toml` keys are ignored and new keys are additive. |
| F7 | INFO | The `cursor` meta row already carries `epoch_history`. This is exactly what a snapshot needs so that a post-trim node or follower can answer `OffsetForLeaderEpoch` for epochs below `log_start`. `Rf1Log::trim_prefix` already calls `epochs.truncate_from_start`. | storage-format.md §7; `rf1.rs:592` | No change. OSNP carries the cursor row verbatim. |

---

## Standard Stack

All versions were verified on crates.io on 2026-11-19. Compatibility was checked against the dependency requirements of each crate's exact version.

### Core (new in Phase 8)
| Library | Version | Purpose | Why |
|---------|---------|---------|-----|
| `opentelemetry` | **0.33.1** | OTel API (traces, logs, metrics) | Current. All `opentelemetry*` crates must share 0.33. |
| `opentelemetry_sdk` | **0.33.1** (`features = ["trace","logs","rt-tokio"]` as needed; add `testing` as a dev-dependency for `InMemorySpanExporter`) | SDK: batch processors, `IdGenerator`, `TraceContextPropagator` | Its deps are `opentelemetry ^0.33.1` and `rand ^0.9` (0.9.5 is already in the lock). |
| `opentelemetry-otlp` | **0.33.1**, `default-features = false, features = ["grpc-tonic","trace","logs","tls-aws-lc"]` (add `"metrics"` only if OTLP metrics are wanted) | OTLP exporter | Requires `tonic ^0.14.1`, `prost ^0.14` and `tonic-prost ^0.14.1` (via opentelemetry-proto), so it resolves to the locked **0.14.9 / 0.14.6**. No duplicates. **Never enable `zstd-tonic` or `zstd-http` (zstd-sys is C).** The defaults (`http-proto`, `reqwest-blocking-client`) must be off. |
| `tracing-opentelemetry` | **0.34.0** | Bridges node `tracing` spans to OTel | Requires `opentelemetry ^0.33.0` and `tracing-subscriber ^0.3.22` (0.3.23 is locked). |
| `opentelemetry-appender-tracing` | **0.33.1** | Node `tracing` events → OTLP logs | Default features are empty. |
| `metrics-exporter-prometheus` | **0.18.3**, `default-features = false` | `/metrics` text exposition via `PrometheusHandle::render()` | Depends on `metrics ^0.24.5` (0.24.6 locked) and `metrics-util ^0.20`. With default features off, it pulls **no hyper server, no hyper-rustls and no prost**. `install_recorder()` needs no feature, but the caller must run upkeep. |
| `rusty-s3` | **0.12.0** (default features: `rustcrypto`, `full`) | SigV4 signing and S3 action builders (sans-IO) | Requires `jiff ^1.0` (unifies with the 1.0.1 pin), `hmac ^0.13` and `sha2 ^0.11` (both already in the lock), `url`, `percent-encoding` and `zeroize`. BSD-2-Clause. Actions: Put/Get/Head/Delete/DeleteObjects, ListObjectsV2, Create/Upload/Complete/Abort multipart. ~2.1M downloads. |
| `reqwest` | 0.13.5 (workspace pin), add features **`rustls`, `stream`** for `ostro-objstore` | HTTP transport for S3 | Already in the tree. The `rustls` feature uses aws-lc-rs + rustls-platform-verifier. Do **not** enable `zstd` or `native-tls`. |
| `croner` | **4.0.3**, `default-features = false, features = ["jiff"]` | Cron parsing and next-occurrence (seconds field, `L`, `#`, `W`, DST rules) | The most-used cron crate (13.5M downloads). MIT. It brings **jiff 0.2.x** (a duplicate major; `multiple-versions = "warn"`, not banned). Its default feature is `chrono`, which **must be off**. |
| `jiff` 0.2 (transitive, also a direct dependency of the adapter crate only) | 0.2.46 | `Zoned` values for croner | Isolated in the adapter (see Pattern 9). |
| `tar` | 0.4.46 | `ostro backup --to file.tar` | Pure Rust. |

### Supporting
| Library | Version | Purpose | When |
|---------|---------|---------|------|
| `lz4_flex` | 0.13.x (same major as lsm-tree's 0.13.2) | Optional per-frame snapshot compression | Pure Rust. The latest is 0.14.2, but use 0.13 so it unifies with lsm-tree. |
| `tracing-subscriber` | 0.3.23 + feature **`json`** | Structured JSON node logs | Add `LogFormat::Json`. |
| `subtle` 2.6.1 or `hmac::Mac::verify_slice` | — | Constant-time signature comparison | Prefer `Mac::verify_slice` (already in hmac 0.13). |
| `s3s` / `s3s-fs` | 0.17.0 (dev-dependency only) | In-process S3 server for protocol tests without Docker | s3s-fs depends on chrono, but `deny.toml` has `exclude-dev = true`. Verify that cargo-deny ignores it; if not, use SeaweedFS in Docker (`#[ignore]`). |
| `webpki-root-certs` (or the reqwest `tls_certs_merge` API) | latest | CA roots when the binary runs `FROM scratch` (no `/etc/ssl/certs`) | Only for the distroless/scratch image path. Flag for Phase 14 but wire the config knob now. |

### Alternatives considered
| Instead of | Could use | Tradeoff / verdict |
|------------|-----------|--------------------|
| rusty-s3 + reqwest | `object_store` 0.14.2 | **Rejected while chrono and humantime are required** (deny ban + unmaintained advisories). Re-evaluate when arrow-rs-object-store #909/#908 land. The `ostro-objstore` trait keeps a swap cheap. |
| rusty-s3 + reqwest | `opendal` 0.59.5 | Large crate family (about 90 sub-crates). Its dependency graph was not audited for chrono. Not worth it for S3 plus local. |
| croner (jiff 0.2) | Hand-rolled cron on jiff 1.0 | **Don't.** DST gap and overlap semantics are subtle (croner #67 fixed an extra run after a DST gap in 4.0.3). Revisit only if croner never ships jiff 1. Optionally upstream a `jiff1` feature. |
| croner | `jiff-cron` 0.3, `cronexpr` 1.7.2 | Also jiff 0.2. Smaller user base. |
| metrics-exporter-prometheus | `prometheus-client` 0.26 | Would bypass the `metrics` facade that every crate already uses. Rejected. |
| OTLP gRPC (tonic) | OTLP http/protobuf via reqwest | gRPC reuses tonic. Offer `protocol = "grpc"` only in Phase 8. HTTP can be added later (`http-proto` + `reqwest-blocking-client`) if users need it. |
| Logical snapshot | Physical fjall file copy | **Rejected.** fjall has **no checkpoint or backup API** (verified: the `Keyspace` methods are only `start_ingestion` plus CRUD/iter). Its file layout is explicitly not Ostro's format (storage-format.md §1). Follower install across versions needs a logical format. |
| Uploader-sealed chunks | Rotating WAL segments every 1 s | Rejected. It would create about 86k segment files per partition per day, and reopen grows with segment count (02-16 todo). |

**Installation (root `Cargo.toml` `[workspace.dependencies]`):**
```toml
opentelemetry = "0.33.1"
opentelemetry_sdk = { version = "0.33.1", default-features = false, features = ["trace", "logs", "rt-tokio"] }
opentelemetry-otlp = { version = "0.33.1", default-features = false, features = ["grpc-tonic", "trace", "logs", "tls-aws-lc"] }
tracing-opentelemetry = "0.34.0"
opentelemetry-appender-tracing = "0.33.1"
metrics-exporter-prometheus = { version = "0.18.3", default-features = false }
rusty-s3 = "0.12.0"
# chrono is banned: the jiff backend only (pulls jiff 0.2 inside the ostro-triggers adapter)
croner = { version = "4.0.3", default-features = false, features = ["jiff"] }
jiff02 = { package = "jiff", version = "0.2.46", default-features = false, features = ["alloc", "std"] }
tar = "0.4.46"
lz4_flex = { version = "0.13", default-features = false, features = ["std"] }
# existing pins: enable features where used
# tracing-subscriber: add "json"; reqwest: add "rustls", "stream" (ostro-objstore only); jiff: add "tzdb-bundle-always"
```
Verify `rt-tokio` against the 0.33 feature list before pinning. In 0.28+ the default batch processors run on SDK-owned threads, so `rt-tokio` may be unnecessary. That is MEDIUM confidence.

---

## Architecture Patterns

### Recommended crate structure
```
crates/
├── ostro-objstore/        # NEW. ObjectStore trait; LocalFs + S3 (rusty-s3 + reqwest). Shared with Phase 6 FX-05
├── ostro-backup/          # NEW. bottomless uploader, manifest, remote GC, restore, backup file (tar)
├── ostro-triggers/        # NEW. cron engine (croner + jiff 0.2 adapter, pure), webhook verifiers (pure)
├── ostro-partition/
│   ├── src/store/snapshot.rs      # OSNP v1 writer/reader + install (owns TABLES, digest)
│   ├── src/store/blobrefs.rs      # exhaustive Table -> PayloadRef visitor (GC + upload)
│   ├── src/apply/schedule.rs      # Cmd::ScheduleFire apply (watermark, overlap)
│   ├── src/leader/snapshot.rs     # snapshot service (cadence, retention, floor watch)
│   ├── src/leader/retention.rs    # dynamic floors (snapshot + upload watches)
│   └── src/leader/cron.rs         # owner-partition cron driver
├── ostro-log/src/cut.rs           # HwVector: an atomic snapshot of every partition's HW (consistent cut)
├── ostro-blob/                    # + put() touches mtime on existing blobs; on_put observer; gc lock; sweep()
├── ostro-engine/src/telemetry.rs  # span hooks (opentelemetry API only, no SDK)
├── ostro-server/src/telemetry.rs  # subscriber (pretty/compact/json), OTLP pipelines, Prometheus recorder + upkeep
├── ostro-api/                     # /metrics, /hooks/{name}, schedules + webhooks CRUD, admin backup stream, run logs
└── ostro-cli/                     # ostro backup / restore / schedules / webhooks verbs
docs/snapshot-format.md            # NEW. OSNP v1 + remote layout (Phase 12 reads this)
```

### Pattern 1: Snapshot creation (logical export from one fjall snapshot)

**What:** `PartitionStore::export_snapshot(&self, w: impl Write) -> SnapshotInfo` opens `self.db.snapshot()`, a cross-keyspace MVCC snapshot. It reads `applied_offset`, `state_hash` and `cursor` **from that same snapshot**, then streams every keyspace in `TABLES` order.

**Why it is consistent with `applied_offset` (HIGH, code-verified in fjall 3.1.13):**
- `OwnedWriteBatch::commit` allocates **one** seqno for the whole batch (`batch/mod.rs:113`). It inserts every item at that seqno and only then calls `snapshot_tracker.publish(batch_seqno)` (line 164).
- Ostro commits each apply chunk, including `applied_offset`, `state_hash` and `cursor`, as one batch (storage-format.md §7).
- So any `db.snapshot()` sees each apply chunk entirely or not at all. The `applied_offset` row read from it names exactly the state it shows.
- **The apply thread does not need to pause.** Run the export on a blocking thread.

**Costs to plan for:**
- A long-lived snapshot pins MVCC versions, so compaction cannot drop old versions until it is released. That means temporary space amplification.
- A full scan pollutes the 16 MiB per-partition block cache (the 02-16 open concern). Measure apply throughput during export (see Pitfall 6).

**Durability:** write to `snapshots/pNNNN/.tmp-<off>`, `sync_all`, rename to `snapshots/pNNNN/<applied 20-digit>.osnap`, fsync the directory. **Only then** publish the snapshot floor.

### Pattern 2: Snapshot format OSNP v1 (one format for restore and follower install)

```
OSNP v1 file = Header ‖ Frame* ‖ Trailer-frame
Header (64 B, LE): magic "OSNP" u32 | snap_ver u16 = 1 | flags u16 (bit0 = lz4 frames)
                   | partition u16 | partition_count u16 | state_format u32 (meta format_ver)
                   | applied_offset u64 (exclusive: next offset to apply) | latest_epoch u32
                   | created_at_ms u64 | reserved [u8; 16] = 0 | header_crc32c u32
Frame: u32 LE len | u8 kind | payload | u32 LE crc32c(kind ‖ payload)
  kind 1 TableBegin { u16 name_len, name }
  kind 2 Rows       { (u32 klen, key, u32 vlen, value)* }  (≤ 1 MiB uncompressed; lz4 if flag)
  kind 3 TableEnd   { u64 row_count }
  kind 4 BlobRefs   { u32 n, [sha256; n] sorted, deduplicated }   (refs of THIS state; GC + restore)
  kind 5 Trailer    { [u8;32] logical_digest, [u8;32] state_hash, u64 total_rows, u64 frames }
Unknown frame kinds < 128: reader error. ≥ 128: skippable (forward-compatible extension).
```
- `logical_digest` uses **exactly the `store/digest.rs` layout**, so `store.digest()` after install must equal the trailer. That is the cheap, strong install check.
- Meta `checkpointed_offset` is **not exported**, matching the digest exclusion. Install writes it as `applied_offset`.
- The `cursor` row (with `epoch_history`) is exported verbatim (F7).
- Remote key: `p/<pppp>/snap/<applied 20-digit>-<sha256 hex>.osnap`, where the sha256 covers the whole file. Phase 12 streams the same bytes frame by frame over its fetch RPC.

### Pattern 3: Snapshot install (staged directory, then atomic rename)
1. Create `state/.install-pNNNN` and open a fresh fjall DB there with production `StoreConfig`.
2. For each table, use `keyspace.start_ingestion()` and `write(k, v)` in key order, then `finish()`. This was verified in fjall 3.1.13: ingestion writes tables directly and bypasses memtables and journal. Write the meta keyspace **last** through a `WriteBatch`, then `persist(SyncAll)`.
3. Close, reopen, check `digest() == trailer.logical_digest` and `meta.applied_offset == header.applied_offset`.
4. Close. Rename the old `state/pNNNN` aside, if present. Rename the staging directory to `state/pNNNN`. fsync `state/`. Delete the old directory.

The fsync behaviour of ingestion is MEDIUM confidence (the source shows atomic registration, but fsync was not traced). The reopen + digest check plus the directory rename make correctness independent of it. Fallback: `WriteBatch` chunks of about 4 MiB if ingestion misbehaves.

### Pattern 4: Enabling WAL trimming safely
Replace the static `RetentionConfig.snapshot_floor: Offset` with `watch::Receiver<Offset>` floors:
```
local_floor  = applied_offset of the OLDEST retained local snapshot (keep 2: defence against a corrupt latest)
upload_floor = min(latest uploaded remote snapshot offset, uploaded chunk end)   # only if bottomless enabled
floor        = min(force_checkpoint(), local_floor, upload_floor?, retention_floor?)
```
- The rest of the existing pass stays: `force_checkpoint` → `ReadyBarrier::mint(floor)` → `trim_prefix`, which deletes only whole non-active segments with `last < floor`.
- **Invariant (SC4):** `log_start ≤ applied_offset(oldest retained snapshot)`, so every retained snapshot plus the log rebuilds the state.

`Node::open` changes:
- `applied < log_start` (today "snapshot required") **or** missing state → install the newest local snapshot with `log_start ≤ S ≤ LEO`. Then replay `[S, HW)` as today.
- No usable local snapshot → fail the partition with "snapshot required: run `ostro restore`". Do not auto-fetch from remote in Phase 8.
- The STOR-06 rebuild (delete `state/` and reopen) now means "newest local snapshot + tail". Update `ostro-crashtest`'s `rebuild_and_compare` accordingly.

**Cadence:** take a snapshot when `HW - last_snapshot ≥ snapshot_bytes_or_offsets` (default ≈ one WAL segment's worth, 256 MiB of envelopes), or every `snapshot_interval` (default 1 h), whichever comes first, with at least `min_snapshot_interval` (60 s) between snapshots. Skip the snapshot if nothing was applied.

### Pattern 5: Bottomless upload with a consistent cut

**Problem found (HIGH):**
- Partitions exchange messages through the outbox and shuffler with `(src, seq)` dedupe.
- If partition A is restored to a point *after* `OutboxTruncated(seq)`, but target B is restored to a point *before* the matching `InboundBatch`, the message is lost.
- In the reverse case, A's restored `outbox_next_seq` is below `B.inbound_seen[A]`, so A's next messages are **silently dropped as duplicates**.
- So independently timed per-partition restore points break exactly-once.

**Solution:** restore every partition to the same **causally consistent cut**.
- A send is durable in A, below HW_A, before the shuffler can deliver it. Therefore a vector of HWs read **atomically at one instant** is causally closed.
- The WAL has **per-partition** queues and commit callbacks, so there is no global order to rely on. Instead, add `HwVector` to `ostro-log`: a `Mutex<Vec<Offset>>` updated inside the single commit callback (`node.rs:130`), next to `send_if_modified`.
- `LogNode::cut() -> Vec<Offset>` reads it under the lock. Cost: one uncontended lock per group commit.

**Uploader (one node-level task, leader services only, after all barriers open), on every tick (`seal_interval`, default 1 s):**
1. `cut = log_node.cut()`.
2. **Blobs first:** upload every local blob put before the cut read that is not yet uploaded. Blob puts happen before the append that references them, so this covers every ref below the cut. Track new puts through a `BlobStore` `on_put` observer channel. At startup, reconcile the local listing against a remote `LIST blobs/`.
3. **Chunks per partition:** read verified batches `[uploaded_end_p, cut_p)` (`ReplicatedLog::read`, which is HW-capped), **split at epoch boundaries**, and PUT `p/<pppp>/seg/<epoch 10d>-<base 20d>-<end 20d>.oseg`.
4. **Meta snapshot**, if the meta applied index changed, captured **after** step 1. Modules and schedules referenced by runs below the cut were committed before those runs. PUT `meta/<index 20d>.msnap`.
5. **Manifest:** PUT `manifest/current` with `If-Match: <previous etag>`.

Contents of a chunk object (`.oseg`):
```
OSEG v1: magic "OSEG" | ver u16 | partition u16 | epoch u32 | base u64 | end u64 | count u32
       | prev_chunk_sha256 [32] | body: (u32 LE len ‖ OSTR envelope)* | trailer sha256(header ‖ body)
```
- Store **OSTR envelopes** (the frozen unit, with CRC32C), not vendored WAL frames.
- `VerifiedBatch` holds decoded commands, and envelope encoding is deterministic. So either re-encode, asserting byte-identity in a test, or (preferred, and also useful for Phase 12 fetch) add a `read_envelopes` variant that returns the raw envelope bytes.
- `prev_chunk_sha256` links each partition's chunks into a **hash chain**.

Contents of the manifest (`manifest/current`, small: about 1–3 KB at P=16; postcard or JSON plus sha256):
```
{ format: 1, cluster_id, node_id, incarnation, gen: u64, prev_manifest_sha256,
  cut_at_ms, cut: [ {p, cut_offset, last_chunk_key, last_chunk_sha256,
                     snapshot_key, snapshot_sha256, snapshot_offset,
                     verify_offset, verify_state_hash} ],
  meta_snapshot_key, meta_snapshot_sha256, ostro_version }
```
- `verify_offset` / `verify_state_hash` is the `(applied_offset, state_hash)` pair the leader observed at tick time, where `applied ≤ cut`. Restore checks the rolling hash there.
- Write an immutable copy `manifest/gen/<gen 20d>` on every snapshot round, for point-in-time-restore anchors.
- **Seal semantics:** BACK-01 says "sealed segments". Here, *sealed* means the uploader closed the chunk at a tick. A chunk is never appended to after upload. State this interpretation in the docs.

**Fencing / prefix ownership:**
- `LOCK` object `{cluster_id, node_id, incarnation}` created with `If-None-Match: *` when bottomless is first enabled.
- The manifest PUT uses `If-Match`. A failed precondition means another writer owns the prefix: stop uploading, set `ostro_backup_fenced = 1`, and log an error.
- A node restored from the bucket gets a new incarnation and needs `--takeover` to continue uploading to the same prefix. Otherwise the operator configures a new prefix.
- Conditional PUT support: AWS S3 (2024), SeaweedFS and R2. This is MEDIUM confidence and must be verified against SeaweedFS in tests. Keep a LOCK re-read fallback for providers without `If-Match` on PUT.

**Remote retention:**
- Keep the last `K` remote snapshots per partition (default 3) and every chunk with `end > oldest kept snapshot offset`. Delete older chunks and snapshots after the new manifest is durable.
- Remote blobs: see Pattern 7.
- **Cost note (document it):** at P=16 busy partitions and a 1 s seal, that is about 17 PUT/s ≈ 1.47M PUT/day ≈ $7/day on S3 Standard. Idle partitions upload nothing. `seal_interval` is configurable, and 5–10 s is recommended when RPO allows.

### Pattern 6: Restore (`ostro restore --from s3://bucket/prefix [--at <gen|time>] --data-dir <empty>`)
1. Refuse a non-empty data directory.
2. GET the manifest (`current` or `gen/<n>`). Verify its sha and format. `--at` walks the gen manifests.
3. Write `node.toml` (`format_version = 1`, `partitions = P`).
4. Meta: download the meta snapshot, verify its sha, and bootstrap the single-voter meta store from it. This needs a `MetaHandle` "bootstrap from snapshot" entry point (META snapshot encoding is frozen, so it is safe).
5. Blobs: download every remote blob listed in the restored snapshots' `BlobRefs` frames, plus module shas from meta. Simplest correct option: download all remote blobs.
6. Per partition, in parallel:
   - download the snapshot (sha check, frame CRCs, digest after install, Pattern 3);
   - LIST `p/<pppp>/seg/` with start-after at the snapshot offset and download the chunks;
   - verify the chain: `prev_sha` links, `base == prev end`, epochs non-decreasing, every envelope CRC32C, last chunk sha == manifest;
   - re-wrap the envelopes `[S, cut)` into WAL V2 records (`WalRecord` encoder) in `wal/p/<id>/wal_<id>_0.log`, truncated exactly at `cut` (a batch boundary), then fsync the file and the directory.
7. **Two-step verified replay:** open Inspect mode with HW capped at `verify_offset` and check that `state_hash` equals the manifest value. Then raise the cap to `cut`. Report the cut time, the RPO estimate (`now_at_failure - cut_at_ms` is unknown offline, so report `cut_at_ms`) and per-partition offsets and hashes.
8. A normal `ostro server` start then bumps the epoch (`LeaderAnnounced`) as usual.

### Pattern 7: Blob GC (local, plus delayed remote)

**Key fact (HIGH, from the `ostro-blob` docs and the apply design):** apply never reads blob bytes, so **replaying the log needs no blobs**. Blobs are needed only for refs held by *current state* and for in-flight work.

**Local GC (node service, default every 1 h, after every barrier is open):**
- **Mark** = ⋃ over partitions of `blob_refs(state)`, taken from one fjall snapshot per partition with the exhaustive `Table` visitor (F5), ∪ meta `Module` shas (modules are stored in `BlobStore`, 04-17) ∪ shas referenced by retained local snapshots (their `BlobRefs` frame, cached as `<snap>.blobrefs`).
- **Sweep:** under `BlobStore`'s GC write lock (`put` takes the read lock), delete every blob that is unmarked **and** has an mtime older than `gc_start - grace` (default grace 1 h).
- **Required change:** `BlobStore::put` on an *existing* blob must **touch its mtime** (`File::set_modified`). Otherwise an old, unreferenced blob that is re-put by an API call can be swept between put and apply (Pitfall 3).

**Remote GC:** when local GC deletes sha X, append `(X, deleted_at)` to a local ledger. Delete remote X only after `deleted_at + remote_retention_window`, where the window is the age of the oldest kept remote snapshot. Then no retained remote restore point references X. Re-referencing X goes through `put`, which is young again and re-uploaded.

### Pattern 8: Cron (owner partition, watermarked command)

**Design (matches docs/03 §2.2 `ScheduleFired`):**
- **Owner:** `owner(schedule) = xxh3(name) % P`, using the keyed-routing family with a reserved namespace.
- **Driver:** the owner partition's **leader service** `leader/cron.rs` watches meta `Schedule` records. For each schedule it owns, it keeps the next fire times in the existing timer-window style.
- **Fire:** when a fire time is due, the driver submits **`Cmd::ScheduleFire{schedule, fire_time_ms, start: StartRun, overlap, manual: bool}`** (a new tag from the F4 reservation).
- **Apply, in the owner partition,** using a new `ScheduleState` keyspace (`str schedule → postcard {watermark_ms, last_run, buffered: Option<fire_time>, fired, skipped}`):
  - `!manual && fire_time ≤ watermark` → `Duplicate`. This gives exactly-once across restarts, leader changes and dedupe-row expiry.
  - Overlap is checked **locally**, because the schedule's runs are started **in the owner partition**. `last_run` non-terminal:
    - `Skip`: advance the watermark and record a skip.
    - `BufferOne`: store `buffered`. The on_terminal hook of `last_run` starts it.
    - `AllowAll`: start.
    - `CancelOther` / `TerminateOther`: local RequestCancel / TerminateRun, then start.
  - The run is started with the existing dedupe row `("__cron/<schedule>", fire_time)` as a second guard.
  - Advance the watermark atomically in the same batch.
- **Catch-up** (on leadership or after downtime): the driver computes the fire times in `(watermark, now]` and applies the policy:
  - `Skip`: fire none; one ScheduleFire with `catchup_skip` advances the watermark.
  - `One`: fire the latest only.
  - `All`: every fire time within `catchup_window`, capped at N, for example 100.
- **Pause:** meta CAS `paused = true`. The driver stops, and the watermark stays.
- **Resume:** catch-up policy applies.
- **Trigger-now:** `ScheduleFire{manual: true, fire_time = now}`. It does not touch the watermark, and its dedupe key includes a request id.
- The existing outbox `CronStart` (frozen tag 5) stays valid but unused. Document that.

### Pattern 9: croner + jiff 0.2 adapter (contain the duplicate major)
- `ostro-triggers::cron` exposes only jiff 1.0 / `i64 ms` types.
- Inside, `tz_bytes(name)` reads `/usr/share/zoneinfo/<name>`, falling back to the bundled `jiff-tzdb` bytes. The **same TZif bytes** build `jiff::tz::TimeZone::tzif(name, bytes)` (1.0) and `jiff02::tz::TimeZone::tzif(...)` (0.2), so both majors agree on the rules.
- Enable jiff 1.0 `tzdb-bundle-always`. A `FROM scratch` image has no zoneinfo, and timezone-aware schedules would otherwise fail.
- `next_after(expr, tz, after_ms) -> Option<i64>` calls `croner::Cron::find_next_occurrence(&zoned02, false)`.
- Test DST vectors: America/New_York 2026-03-08 and 2026-11-01, Europe/Berlin 2026-03-29 and 2026-10-25, Australia/Lord_Howe (30-minute DST).
- MEDIUM confidence. The first task is a spike: croner + jiff 0.2 compiles under the deny policy, and `TimeZone::tzif` exists with that shape in both majors.

### Pattern 10: Webhooks
- **Route:** `POST /hooks/{name}`. Take the **raw body bytes**: no JSON extractor before verification, and a body limit from config (default 1 MiB).
- **Lookup:** meta `Webhook` record (F3) with `verifier` (and its secret reference via the Phase 6 secrets resolver when it exists; until then, a sealed meta secret), `action` and an `idempotency` extractor (`header:<name>` | `json:<pointer>` | `none`).
- **Verifiers** (pure functions in `ostro-triggers::webhook`; constant-time `Mac::verify_slice`):

| Kind | Header(s) | Signed content | Algo / encoding | Replay window | Dedupe key default |
|---|---|---|---|---|---|
| Stripe | `Stripe-Signature: t=<unix>,v1=<hex>[,v1=…][,v0=…][,secret=…,destination=…,payload=…]` | `"{t}.{raw_body}"` | HMAC-SHA256 hex with the endpoint secret (`whsec_…`, used as-is) | `|now - t| ≤ 300 s` (configurable) | JSON `/id` (`evt_…`) |
| GitHub | `X-Hub-Signature-256: sha256=<hex>` | raw body | HMAC-SHA256 hex | none (no timestamp) | header `X-GitHub-Delivery` |
| Slack | `X-Slack-Signature: v0=<hex>`, `X-Slack-Request-Timestamp` | `"v0:{ts}:{raw_body}"` | HMAC-SHA256 hex | 300 s | `X-Slack-Retry-Num` aware; key from body (`event_id`) |
| Generic HMAC | configurable header, optional prefix, optional timestamp header + template | configurable (`{body}` or `{ts}.{body}`) | sha256 (sha1 opt-in, legacy), hex or base64 | optional | configurable |
| (preset) Standard Webhooks | `webhook-id`, `webhook-timestamp`, `webhook-signature: v1,<b64> …` | `"{id}.{ts}.{body}"` | HMAC-SHA256 base64; secret `whsec_<b64>` | 300 s | `webhook-id` |

  Stripe rules:
  - accept any number of `v1` entries (secret rotation produces one per active secret);
  - **ignore unknown entries** (`secret=`, `destination=` and `payload=` are unsigned debug metadata and must never be trusted);
  - test events also carry `v0`.

  Slack rules: answer `{"type":"url_verification"}` with the `challenge`. Respond within 3 s.
- **Actions:**
  - `start{workflow, version?, input: body|json-pointer}` → StartRun with `idempotency_key` and `ConflictPolicy::ReturnExisting`. A duplicate delivery returns the existing run with 200. The dedupe window equals run retention (D9, ≥ 24 h). Stripe retries for up to 3 days, so document "retention ≥ 72 h for Stripe hooks".
  - `signal{workflow, run_by: json-pointer to run id | (workflow, idempotency json-pointer), name}` → `SignalRun` with `request_id = delivery key` (D13 SignalSeen dedupe).
  - `complete_token{token: json-pointer}` → `CompleteToken`.
- **Response:** 2xx fast, after the command is acknowledged (applied), never after workflow completion.

  SC1's "Stripe webhook → workflow → signal round trip" is satisfied by:
  1. Hook A `start` (key `/data/object/id` = checkout session id).
  2. The workflow waits on signal `paid`.
  3. Hook B `signal` (run by `(workflow, /data/object/id)`), which completes the run.

### Pattern 11: Traces (OBS-01)
- **Trace id:** from the run's stored `traceparent` (`StartRun` field 17, 04-09), else derived as `blake3("ostro/trace" ‖ run_id)[..16]`.
- **Span ids** are deterministic: `blake3("ostro/span" ‖ run_id ‖ kind ‖ n)[..8]`. Spans:
  - `workflow.run`: n = 0;
  - `activation`: n = (attempt, activation counter);
  - `step`: kind `step` with the journal seq;
  - `effect:http`: kind `effect` with the op id.
- **opentelemetry 0.33.1 `SpanBuilder` has no `trace_id`/`span_id`/`end_time` fields** (verified on docs.rs). So:
  - set ids through a **custom `IdGenerator`** (`TracerProviderBuilder::with_id_generator`, verified), which reads a thread-local "next ids" override set right before `start_with_context`;
  - set start times with `SpanBuilder::with_start_time` and end times with `Span::end_with_timestamp` (both verified);
  - set parents with a remote parent `SpanContext` in a `Context`, so trace ids are inherited.
- **No re-emission on replay:**
  - Activation spans are emitted live.
  - Step and effect spans are emitted **only for live execution** (the host already knows `is_replaying`).
  - The `workflow.run` span (start = `created_at`, end = `completed_at`) is emitted from the **post-apply `Action::RunCompleted`**. Actions are off during startup catch-up (02-12), so recovery never re-emits it.
  - Children reference the run span id before it is exported. OTLP backends accept a late parent.
- **Propagation in:** parse and validate W3C `traceparent` at start (already stored).
- **Propagation out:** a helper `inject_traceparent(&SpanContext, &mut http::HeaderMap)` (`TraceContextPropagator`). Phase 6's HTTP effect must call it. If Phase 8 lands first, test the helper directly and add a cross-phase integration test when FX-01 lands.
- **Node-internal spans** (API requests, apply batches): `tracing` + `tracing-opentelemetry` 0.34 layer.
- **Engine boundary:** `ostro-engine` depends on the `opentelemetry` API crate only. The SDK is configured in `ostro-server`.

### Pattern 12: Metrics exposition (OBS-02)
- `PrometheusBuilder::new().set_buckets_for_metric(Matcher::Suffix("_seconds".into()), &[...])?.install_recorder()?` returns a `PrometheusHandle`.
- Spawn a task that calls `handle.run_upkeep()` every 5 s. The docs say the caller is responsible for upkeep when not using the listener.
- Serve `GET /metrics` from axum with `handle.render()` and `Content-Type: text/plain; version=0.0.4`.
- **Set buckets explicitly.** The docs contradict themselves on summaries vs histograms, so test that `_bucket` lines are rendered.
- Install the recorder once per process. Tests use `metrics::with_local_recorder` (the 02-16 measurement tests already capture with a test recorder).

Series (labels bounded; **never `run_id`**):

| Area | Series |
|---|---|
| Queues | `ostro_queue_ready{queue}` gauge, `ostro_queue_lag_seconds{queue}` histogram (ready_at → granted), `ostro_queue_worker_demand{queue}` (Phase 7 sets it; define now) |
| Partition / log | `ostro_partition_apply_lag{partition}` (HW − applied), `ostro_partition_hw_lag{partition,replica}` (RF=1: 0), `ostro_isr_size{partition}`, `ostro_wal_log_start{partition}`, `ostro_wal_log_end{partition}` |
| Steps / runs | `ostro_runs_started_total{workflow}`, `ostro_runs_completed_total{workflow,status}`, `ostro_steps_total{workflow,outcome}`, `ostro_step_duration_seconds{workflow}` |
| Replay | `ostro_replay_events_total`, `ostro_replay_duration_seconds` |
| Fuel / memory | `ostro_fuel_consumed_total{workflow}`, `ostro_instance_memory_bytes`, `ostro_wasm_memory_budget_bytes`, `ostro_parked_instances` |
| DLQ | `ostro_dlq_total{reason}` (Phase 5 nondeterminism/fatal failures now; Phase 11 extends) |
| Raft | `ostro_raft_term`, `ostro_raft_commit_index`, `ostro_raft_applied_index`, `ostro_raft_is_leader` |
| Snapshots / backup | `ostro_snapshot_duration_seconds`, `ostro_snapshot_bytes{partition}`, `ostro_backup_cut_age_seconds`, `ostro_backup_upload_bytes_total{kind}`, `ostro_backup_failures_total{kind}`, `ostro_backup_fenced`, `ostro_blob_gc_deleted_total` |
| Triggers | `ostro_webhook_requests_total{hook,outcome}`, `ostro_cron_fires_total{schedule,outcome}` (outcome = started/skipped/buffered/duplicate) |
| Guest logs | `ostro_guest_log_lines_total{level}`, `ostro_guest_log_dropped_total{reason}` (rate/bound) |

The existing series (`ostro_wal_*`, `ostro_apply_*`, `ostro_activation_*`, `ostro_step_retries_total`, `ostro_timer*`, `ostro_shuffler_*`, `ostro_outbox_rows`, `ostro_aot_cache_tamper_total`) appear automatically.

### Pattern 13: Guest logs (OBS-03)
**What exists** (`ostro-engine/src/host/log.rs`): lines are dropped while replaying, bounded per *activation* (1000 lines / 1 MiB), and emitted as `tracing` events with fields concatenated into one string.

**Add a `GuestLogSink` (node-level):**
- **(a)** A per-run **token bucket** (default 50 lines/s, burst 500) and a per-run **lifetime cap** (default 10,000 lines / 10 MiB, in memory and best-effort across restarts; document it). There is also a node-wide bucket (5k lines/s). Drops are counted.
- **(b)** A per-run **ring buffer** (last 1,000 lines, LRU over runs, bounded by a node byte budget) served at `GET /v1/runs/{id}/logs`. It is non-durable: logs are never journaled.
- **(c)** Node log via `tracing` with `guest.fields` as a JSON string (dynamic keys cannot be static `tracing` fields).
- **(d)** An **OTLP LogRecord** via the `opentelemetry` Logger API directly, with real attributes (`run.id`, `workflow`, `attempt`, `trace_id`/`span_id` of the activation span).

Add `LogFormat::Json` (tracing-subscriber `json`).

Exclude `opentelemetry*` targets from the appender layer to avoid an export feedback loop (SDK `internal-logs` emits tracing events).

### Anti-patterns to avoid
- **Physical copies of fjall directories as "snapshots":** the layout is not a format, and a copy is not consistent while fjall is running.
- **Pausing the apply thread to snapshot:** this is unnecessary (batch-atomic MVCC) and adds latency.
- **Per-partition restore points chosen independently:** this breaks outbox exactly-once (Pattern 5).
- **Trimming local segments not yet uploaded:** with bottomless on, RPO silently grows. `upload_floor` must bound the trim.
- **Relying on dedupe rows for cron exactly-once:** they expire with run retention. Use the watermark.
- **A `tracing` span held open across a run's lifetime:** runs last days and are evicted. Use deterministic ids plus explicit timestamps.
- **Parsing the webhook body as JSON before verifying the signature:** re-serialization changes the bytes.
- **Labels with `run_id`, idempotency keys or payload-derived values:** unbounded cardinality.

---

## Don't Hand-Roll

| Problem | Don't build | Use instead | Why |
|---------|-------------|-------------|-----|
| SigV4 request signing | Canonical request / signing-key code | `rusty-s3` 0.12 | Many edge cases (URI encoding, payload hash modes, presign, multipart XML). |
| Cron next-occurrence with DST | Own cron evaluator | `croner` 4.0.3 (jiff backend) | DST gap and overlap semantics; L/W/# modifiers. |
| HMAC + constant-time compare | `==` on hex strings | `hmac 0.13` `Mac::verify_slice` | Timing leaks. |
| Prometheus text format | Own renderer | `metrics-exporter-prometheus` | Escaping, histogram buckets, upkeep. |
| OTLP wire protocol | Own protobufs | `opentelemetry-otlp` (tonic) | Batching, retries, env vars (`OTEL_EXPORTER_OTLP_*`). |
| W3C traceparent parsing | Regex | `opentelemetry_sdk::propagation::TraceContextPropagator` | Version and flags rules. |
| tar writing | Own archive format | `tar` 0.4.46 | Headers, long names. |
| Timezone database | Embedded tables | jiff `tzdb-bundle-always` / system zoneinfo | Updates, correctness. |
| Snapshot consistency | Pausing apply or a custom MVCC layer | fjall `Database::snapshot()` | Already batch-atomic (verified). |

**Key insight:** the parts Ostro must own are the ones that touch its correctness invariants: snapshot format, consistent cut, trim floor, watermarked cron, and the GC mark set. Everything protocol-shaped (S3, OTLP, Prometheus, HMAC, cron syntax) has a maintained crate.

---

## Common Pitfalls

### Pitfall 1: Inconsistent cross-partition restore
**What goes wrong:** lost or duplicated child starts and signals after restore, or messages silently deduped away.
**Why:** each partition was restored to its own latest offset.
**How to avoid:** the HwVector consistent cut (Pattern 5). Restore truncates every partition at `cut_p`.
**Warning signs:** after restore, `outbox_next_seq[A→B] ≤ inbound_seen_B[A]`. Add this as a restore-time assertion.

### Pitfall 2: Trimming beyond what a snapshot covers
**What goes wrong:** "snapshot required" with no usable snapshot (SC4 violated).
**How to avoid:** publish the floor only after snapshot fsync, rename and directory fsync. Floor = the oldest *retained* snapshot. Keep 2 locally. Add a property test with random snapshot/trim/kill sequences.
**Warning signs:** `ostro_wal_log_start > min retained snapshot offset`.

### Pitfall 3: GC deletes a blob that is about to be referenced
**What goes wrong:** a run references a missing blob, and the activation fails.
**Why:** content addressing means `put` of an existing old blob is a no-op that leaves the mtime old. GC runs between put and apply.
**How to avoid:** `put` touches the mtime; grace period; GC lock vs `put`; the mark includes meta module shas and retained snapshot refs.

### Pitfall 4: Object store cannot be used under the deny policy
**What goes wrong:** adding `object_store` fails `cargo deny` (chrono ban) and advisories (chrono, humantime unmaintained).
**How to avoid:** `ostro-objstore` on rusty-s3. Tell Phase 6 (FX-05 text names `object_store`).

### Pitfall 5: The ci-local forbidden-crate grep matches the exporter
**What goes wrong:** `grep -E 'chronik|tokio-uring|prometheus '` matches `metrics-exporter-prometheus v0.18.3`.
**How to avoid:** anchor it, for example `grep -E '(^|[ ─])(chronik[a-z_-]*|tokio-uring|prometheus) v[0-9]'`. Add a negative fixture asserting that `metrics-exporter-prometheus` passes and `prometheus` fails.

### Pitfall 6: Snapshot export, GC mark scan and the block cache (02-16 open concern)
**What goes wrong:** full-table scans evict the hot `runs` working set from the 16 MiB per-partition block cache. RMW apply throughput already sits at 80.8k cmds/s, below the 90k rule.
**How to avoid:**
- Low cadence (size or 1 h).
- Export one partition at a time.
- Measure apply throughput during export in the 05-15 perf harness and report it.
- Size `cache_size` from the node memory budget (`budget / P`, the 05-15 plan). lsm-tree 3.1.10 has no "don't fill cache" read option (grep found none).
- Restore uses ingestion, which bypasses memtables.

### Pitfall 7: Two writers to one bucket prefix
**What goes wrong:** the restored node and the original both upload, and the chain forks.
**How to avoid:** LOCK + incarnation; `If-Match` on the manifest; `--takeover`; the `ostro_backup_fenced` metric.

### Pitfall 8: S3 cost and object count at a 1 s seal
**What goes wrong:** a surprising PUT bill and LIST-heavy restores.
**How to avoid:** skip idle partitions; make the interval configurable; restore lists only from the snapshot offset onward; remote retention deletes covered chunks; document the cost table.

### Pitfall 9: Duplicate jiff majors
**What goes wrong:** two tz databases disagree on rules, or croner pulls chrono through default features.
**How to avoid:** `default-features = false, features = ["jiff"]`; build both TimeZones from the same TZif bytes; a CI check `cargo tree -i chrono` is empty (deny already enforces it).

### Pitfall 10: Re-emitted spans and missing parents
**What goes wrong:** duplicate step spans after evict/replay, or orphan spans.
**How to avoid:** live-only emission; run span from post-apply actions (off during catch-up); deterministic ids; a test with an evicted, re-activated run asserting exactly one span per (run, seq).

### Pitfall 11: OTel exporter and runtime lifecycle
**What goes wrong:** spans are lost at shutdown, or the tonic exporter is created outside a Tokio runtime.
**How to avoid:** build providers inside the server's Tokio runtime. Call `provider.shutdown()` in `Server::shutdown` after the services stop. Default batch processors run on SDK threads (opentelemetry-otlp docs).

### Pitfall 12: TLS roots in `FROM scratch` images
**What goes wrong:** S3 over HTTPS fails, because rustls-platform-verifier finds no system roots.
**How to avoid:** a config knob for a bundled root set (webpki roots) or a CA file. Test once in the scratch image (Phase 14 artefact, wired now).

### Pitfall 13: Cron and tzdb changes across upgrades
**What goes wrong:** a new tzdb shifts a future fire time, and an instance near the boundary is skipped or doubled.
**How to avoid:** the watermark makes `fire_time ≤ watermark` impossible to re-fire. Document that a tz rule change can shift one instance.

---

## Code Examples

### Batch-consistent export (verified fjall semantics)
```rust
// Source: fjall-3.1.13 src/db.rs:149 (Database::snapshot), src/batch/mod.rs:113-164 (one seqno per batch)
pub fn export_snapshot<W: std::io::Write>(&self, w: W) -> Result<SnapshotInfo, StoreError> {
    let snap = self.db.snapshot(); // cross-keyspace, batch-atomic
    let meta = read_meta_from(&snap, self.ks(Table::Meta))?; // applied_offset/state_hash/cursor FROM THIS SNAPSHOT
    let mut out = OsnpWriter::new(w, &meta, self.partition, self.count)?;
    for t in TABLES {
        out.table_begin(t.name())?;
        for g in snap.iter(self.ks(t)) {
            let (k, v) = g.into_inner()?;
            if t == Table::Meta && &*k == K_CHECKPOINTED { continue; } // same exclusion as digest.rs
            out.row(&k, &v)?;          // also feeds the blake3 digest + blob-ref visitor
        }
        out.table_end()?;
    }
    out.finish() // BlobRefs frame + Trailer{digest, state_hash, rows, frames}
}
```

### Consistent cut in the commit callback
```rust
// crates/ostro-log/src/node.rs (inside the existing commit callback, next to send_if_modified)
let mut v = hw_vector.lock().expect("hw vector");      // one uncontended lock per group commit
if durable_end > v[pid] { v[pid] = durable_end; }
// LogNode::cut(): hw_vector.lock().clone()   -> causally closed (send durable before receive appended)
```

### Prometheus endpoint (no hyper listener)
```rust
// Source: docs.rs metrics-exporter-prometheus 0.18.3 PrometheusBuilder (install_recorder: caller runs upkeep)
let handle = PrometheusBuilder::new()
    .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), &[0.001,0.005,0.01,0.05,0.1,0.5,1.0,5.0,30.0])?
    .install_recorder()?;
let h = handle.clone();
tokio::spawn(async move { loop { tokio::time::sleep(Duration::from_secs(5)).await; h.run_upkeep(); } });
let app = Router::new().route("/metrics", get(move || { let h = handle.clone(); async move {
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], h.render()) } }));
```

### OTLP traces over the existing tonic
```rust
// Source: docs.rs opentelemetry-otlp 0.33.1
let exporter = opentelemetry_otlp::SpanExporter::builder()
    .with_tonic().with_endpoint(cfg.endpoint.clone()).with_timeout(Duration::from_secs(5)).build()?;
let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
    .with_id_generator(OstroIdGenerator::default()) // deterministic ids via thread-local override, else random
    .with_batch_exporter(exporter)
    .with_resource(resource)
    .build();
```

### Stripe verification
```rust
// Source: Stripe docs (header format, unknown entries must be accepted); hmac 0.13 Mac::verify_slice
fn verify_stripe(secret: &[u8], header: &str, body: &[u8], now: i64, tol: i64) -> Result<(), HookError> {
    let mut t = None; let mut sigs = Vec::new();
    for part in header.split(',') {
        match part.split_once('=') { Some(("t", v)) => t = v.parse::<i64>().ok(),
                                     Some(("v1", v)) => sigs.push(v), _ => {} } // ignore v0/secret/destination/payload
    }
    let t = t.ok_or(HookError::Malformed)?;
    if (now - t).abs() > tol { return Err(HookError::Stale); }
    for s in sigs {
        let Ok(raw) = hex_decode(s) else { continue };
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).map_err(|_| HookError::Config)?;
        mac.update(t.to_string().as_bytes()); mac.update(b"."); mac.update(body);
        if mac.verify_slice(&raw).is_ok() { return Ok(()); }
    }
    Err(HookError::BadSignature)
}
```

---

## State of the Art

| Old approach | Current approach | When changed | Impact |
|--------------|------------------|--------------|--------|
| `object_store` as the default Rust S3 client | Blocked for Ostro by chrono and humantime advisories | RUSTSEC-2026-0543 / -0542 (2026-11-17) | Use rusty-s3 0.12 (jiff 1.0) |
| jiff 0.2 ecosystem | jiff 1.0.0 / 1.0.1 (2026-11-15 / 18) | Last week | Cron crates lag. Expect croner jiff-1 support soon; drop the adapter's jiff 0.2 then |
| `SpanBuilder.trace_id/span_id/end_time` | Removed from the public builder in 0.33; use `IdGenerator` + `end_with_timestamp` | OTel Rust 0.2x–0.33 | Deterministic ids need a custom generator |
| BatchSpanProcessor on an async runtime | SDK background threads by default | OTel Rust 0.28+ | Simpler setup; `rt-tokio` probably not needed |
| S3 eventual consistency | Strong read-after-write (2020); conditional writes If-None-Match (Aug 2024), If-Match (Nov 2024) | — | The manifest CAS and LOCK patterns work on AWS. Verify on SeaweedFS and R2 |

**Deprecated / outdated:**
- docs/03 §3 "S3/GCS/Azure/local backends from `chronik-storage::object_store`": superseded (rusty-s3 S3-compatible plus local). Azure is deferred.
- docs/03 "periodic `state/pNNNN` (fjall) snapshots": these are logical OSNP files, not fjall directory copies.

---

## Out of scope (verified)

- **Kafka / Chronik topic-consumer triggers:** not in TRIG-01..03 or anywhere in REQUIREMENTS.md. If added later:
  - `rdkafka` 0.39 is excluded (librdkafka C, and its cmake build is listed as "what not to use" in STACK.md);
  - pure-Rust options are `rskafka` 0.6.0 (last release 2025-03, low-level, no consumer groups) or `kafka-protocol` 0.18.0 (codec only, 2026-08) plus an Ostro-owned consumer loop;
  - re-run cargo-deny on the chosen crate's tree.
- Azure Blob and native GCS JSON API backends.
- Auto-fetching a snapshot from remote on local corruption. Phase 8 is operator-driven (`ostro restore`).
- Follower snapshot install transport (Phase 12).

---

## Open Questions

1. **Does cargo-deny with `exclude-dev = true` fully ignore chrono pulled in by the dev-only `s3s-fs`?**
   - What we know: the `[graph] exclude-dev = true` setting is present.
   - Recommendation: verify in the 08-03 Wave 0 task. If it fails, use SeaweedFS in Docker (`#[ignore]`, human-run) for S3 protocol tests and LocalFs for the rest.
2. **Conditional PUT (`If-Match`) support on R2 and SeaweedFS,** and whether rusty-s3 signs extra headers added through `headers_mut()`.
   - Recommendation: an integration test against SeaweedFS. Keep the LOCK re-read fallback.
3. **fjall ingestion fsync behaviour.**
   - Recommendation: Pattern 3's reopen + digest + directory rename makes it non-critical. Measure ingestion against WriteBatch install speed.
4. **jiff 0.2 `TimeZone::tzif` signature, and croner jiff-1 timing.**
   - Recommendation: a spike task in 08-05. If croner ships a jiff-1 feature during Phase 8, drop the adapter.
5. **Restore of the meta store.** Does `RaftDriver` (vendored) support bootstrapping a single voter from an external snapshot?
   - Recommendation: check in 08-15. The fallback is replaying a synthetic `Put` stream from the snapshot records, which is deterministic but loses `updated_index` fidelity.
6. **Per-run guest-log lifetime cap across restarts** is in memory only. Is that acceptable for "bounded per run"?
   - Recommendation: accept and document it (logs are never journaled). Revisit with Phase 11 UI needs.
7. **Pre-freeze findings F2–F4 need Phase 5 action.** If Phase 5 freezes without them, Phase 8 must use `MetaRecord::ScheduleV2`, a META_FORMAT=2 decoder for the `webhooks` map, and post-hoc tag coordination with Phases 6 and 7.

---

## Proposed Plan Breakdown

Granularity is fine and parallelization is on. Phase 8 needs Phase 5 complete (frozen formats), and the F4 reservation table must exist.

| Plan | Wave | Depends on | Scope | Requirements |
|------|------|------------|-------|--------------|
| **08-01** Phase gate + contract | 1 | Phase 5 done | Confirm the F1–F4 outcomes. `Cmd::ScheduleFire` (reserved tag), `ScheduleState` keyspace (append to TABLES), InvalidCode appends, apply arms returning NotImplemented. `MetaRecord::Webhook` / `Schedule` fields (whatever pre-freeze did not land). Workspace pins (Standard Stack). New crate skeletons `ostro-objstore`, `ostro-backup`, `ostro-triggers`. **ci-local grep fix + deny fixtures.** Wave 0 test stubs. Golden lines appended. | all (contract) |
| **08-02** Snapshot format OSNP v1 | 2 | 08-01 | `store/snapshot.rs` writer/reader/install (Patterns 1–3), `store/blobrefs.rs`, `docs/snapshot-format.md`. Tests: round-trip digest, corruption per frame, export during concurrent apply (batch consistency), install staging crash. | BACK-04 |
| **08-03** ostro-objstore | 2 | 08-01 | `ObjectStore` trait (put, put_if_absent, put_if_match, get, get_range, head, list(prefix, start_after), delete, multipart ≥ 64 MiB); LocalFs (tmp + fsync + rename, emulated conditions under a lock); S3 via rusty-s3 + reqwest (retries, backoff, path-style/virtual-host, region, endpoint for R2/SeaweedFS); TLS roots knob. Tests: LocalFs unit, S3 against s3s-fs (or SeaweedFS `#[ignore]`). | BACK-01 (transport), FX-05 seam |
| **08-04** Telemetry foundation | 2 | 08-01 | `[telemetry]` config (otlp endpoint/protocol/headers/sampling, metrics on/off); `LogFormat::Json`; Prometheus recorder + `/metrics` + upkeep + buckets; tracing-opentelemetry layer; appender layer with an otel-target filter; provider shutdown. Tests: scrape shows existing series; in-process OTLP collector (tonic `TraceServiceServer` from opentelemetry-proto) receives a span. | OBS-02 (base), OBS-01/03 (pipes) |
| **08-05** Cron engine (pure) | 2 | 08-01 | `ostro-triggers::cron`: croner + jiff 0.2 adapter, shared TZif, `next_after`, catch-up enumeration, validation errors. DST vector tests + proptest (monotonic, no duplicates). **Spike first.** | TRIG-02/03 (logic) |
| **08-06** Webhook verifiers (pure) | 2 | 08-01 | Stripe/GitHub/Slack/generic/Standard-Webhooks verifiers, idempotency extractors (header/JSON pointer), tolerance; vendor test vectors (see Validation). | TRIG-01 (logic) |
| **08-07** Snapshot service + trim enablement | 3 | 08-02 | `leader/snapshot.rs` (cadence, keep 2, floor watch); retention with dynamic floors; `Node::open` snapshot install; crashtest rebuild path; failpoints `snapshot::before_rename`, `snapshot::before_dir_fsync`, `trim::after_delete`. Proptest: random snapshot/trim/kill → recoverable + digest equal. | BACK-04, SC4 |
| **08-08** ScheduleFire apply | 3 | 08-01 | `apply/schedule.rs`: watermark, overlap policies, buffered fire through the on_terminal hook, manual fires, local start with the `__cron` dedupe; `rebuild_digest` generator. | TRIG-02/03 (apply) |
| **08-09** Guest-log pipeline | 3 | 08-04 | `GuestLogSink` (token buckets, lifetime cap, ring buffer, OTLP LogRecord), `GET /v1/runs/{id}/logs`, metrics; log-spammer guest test. | OBS-03 |
| **08-10** Run / activation / step spans | 3 | 08-04 | `OstroIdGenerator`, engine span hooks (live-only), run span on `Action::RunCompleted`, traceparent validation, `inject_traceparent` helper. Tests with `InMemorySpanExporter`: hierarchy, no re-emission after evict/replay, parent = caller traceparent. | OBS-01 |
| **08-11** Bottomless uploader | 4 | 08-03, 08-07 | `ostro-log` HwVector + `cut()`; `read_envelopes`; node uploader task (blobs → chunks → meta → manifest), LOCK/incarnation/If-Match fencing, `upload_floor` into retention, remote retention, metrics, failpoints `upload::before_manifest`. | BACK-01 |
| **08-12** Cron service + schedule API | 4 | 08-05, 08-08 | `leader/cron.rs` owner driver, catch-up on leadership, pause/resume/trigger-now; REST/gRPC schedules CRUD (meta CAS); CLI `ostro schedules`. | TRIG-02/03 |
| **08-13** Webhook endpoint + API | 4 | 08-06 | `POST /hooks/{name}` (raw body, limit), meta Webhook CRUD (REST/gRPC/CLI), actions start/signal/complete_token, Slack challenge, metrics, OpenAPI snapshot update. | TRIG-01 |
| **08-14** Metrics coverage | 4 | 08-04 | Instrument the series table (dispatcher queues/lag, engine fuel/memory/replay, DLQ, Raft driver gauges, apply lag); docs metric reference. | OBS-02 |
| **08-15** Restore + backup | 5 | 08-11 | `ostro restore --from <url> [--at] [--takeover]`, online `GET /v1/admin/backup` tar stream + `ostro backup --to <file>`, offline `--data-dir` variant, restore from a file (untar → LocalFs → the same path), two-step verified replay, meta bootstrap. | BACK-02, BACK-03 |
| **08-16** Blob GC | 5 | 08-07, 08-11 | `BlobStore` mtime touch, GC lock, `on_put` observer (if not done in 08-11), local mark/sweep incl. meta modules and snapshot refs, remote delayed deletion ledger; failpoint `blobgc::before_unlink`. | BACK-01 (blobs), D9 follow-up |
| **08-17** End-to-end proofs | 6 | 08-09..08-16 | SC1 Stripe round trip (signed in test) + duplicate delivery → one run; SC2 cron exactly-once across kill -9 loops + overlap/catch-up/pause/trigger-now; SC3 restore on a fresh dir reproduces the state hash, RPO ≤ seal interval measured, only-leader/below-HW asserted, file backup round trip; SC5 trace tree + `/metrics` + guest-log bounds; crash campaign scenarios (kill during snapshot/trim/upload/GC). | all |
| **08-18** Phase gate | 7 | all | docs (operations: bottomless config, cost table, restore runbook; snapshot-format.md; storage-format additions), ci-local full, VALIDATION, human_needed items (real S3/R2, fresh-machine restore timing, Jaeger/Grafana visual check). | — |

Wave summary:
- W1: 08-01.
- W2: 02, 03, 04, 05, 06 (five in parallel, disjoint files).
- W3: 07, 08, 09, 10.
- W4: 11, 12, 13, 14.
- W5: 15, 16.
- W6: 17.
- W7: 18.

**File-ownership hotspots** (serialize or split carefully):
- `ostro-partition/src/store/mod.rs` (TABLES): only 08-01.
- `leader/retention.rs`: 08-07, then 08-11 adds `upload_floor`.
- `ostro-blob/src/lib.rs`: 08-11 (observer), then 08-16 (GC). Or move the observer into 08-16 and have 08-11 consume it.
- `ostro-api` router: 08-04 (`/metrics`), 08-09 (logs), 08-13 (hooks), 08-15 (admin backup). Each adds a separate route module.

**Cross-phase seams:**
- Phase 6 FX-01 must call `inject_traceparent`.
- Phase 6 FX-05 should reuse `ostro-objstore`.
- Phase 7 workers' activations must propagate the activation span context over the Execute stream (add a `traceparent` field to its proto).

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | `cargo test` / cargo-nextest (`.config/nextest.toml`: default, ci and crash profiles), proptest 1.12 + proptest-state-machine 0.9, insta 1, `fail` 0.5.1 failpoints, `ostro-crashtest` (kill -9 harness) |
| Config file | `.config/nextest.toml` (crash profile for kill loops); `scripts/ci-local.sh` (`FAILPOINT_CRATES`; add `ostro-backup`) |
| Quick run command | `cargo test -p <crate> --test <suite> --locked` (per plan; each < 30 s) |
| Full suite command | `bash scripts/ci-local.sh` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test type | Automated command | File exists? |
|--------|----------|-----------|-------------------|--------------|
| TRIG-01 | Each verifier accepts valid and rejects tampered body, wrong secret, stale timestamp and malformed header. Vectors: GitHub docs (secret `It's a Secret to Everybody`, body `Hello, World!` → `sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17`), Slack docs example (`v0=a2114d57…b503`), Stripe computed in test (multiple v1, unknown entries ignored). The vectors are recalled from vendor docs: re-verify while implementing. | unit | `cargo test -p ostro-triggers --test webhook_verify` | ❌ Wave 0 |
| TRIG-01 | `POST /hooks/{name}`: valid → run started; duplicate delivery (same key) → same run id, one run; bad signature → 401 and no command; Slack challenge echoed; signal action resolves the run and dedupes via `request_id` | integration (TestServer) | `cargo test -p ostro-server --test webhooks` | ❌ Wave 0 |
| TRIG-01 / SC1 | Stripe round trip: hook A starts the workflow, the workflow waits on signal, hook B signals, the run completes | e2e | `cargo test -p ostro-server --test triggers_e2e stripe_round_trip` | ❌ Wave 0 |
| TRIG-02 | DST vectors (NY, Berlin, Lord_Howe), seconds field, `L`/`#`; `next_after` monotonic; enumeration of `(watermark, now]` has no duplicates | unit + proptest | `cargo test -p ostro-triggers --test cron` | ❌ Wave 0 |
| TRIG-02 | ScheduleFire apply: `fire_time ≤ watermark` → Duplicate; watermark atomic with run start; rebuild digest equal | unit (apply) + proptest | `cargo test -p ostro-partition --test apply_schedule && cargo test -p ostro-partition --test rebuild_digest` | ❌ Wave 0 / ✅ (extend) |
| TRIG-02 / SC2 | Each `(schedule, fire_time)` starts exactly once across N kill -9 restarts (1 s schedule, count runs per fire_time) | crash loop (`#[ignore]`, crash profile) | `cargo nextest run -P crash -p ostro-crashtest --run-ignored only cron_exactly_once` | ❌ Wave 0 |
| TRIG-03 | Overlap Skip/BufferOne/AllowAll/CancelOther; catch-up Skip/One/All after simulated downtime (FakeClock); pause stops fires and resume applies catch-up; trigger-now starts without moving the watermark | integration | `cargo test -p ostro-server --test schedules` | ❌ Wave 0 |
| BACK-04 | OSNP round trip: installed digest == trailer == source digest; every single-frame corruption detected; export during concurrent apply yields a consistent `applied_offset` | unit + proptest | `cargo test -p ostro-partition --test snapshot` | ❌ Wave 0 |
| BACK-04 | Snapshot + tail rebuild == full replay digest (random snapshot offset k) | proptest | `cargo test -p ostro-partition --test rebuild_digest snapshot_tail` | ✅ (extend) |
| BACK-04 / SC4 | Random snapshot/trim/kill sequences: never "snapshot required" without a usable snapshot; `log_start ≤ oldest retained snapshot` | proptest + failpoints | `cargo test -p ostro-partition --features failpoints --test trim_safety` | ❌ Wave 0 |
| BACK-01 | Uploader: only below HW (chunk end ≤ HW at read), only on leader services (barrier closed → no uploads), keys partition/epoch/base, epoch split, chain hashes, blobs uploaded before the referencing chunk, meta snapshot after the cut | integration (LocalFs backend) | `cargo test -p ostro-backup --test uploader` | ❌ Wave 0 |
| BACK-01 | Consistent cut: a cross-partition workload (children/signals) restored at any manifest gen → `outbox_next_seq` / `inbound_seen` consistent, no lost or deduped messages | integration + proptest | `cargo test -p ostro-backup --test cut_consistency` | ❌ Wave 0 |
| BACK-01 | Fencing: a second writer with a different incarnation → precondition failure → uploads stop, `ostro_backup_fenced = 1` | integration (LocalFs emulation; SeaweedFS `#[ignore]`) | `cargo test -p ostro-backup --test fencing` | ❌ Wave 0 |
| BACK-01 | S3 backend against an in-process S3 (s3s-fs) or SeaweedFS: put/get/range/list/delete/multipart/conditional | integration | `cargo test -p ostro-objstore --test s3` (SeaweedFS: `-- --ignored`) | ❌ Wave 0 |
| BACK-02 / SC3 | kill -9 during load → `ostro restore --from file://…` into an empty dir → state_hash at `verify_offset` matches the manifest; every ack with offset < cut is present; RPO (ack time of the last lost write − cut_at) ≤ seal interval + upload latency | crash loop (`#[ignore]`) | `cargo nextest run -P crash -p ostro-crashtest --run-ignored only restore_rpo` | ❌ Wave 0 |
| BACK-03 | `ostro backup --to f.tar` then `ostro restore --from f.tar` round trip (online and offline) → identical digests | e2e (spawned binary) | `cargo test -p ostro --test backup_cli` | ❌ Wave 0 |
| BACK-01 (blobs) | GC deletes unreferenced old blobs; keeps referenced, young, module and snapshot-ref blobs; re-put of an old blob survives a concurrent sweep; remote deletion delayed by the window | unit + integration + failpoint | `cargo test -p ostro-blob --test gc && cargo test -p ostro-backup --test remote_gc` | ❌ Wave 0 |
| OBS-01 | Span tree run → activation → step/effect with the caller's trace id; after evict + reactivate, exactly one span per (run, seq); run span emitted once and not after restart catch-up; outbound helper injects a valid traceparent | integration (`InMemorySpanExporter`) | `cargo test -p ostro-server --test tracing_spans` | ❌ Wave 0 |
| OBS-01 | OTLP export reaches an in-process tonic collector (no duplicate tonic/prost in `cargo tree -d`) | integration | `cargo test -p ostro-server --test otlp_export` | ❌ Wave 0 |
| OBS-02 | `/metrics` 200, text format, contains the queue/lag/step/replay/fuel/memory/DLQ/HW-lag/ISR/Raft series with `_bucket` lines for `_seconds` histograms; no `run_id` label anywhere | integration | `cargo test -p ostro-server --test metrics_endpoint` | ❌ Wave 0 |
| OBS-02 | ci-local forbidden grep passes with metrics-exporter-prometheus and fails with the `prometheus` crate | script fixture | `bash scripts/check-deny-fixtures.sh` | ✅ (extend) |
| OBS-03 | Log-spamming guest: per-run rate and lifetime caps enforced, drops counted, ring buffer returns the last N lines, nothing emitted during replay, JSON format has structured fields, OTLP LogRecord carries `run.id` + trace correlation | integration | `cargo test -p ostro-server --test guest_logs` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** the plan's quick command(s) above, each < 30 s.
- **Per wave merge:** `cargo test --workspace --locked` + `cargo deny check` + the failpoint suites.
- **Phase gate:** `bash scripts/ci-local.sh` green, the `#[ignore]` crash loops run locally (cron_exactly_once, restore_rpo, and snapshot/trim/upload kill scenarios added to the campaign), then `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `crates/ostro-triggers/tests/{cron.rs, webhook_verify.rs}` (TRIG-01/02/03 logic)
- [ ] `crates/ostro-partition/tests/{snapshot.rs, trim_safety.rs, apply_schedule.rs}` and a `rebuild_digest.rs` snapshot_tail case (BACK-04, TRIG-02)
- [ ] `crates/ostro-objstore/tests/{localfs.rs, s3.rs}` + the s3s-fs dev-dependency decision (Open Question 1)
- [ ] `crates/ostro-backup/tests/{uploader.rs, cut_consistency.rs, fencing.rs, remote_gc.rs}`
- [ ] `crates/ostro-blob/tests/gc.rs`
- [ ] `crates/ostro-server/tests/{webhooks.rs, schedules.rs, triggers_e2e.rs, tracing_spans.rs, otlp_export.rs, metrics_endpoint.rs, guest_logs.rs}` + an in-process OTLP collector helper in `ostro-testkit`
- [ ] `crates/ostro-crashtest/tests/{cron_exactly_once.rs, restore_rpo.rs}` + failpoints `snapshot::*`, `trim::after_delete`, `upload::before_manifest`, `blobgc::before_unlink`
- [ ] `guests/log-spammer` (cdylib) for OBS-03
- [ ] `FAILPOINT_CRATES` += `ostro-backup` (ci-local + ci.yml together)
- [ ] ci-local forbidden-crate regex + fixture (Pitfall 5)

---

## Sources

### Primary (HIGH confidence)
- crates.io API (queried 2026-11-19): versions, features and dependency lists for opentelemetry, opentelemetry_sdk, opentelemetry-otlp, opentelemetry-proto, opentelemetry-appender-tracing 0.33.1; tracing-opentelemetry 0.34.0; metrics-exporter-prometheus 0.18.3; metrics-util 0.20.4; object_store 0.14.2 (**chrono REQUIRED**); croner 4.0.3 (jiff ^0.2.35); rusty-s3 0.12.0 (jiff ^1.0); jiff 1.0.1 / 0.2.46 features; reqwest 0.13.5 features; jiff-cron, cronexpr, cronp; s3s/s3s-fs 0.17.0; rdkafka 0.39.0, rskafka 0.6.0, kafka-protocol 0.18.0; tar 0.4.46; lz4_flex.
- fjall 3.1.13 source (`~/.cargo/registry`): `db.rs:149` `Database::snapshot`, `batch/mod.rs:100-164` (single seqno, publish after insert), `ingestion.rs`, `keyspace/mod.rs:300` `start_ingestion`; lsm-tree 3.1.10 `tree/ingest.rs` (bypasses memtables; no cache-bypass read option found).
- Ostro code: `ostro-log/src/{replicated.rs, rf1.rs, node.rs, recovery.rs, verify.rs, barrier.rs}`, `ostro-partition/src/{leader/retention.rs, store/digest.rs, apply/inbound.rs, node.rs}`, `ostro-meta/src/{records.rs, command.rs, state.rs, sm.rs}`, `ostro-engine/src/host/{log.rs, state.rs}`, `ostro-blob/src/lib.rs`, `ostro-server/src/logging.rs`, `deny.toml`, `scripts/ci-local.sh`, `Cargo.toml` / `Cargo.lock`.
- docs.rs: opentelemetry 0.33.1 `SpanBuilder` (no trace_id/span_id/end_time) and `Span` (`end_with_timestamp`); opentelemetry_sdk 0.33.1 `IdGenerator`, `TracerProviderBuilder::with_id_generator`; opentelemetry-otlp 0.33.1 crate docs (tonic builder, env vars, batch processors on SDK threads); metrics-exporter-prometheus 0.18.3 `PrometheusBuilder` (install_recorder, caller runs upkeep); rusty-s3 0.12.0 actions; reqwest 0.13.5 `ClientBuilder` TLS methods.
- RUSTSEC-2026-0543 (rustsec.org): chrono unmaintained, issued 2026-11-17.
- Project docs: ROADMAP.md (Phases 8 and 12), REQUIREMENTS.md, PROJECT.md, STATE.md, 05-DECISIONS.md, 05-RESEARCH.md, 05-02/05-19 plans, docs/storage-format.md, docs/03-architecture.md, .planning/research/STACK.md.

### Secondary (MEDIUM confidence)
- github.com/apache/arrow-rs-object-store issues #909 (chrono) and #908 (humantime), open 2026-11-17; main `Cargo.toml` still requires chrono.
- github.com/hexagon/croner-rust main `Cargo.toml` (jiff 0.2.35 optional, chrono default) and PR list (#68 DST fix in 4.0.3).
- Stripe docs (manage-webhook-endpoints / signature troubleshooting): header entries, unknown entries, v0 on test events, multiple secrets during rotation, 3-day retries, event-id dedupe.
- S3 conditional writes (If-None-Match 2024-08, If-Match 2024-11): from training knowledge plus widespread reporting; verify on SeaweedFS and R2.

### Tertiary (LOW confidence, validate during implementation)
- GitHub and Slack signature test vectors (recalled from vendor docs).
- `opentelemetry_sdk` `testing` feature name / `InMemorySpanExporter` path in 0.33.
- Whether `rt-tokio` is still needed for OTel 0.33 batch processors.
- s3s-fs support for conditional headers.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH for versions and compatibility (crates.io dependency data). MEDIUM for the croner adapter (jiff 0.2 duplicate) and rusty-s3 conditional-header signing.
- Architecture: MEDIUM. The designs are new. The key safety arguments (batch-atomic snapshot, causally closed cut, replay needs no blobs, watermark exactly-once) were each checked against code.
- Pitfalls: HIGH for the deny, ci-grep and object_store conflicts (reproduced). MEDIUM for performance effects (to be measured).

**Research date:** 2026-11-19
**Valid until:** 2026-12-03 for the crate ecosystem, which is fast-moving: jiff 1.0 adoption by croner, object_store's chrono removal, and Wasmtime 51 on 2026-12-20. The architecture stays valid until the Phase 5 freeze outcome is known (F1–F4).
