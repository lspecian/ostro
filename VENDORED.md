# Vendored code

Upstream: Chronik Stream (https://github.com/lspecian/chronik-stream), Apache-2.0
Commit: 08715f4867c9f40e409c68d8e212bd9479d620c0 (2026-11-18)
Transitive upstream for vendor/ostro-raft-rs and vendor/ostro-raft-proto: tikv/raft-rs v0.7.0 (Apache-2.0, TiKV Project Authors), as modified by Chronik for prost 0.12.

## Rules

- Nothing in the build or dependency graph references the Chronik repository. The vendored crates are path dependencies inside this workspace, there is no `[patch]` section anywhere, and no vendoring script points at an upstream checkout. The URL above (and in NOTICE) is attribution only.
- Vendored crates stay on `edition = "2021"` and use explicit manifests (`publish = false`, `license = "Apache-2.0"`). They do not inherit `[workspace.lints]` and have an empty `[lints.rust]` table. Each has a `rustfmt.toml` with `disable_all_formatting = true`, so `cargo fmt --all` never rewrites them.
- Every modified or rewritten `.rs` file has the header line `// Vendored from chronik-stream@08715f4 <upstream path>; modified by Ostro (see VENDORED.md).` (Apache-2.0 §4(b)). It sits below the original TiKV/etcd copyright header, which is kept intact. Files marked unchanged are byte-identical to upstream.
- Every file under `vendor/` has a row below. Status values: `unchanged`, `modified`, `rewritten`, `ostro-authored`.

## vendor/ostro-raft-proto

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| vendor/ostro-raft-proto/Cargo.toml | crates/raft-proto/Cargo.toml | 24 | rewritten | Package renamed to `ostro-raft-proto` (lib name `raft_proto` kept). prost/prost-build 0.12 → 0.14.6, protox 0.10.0 build-dep added, `[workspace]` removed, `publish = false` |
| vendor/ostro-raft-proto/build.rs | crates/raft-proto/build.rs | 11 | rewritten | `protox::compile` + `prost_build::Config::compile_fds`, so no `protoc` is needed. `config.bytes(["."])` kept |
| vendor/ostro-raft-proto/rustfmt.toml | — | — | ostro-authored | `disable_all_formatting = true` |
| vendor/ostro-raft-proto/proto/eraftpb.proto | crates/raft-proto/proto/eraftpb.proto | 115 | unchanged | — |
| vendor/ostro-raft-proto/src/lib.rs | crates/raft-proto/src/lib.rs | 611 | unchanged | — (the prost 0.12 → 0.14 move needed no source edits) |
| vendor/ostro-raft-proto/src/confchange.rs | crates/raft-proto/src/confchange.rs | 146 | unchanged | — |
| vendor/ostro-raft-proto/src/confstate.rs | crates/raft-proto/src/confstate.rs | 26 | unchanged | — |

## vendor/ostro-raft-rs

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| vendor/ostro-raft-rs/Cargo.toml | crates/raft/Cargo.toml | 40 | rewritten | Package renamed to `ostro-raft-rs` (lib name `raft` kept). `raft-proto = { package = "ostro-raft-proto", path = ... }`. fxhash → rustc-hash 2, thiserror 1 → 2, rand 0.8 → 0.9, prost 0.12 → 0.14.6. `default = ["prost-codec"]` (`default-logger` is no longer a default). `default-logger = ["dep:slog-stdlog"]`. Dropped fail, slog-term, slog-envlogger, slog-async and the `failpoints` feature. `[workspace]` removed |
| vendor/ostro-raft-rs/rustfmt.toml | — | — | ostro-authored | `disable_all_formatting = true` |
| vendor/ostro-raft-rs/src/lib.rs | crates/raft/src/lib.rs | 596 | modified | `fxhash::FxHasher` → `rustc_hash::FxHasher`. The `default_logger()` body uses `slog_stdlog::StdLog` instead of slog-term/envlogger. `#![deny(clippy::all)]` → `#![allow(clippy::all)]`, so newer clippy lints don't break workspace clippy |
| vendor/ostro-raft-rs/src/raft.rs | crates/raft/src/raft.rs | 2970 | modified | rand 0.9 API (`rand::rng().random_range(..)`). Removed the two `cfg(feature = "failpoints")` blocks (`use fail::fail_point` and `fail_point!("before_step")`) |
| vendor/ostro-raft-rs/src/raft_log.rs | crates/raft/src/raft_log.rs | 1923 | unchanged | — |
| vendor/ostro-raft-rs/src/raw_node.rs | crates/raft/src/raw_node.rs | 831 | unchanged | — |
| vendor/ostro-raft-rs/src/storage.rs | crates/raft/src/storage.rs | 813 | unchanged | — |
| vendor/ostro-raft-rs/src/log_unstable.rs | crates/raft/src/log_unstable.rs | 482 | unchanged | — |
| vendor/ostro-raft-rs/src/tracker.rs | crates/raft/src/tracker.rs | 388 | unchanged | — |
| vendor/ostro-raft-rs/src/tracker/inflights.rs | crates/raft/src/tracker/inflights.rs | 426 | unchanged | — |
| vendor/ostro-raft-rs/src/tracker/progress.rs | crates/raft/src/tracker/progress.rs | 411 | unchanged | — |
| vendor/ostro-raft-rs/src/tracker/state.rs | crates/raft/src/tracker/state.rs | 40 | unchanged | — |
| vendor/ostro-raft-rs/src/confchange.rs | crates/raft/src/confchange.rs | 15 | modified | Removed `#[cfg(test)] pub mod datadriven_test;` |
| vendor/ostro-raft-rs/src/confchange/changer.rs | crates/raft/src/confchange/changer.rs | 357 | unchanged | — |
| vendor/ostro-raft-rs/src/confchange/restore.rs | crates/raft/src/confchange/restore.rs | 107 | unchanged | — |
| vendor/ostro-raft-rs/src/quorum.rs | crates/raft/src/quorum.rs | 74 | modified | Removed `#[cfg(test)] pub mod datadriven_test;` |
| vendor/ostro-raft-rs/src/quorum/joint.rs | crates/raft/src/quorum/joint.rs | 98 | modified | `#[allow(dead_code)]` on the test-only `new_joint_from_majorities` and `describe`, whose only callers were the excluded datadriven tests |
| vendor/ostro-raft-rs/src/quorum/majority.rs | crates/raft/src/quorum/majority.rs | 255 | modified | `#[allow(dead_code)]` on the test-only `describe` (only caller was the excluded datadriven tests) |
| vendor/ostro-raft-rs/src/config.rs | crates/raft/src/config.rs | 219 | unchanged | — |
| vendor/ostro-raft-rs/src/errors.rs | crates/raft/src/errors.rs | 174 | unchanged | — (uses `prost::DecodeError`, so thiserror 2 works as-is) |
| vendor/ostro-raft-rs/src/read_only.rs | crates/raft/src/read_only.rs | 136 | unchanged | — |
| vendor/ostro-raft-rs/src/status.rs | crates/raft/src/status.rs | 53 | unchanged | — |
| vendor/ostro-raft-rs/src/util.rs | crates/raft/src/util.rs | 178 | unchanged | — |

## vendor/ostro-raft-driver

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| vendor/ostro-raft-driver/Cargo.toml | crates/chronik-raft/Cargo.toml | 57 | rewritten | Package `ostro-raft-driver`. Deps: ostro-raft-rs (`default-features = false, features = ["prost-codec"]`), prost 0.14.6, crc32fast, tokio (`sync`), tracing, slog, slog-stdlog. Dev-deps: tokio (full, test-util), tempfile, serde_json. Dropped chronik-common, chronik-monitoring, chronik-raft-bridge, tonic, prost-types, bincode, dashmap, serde, futures, async-trait, anyhow and tonic-build |
| vendor/ostro-raft-driver/rustfmt.toml | — | — | ostro-authored | `disable_all_formatting = true` |
| vendor/ostro-raft-driver/src/lib.rs | crates/chronik-raft/src/lib.rs | 27 | rewritten | Only `pub mod driver; pub mod log_store;`, plus re-exports of `Applied, DriverConfig, Outbox, ProposeError, RaftDriver, RaftHandle, RaftStatus, StateMachine, LogStoreOptions, RaftLogStore` and `pub use raft;`. Crate-level `#![allow(clippy::all)]` |
| vendor/ostro-raft-driver/src/driver.rs | crates/chronik-raft/src/driver.rs | 1123 | modified | The deprecated `raft::is_empty_snap(ready.snapshot())` → `ready.snapshot().is_empty()`. **P1** (test-only timeouts, load hardening) in `mod tests`: `config()` `election_tick` 10 → 50 (election timeout 100–200 ms → 500–1000 ms, heartbeat unchanged) so a driver thread stalled on fsync under heavy machine load no longer deposes a healthy leader mid-test (`OutcomeUnknown` in `isolated_leader_steps_down_and_its_proposal_is_lost` / `lagging_node_catches_up_by_snapshot`); `Cluster::leader` and `Cluster::settle` hang guards 10 s → 30 s. No non-test code changed |
| vendor/ostro-raft-driver/src/log_store.rs | crates/chronik-raft/src/log_store.rs | 1184 | unchanged | — |

## Excluded upstream files

| Upstream path | Reason |
|---|---|
| crates/raft/src/confchange/datadriven_test.rs (86 lines) | Needs tikv's `datadriven` crate, which Chronik never vendored, so it never compiled upstream either |
| crates/raft/src/quorum/datadriven_test.rs (323 lines) | Same as above |
| crates/raft/src/confchange/testdata/*.txt (8 files) | Used only by the excluded datadriven tests |
| crates/raft/src/quorum/testdata/*.txt (5 files) | Used only by the excluded datadriven tests |
| crates/raft/Cargo.lock, crates/raft-proto/Cargo.lock | The workspace has a single Cargo.lock |
| crates/chronik-raft/src/net.rs (560 lines) | tonic 0.11 gRPC transport. Ostro writes its own `Outbox` in Phase 4 (META-01) |
| crates/chronik-raft/src/config.rs, error.rs, rpc.rs, storage.rs, transport.rs, prost_bridge.rs, transport/grpc.rs, transport/memory.rs | Legacy/stub modules superseded by driver.rs/log_store.rs. `error.rs` drags in tonic and bincode |
| crates/chronik-raft/build.rs, proto/raft_rpc.proto, proto/catalog_rpc.proto | tonic-build 0.11 and protoc codegen for the excluded transport |
| crates/chronik-raft-bridge (src/lib.rs 47 lines, Cargo.toml) | Existed only to bridge two prost versions. With a single prost 0.14 it isn't needed. The vendored driver.rs and log_store.rs call `prost::Message` directly and never referenced it |
| crates/chronik-wal/src/io_uring_thread.rs (373 lines) | tokio-uring io_uring thread; the io_uring path was removed (it also measured slower upstream) |
| crates/chronik-wal/src/fsync.rs (499 lines) | Uses chronik_common::metrics; `FsyncBatcher` was constructed but never used |
| crates/chronik-wal/src/checkpoint.rs (215 lines) | `CheckpointManager` was constructed but never used by the vendored path (`CheckpointConfig` stays in config.rs) |
| crates/chronik-wal/src/compaction.rs (657 lines) | Kafka-style key compaction, not needed by Ostro |
| crates/chronik-wal/src/periodic_flusher.rs (121 lines) | Not used by GroupCommitWal |
| crates/chronik-wal/src/segment.rs (254 lines) | Legacy `WalSegment`, the only chrono user; GroupCommitWal owns segments |
| crates/chronik-wal/src/{audit,streaming,io,rotation,checkpoint}/*.rs (0–3 lines each) | Empty stub modules |
| crates/chronik-wal/src/tests/unit_tests.rs (895 lines) | Tests the excluded segment.rs |
| crates/chronik-wal/src/tests/rotation_load_test.rs (794 lines) | Wall-clock performance floor, flaky in CI. May return later as `#[ignore]` |
| crates/chronik-wal/src/tests/{wal_durability_stress,wal_validation_test,async_io_bench}.rs (490+483+242 lines) | Not compiled upstream either (absent from tests/mod.rs) |
| crates/chronik-wal/src/tests/mod.rs, src/tests/README.md | Only index the excluded tests |
| crates/chronik-wal/RELEASE.md (290 lines) | Upstream release notes |

## vendor/ostro-wal

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| vendor/ostro-wal/Cargo.toml | crates/chronik-wal/Cargo.toml | 65 | rewritten | Package `ostro-wal`, explicit metadata, `publish = false`, edition 2021, empty `[lints.rust]`. Deps: tokio (`fs, io-util, sync, rt, time, macros`), bytes, byteorder, tracing, thiserror 2 (was 1), dashmap 6 (was 5.5), crc32fast, serde, serde_json, libc, metrics 0.24.6. Dev-deps: tokio (full, test-util), tempfile. Dropped chronik-common, chronik-monitoring, tokio-uring, bincode, chrono, uuid, anyhow, futures, async-trait, parking_lot, tokio-util, crossbeam, memmap2, ring, zstd, proptest, criterion, regex, rand (unused by the vendored tests) and all features (`async-io`, `wal`, `wal-encryption`, `wal-compression`). **P3** optional `fail` 0.5.1 and feature `failpoints = ["dep:fail", "fail/failpoints"]`; `[[test]] poison` (`required-features = ["failpoints"]`) and `[[test]] dir_fsync` |
| vendor/ostro-wal/rustfmt.toml | — | — | ostro-authored | `disable_all_formatting = true` |
| vendor/ostro-wal/src/lib.rs | crates/chronik-wal/src/lib.rs | 76 | rewritten | Declares only the vendored modules (error, manager, record, config, buffer_pool, group_commit, io_priority, truncate, segment_index). Re-exports `WalError, Result, WalManager, WalRecord, WalConfig, CompressionType, CheckpointConfig, RecoveryConfig, RotationConfig, FsyncConfig, GroupCommitWal, GroupCommitConfig, PartitionMetrics, TruncateOutcome` and `Error`. `recover_all` and `RecoveryResult` kept. Crate-level `#![allow(clippy::all)]`; `cfg_attr(not(feature = "wal"))` removed |
| vendor/ostro-wal/src/group_commit.rs | crates/chronik-wal/src/group_commit.rs | 2837 | modified | `MetricsRecorder::record_wal_batch` → `metrics::counter!("ostro_wal_batches_total").increment(1)` + `metrics::histogram!("ostro_wal_batch_records")`. All 26 `cfg(all(target_os = "linux", feature = "async-io"))` items removed (io_uring writer variant, `IoUringThreadHandle` import, `io_uring_handle` fields/params/args). `GroupCommitConfig::use_io_uring` and its env reader removed. `CHRONIK_WAL_PROFILE` (Default → `low_resource()`), `CHRONIK_WAL_ROTATION_SIZE` (→ `DEFAULT_ROTATION_SIZE_BYTES` = 250MB) and `CHRONIK_WAL_TAIL_CACHE_BYTES` (→ new field `GroupCommitConfig::tail_cache_bytes`, default `DEFAULT_TAIL_CACHE_BYTES` = 2MiB) env reads removed. `is_raft_topic` backpressure exemption: `"__chronik_metadata"` → `"__ostro_"` prefix (kept, not removed; `__raft` prefix unchanged). Unused `partition_key` param of `WalWriter::create` removed. `io_priority::set_wal_priority()` call kept. **P1** (Ostro STOR-07, fsyncgate) write/fsync errors poison the partition queue (`PartitionCommitQueue::poisoned`, `WalError::Poisoned`, pending waiters failed, worker stops, best-effort `set_len(durable_len)`; no commit callback and no `committed_through` advance for the failed batch; seal, rotation and truncation I/O failures poison too; enqueue refuses writes under the pending lock once poisoned). **P2** `fsync_dir` after segment create/rotate, truncation `set_len` and segment deletion (`fsync_dir`/`fsync_dir_blocking` are `pub`; queue creation also fsyncs the topic and base dirs). **P3** `fail` failpoints `wal::write` (eio \| enospc partial write), `wal::fsync`, `wal::dir_fsync` behind feature `failpoints`. `GroupCommitWal::is_poisoned` added. **P4** (test-only timeout, load hardening) `queue_creation_tests::the_first_write_to_every_new_partition_commits` per-write hang guard 20 s → 60 s (400 first writes each fsync a new segment and its dirs; over 20 s under heavy machine load) |
| vendor/ostro-wal/src/manager.rs | crates/chronik-wal/src/manager.rs | 1190 | modified | Never-used `CheckpointManager`/`FsyncBatcher` fields and construction removed. Dead `extract_offset_range` + `MinimalCanonicalRecord` (bincode decode of a Kafka CanonicalRecord, zero callers) deleted. `CHRONIK_WAL_PROFILE` log/comment text now describes config fields. Unused `PathBuf` import removed. Test configs no longer set `async_io` |
| vendor/ostro-wal/src/record.rs | crates/chronik-wal/src/record.rs | 723 | modified | Comment-only: the `verify_checksum` rationale no longer names the removed serializer, plus a note that V2 verification is a no-op until Phase 2's envelope CRC |
| vendor/ostro-wal/src/config.rs | crates/chronik-wal/src/config.rs | 236 | modified | `AsyncIoConfig` (io_uring knobs) and the `WalConfig::async_io` field removed. `CompressionType::Zstd` (behind the never-enabled `wal-compression` feature) removed |
| vendor/ostro-wal/src/error.rs | crates/chronik-wal/src/error.rs | 66 | modified | `SerializationError(#[from] bincode::Error)` removed. **P1** `Poisoned(String)` variant added |
| vendor/ostro-wal/src/io_priority.rs | crates/chronik-wal/src/io_priority.rs | 112 | modified | Doc example `chronik_wal::` → `ostro_wal::` |
| vendor/ostro-wal/src/buffer_pool.rs | crates/chronik-wal/src/buffer_pool.rs | 422 | unchanged | — |
| vendor/ostro-wal/src/segment_index.rs | crates/chronik-wal/src/segment_index.rs | 935 | unchanged | — |
| vendor/ostro-wal/src/truncate.rs | crates/chronik-wal/src/truncate.rs | 507 | unchanged | — |
| vendor/ostro-wal/tests/suffix_truncation.rs | crates/chronik-wal/tests/suffix_truncation.rs | 434 | modified | `chronik_wal` → `ostro_wal`; `#![allow(clippy::all)]` |
| vendor/ostro-wal/tests/idle_seal_rotation.rs | crates/chronik-wal/tests/idle_seal_rotation.rs | 188 | modified | `chronik_wal` → `ostro_wal`; `#![allow(clippy::all)]` |
| vendor/ostro-wal/tests/poison.rs | — | — | ostro-authored | P1/P3 tests (feature `failpoints`, run `--test-threads=1`): a failed write/fsync is never Ok, the commit callback never fires for it, queued and later appends get `Poisoned`, ENOSPC partial writes are cut back, other partitions stay healthy |
| vendor/ostro-wal/tests/dir_fsync.rs | — | — | ostro-authored | P2 tests: `fsync_dir` Ok/Err, create/rotate/truncate paths still produce visible, parseable segments |

**Metrics:** the WAL emits `ostro_wal_batches_total` (counter, one per committed batch) and `ostro_wal_batch_records` (histogram, records per batch) through the `metrics` facade. Library code reads no environment variables; every knob is a `GroupCommitConfig` / `WalConfig` field.

**Answered research gap:** no WAL recovery path decodes `canonical_data` as Kafka. The only decoder, `manager.rs::extract_offset_range` (bincode → `MinimalCanonicalRecord`), had zero callers at 08715f4 and was deleted. `canonical_data` is opaque bytes to the vendored WAL.

## Third-party WIT (not under vendor/)

WASI 0.2.12 WIT for Ostro's deterministic WASI shim (plan 03-04, ENG-02). Source: the `wasip2` crate, version `1.0.4+wasi-0.2.12` (crates.io; upstream WebAssembly WASI, https://github.com/WebAssembly/WASI). Licence: `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` (see NOTICE). These files live outside `vendor/`, so `scripts/check-vendored.sh` does not scan them; this table is their provenance record. Re-vendoring steps: `crates/ostro-engine/wit-wasi/README.md`.

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| crates/ostro-engine/wit-wasi/deps/io.wit | wasip2 1.0.4+wasi-0.2.12 crate wit/deps/io.wit | 299 | unchanged | — |
| crates/ostro-engine/wit-wasi/deps/clocks.wit | wasip2 1.0.4+wasi-0.2.12 crate wit/deps/clocks.wit | 162 | unchanged | — (the `@unstable(feature = clocks-timezone)` interface stays; it is not in the shim world) |
| crates/ostro-engine/wit-wasi/deps/random.wit | wasip2 1.0.4+wasi-0.2.12 crate wit/deps/random.wit | 92 | unchanged | — |
| crates/ostro-engine/wit-wasi/deps/cli.wit | wasip2 1.0.4+wasi-0.2.12 crate wit/deps/cli.wit | 233 | modified | Lines 140–233 deleted: the `world imports` and `world command` blocks, which import `wasi:filesystem/*` and `wasi:sockets/*` (not vendored; Ostro guests get neither). Lines 1–139 (all interfaces) are byte-identical |
| crates/ostro-engine/wit-wasi/shim.wit | — | — | ostro-authored | World `ostro:wasi-shim/shim`: the 18 allowed `wasi:*@0.2.12` interfaces |
| crates/ostro-engine/wit-wasi/README.md | — | — | ostro-authored | Provenance and re-vendoring steps |

## Ported into Ostro crates

Pure logic ported from Chronik into Ostro-owned crates (not under `vendor/`, so `scripts/check-vendored.sh` does not scan them). Each file carries the header `// Ported from chronik-stream@08715f4 <upstream path>; modified by Ostro (see VENDORED.md).`

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| crates/ostro-log/src/epoch_cache.rs | crates/chronik-server/src/replication/leader_epoch.rs | 36–187, 435–621 | modified | Only `LeaderEpochCache` (renamed `EpochCache`: `assign`, `epoch_for_offset`, `end_offset_for_epoch`, `truncate_from_end`, `truncate_from_start`) and its unit tests. Ostro `Epoch(u32)`/`Offset(u64)` replace `i32`/`i64`; Kafka's `(UNDEFINED_EPOCH, UNDEFINED_OFFSET)` answer is `None`; the unstamped-epoch (`-1`) case and its test are gone (every envelope carries an epoch); `from_pairs`/`to_pairs` added; `LeaderEpochStore` (DashMap + checkpoint files) and the Kafka batch stamping helpers are not ported (the history is rebuilt from the WAL at open) |
