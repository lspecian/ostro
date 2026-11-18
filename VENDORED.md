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
| vendor/ostro-raft-driver/src/driver.rs | crates/chronik-raft/src/driver.rs | 1123 | modified | The deprecated `raft::is_empty_snap(ready.snapshot())` → `ready.snapshot().is_empty()` |
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

## vendor/ostro-wal

Added by plan 01-06.
