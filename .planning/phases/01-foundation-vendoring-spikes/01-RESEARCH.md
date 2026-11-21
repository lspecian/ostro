# Phase 1: Foundation, Vendoring & Spikes - Research

**Researched:** 2026-11-18
**Domain:** Rust workspace bootstrap, vendoring and modernising Chronik code, cargo-deny/CI matrix (musl/zigbuild/macOS), Wasm guest toolchain spike (S2), redb durability spike (S3)
**Confidence:** HIGH for vendoring and toolchain. The vendoring was prototyped and compiled locally on Rust 1.100.0, with tests green. MEDIUM for the CI legs that can't run locally (aarch64, macOS, zig). HIGH for the S2 randomness findings, which were measured locally. The S2 memory and latency numbers and the S3 numbers are still to be produced by the phase.

<user_constraints>
## User Constraints

There is no CONTEXT.md for this phase; `/gsd:discuss-phase` was not run. The locked constraints below come from PROJECT.md, STATE.md and research/SUMMARY.md. The planner must honour them.

### Locked Decisions (from PROJECT.md / SUMMARY.md)
- **Vendor Chronik code into the Ostro workspace.** Apache-2.0, with attribution and source commit `08715f4867c9f40e409c68d8e212bd9479d620c0`. Modernise it inside Ostro. **The Chronik repo is not modified. No git deps or path deps point to it.**
- Vendor exactly this set:
  - the chronik-raft `driver.rs` and `log_store.rs` (with the `StateMachine`/`Outbox` traits)
  - the vendored `raft` and `raft-proto` crates
  - from chronik-wal: group commit, segment handling, segment index, and truncate (`plan_segment_truncation`)
- Replication algorithms are **not** vendored in Phase 1 (that is Phase 12). The object store is never vendored (use the `object_store` crate later).
- Modernisation is mandatory (FOUND-03):
  - no chronik-common or chronik-monitoring
  - the `metrics` facade instead of `MetricsRecorder`
  - no tokio-uring
  - the raft crates renamed so that no `[patch]` is needed
- The crate layout is ostro-types, ostro-log, ostro-partition, ostro-meta, ostro-engine, ostro-bridge, ostro-proto, ostro-api, ostro-blob, ostro (binary) and ostro-cli.
- Toolchain: Rust **1.100**, Wasmtime **50.0.2**, redb **4.6** (not 5.0), tonic/prost **0.14**, axum **0.8**, rustls + aws-lc-rs, mimalloc for musl builds, cargo-zigbuild.
- Guests use **no WASI imports**: componentize-py 0.25.1 with `--stub-wasi`, jco 1.42 with `--disable all`, and Rust `wasm32-wasip2` with wit-bindgen 0.63. Don't use cargo-component or esbuild.
- The WIT interface is synchronous (no CM-async in v1).
- Licence: Apache-2.0.

### Claude's Discretion
- Where the vendored crates go and what they're named, as long as there is no `[patch]`.
- CI provider details, provided the workflows are GitHub Actions.
- How the spike harness, benchmarks and reports are structured.
- The *recommendations* for the three decisions. The phase must log the final decisions in PROJECT.md.

### Deferred Ideas (OUT OF SCOPE)
- `ReplicatedLog` trait and RF=1 implementation (STOR-04, Phase 2)
- Raft `Outbox` transport on tonic (META-01, Phase 2/13)
- Porting the replication algorithms (Phase 12)
- macOS notarization/signing (release pipeline, Phase 14). A smoke test is optional.
- SDKs. Spike guests are throwaway code.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| FOUND-01 | Workspace builds on pinned Rust 1.100 with the research crate layout | Rust 1.100.0 exists and was installed (`rustc 1.100.0 (0ef1bc386 2026-11-09)`). The workspace template is in "Architecture Patterns". wasmtime 50.0.2, tonic 0.14.9, axum 0.8.9, redb 4.6.0 and mimalloc compiled together on 1.100.0 locally |
| FOUND-02 | Chronik code vendored with attribution, commit and `VENDORED.md` | The file manifest with line counts is in "Vendoring Manifest". Apache-2.0 §4 obligations are under "Pitfalls" |
| FOUND-03 | No chronik-common/monitoring, `metrics` facade, no tokio-uring, no `[patch]` | Prototyped. Every change is listed in "Modernisation Recipe". 176 tests pass on 1.100.0 |
| FOUND-04 | cargo-deny bans duplicate tonic/prost/hyper/axum/opentelemetry and disallowed licences | A `deny.toml` using `deny-multiple-versions` was verified with cargo-deny 0.21.0. A deliberate prost 0.12 duplicate made it exit with code 2 |
| FOUND-05 | CI matrix: linux x86_64 + aarch64 (musl via cargo-zigbuild, mimalloc) and macOS | Workflow design in "CI Design". `cargo zigbuild test` exists (in the source). `ubuntu-24.04-arm` is available to private repos (2026-01-29) |
| FOUND-06 | S2 and S3 results recorded; three decisions logged | "Spike S2 Procedure" and "Spike S3 Procedure". Preliminary S2 measurements are already taken (see "S2 Preliminary Findings") |
</phase_requirements>

## Summary

The vendoring is low-risk. I copied the needed Chronik files into a scratch workspace and compiled them on Rust **1.100.0**:

- The raft crates were renamed (`ostro-raft-rs` and `ostro-raft-proto`, keeping the lib names `raft` and `raft_proto`) and moved to **prost 0.14.6 with `protox` codegen**, so no `protoc` is needed.
- The chronik-raft `driver.rs` and `log_store.rs` were copied unchanged. They depend only on raft, prost, tokio (sync), tracing, crc32fast and slog.
- The trimmed chronik-wal was copied with these changes: chronik-monitoring replaced by `metrics`, io_uring removed, and the unused fsync/checkpoint managers dropped.

All of it compiled. **176 vendored tests passed** (raft 45, driver/log_store 26, WAL 73 unit + 17 integration, plus doctests). cargo-deny 0.21.0 then passed advisories, bans, licences and sources, after three more changes forced by advisories: drop `bincode` (dead code only), replace `fxhash` with `rustc-hash`, and drop the raft `default-logger` slog-term/chrono chain (**chrono is now flagged unmaintained, RUSTSEC-2026-0543**).

The S2 guest spike already produced decisive data, from a 100-line Wasmtime 50.0.2 harness run against componentize-py 0.25.1 and jco 1.42.0 builds of a WIT that uses `resource step`:

1. **`resource step` works end-to-end in both Python and TS.**
2. **Neither toolchain gives per-run randomness or time.**
   - **Python (`--stub-wasi`):** the runtime deliberately skips its `random.seed()` reseed when stubbed (verified in the source). Every instance of a build returns the same `random.random()` sequence, `hash()` value and set order. A different build gives different values, even with `PYTHONHASHSEED=0`. `time.time()` and `uuid.uuid4()` **trap** (`unreachable` stubs).
   - **TS (`--disable all`):** `Math.random()` and `crypto.getRandomValues()` are identical across instances *and across builds*. `Date.now()` returns the **frozen build (pre-init) timestamp**.
3. **Builds are not reproducible.**
   - Three Python builds of the same source gave three different shas, with sizes varying by up to 39 KB.
   - Two TS builds differed by 2 bytes.

The SDKs (Phase 9/10) must therefore patch and reseed every ambient source from `ctx`, and `ostro build` reproducibility needs separate work.

The CI cannot be fully verified locally:
- There is no git remote.
- zig was not installed. Fetching cargo-zigbuild from GitHub releases was blocked by the sandbox in this session.
- macOS and aarch64 runners exist only on GitHub.

The plan must therefore split FOUND-05 into two parts:
- **(a) Locally verifiable now:**
  - the workflow files
  - `cargo build`, `cargo test` and `cargo deny` on the x86_64 glibc host
  - workflow linting
- **(b) Verified on first push:** the zigbuild musl legs, aarch64 and macOS. Mark these as `human_needed`/deferred verification, not as failed.

**Primary recommendation:**
- Put the vendored code in four `vendor/` crates (`ostro-raft-rs`, `ostro-raft-proto`, `ostro-raft-driver`, `ostro-wal`), keeping edition 2021, with the exact changes from the Modernisation Recipe.
- Give the skeleton crates real dependencies on wasmtime, tonic, axum, redb and mimalloc, so that CI and cargo-deny exercise the hard parts of the dependency graph from day one.
- Run S2/S3 as separate excluded cargo workspaces under `spikes/`.

## Standard Stack

### Core (Phase 1 uses)
| Library / Tool | Version | Purpose | Why Standard |
|---|---|---|---|
| Rust toolchain | **1.100.0** (`rust-toolchain.toml`) | Everything | Verified on static.rust-lang.org and installed locally. Wasmtime 50 MSRV is 1.97 |
| prost / prost-build | **0.14.6** | Vendored raft-proto codegen, Ostro protos | One prost version across the workspace, required by the cargo-deny ban |
| protox | **0.10.0** | Pure-Rust `.proto` → FileDescriptorSet (no `protoc`) | Verified: `prost_build::Config::compile_fds(protox::compile(..))` and `tonic_prost_build::configure().compile_fds(..)` both work |
| tonic / tonic-prost / tonic-prost-build | **0.14.9** | ostro-proto skeleton | Verified compiling with protox |
| wasmtime | **50.0.2** (`default-features = false`) | ostro-engine skeleton, S2 harness | Verified compiling on 1.100 |
| redb | **4.6.0** | ostro-partition skeleton, S3 | `Durability` has only `None` and `Immediate`: "None … not persisted unless followed by a commit with Durability::Immediate" (docs.rs) |
| metrics | **0.24.6** | Replaces chronik-monitoring/common in the vendored WAL | Facade only. The exporter is wired in a later phase |
| mimalloc | **0.1.52** | `#[global_allocator]` in the `ostro` binary | musl malloc contention (Pitfall 19) |
| cargo-deny | **0.21.0** | FOUND-04 | Verified locally with the official musl binary (sha256 matched) |
| cargo-zigbuild | **0.24.1** | musl static builds | Has the `build`, `test`, `run`, `check` and `clippy` subcommands (verified in `src/bin/cargo-zigbuild.rs` @ v0.24.1) |
| zig | **0.15.2** (pin) | Linker for zigbuild | The research targeted 0.15/0.16. 0.17.1 is the latest but its zigbuild compatibility is unverified (LOW) |

### Vendored crates' third-party dependencies (after modernisation, verified building)
| Crate | Deps |
|---|---|
| ostro-raft-proto | bytes 1, prost 0.14.6, lazy_static 1.4; build: prost-build 0.14.6, protox 0.10.0 |
| ostro-raft-rs | bytes, rustc-hash 2 (was fxhash), getset 0.1, prost 0.14.6, thiserror 2 (was 1), rand 0.9 or 0.10 (was 0.8), slog 2, slog-stdlog 4 (optional) |
| ostro-raft-driver | ostro-raft-rs (`default-features=false, features=["prost-codec"]`), prost, crc32fast, tokio (`sync`), tracing, slog, slog-stdlog; dev: tokio full+test-util, tempfile, serde_json |
| ostro-wal | tokio (`fs, io-util, sync, rt, time, macros`), bytes, byteorder, tracing, thiserror 2, dashmap 6 (was 5.5), crc32fast, serde, serde_json, libc, metrics; dev: tokio full, tempfile, rand |

**Note on rand:** use **rand 0.9** in ostro-raft-rs. metrics-exporter-prometheus pulls in rand 0.9 (via metrics-util), so this avoids a duplicate. The rand 0.9/0.10 change is `rand::rng().random_range(..)`. In 0.10 the trait is `rand::RngExt`, which was verified compiling.

### Supporting (spikes only, not in the main workspace)
| Tool | Version | Install |
|---|---|---|
| componentize-py | 0.25.1 | `uv tool run --from componentize-py==0.25.1 componentize-py ...` (uv 0.7.15 is present locally and works) |
| jco / componentize-js | 1.42.0 / 0.23.0 | `npm install @bytecodealliance/jco@1.42.0 @bytecodealliance/componentize-js@0.23.0` (76 packages, about 10 s) |
| wit-bindgen | 0.63.0 | Rust guest crate dep. `rustup target add wasm32-wasip2 --toolchain 1.100.0` |
| fjall (optional comparator) | 3.1.13 | Only if redb misses the S3 threshold |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|---|---|---|
| protox in build.rs | Commit the generated `eraftpb.rs` | No build-dep tree (petgraph etc.), but regeneration becomes manual. protox is pure Rust and was verified, so use it |
| Fixing raft datadriven tests | Vendor tikv `datadriven` from raft-rs v0.7.0 | Chronik's copy already dropped the dev-dep, so those two test modules never compiled. Remove them and record that in VENDORED.md |
| Separate `vendor/` crates | Inline the modules into ostro-meta/ostro-log | Separate crates keep provenance auditable, keep edition 2021 and allow per-crate lint relaxations |
| wasmtime `cache` feature | (drop it) | Wasmtime's own disk cache pulls in `zstd-sys` (C). Ostro builds its own AOT cache (ENG-07), so **leave `cache` out** of the feature list |

**Installation (local dev):**
```bash
rustup toolchain install 1.100.0 --profile minimal -c clippy,rustfmt \
  -t x86_64-unknown-linux-musl,aarch64-unknown-linux-musl,wasm32-wasip2
# cargo-deny: official release binary (verified sha) or: cargo install --locked cargo-deny@0.21.0
cargo install --locked cargo-deny@0.21.0 cargo-zigbuild@0.24.1 cargo-nextest
pip3 install ziglang==0.15.2   # or download zig 0.15.2; cargo-zigbuild finds `python -m ziglang`
```

## Vendoring Manifest

Source: `chronik-stream` @ `08715f4867c9f40e409c68d8e212bd9479d620c0` (2026-11-18). Line counts are from `wc -l` at that commit.

**Getting the source:**
- The reference clone lives in a session scratchpad: `<scratchpad>/src/chronik-stream`. **It may not survive into the executor's session.**
- The plan's first vendoring task must therefore do this: `git clone https://github.com/lspecian/chronik-stream "$TMPDIR/chronik" && git -C "$TMPDIR/chronik" checkout 08715f4867c9f40e409c68d8e212bd9479d620c0`, outside the workspace.
- Copy the files, then never reference that path. **Do not commit a vendoring script that points at the repo.**

### → `vendor/ostro-raft-proto` (from `crates/raft-proto`, itself tikv/raft-rs v0.7.0, TiKV Authors)
| Upstream file | Lines | Action |
|---|---:|---|
| `proto/eraftpb.proto` | 115 | copy unchanged |
| `src/lib.rs` | 611 | copy unchanged (`include!(OUT_DIR/eraftpb.rs)`, prost accessors) |
| `src/confchange.rs` | 146 | copy unchanged |
| `src/confstate.rs` | 26 | copy unchanged |
| `build.rs` | 11 | **rewrite**: protox + `prost_build::Config::compile_fds`, `config.bytes(["."])` |
| `Cargo.toml` | 24 | **rewrite**: package `ostro-raft-proto`, `[lib] name = "raft_proto"`, prost 0.14.6, remove `[workspace]`, add `[features] prost-codec = []` |
| `Cargo.lock` | 364 | do not copy |

### → `vendor/ostro-raft-rs` (from `crates/raft`, tikv/raft-rs v0.7.0 modified by Chronik)
| Upstream file | Lines | Action |
|---|---:|---|
| `src/lib.rs` | 596 | modify: `fxhash::FxHasher` → `rustc_hash::FxHasher`; `default_logger()` body uses `slog_stdlog::StdLog` instead of slog-term/envlogger |
| `src/raft.rs` | 2970 | modify: rand API (`rand::rng().random_range`); fix or declare the `failpoints` cfg (unexpected_cfgs warning) |
| `src/raft_log.rs` | 1923 | copy |
| `src/raw_node.rs` | 831 | copy |
| `src/storage.rs` | 813 | copy |
| `src/log_unstable.rs` | 482 | copy |
| `src/tracker.rs` | 388 | copy |
| `src/tracker/inflights.rs` | 426 | copy |
| `src/tracker/progress.rs` | 411 | copy |
| `src/tracker/state.rs` | 40 | copy |
| `src/confchange.rs` | 15 | modify: remove `#[cfg(test)] pub mod datadriven_test;` |
| `src/confchange/changer.rs` | 357 | copy |
| `src/confchange/restore.rs` | 107 | copy |
| `src/quorum.rs` | 74 | modify: remove `#[cfg(test)] pub mod datadriven_test;` |
| `src/quorum/joint.rs` | 98 | copy |
| `src/quorum/majority.rs` | 255 | copy |
| `src/config.rs` | 219 | copy |
| `src/errors.rs` | 174 | copy (`prost::DecodeError`, so thiserror 2 works unchanged) |
| `src/read_only.rs` | 136 | copy |
| `src/status.rs` | 53 | copy |
| `src/util.rs` | 178 | copy |
| `src/confchange/datadriven_test.rs`, `src/quorum/datadriven_test.rs` | 86 + 323 | **exclude** (they need tikv's `datadriven` crate, which Chronik never vendored) |
| `src/*/testdata/*.txt` (13 files) | ~1,300 | exclude (only used by the datadriven tests) |
| `Cargo.toml` | 40 | **rewrite**: package `ostro-raft-rs`, `[lib] name = "raft"`, `raft-proto = { package = "ostro-raft-proto", path = "../ostro-raft-proto" }`, `default = ["prost-codec"]`, `default-logger = ["dep:slog-stdlog"]`, drop `fail`, `slog-term`, `slog-envlogger`, `slog-async` |
| `Cargo.lock` | 996 | do not copy |

Subtotal: about 10,544 Rust lines.

### → `vendor/ostro-raft-driver` (from `crates/chronik-raft`)
| Upstream file | Lines | Action |
|---|---:|---|
| `src/driver.rs` | 1123 | copy. Optionally replace the deprecated `raft::is_empty_snap(..)` with `ready.snapshot().is_empty()` (line 564) |
| `src/log_store.rs` | 1184 | copy unchanged |
| `src/lib.rs` | 27 | **rewrite** (4 lines): `pub mod driver; pub mod log_store;` plus re-exports of `Applied, DriverConfig, Outbox, ProposeError, RaftDriver, RaftHandle, RaftStatus, StateMachine, LogStoreOptions, RaftLogStore` |
| `src/net.rs` | 560 | **exclude**: tonic 0.11 transport. Ostro writes its own `Outbox` later (META-01) |
| `src/{config,error,rpc,storage,transport,prost_bridge}.rs`, `src/transport/{grpc,memory}.rs` | 41+62+248+131+52+28+348+159 | **exclude**: legacy/STUB modules superseded by driver/log_store (ARCHITECTURE.md). `error.rs` drags in tonic + bincode |
| `build.rs`, `proto/raft_rpc.proto`, `proto/catalog_rpc.proto` | 14+156+42 | **exclude** (tonic-build 0.11, protoc) |
| `Cargo.toml` | 57 | **rewrite**: no chronik-common, chronik-monitoring, chronik-raft-bridge, tonic, prost-types, bincode, dashmap, serde, futures, async-trait or anyhow |

`chronik-raft-bridge` (47 lines) is **not vendored**. It existed only to bridge two prost versions. With a single prost 0.14, call `prost::Message::{encode, decode}` directly.

### → `vendor/ostro-wal` (from `crates/chronik-wal`)
| Upstream file | Lines | Action |
|---|---:|---|
| `src/group_commit.rs` | 2837 | modify. (1) Delete `use chronik_monitoring::MetricsRecorder;` and replace `MetricsRecorder::record_wal_batch(batch_count as u64)` (line 1417) with `metrics::counter!(..).increment(1)` / `metrics::histogram!(..)`. (2) Delete every `#[cfg(all(target_os = "linux", feature = "async-io"))]` item (26 sites), the `IoUringThreadHandle` import and the `io_uring_handle` fields/params. (3) Remove or rename the `CHRONIK_WAL_*` env reads (lines 196, 221, 244, 482). Library code should not read env, so prefer explicit config. (4) Change `"__chronik_metadata"` in `is_raft_topic` (line 1049) to an Ostro reserved prefix, or remove it |
| `src/manager.rs` | 1190 | modify: remove the `CheckpointManager`/`FsyncBatcher` fields and construction. They are constructed but **never used** (only at lines 31/32, 55–60, 88/89). Delete the dead `extract_offset_range` + `MinimalCanonicalRecord` (lines ~1096–1180, **no callers**), which removes bincode. Update the `CHRONIK_WAL_PROFILE` log text |
| `src/record.rs` | 723 | copy (V1/V2 `WalRecord`. Note that `verify_checksum()` is a no-op for V2; Phase 2 adds CRC verification) |
| `src/segment_index.rs` | 935 | copy |
| `src/truncate.rs` | 507 | copy |
| `src/buffer_pool.rs` | 422 | copy (`record.rs` uses `PooledBuffer`) |
| `src/config.rs` | 236 | copy. It keeps the `CheckpointConfig`/`FsyncConfig` types, which the tests use. Optionally drop the `AsyncIoConfig` io_uring knobs |
| `src/error.rs` | 66 | modify: drop `SerializationError(#[from] bincode::Error)` |
| `src/io_priority.rs` | 112 | modify: `chronik_wal::` → `ostro_wal::` in the doc example. It's Linux `ioprio_set` via libc, cfg-gated, with a no-op elsewhere. Alternatively drop it and its single call site in `group_commit.rs:1214` |
| `src/lib.rs` | 76 | **rewrite**: only the vendored modules, plus the `RecoveryResult` struct. Drop `recover_all` (it uses `WalManager::recover`, but keeping it is harmless) |
| `tests/suffix_truncation.rs` | 434 | copy, `chronik_wal` → `ostro_wal` |
| `tests/idle_seal_rotation.rs` | 188 | copy, `chronik_wal` → `ostro_wal` |
| `src/io_uring_thread.rs` | 373 | **exclude** (tokio-uring) |
| `src/fsync.rs` | 499 | **exclude** (`chronik_common::metrics`; FsyncBatcher unused) |
| `src/checkpoint.rs` | 215 | **exclude** (unused by the vendored path) |
| `src/compaction.rs` | 657 | **exclude** (Kafka-style compaction, not needed) |
| `src/periodic_flusher.rs` | 121 | **exclude** |
| `src/segment.rs` | 254 | **exclude** (legacy `WalSegment`, the only user of chrono; GroupCommitWal owns segments) |
| `src/{audit,streaming,io,rotation}/*`, `src/checkpoint/*` | 0–3 each | exclude (empty stubs) |
| `src/tests/unit_tests.rs` | 895 | exclude (tests the excluded `segment.rs`) |
| `src/tests/rotation_load_test.rs` | 794 | exclude (wall-clock perf floor, flaky in CI). It may be copied later as `#[ignore]` |
| `src/tests/{wal_durability_stress,wal_validation_test,async_io_bench}.rs` | 490+483+242 | exclude. Chronik didn't compile them either (not in `tests/mod.rs`) |
| `RELEASE.md` | 290 | exclude |

Subtotal: about 7,028 src lines plus 622 test lines.

**Answer to an open research gap (Phase 2 flag):** `manager.rs::extract_offset_range` is the *only* code that bincode-decodes `canonical_data` as a Kafka `CanonicalRecord`, and it has **zero callers** at `08715f4`. rustc flagged it as dead after vendoring, and a grep confirmed it. No WAL recovery path decodes `canonical_data` as Kafka. The function is deleted in Phase 1. Confidence: HIGH.

## Modernisation Recipe (prototype-verified)

| Step | Change | Verified |
|---|---|---|
| 1 | Rename the crates (`ostro-raft-rs`/`ostro-raft-proto`), keep `[lib] name = "raft"`/`"raft_proto"` so the vendored source is unchanged, use path deps, **no `[patch]`** | ✅ builds |
| 2 | Move raft-proto to prost 0.14.6 with protox (no protoc). The prost 0.12→0.14 change needed **no source edits** in raft or raft-proto | ✅ builds, 45 raft tests pass |
| 3 | raft: thiserror 1→2, rand 0.8→0.9/0.10, fxhash→rustc-hash, slog-term/envlogger→slog-stdlog, remove the datadriven test modules | ✅ |
| 4 | Driver crate: only driver.rs and log_store.rs. Depend on raft with `default-features = false, features = ["prost-codec"]` | ✅ 26 tests pass (multi-thread tokio, about 2.5 s) |
| 5 | WAL: the `metrics` facade, io_uring cfg blocks removed, unused managers removed, bincode/chrono dropped, dashmap 6, thiserror 2 | ✅ 73 unit + 17 integration tests pass |
| 6 | `publish = false` on every workspace crate (otherwise cargo-deny's `wildcards` check rejects path deps) | ✅ |
| 7 | cargo-deny: advisories, bans, licences and sources all `ok` | ✅ |

Leftover warnings to fix or allow in the vendored crates:
- `unexpected_cfgs` for `failpoints` in raft.rs. Declare a `failpoints = []` feature or delete the two cfg blocks.
- The deprecated `is_empty_snap` in driver.rs.
- An unused `PathBuf` import and an unused `partition_key` in the WAL.

Recommendation: fix them, because they are trivial and each one goes in VENDORED.md. Run `clippy -D warnings` on `ostro-*` crates only and exclude `vendor/*` (`cargo clippy --workspace --exclude ostro-raft-rs --exclude ostro-raft-proto --exclude ostro-raft-driver --exclude ostro-wal -- -D warnings`), or give vendored crates `[lints] workspace = false`.

## Architecture Patterns

### Recommended Project Structure
```
ostro/
├── Cargo.toml                 # virtual workspace, resolver = "3"
├── rust-toolchain.toml        # channel = "1.100.0"
├── deny.toml
├── LICENSE                    # Apache-2.0 full text
├── NOTICE                     # attribution: Chronik Stream (Apache-2.0), TiKV raft-rs (Apache-2.0)
├── VENDORED.md
├── crates/
│   ├── ostro-types/  ostro-log/  ostro-partition/  ostro-meta/  ostro-engine/
│   ├── ostro-bridge/ ostro-proto/ ostro-api/ ostro-blob/ ostro/ ostro-cli/
├── vendor/
│   ├── ostro-raft-proto/  ostro-raft-rs/  ostro-raft-driver/  ostro-wal/
├── spikes/                    # EXCLUDED from workspace; own Cargo.lock each
│   ├── wit/spike.wit
│   ├── s2-guests/{python,ts,rust}/
│   ├── s2-harness/            # wasmtime 50 measurement harness
│   └── s3-redb-bench/
├── ci/deny-fixtures/{dup-prost,bad-licence}/   # negative tests for FOUND-04 (excluded)
├── docs/spikes/{S2-guest-toolchains.md,S3-redb-durability.md}
├── scripts/{check-vendored.sh,ci-local.sh}
└── .github/workflows/ci.yml
```

### Pattern 1: Workspace manifest
```toml
# Cargo.toml
[workspace]
resolver = "3"
members = ["crates/*", "vendor/*"]
exclude = ["spikes", "ci/deny-fixtures"]

[workspace.package]
version = "0.1.0"
edition = "2024"            # Ostro crates. vendor/* keep edition = "2021" in their own manifests
rust-version = "1.100"
license = "Apache-2.0"
publish = false

[workspace.dependencies]
# vendored
raft        = { package = "ostro-raft-rs", path = "vendor/ostro-raft-rs", default-features = false, features = ["prost-codec"] }
ostro-raft-driver = { path = "vendor/ostro-raft-driver" }
ostro-wal   = { path = "vendor/ostro-wal" }
# stack (versions verified 2026-11-18)
wasmtime    = { version = "=50.0.2", default-features = false, features = ["runtime","cranelift","component-model","async","pooling-allocator","parallel-compilation","std","addr2line","demangle","coredump"] }
tokio       = { version = "1.54", features = ["full"] }
tonic       = { version = "0.14.9", default-features = false, features = ["codegen","transport","router"] }
tonic-prost = "0.14.9"
prost       = "0.14.6"
axum        = { version = "0.8.9", features = ["http2","json","macros"] }
redb        = "4.6.0"
metrics     = "0.24.6"
mimalloc    = "0.1.52"
tracing     = "0.1.44"
clap        = { version = "4.6.8", features = ["derive","env"] }
thiserror   = "2"
# build
protox      = "0.10.0"
prost-build = "0.14.6"
tonic-prost-build = "0.14.9"

[workspace.lints.rust]
unsafe_op_in_unsafe_fn = "deny"
[workspace.lints.clippy]
dbg_macro = "deny"

[profile.release]
lto = "fat"
codegen-units = 1
strip = "debuginfo"
# panic strategy: leave default (unwind) until Phase 3 decides (catch_unwind around guest calls)
```
```toml
# rust-toolchain.toml
[toolchain]
channel = "1.100.0"
components = ["clippy", "rustfmt"]
targets = ["x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl", "wasm32-wasip2"]
profile = "minimal"
```

### Pattern 2: Skeleton crates that carry real dependencies
Each skeleton crate should hold *one* small, real use of its key dependency. That way the CI matrix proves the hard builds on every target now, and the cargo-deny duplicate bans see tonic, prost, hyper and axum in the graph.

| Crate | Phase-1 content |
|---|---|
| ostro-types | `pub const CODEC_VERSION: u16 = 1;` placeholder types (no heavy deps) |
| ostro-log | depends on `ostro-wal`; re-export / smoke test that opens a `GroupCommitWal` in a tempdir |
| ostro-partition | depends on `redb`; test: create db, write txn with `Durability::None`, then `Immediate` |
| ostro-meta | depends on `ostro-raft-driver`; test: single-voter `RaftDriver::start` + propose via a no-op `Outbox` (mirrors the vendored driver tests) |
| ostro-engine | depends on `wasmtime`; `pub fn engine_config() -> Config` with `cranelift_nan_canonicalization(true)`, `relaxed_simd_deterministic(true)`, `consume_fuel(true)`, `epoch_interruption(true)`; test builds an `Engine` |
| ostro-proto | `build.rs` with protox + `tonic_prost_build::configure().compile_fds(..)` on a `health.proto` (verified) |
| ostro-api | depends on axum + ostro-proto; `Routes::into_axum_router()` compiles |
| ostro-bridge, ostro-blob, ostro-cli | empty libs (deps come later) |
| ostro | bin: `#[global_allocator] static GLOBAL: mimalloc::MiMalloc`, clap `--version`, `build-info` subcommand printing target/allocator |

### Pattern 3: VENDORED.md + per-file notices
```markdown
# Vendored code
Upstream: Chronik Stream (https://github.com/lspecian/chronik-stream), Apache-2.0
Commit: 08715f4867c9f40e409c68d8e212bd9479d620c0 (2026-11-18)
Transitive upstream for vendor/ostro-raft-rs and vendor/ostro-raft-proto: tikv/raft-rs v0.7.0 (Apache-2.0, TiKV Project Authors), as modified by Chronik for prost 0.12.

| Ostro path | Upstream path | Upstream lines | Status | Local modifications |
|---|---|---:|---|---|
| vendor/ostro-wal/src/group_commit.rs | crates/chronik-wal/src/group_commit.rs | 2837 | modified | MetricsRecorder → metrics facade; io_uring paths removed; CHRONIK_* env reads removed |
| ... one row per file ... |
## Excluded upstream files
| Upstream path | Reason |
```
- Each **modified** file gets a header line: `// Vendored from chronik-stream@08715f4 crates/…; modified by Ostro (see VENDORED.md).` Apache-2.0 §4(b) requires prominent notice of changes. Keep the existing TiKV/etcd copyright headers.
- `scripts/check-vendored.sh` (run in CI) asserts:
  1. every file under `vendor/` (except Cargo.toml/build.rs written fresh) has a row in VENDORED.md
  2. `grep -rn 'chronik_common\|chronik_monitoring\|tokio-uring\|tokio_uring\|\[patch' --include='*.rs' --include='Cargo.toml' .` returns nothing
  3. `cargo metadata --format-version 1` has no `git+` sources and no path outside the repo
  4. no file contains `/tmp/` or the scratchpad path.

  The VENDORED.md and NOTICE attribution URL is allowed. "Nothing references the Chronik repo" means no build or dependency reference.

### Anti-Patterns to Avoid
- **Migrating vendored crates to edition 2024.** 2024 changes temporary drop order in tail expressions and `if let` scrutinees. group_commit.rs holds locks in such expressions. Keep `edition = "2021"` in `vendor/*`.
- **Keeping `[workspace]` in the vendored raft manifests.** Chronik put `[workspace]` in them to keep them out of its workspace. Remove it or they won't join Ostro's workspace.
- **Library code reading env vars** (`CHRONIK_WAL_PROFILE`, …). It is a hidden config channel. Remove it, or rename and document it.
- **Putting spike crates in the main workspace.** That pollutes `Cargo.lock` and cargo-deny with spike-only deps (wasmtime CLI-ish features, fjall). Use `exclude = ["spikes"]`.
- **`Config::wasm_threads(false)`.** It **does not exist** when wasmtime's `threads` feature is off (compile error verified). With `default-features = false` and no `threads`, the threads proposal is already unavailable. Don't call it.
- **Returning `anyhow::Result` around wasmtime calls.** In 50.0.2, `wasmtime::Error` did not convert into `anyhow::Error` with `?` under our feature set (compile error verified). Use `wasmtime::Result` at the engine boundary.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| protoc-free codegen | shelling out to protoc, a vendored protoc | `protox` 0.10 + `compile_fds` | Verified for both prost-build and tonic-prost-build |
| Duplicate-version policing | a custom `cargo tree -d` grep | cargo-deny `deny = [{ crate, deny-multiple-versions = true }]` | Exact per-crate semantics, verified |
| musl cross-linking | Docker `cross` images, musl-gcc | cargo-zigbuild + zig | Handles aws-lc-sys/mimalloc C builds. `cargo zigbuild test` exists |
| Component import inspection | wasm-tools in the harness | `Component::component_type().imports(&engine)` | Verified in the S2 harness. No extra tool needed |
| Raft | anything new | the vendored driver/log_store | Already debugged in Chronik |

## Common Pitfalls

### Pitfall 1: cargo-deny `wildcards` rejects path deps
**What goes wrong:** `error[wildcard]: found 1 wildcard dependency … allow-wildcard-paths … does not apply to public crates`.
**How to avoid:** `publish = false` on every workspace crate (via `[workspace.package] publish = false`). Set `allow-wildcard-paths = true` in deny.toml. Verified.

### Pitfall 2: Advisories fail on vendored transitive deps
**What goes wrong:** RUSTSEC-2025-0141 (bincode unmaintained), RUSTSEC-2025-0057 (fxhash) and **RUSTSEC-2026-0543 (chrono unmaintained, all versions)**. chrono came in through raft's `default-logger` → slog-term.
**How to avoid:** apply the Modernisation Recipe steps 3 and 5. Don't paper over these with `ignore`. Future note: croner 4 has an optional `jiff` backend (jiff 1.0.1 exists), so prefer it over chrono in the cron phase.

### Pitfall 3: Workspace members are checked with *their own* default features
Disabling `default-logger` only in the consumer wasn't enough. cargo-deny still saw `ostro-raft-rs` with default features. **Change the vendored crate's `default` feature set itself.**

### Pitfall 4: S2 measurements that flatter or penalise wrongly
- **CoW memory init needs mmap'd artefacts.** `Component::deserialize(&bytes)` copies memory images. Use `Component::deserialize_file(path)` for the instantiate and RSS numbers. Preliminary run: Python on-demand instantiate p50 was 435 µs with in-memory deserialize. The formal run must use `deserialize_file` + pooling.
- **The first instantiate includes lazy init.** Python's first was 6.3 ms and the second 0.49 ms. Report the first and the steady-state p50/p99 separately.
- **Process max RSS includes Cranelift compile** (about 500 MB). Compile in a separate subcommand or process from the instance-memory measurement.

### Pitfall 5: WSL2 fsync semantics distort S3
WSL2 ext4 sits on a VHDX. fsync cost differs from bare-metal NVMe, and `/mnt/c` (9p) is drastically slower. **Run S3 on the Linux filesystem (e.g. `$HOME` or the scratch dir), never on `/mnt/*`.** Record the device and filesystem in the report. Treat the numbers as relative between durability tiers, not as absolute NFR-2.5 evidence. Optionally re-run on a CI runner.

### Pitfall 6: Apache-2.0 obligations
Ship `LICENSE` + `NOTICE`. Keep the upstream copyright headers. Mark modified files. Chronik has a LICENSE but **no NOTICE file**, so Ostro's NOTICE should credit "Chronik Stream (Apache-2.0)" and "TiKV raft-rs (Apache-2.0)".

### Pitfall 7: Unverifiable-locally success criteria
- There is no git remote (`git remote -v` is empty).
- zig is not installed.
- The sandbox blocked downloading cargo-zigbuild/zig release binaries in this research session. The executor may need user approval, or `pip install ziglang` / `cargo install cargo-zigbuild`.
- macOS and aarch64 legs exist only on GitHub.

**How to handle:** the plan's verification for FOUND-05 lists (a) local checks: workflow lint, x86_64 glibc build/test/deny, and an optional local x86_64 musl zigbuild if zig can be installed; and (b) `human_needed: push to GitHub and confirm all matrix legs are green`. Phase verification should then report PARTIAL, not FAIL.

## Code Examples

### deny.toml (verified with cargo-deny 0.21.0)
```toml
[graph]
targets = ["x86_64-unknown-linux-gnu","x86_64-unknown-linux-musl","aarch64-unknown-linux-musl","aarch64-apple-darwin","x86_64-apple-darwin"]
all-features = false
exclude-dev = true

[advisories]
version = 2
yanked = "deny"
ignore = []

[licenses]
version = 2
allow = ["Apache-2.0","Apache-2.0 WITH LLVM-exception","MIT","BSD-2-Clause","BSD-3-Clause","ISC","Unicode-3.0","Zlib"]
# add only when a real dep needs it, with a comment: e.g. "MIT-0", "CC0-1.0", "BSL-1.0", "Unlicense" (seen as OR-alternatives in the graph today)
confidence-threshold = 0.9

[bans]
multiple-versions = "warn"      # unavoidable dupes today: hashbrown, syn, itertools, getrandom, foldhash, cpufeatures
wildcards = "deny"
allow-wildcard-paths = true
deny = [
  { crate = "tonic",             deny-multiple-versions = true },
  { crate = "tonic-prost",       deny-multiple-versions = true },
  { crate = "prost",             deny-multiple-versions = true },
  { crate = "prost-types",       deny-multiple-versions = true },
  { crate = "hyper",             deny-multiple-versions = true },
  { crate = "http",              deny-multiple-versions = true },
  { crate = "axum",              deny-multiple-versions = true },
  { crate = "opentelemetry",     deny-multiple-versions = true },
  { crate = "opentelemetry_sdk", deny-multiple-versions = true },
  { crate = "wasmtime",          deny-multiple-versions = true },
  { crate = "openssl-sys",  reason = "rustls + aws-lc-rs only" },
  { crate = "native-tls",   reason = "rustls only" },
  { crate = "tokio-uring",  reason = "io_uring removed from vendored WAL" },
  { crate = "prometheus",   reason = "use the metrics facade" },
  { crate = "chrono",       reason = "RUSTSEC-2026-0543 unmaintained; use jiff" },
]

[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-registry = ["https://github.com/rust-lang/crates.io-index"]
allow-git = []
```
**Negative test (verified mechanism):** adding a crate that depends on `prost = "0.12"` made `cargo deny check bans` print `error[duplicate]: found 2 duplicate entries for crate 'prost'` and exit with **2**. Add the fixtures as separate tiny workspaces under `ci/deny-fixtures/` that reuse the root `deny.toml` (`cargo deny --manifest-path ci/deny-fixtures/dup-prost/Cargo.toml --config deny.toml check bans`). CI asserts a **non-zero** exit for `dup-prost` (prost 0.12 + 0.14) and for `bad-licence` (a path crate with `license = "GPL-3.0-only"`).

### Raft-proto build.rs (verified)
```rust
fn main() {
    let fds = protox::compile(["proto/eraftpb.proto"], ["proto/"]).expect("protox");
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.compile_fds(fds).expect("Failed to compile protos");
    println!("cargo:rerun-if-changed=proto/eraftpb.proto");
}
```

### WAL metrics swap (verified compiling)
```rust
// group_commit.rs, was: MetricsRecorder::record_wal_batch(batch_count as u64);
metrics::counter!("ostro_wal_batches_total").increment(1);
metrics::histogram!("ostro_wal_batch_records").record(batch_count as f64);
```

### CI design (.github/workflows/ci.yml, sketch)
```yaml
name: ci
on: { push: { branches: [main] }, pull_request: {} }
env: { CARGO_TERM_COLOR: always, RUST_BACKTRACE: 1 }
jobs:
  lint:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v5
      - run: rustup toolchain install          # installs channel from rust-toolchain.toml
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets --exclude ostro-raft-rs --exclude ostro-raft-proto --exclude ostro-raft-driver --exclude ostro-wal -- -D warnings
      - uses: EmbarkStudios/cargo-deny-action@v2   # pin to a commit SHA in the real file
        with: { command: check, arguments: --all-features }
      - run: scripts/check-vendored.sh
      - run: scripts/check-deny-fixtures.sh   # asserts dup-prost and bad-licence FAIL
  test:
    strategy:
      fail-fast: false
      matrix:
        include:
          - { name: linux-x86_64-musl,  os: ubuntu-24.04,     target: x86_64-unknown-linux-musl,  zig: true }
          - { name: linux-aarch64-musl, os: ubuntu-24.04-arm, target: aarch64-unknown-linux-musl, zig: true }
          - { name: macos-aarch64,      os: macos-15,         target: aarch64-apple-darwin,       zig: false }
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v5
      - run: rustup toolchain install
      - uses: Swatinem/rust-cache@v2
      - if: matrix.zig
        uses: mlugg/setup-zig@v2
        with: { version: 0.15.2 }
      - if: matrix.zig
        uses: taiki-e/install-action@v2
        with: { tool: cargo-zigbuild@0.24.1 }
      - if: matrix.zig
        run: |
          cargo zigbuild test --workspace --target ${{ matrix.target }}
          cargo zigbuild --release -p ostro --target ${{ matrix.target }}
          file target/${{ matrix.target }}/release/ostro | tee /dev/stderr | grep -q 'statically linked'
          target/${{ matrix.target }}/release/ostro build-info | grep -q 'allocator=mimalloc'
      - if: ${{ !matrix.zig }}
        run: |
          cargo test --workspace
          cargo build --release -p ostro
          target/release/ostro build-info
```
Notes:
- Native runners mean musl test binaries run natively on both arches. A musl static binary runs on a glibc host.
- `ubuntu-24.04-arm` has 2 vCPU in private repos (GitHub changelog 2026-01-29). The macOS label `macos-15` is arm64.
- Pin third-party actions to commit SHAs.
- Optionally add a `linux-x86_64-gnu` leg running plain `cargo test` for fast feedback.
- Local validation of the YAML: `docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest` (docker is present; optional).

## Spike S2 Procedure (guest toolchains)

**Shared WIT** (`spikes/wit/spike.wit`, already proven to build in both toolchains):
```wit
package ostro:spike@0.1.0;
interface journal {
    resource step { seq: func() -> u64; complete: func(output: string) -> bool; }
    variant step-start { replayed(string), execute(step) }
    begin-step: func(name: string) -> step-start;
    begin-step-u64: func(name: string) -> u64;          // fallback shape for the decision
    complete-u64: func(handle: u64, output: string) -> bool;
}
interface context { now-ms: func() -> u64; random-u64: func() -> u64; }
world spike {
    import journal; import context;
    export run: func(input: string) -> string;
    export probe-random: func() -> string;   // py: random.random() x3 ; ts: Math.random() x3
    export probe-time: func() -> string;     // py: time.time()       ; ts: Date.now()
    export probe-hash: func() -> string;     // py: hash("ostro"), list(set) ; ts: Object.keys order
    export probe-uuid: func() -> string;     // py: uuid.uuid4()      ; ts: crypto.getRandomValues
    // add: run-reseeded: func() -> string — guest calls random.seed(context.random_u64()) / overrides Math.random, then draws
}
```

**Builds:**
- Python: `uv tool run --from componentize-py==0.25.1 componentize-py -d ../wit -w spike componentize --stub-wasi -p . app -o py.wasm`. Bindings style on 0.25.1: `import wit_world; from wit_world.imports import journal, context; class WitWorld(wit_world.WitWorld): ...`. Resources come out as Python classes with a context manager (`with start.value as step:`). Variants become `journal.StepStart_Replayed` / `journal.StepStart_Execute`. Generate stubs to inspect with `... bindings gen/`.
- TS: `npx jco componentize src/index.ts --wit ../wit --world-name spike --disable all -o ts.wasm`. Imports are `import { beginStep } from 'ostro:spike/journal@0.1.0'`. Variants are `{tag:'execute', val: step}`. Exports are camelCase functions.
- Rust: a `cdylib` crate with `wit-bindgen = "0.63"`, `wit_bindgen::generate!({ path: "../wit", world: "spike" }); struct G; impl Guest for G {..} export!(G);`, built with `cargo build --release --target wasm32-wasip2`. **Record its imports.** wasip2 std may import `wasi:cli/*`, which Ostro's deploy-time validation would reject, so this is a finding for ENG-02/Phase 4.
- Also record **AOT size** (`engine.precompile_component(&bytes)?.len()`), gzip size, and `wasm-tools strip` size if wasm-tools is available.

**Harness** (`spikes/s2-harness`, separate workspace, wasmtime 50.0.2). The working version lives in this session's scratch dir; re-create it from this outline:
- `bindgen!({ path: "../wit", world: "spike" })`. Implement `journal::Host`, `journal::HostStep` (backed by `ResourceTable`) and `context::Host`, plus a `HasData` marker. Register with `Spike::add_to_linker::<Host, D>(&mut linker, |h| h)`.
- Engine config: `cranelift_nan_canonicalization(true)`, `relaxed_simd_deterministic(true)`. A second pass uses `InstanceAllocationStrategy::Pooling(PoolingAllocationConfig::default())`.
- Subcommands, each run as its own process:
  - `compile <wasm> <out.cwasm>`: time and peak RSS via `/usr/bin/time -f %M`.
  - `instantiate <cwasm>`: `Component::deserialize_file`, `SpikePre::new(linker.instantiate_pre(&c)?)`. Report the first instantiate, then 1,000× p50/p99 for on-demand and for pooling, plus the first-call latency of `run`.
  - `memory <cwasm> N=50`: a `ResourceLimiter` records the max `desired` in `memory_growing` (declared linear memory). The `/proc/self/status` VmRSS delta is measured after N live instances that each made one call, divided by N, then measured again after drop (pool retention).
  - `probes <cwasm>`: calls each probe on 2 fresh instances, plus `run-reseeded` with 2 different host seeds and twice with the same seed. Records traps with the first backtrace line.
  - `cold <cwasm>`: a fresh process does deserialize_file → instantiate → call. Run 20× and take the median.
- Reproducibility: build each guest **3×** (Python with and without `PYTHONHASHSEED=0`) and compare sha256. If they differ, locate the differing sections (`cmp -l | head`, `wasm-tools objdump`).
- Compare against NFR-2.3 (Python p50 < 10 ms, TS/Rust < 1 ms from warm InstancePre).

### S2 Preliminary Findings (measured 2026-11-18, x86_64 WSL2, on-demand allocator, in-memory deserialize, n=1 build per row)
| Metric | Python (componentize-py 0.25.1 `--stub-wasi`) | TS (jco 1.42.0 `--disable all`) |
|---|---|---|
| `.wasm` size | 18,508,365 B | 12,014,255 B |
| AOT `.cwasm` | 32,867,080 B | 33,604,416 B |
| precompile time | 0.86–1.07 s | 0.99–1.02 s |
| process peak RSS (incl. compile) | ~500 MB | ~545 MB |
| deserialize (in-memory) | ~20 ms | ~18 ms |
| first instantiate / steady p50 / p99 | 6.3 ms / 435 µs / 634 µs | 3.9 ms / 9 µs / 25 µs |
| first call of `run` | ~1.0–1.3 ms | ~0.7–0.9 ms |
| declared linear memory | 13.2 MB | 9.5 MB |
| RSS per live instance (after 1 call) | ~5.5 MB | ~2.4 MB |
| imports | only `ostro:spike/*` | only `ostro:spike/*` |
| extra exports | **`exports` interface with `init(app-name, symbols, stub-wasi)`** (componentize-py internal) | none |
| `resource step` round trip | ✅ works | ✅ works |
| random across instances of one build | **identical** (`random.seed()` reseed skipped when `STUB_WASI`, runtime/src/lib.rs @ v0.25.1) | **identical** |
| random across builds | differs | **identical** (fixed deterministic seed) |
| hash()/set order | baked per build, identical across instances, differs per build **even with PYTHONHASHSEED=0** | n/a (insertion order) |
| time | `time.time()` **traps** (`unreachable` stub) | `Date.now()` returns the **frozen build timestamp** (1795021009988 = ts-a.wasm mtime) |
| uuid / crypto random | `uuid.uuid4()` **traps** | `crypto.getRandomValues` identical across instances and builds |
| build reproducibility | 3 builds → 3 shas (sizes 18,508,365 / 18,513,080 / 18,547,211) | 2 builds → 2 shas (2-byte size difference) |

Implications for decisions and later phases:
- **Step handle: recommend `resource step`.** It works in Python and TS, both toolchains generate idiomatic bindings (Python context manager, TS class), and the u64 fallback stays in the spike WIT for comparison. The formal S2 must still confirm the Rust guest and that a dropped-without-complete resource is observable (`HostStep::drop` is called).
- **Pre-init randomness:** confirmed as "baked or fixed, never reseeded" for both. The SDKs (Phase 9/10) must at activation start: `random.seed(ctx.random_u64())`, and replace `time.time`, `time.time_ns`, `datetime.now`, `uuid.uuid4`, `os.urandom` and `secrets` (Python) and `Math.random`, `crypto.getRandomValues`, `Date.now`, `new Date()` and `performance.now` (JS) with `ctx`-backed versions. Python `hash()`/set order is baked per build. It is deterministic for a pinned module sha, so replay is safe, but it must never feed command arguments without sorting.
- **Python NFR-2.3:** steady-state instantiate is already about 0.4 ms (target < 10 ms) before pooling and CoW. Memory is about 5.5 MB RSS per instance, at the low end of the feared 10–50 MB. Re-measure with pooling + `deserialize_file` and a realistic workflow.
- **Reproducible builds** are an open problem for `ostro build` (CLI-04 / Phase 10). It is not a Phase 1 blocker. Record it in the S2 report.

## Spike S3 Procedure (redb durability tiers)

`spikes/s3-redb-bench` (separate workspace, `redb = "=4.6.0"`, optional `fjall = "=3.1.13"`):
- **Workload:** emulate `apply_batch`.
  - Each batch is B=64 commands. Each command writes 3 rows (runs `[u8;16]→~200 B`, an index row, and an outbox or timer row) and deletes 1 row.
  - Each batch also writes the `applied_offset` key.
  - Run 200k commands after a warm-up. Keys are random 16-byte run ids, which is the realistic B-tree case. A sequential-key variant is optional.
- **Tiers** (each in a fresh db file on the Linux filesystem):
  - T1: every batch commits with `Durability::Immediate` (the baseline, "fsync per batch").
  - T2: every batch commits with `Durability::None`, plus an `Immediate` commit every 100 ms (time-based) or every 256 batches. **This is the planned design.**
  - T3: every commit is `None` (upper bound).
  - T2 again with `set_two_phase_commit(true)`.
  - T2 again with `set_quick_repair(true)`.
- **API** (redb 4.6, verified on docs.rs):
  - `let mut tx = db.begin_write()?; tx.set_durability(Durability::None)?; { let mut t = tx.open_table(DEF)?; t.insert(k, v)?; } tx.commit()?;`
  - `set_durability` returns `Result<(), SetDurabilityError>`.
  - `Durability::None` is "not persisted to disk unless followed by a commit with Durability::Immediate".
- **Report:**
  - commands/s and batches/s
  - commit latency p50/p99/max per tier
  - Immediate-checkpoint latency in T2
  - db file size at the end
  - machine, filesystem and kernel
  - also the equivalent steps/s, assuming about 3 commands per step (state the assumption)
- **Crash sanity (cheap, recommended):** in T2, run as a child process, `kill -9` it mid-run, reopen the database, and assert that it opens (repair if needed) and that the persisted `applied_offset` ≤ the last checkpointed value.
- **Decision rule (proposed):** keep redb if T2 sustains **≥ 3× the NFR-2.5 apply rate**, i.e. ≥ 30k steps/s ≈ ≥ 90k commands/s at 3 commands per step, on the dev machine, and T2's p99 is less than one group-commit window. If it doesn't, run the same workload on fjall (`Database::builder(..).open()`, keyspace inserts, `db.persist(PersistMode::SyncAll)` every 100 ms) and log the comparison. Let the planner/user confirm the thresholds.

## Decisions to Log in PROJECT.md (FOUND-06)

| Decision | Evidence source | Research recommendation |
|---|---|---|
| Step handle: `resource step` vs `u64` | S2 (round trip in py/ts/rust, binding ergonomics, drop semantics) | **resource**. The preliminary py+ts round trip already works |
| State store: redb vs fjall | S3 | **redb 4.6** unless T2 misses the rule above |
| Wasmtime pin policy | Wasmtime stability page: every multiple of 12 is an LTS supported 24 months; normal releases are supported 2 months (current + previous); security fixes are backported to all supported releases; a new major lands on the 20th of each month | **Track the latest major monthly until 1.0, pinned exactly (`=50.0.2`), one bump PR per release with the replay corpus green. Ship 1.0 on 60 LTS (~Aug 2027).** Fallback: freeze on 48 LTS (supported to ~Aug 2028) if monthly bumps cost more than about 1 day each. Bump `rust-toolchain.toml` together with Wasmtime's MSRV |

Log the decisions as rows in the PROJECT.md "Key Decisions" table, with outcome "Decided (Phase 1, S2/S3)" and links to `docs/spikes/*.md`.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| prost-build + system protoc | protox + `compile_fds` | protox 0.10 / prost-build 0.14 | No protoc in CI or on dev machines |
| tonic-build prost path | `tonic-prost-build` | tonic 0.14 | Different crate and API name |
| cargo-deny global `multiple-versions = deny` | per-crate `deny-multiple-versions = true` | cargo-deny ≥0.14 | Targeted bans without skipping half the graph |
| Linux arm64 CI via QEMU/cross | native `ubuntu-24.04-arm` (private repos since 2026-01-29) | 2026-01 | Real aarch64 test runs |
| chrono | jiff (chrono is unmaintained per RUSTSEC-2026-0543) | 2026 | Avoid chrono everywhere |

## Open Questions

1. **Does cargo-zigbuild 0.24.1 + zig 0.15.2 build wasmtime + aws-lc-sys + mimalloc for aarch64 musl?**
   - Known: the research says yes (README claims). It was not verified here because zig was unavailable.
   - Recommendation: the first CI push is the test. Fallback: zig 0.16, or `ring` instead of aws-lc-rs (Pitfall 19).
2. **Does macOS need anything for Cranelift JIT in unsigned test binaries?**
   - Unsigned binaries run JIT fine on GitHub macOS runners. The notarization entitlement (`allow-jit`) only matters for release signing, which is Phase 14.
   - Recommendation: an optional smoke test; don't block on it.
3. **Will GitHub be available?**
   - There is no remote. The user must create the repo and push before FOUND-05 can be fully verified.
   - Recommendation: the plan includes a `checkpoint:human-verify` step for this.
4. **Rust guest WASI imports under wasm32-wasip2** are not yet measured. That is the S2 task.
5. **S3 decision thresholds** are proposed here. The user may want to adjust them.

## Validation Architecture

### Test Framework
| Property | Value |
|---|---|
| Framework | cargo test (built-in) + cargo-nextest (optional, CI) + cargo-deny 0.21.0 + shell checks |
| Config file | none yet. Wave 0 creates `deny.toml`, `rust-toolchain.toml`, `scripts/check-vendored.sh`, `scripts/check-deny-fixtures.sh` |
| Quick run command | `cargo test -p <crate>` (vendored driver tests about 3 s, WAL about 2 s) |
| Full suite command | `cargo build --workspace && cargo test --workspace && cargo deny check && scripts/check-vendored.sh && scripts/check-deny-fixtures.sh` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|---|---|---|---|---|
| FOUND-01 | Workspace builds on 1.100 with 11 crates | build/smoke | `rustc --version \| grep 1.100.0 && cargo build --workspace && cargo metadata --no-deps --format-version 1 \| jq -r '.packages[].name' \| sort` (expect 11 ostro-* + 4 vendor) | ❌ Wave 0 |
| FOUND-01 | Skeleton deps really link (wasmtime Engine, redb txn, tonic codegen, raft single voter) | unit | `cargo test -p ostro-engine -p ostro-partition -p ostro-proto -p ostro-meta -p ostro-log` | ❌ |
| FOUND-02 | Every vendored file listed with commit; no Chronik references | script | `scripts/check-vendored.sh` | ❌ Wave 0 |
| FOUND-03 | Vendored code compiles and its tests pass without chronik-common/monitoring, tokio-uring, `[patch]` | unit + grep | `cargo test -p ostro-raft-rs -p ostro-raft-proto -p ostro-raft-driver -p ostro-wal && ! cargo tree -e normal \| grep -E 'chronik|tokio-uring|prometheus '` | ❌ |
| FOUND-04 | deny passes on the repo and fails on duplicate prost and bad licence | script | `cargo deny check && scripts/check-deny-fixtures.sh` | ❌ Wave 0 |
| FOUND-05 (local part) | x86_64 build/test; YAML valid; optional local musl zigbuild | smoke | `scripts/ci-local.sh` (fmt, clippy, test, deny, vendored check, actionlint via docker if available) | ❌ |
| FOUND-05 (remote part) | aarch64 musl, x86_64 musl and macOS legs green | manual-only (CI) | push to GitHub, then `gh run watch` | needs remote: human_needed |
| FOUND-06 | S2/S3 reports exist with the required metrics; 3 decisions in PROJECT.md | doc check | `test -f docs/spikes/S2-guest-toolchains.md && test -f docs/spikes/S3-redb-durability.md && grep -E 'Step handle\|redb vs fjall\|Wasmtime pin' .planning/PROJECT.md` | ❌ |

### Sampling Rate
- **Per task commit:** `cargo build --workspace && cargo test -p <touched crates>`
- **Per wave merge:** full suite command
- **Phase gate:** full suite green locally, plus the CI matrix green after push (or explicitly recorded as human_needed)

### Wave 0 Gaps
- [ ] `rust-toolchain.toml`, root `Cargo.toml`, `deny.toml`, `LICENSE`, `NOTICE`
- [ ] `scripts/check-vendored.sh` and `scripts/check-deny-fixtures.sh` with `ci/deny-fixtures/{dup-prost,bad-licence}`
- [ ] `scripts/ci-local.sh`
- [ ] Tool install: cargo-deny 0.21.0 (and cargo-zigbuild + zig if a local musl build is attempted; may need user approval)

## Sources

### Primary (HIGH confidence)
- Chronik source @ `08715f4867c9f40e409c68d8e212bd9479d620c0`: crate manifests, imports and `wc -l` of `crates/{chronik-raft,chronik-wal,raft,raft-proto,chronik-raft-bridge}`
- Local prototype (2026-11-18, Rust 1.100.0): vendored crates compiled and tested, and cargo-deny 0.21.0 run, including the negative duplicate test
- Local S2 measurements with componentize-py 0.25.1, jco 1.42.0 and the Wasmtime 50.0.2 crate harness
- componentize-py v0.25.1 `src/stubwasi.rs` (stubs are `unreachable`) and `runtime/src/lib.rs` (reseed skipped when `STUB_WASI`), via the GitHub API
- StarlingMonkey `runtime/engine.cpp`: `js::ResetMathRandomSeed` in `finish_pre_initialization`
- cargo-zigbuild v0.24.1 `src/bin/cargo-zigbuild.rs` (Test subcommand)
- https://embarkstudios.github.io/cargo-deny/checks/bans/cfg.html (`deny-multiple-versions`)
- https://docs.rs/redb/4.6.0/redb/enum.Durability.html and https://docs.rs/redb/4.6.0/redb/struct.WriteTransaction.html
- https://docs.wasmtime.dev/stability-release.html (LTS policy)
- static.rust-lang.org `channel-rust-1.100.0.toml` / `channel-rust-stable.toml` (1.100.0, 2026-11-09)
- crates.io API (2026-11-18): prost 0.14.6, protox 0.10.0, tonic-prost-build 0.14.9, redb 4.6.0, fjall 3.1.13, wasmtime 50.0.2, metrics 0.24.6, cargo-deny 0.21.0, cargo-zigbuild 0.24.1, mimalloc 0.1.52, croner 4.0.3 (optional jiff), jiff 1.0.1
- RustSec advisories as reported by cargo-deny: RUSTSEC-2025-0141, RUSTSEC-2025-0057, RUSTSEC-2026-0543

### Secondary (MEDIUM confidence)
- https://github.blog/changelog/2026-01-29-arm64-standard-runners-are-now-available-in-private-repositories (arm64 private runners, 2 vCPU)
- https://github.com/bytecodealliance/ComponentizeJS README (`disableFeatures` semantics: "random numbers … fully deterministic")
- https://bugzilla.mozilla.org/show_bug.cgi?id=1909217 (SpiderMonkey reseed API)
- https://github.com/rust-cross/cargo-zigbuild README (`pip3 install ziglang`, musl guidance)

### Tertiary (LOW confidence)
- Zig 0.15.2 vs 0.17 compatibility with cargo-zigbuild 0.24.1 (unverified)
- Exact GitHub action major versions (`actions/checkout@v5`, `mlugg/setup-zig@v2`, `taiki-e/install-action@v2`, `EmbarkStudios/cargo-deny-action@v2`). Verify and pin SHAs when writing the workflow

## Metadata

**Confidence breakdown:**
- Vendoring manifest and modernisation: HIGH. Prototyped end to end on 1.100.0 with tests green.
- cargo-deny config: HIGH. Run locally, including the negative test.
- CI matrix: MEDIUM. The design is sound, but the zig, aarch64 and macOS legs are unverified locally.
- S2 behaviour findings: HIGH (measured plus source-confirmed). The S2 performance numbers are MEDIUM/preliminary (n=1, on-demand allocator, in-memory deserialize).
- S3: procedure only. The numbers are for the phase to produce.

**Research date:** 2026-11-18
**Valid until:** 2026-12-18. Wasmtime releases monthly (51 on 2026-12-20), so re-check the pins then.
