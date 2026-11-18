# Stack Research

**Domain:** Single-binary, polyglot Wasm durable-execution engine in Rust (Ostro)
**Researched:** 2026-11-18
**Confidence:** HIGH for host crates and versions (checked against crates.io, PyPI, npm, GitHub releases and Wasmtime release notes on the research date). MEDIUM for guest toolchain behaviour (measured locally, but only with hello-world builds).

> Method: every version below comes from the crates.io, PyPI and npm registry APIs or GitHub releases, queried on 2026-11-18. Guest toolchain sizes and timings were **measured locally** with componentize-py 0.25.1, jco 1.42.0 and the Wasmtime 50.0.2 CLI (x86_64 Linux). Training data was used only to know where to look.

---

## TL;DR (prescriptive)

1. **Wasmtime 50.0.x now; plan to ship 1.0 on the 60.0 LTS (Aug 2027).** Use the Component Model with async host functions running on fibers. Don't make the `ostro:runtime` WIT depend on Component-Model-async (`async func`, `stream`, `future`) in v1, because the Python and JS guest toolchains don't fully support it yet.
2. **Guests target the WASI 0.2 / sync canonical ABI with no WASI imports.** The guest build flags are `--stub-wasi` for componentize-py, `--disable all` for jco, and wit-bindgen plus `wasm32-wasip2` for Rust. Ostro's own WIT supplies time, random and I/O. This is determinism by construction.
3. **Rust guests use `cargo build --target wasm32-wasip2` with `wit-bindgen` 0.63. Do not use `cargo-component`**: it is stale, with no commits since 2025-07, and its README says it is experimental.
4. **TypeScript uses jco 1.42 / componentize-js 0.23 (StarlingMonkey).** jco now bundles TypeScript itself (Rolldown), so **esbuild is no longer needed** in the build pipeline.
5. **Python uses componentize-py 0.25.1**, run hermetically with `uv tool run`.
6. **Storage is redb 4.6.** Apply state with `Durability::None` and make a `Durability::Immediate` checkpoint of `applied_offset`, because the journal is the source of truth. **Hold off on redb 5.0** (unreleased, breaking encoding change).
7. **API is tokio 1.54, axum 0.8 and tonic 0.14 / prost 0.14 on one port** via `tonic::service::Routes::into_axum_router()`. Use rustls 0.23 with **aws-lc-rs** as the only crypto provider.
8. **Chronik conflict (the main finding):** Chronik pins tonic 0.11 / prost 0.12 / axum 0.6 / hyper 0.14 / opentelemetry 0.22 / prometheus 0.13. Also, **Chronik's `[patch.crates-io]` for the vendored `raft` / `raft-proto` does NOT apply when Ostro uses chronik-raft as a git dependency.** Fix this inside Chronik during the "chronik-core" extraction, before Ostro depends on it (details in "Version Compatibility").
9. **Static binaries:** build `x86_64` / `aarch64-unknown-linux-musl` with cargo-zigbuild 0.24 plus mimalloc. Build macOS on native GitHub macOS runners. Pin the toolchain to Rust **1.100.0** (Wasmtime 50 needs at least 1.97).

---

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended | Confidence |
|---|---|---|---|---|
| Rust toolchain | **1.100.0** stable (released 2026-11, per static.rust-lang.org channel file); edition 2024 for Ostro crates | Everything | Wasmtime 50 has MSRV **1.97.0**, and its MSRV moves up roughly every other release. Pin with `rust-toolchain.toml` and bump together with Wasmtime. Chronik crates stay on edition 2021, and mixing editions is fine. Note: the local dev machine has rustc 1.94.1, which is **too old for Wasmtime 50**. | HIGH |
| `wasmtime` | **50.0.2** (2026-11-16) | Component host, Cranelift JIT/AOT, pooling allocator, fuel, epochs | The reference Component Model runtime. Monthly majors, released on the 20th. **Every multiple of 12 is an LTS supported for 24 months**: 36 LTS runs to about Aug 2027, 48 LTS (Aug 2026) to about Aug 2028, and **60 LTS comes about Aug 2027**. Track the latest major monthly before 1.0 (the API still churns, e.g. epoch callbacks changed in 50). Ship 1.0 on an LTS so you get security backports; 50.0.2 alone fixed 8 GHSAs. | HIGH |
| `wasmtime-wasi` | **50.0.2** (features `p2` only; default is `p1,p2,p3`) | Only if any `wasi:*` imports are linked at all | Ostro guests are built without WASI imports (see Patterns). Keep `wasmtime-wasi` out of the hot path, or use it with `default-features = false, features = ["p2"]` behind custom clock/random implementations, to tolerate stray imports. | MEDIUM |
| `tokio` | **1.54.1** | Async runtime | Required by Wasmtime async, tonic, axum and Chronik. Chronik requires `1.35` or later, which semver-unifies with 1.54 with no conflict. | HIGH |
| `redb` | **4.6.0** (2026-11-13), MSRV 1.90 | Per-partition materialized state (runs, timers, inbox, outbox, `applied_offset`) | Pure Rust, and its only normal dependency is `libc` (verified on crates.io), so it is trivially static. ACID copy-on-write B-tree with MVCC readers. `Durability` is now just `{None, Immediate}`. | HIGH (choice), MEDIUM (maturity, see Pitfalls) |
| `chronik-wal` | git, pinned rev (Chronik 2.18.0, `08715f4`) | Partition journal: segments, group commit, truncation | Already decided (docs/04). Needs dependency surgery first (see Compatibility). | HIGH |
| `chronik-raft` + vendored `raft` / `raft-proto` 0.7 (tikv) | git, pinned rev | Metadata Raft | Already decided. Upstream tikv/raft-rs master **still** uses `prost 0.11` / `protobuf 2` (verified in its Cargo.toml), so keeping the vendored crates is justified. | HIGH |
| `tonic` + `tonic-prost` | **0.14.9** | gRPC control plane, worker Execute stream, Raft transport | Current line. In 0.14 the prost codec moved out into the separate `tonic-prost` / `tonic-prost-build` crates. Depends on hyper 1 / axum 0.8 / tower 0.5. | HIGH |
| `prost` | **0.14.6** | Protobuf messages | Matches tonic 0.14 and opentelemetry-otlp 0.33. | HIGH |
| `axum` | **0.8.9** (features `http2`, `json`, `tokio`) | HTTP/JSON API, webhooks, UI assets, `/metrics` | Same hyper 1 / tower 0.5 stack as tonic 0.14, so one router can serve both protocols. | HIGH |
| `hyper` / `hyper-util` | **1.12.0** / **0.1.21** (`server-auto`, `server-graceful`, `tokio`) | Listener with HTTP/1.1 + h2 + h2c autodetection, TLS via ALPN | `hyper_util::server::conn::auto::Builder` is the standard way to serve HTTP/1 and HTTP/2 (gRPC) on one socket. | HIGH |
| `rustls` / `tokio-rustls` | **0.23.46** / **0.26.6** | TLS / mTLS | No OpenSSL. Default provider is **aws-lc-rs 1.19**. Its non-FIPS build needs only a C compiler: pre-generated bindings, no CMake/Go (verified in aws-lc-rs docs). reqwest 0.13, object_store `aws` and the metrics push-gateway all default to aws-lc-rs too, so standardising on it avoids compiling both `ring` and aws-lc. | HIGH |
| `reqwest` | **0.13.5** (`default-features=false`, features `rustls`, `http2`, `json`, `stream`) | I/O bridge HTTP effect client | 0.13's `default-tls` is rustls + aws-lc-rs + platform verifier. Leave out `system-proxy` unless needed, and handle the SSRF guard with your own connector / DNS resolver hook. | HIGH |
| WIT tooling: `wit-bindgen` (guest) / `wasmtime::component::bindgen!` (host) | **0.63.0** / bundled with wasmtime 50 | Typed bindings for `ostro:runtime@0.1.0` | Standard. `bindgen!` takes `imports: { default: async \| store \| tracing, ... }`, so host imports are async and see the store. | HIGH |

### Guest toolchains (bundled or hermetically fetched by `ostro build`)

| Toolchain | Version | Measured output (hello world, x86_64) | Notes | Confidence |
|---|---|---|---|---|
| Rust: `wasm32-wasip2` target + `wit-bindgen` | Rust ≥1.82 (pin 1.100), wit-bindgen **0.63.0** | not measured (expected 0.1–2 MB) | `wasm32-wasip2` is **Tier 2 on stable** and emits a component directly through `wasm-component-ld`. `wasm32-wasip3` is **Tier 3, nightly only**: don't use it. Use `wit-bindgen` macros in the SDK crate. | HIGH |
| ~~cargo-component~~ | 0.21.1 (2025-03), last commit 2025-07-14 | n/a | **Don't use.** Stale and self-described experimental. Plain `cargo build --target wasm32-wasip2` covers everything Ostro needs. | HIGH |
| Python: `componentize-py` | **0.25.1** (PyPI, 2026-09-12; wheels 15–17 MB, abi3 for linux x86_64/aarch64, macOS, Windows) | **18.4 MB** `.wasm` (6.2 MB gzipped) with `--stub-wasi`. AOT `.cwasm` **32.6 MB**. `wasmtime compile` took 0.9 s with **~460 MB peak RSS**. Whole-process `wasmtime run --invoke` took **~10 ms, 24 MB max RSS** | Pre-initialises the interpreter at build time (component-init). **Resources: supported.** **WASI 0.3 / CM-async: supported in 0.25** (`cli-p3`, `http-p3` examples; `componentize_py_async_support` bundles futures/streams), although wasi.dev still lists componentize-py as 0.2-only. **API churn warning:** the `main` README shows a new `@wit.guest class X(wit.WorldExports)` binding style that **fails on 0.25.1** (which uses `import wit_world; class WitWorld(wit_world.WitWorld)`). Pin exactly, and keep the Ostro Python SDK as the only layer that touches generated bindings. Pure-Python dependencies only. | HIGH (version, size), MEDIUM (p3 maturity) |
| TypeScript/JS: `@bytecodealliance/jco` + `@bytecodealliance/componentize-js` (StarlingMonkey) | jco **1.42.0** (2026-11-13), componentize-js **0.23.0**, StarlingMonkey **v0.5.1** | **12.0 MB** `.wasm` (3.9 MB gzipped) with `--disable all`, which produces **zero WASI imports** (verified with `jco wit`). AOT `.cwasm` **33.5 MB**. Compile took 1.1 s with ~530 MB peak RSS. A `.ts` input built directly with no separate bundler | jco 1.42 bundles TS/JS itself with **Rolldown** (`--bundle`, automatic for TS), so **drop esbuild**. Wizer pre-init is built in, and `--aot` adds Weval AOT. New options: `--backend quickjs` produced **1.47 MB** (4.1 MB `.cwasm`), and `--script-only` produces a shared ~12 MB `engine.wasm` plus a **~1.5 KB per-script component** loaded at runtime through `jco:script-engine/script-loader`. That is a real option for Ostro's module store (one engine per Ostro release, user modules in KBs), at the cost of losing build-time pre-init of user code. node_modules footprint is **358 MB**. | HIGH (version, size), MEDIUM (script-only, QuickJS maturity) |
| Javy | v9.1.0 (2026-07) | n/a | **Don't use.** It targets core Wasm modules with a stdin/stdout I/O model, not WIT-typed components. For a small JS engine, use jco's `--backend quickjs` instead, which is component-native. | MEDIUM |

### Supporting Libraries

| Library | Version | Purpose | When to Use | Confidence |
|---|---|---|---|---|
| `tower` / `tower-http` | **0.5.3** / **0.7.1** | Middleware (auth, limits, tracing, CORS, compression) | Always. Note tower-http **0.7** vs axum's optional 0.6 dependency: you can use 0.7 directly, because axum only depends on tower-http for optional features. | HIGH |
| `tonic-prost-build` + `protox` | **0.14.9** + **0.10.0** | Codegen for Ostro protos **without a system `protoc`** | `protox` compiles `.proto` to a FileDescriptorSet in pure Rust, and `tonic-prost-build` generates code from it. This removes the `protoc` requirement that Chronik's `raft-proto` build currently has (its Cross.toml installs `protobuf-compiler`). Fallback: `protoc-bin-vendored` 3.3.0. | MEDIUM |
| `opentelemetry` / `opentelemetry_sdk` / `opentelemetry-otlp` | **0.33.1** (all three, same version) | OTLP traces (and optionally metrics/logs) | Use `opentelemetry-otlp` with `default-features=false, features=["grpc-tonic","trace","tls-aws-lc"]`: it reuses your tonic 0.14 / prost 0.14, with no extra HTTP client. **Status (upstream README):** Logs and Metrics API/SDK/OTLP are **Stable**, Traces API/SDK/OTLP are **Beta**, and the Prometheus exporter is **Beta**. Expect minor-version churn, so bump all four crates together. | HIGH |
| `tracing` / `tracing-subscriber` / `tracing-opentelemetry` | **0.1.44** / **0.3.23** / **0.34.0** (needs opentelemetry 0.33) | Structured logging, spans, OTel bridge | Always. Chronik's raft/wal crates log through `tracing` 0.1, so their spans flow into Ostro's subscriber for free. | HIGH |
| `metrics` + `metrics-exporter-prometheus` | **0.24.6** + **0.18.3** (`default-features=false`) | Prometheus `/metrics` and the KEDA demand metric | **Preferred over the `prometheus` crate.** Library crates (including a refactored chronik-wal) depend only on the `metrics` facade. The binary installs the exporter and serves `PrometheusHandle::render()` from axum, so no exporter HTTP server is needed. Turn off the default `http-listener` and `push-gateway` features, which would pull hyper-rustls with its own TLS setup. | MEDIUM |
| `prometheus-client` | **0.26.0** | Alternative OpenMetrics client (official Prometheus org) | Only if you want explicit registries with no global facade. | MEDIUM |
| `object_store` | **0.14.2** (features `aws`, `gcp`, `azure`, `tokio`) | Bottomless segments/snapshots, object-store effect | Apache Arrow's crate: small, async, reqwest 0.13 + rustls/aws-lc. Chronik uses **opendal 0.45** (2024-era; current is 0.59.5, which split into ~90 sub-crates). Re-base the extracted `chronik-object-store` on `object_store` rather than carrying opendal 0.45. | MEDIUM |
| `sqlx` | **0.9.1** (released 2026-11-18; features `runtime-tokio`, `tls-rustls-aws-lc-rs`, `postgres`, `mysql`) | SQL effect connector (transactional idempotency token) | Pure Rust drivers for Postgres and MySQL. **0.9.1 is one day old**: pin it, and fall back to 0.8.x or `tokio-postgres` 0.7.18 if regressions show up. Don't enable the `sqlite` features (they bundle C SQLite and aren't needed). | MEDIUM |
| `mimalloc` | **0.1.52** | Global allocator | **Required for musl builds**: musl's malloc is slow under multithreaded allocation. It is C, built by `cc`, statically linked, and works under zig cc. Alternative: `tikv-jemallocator` 0.7.0. | MEDIUM |
| `clap` | **4.6.8** | CLI subcommands | Always. | HIGH |
| `rand_chacha` | **0.10.0** | Deterministic guest RNG seeded from the run id | Implements Ostro's virtual `random`. Pin it: its stream output **must never change** across Ostro versions or replay breaks. Consider vendoring or copying the ChaCha20 core so a crate bump can't silently change the stream. | MEDIUM |
| `postcard` | **1.1.3** | Compact serde encoding for journal/state values | Use for **new** Ostro on-disk formats. Don't use bincode: the bincode GitHub repo is **archived** (verified). Chronik internals can keep bincode 1.3 for now. Alternatively encode everything with prost for cross-language tooling. | MEDIUM |
| `croner` | **4.0.3** | Cron expression parsing (with seconds and timezones) | Cron triggers. Ostro owns the timer wheel, so you only need a parser, not a scheduler (don't use `tokio-cron-scheduler`). | MEDIUM |
| `hickory-resolver` | **0.26.3** | Custom DNS resolution for the egress SSRF guard | Resolve, check against allow-lists and private ranges with `ipnet` 2.12, then connect to the checked IP. This avoids DNS-rebinding time-of-check/time-of-use gaps. | MEDIUM |
| `rust-embed` | **8.14.0** | Embed web UI assets in the binary | Embedded UI. | HIGH |
| `zeroize` | **1.9.2** | Secret wiping in the bridge | Secrets handling. | HIGH |
| `blake3` / `sha2` | **1.8.7** / **0.11.1** | Content addressing (module sha256), AOT cache HMAC keying | Use sha256 for module identity (user-visible, interoperable). blake3 is fine internally. | HIGH |
| `wasmparser` / `wit-component` | must match wasmtime's: **0.261.x** for Wasmtime 50 (latest standalone is 0.263) | Inspecting uploaded components (imports/world check) | Prefer `wasmtime::component::Component::component_type()` for import/export validation. If you need wasm-tools crates directly, pin them to the version Wasmtime uses, to avoid two wasmparser copies. | HIGH |
| `openraft` | 0.10.0 (2026-11-10) | **Not used.** Noted alternative. | Only if the chronik-raft reuse fails. openraft is async-native, actively maintained and storage-agnostic, but adopting it means rewriting the driver and log store that Chronik already debugged. | MEDIUM |

### Development Tools

| Tool | Version | Purpose | Notes |
|---|---|---|---|
| `wasm-tools` | **1.263.0** | Inspect, validate, strip and print WIT of components; `component new` | CI checks on SDK outputs (e.g. assert there are no `wasi:*` imports). Its wasmparser can be newer than Wasmtime's; that's fine for a CLI. |
| `wac-cli` | **0.12.0** | Compose components (connector components into user modules) | Phase 3+ connector composition. Wasmtime 50 can also link component-to-component in the host. |
| binaryen `wasm-opt` | **version_133** (GitHub release) | Optional size/speed optimisation of guest cores | jco already depends on the `binaryen` npm package. **Don't use the `wasm-opt` Rust crate** (0.116.1, last updated 2024-03). Run `wasm-opt` on Rust guest *core* modules only if size matters. |
| `wasmtime` CLI | **50.0.2** | Ad-hoc testing (`wasmtime compile`, `run --invoke`) | The precompiled tarball works; it was used for the measurements above. |
| `uv` | **0.13.9** (2026-11-17) | Hermetic Python toolchain for `ostro build` (`uv tool run --from componentize-py==0.25.1 componentize-py ...`) and building/publishing the Python SDK (`uv build`, `uv publish`, `uv_build` backend) | uv downloads a managed CPython, so users don't need a system Python. componentize-py itself just needs Python ≥3.10 to run. |
| Node.js | **24.x LTS** (tested 24.13) | Runs jco/componentize-js for `ostro build` | Hermetic option: `npx -y @bytecodealliance/jco@1.42.0 componentize ...` with a pinned lockfile in a cache dir. |
| `cargo-zigbuild` | **0.24.1** (with Zig 0.15/0.16) | musl static cross-builds for x86_64 + aarch64 from one Linux host | Preferred over `cross` (last release v0.2.5, 2023-02; Chronik uses its `:main` images). zigbuild needs no Docker, handles aws-lc-sys, mimalloc and Wasmtime's fiber asm through zig cc, and supports `universal2-apple-darwin` when a macOS SDK is present. |
| `cargo-dist` | **0.34.0** | Release artefacts, installers (`curl \| sh`, Homebrew), checksums | Fits the "download one binary" story. |
| `cargo-nextest` | **0.9.152** | Test runner (crash-injection and conformance suites) | Process-per-test isolation suits fsync and crash tests. |
| `cargo-deny` | **0.21.0** | License (Apache-2.0 compat), advisories, duplicate-version bans | **Use `bans.multiple-versions = "deny"` for `tonic`, `prost`, `hyper`, `axum`, `http`, `opentelemetry`** so the Chronik conflicts below can't silently come back. |

---

## Installation

```toml
# ostro/Cargo.toml (workspace) — versions verified 2026-11-18
[workspace.package]
edition = "2024"
rust-version = "1.97"          # wasmtime 50 MSRV; toolchain pinned to 1.100.0

[workspace.dependencies]
wasmtime        = { version = "50.0.2", default-features = false, features = [
  "runtime", "cranelift", "component-model", "async", "pooling-allocator",
  "cache", "parallel-compilation", "std", "addr2line", "demangle", "coredump",
  # "component-model-async"  # only if/when ostro:runtime adopts async func (not v1)
] }                            # disables default gc/threads/stack-switching/wat we don't need
wasmtime-wasi   = { version = "50.0.2", default-features = false, features = ["p2"] }  # optional
tokio           = { version = "1.54", features = ["full"] }
axum            = { version = "0.8.9", features = ["http2", "json", "macros"] }
hyper           = { version = "1.12", features = ["server", "http1", "http2"] }
hyper-util      = { version = "0.1.21", features = ["server-auto", "server-graceful", "tokio"] }
tower           = "0.5.3"
tower-http      = { version = "0.7.1", features = ["trace", "cors", "compression-gzip", "limit"] }
tonic           = { version = "0.14.9", features = ["router", "transport", "tls-aws-lc"] }
tonic-prost     = "0.14.9"
prost           = "0.14.6"
rustls          = { version = "0.23.46", default-features = false, features = ["aws_lc_rs", "std", "tls12", "logging"] }
tokio-rustls    = { version = "0.26.6", default-features = false, features = ["aws_lc_rs"] }
reqwest         = { version = "0.13.5", default-features = false, features = ["rustls", "http2", "json", "stream"] }
redb            = "4.6.0"
object_store    = { version = "0.14.2", features = ["aws", "gcp", "azure", "tokio"] }
sqlx            = { version = "0.9.1", default-features = false, features = ["runtime-tokio", "tls-rustls-aws-lc-rs", "postgres", "mysql", "macros", "json"] }
opentelemetry   = "0.33.1"
opentelemetry_sdk = { version = "0.33.1", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.33.1", default-features = false, features = ["grpc-tonic", "trace", "tls-aws-lc"] }
tracing         = "0.1.44"
tracing-subscriber = { version = "0.3.23", features = ["env-filter", "json"] }
tracing-opentelemetry = "0.34.0"
metrics         = "0.24.6"
metrics-exporter-prometheus = { version = "0.18.3", default-features = false }
clap            = { version = "4.6.8", features = ["derive", "env"] }
mimalloc        = "0.1.52"
postcard        = { version = "1.1.3", features = ["use-std"] }
rand_chacha     = "0.10.0"
zeroize         = "1.9.2"
croner          = "4.0.3"
hickory-resolver = "0.26.3"
rust-embed      = "8.14.0"

# Chronik, after the chronik-core surgery described below (pin a rev, never a branch)
chronik-wal  = { git = "https://github.com/lspecian/chronik-stream", rev = "<rev>", default-features = false }
chronik-raft = { git = "https://github.com/lspecian/chronik-stream", rev = "<rev>" }

[workspace.dependencies.tonic-prost-build]
version = "0.14.9"
# build-deps: protox = "0.10.0"

[profile.release]
lto = "fat"
codegen-units = 1
panic = "abort"      # see pitfall note: keep "unwind" if you rely on catch_unwind around guest calls
strip = "debuginfo"
```

```bash
# Toolchain
rustup toolchain install 1.100.0 --target x86_64-unknown-linux-musl aarch64-unknown-linux-musl wasm32-wasip2
cargo install --locked cargo-zigbuild@0.24.1 cargo-nextest cargo-deny@0.21.0 wasm-tools@1.263.0 wac-cli@0.12.0

# Static Linux binaries
cargo zigbuild --release --target x86_64-unknown-linux-musl
cargo zigbuild --release --target aarch64-unknown-linux-musl

# macOS (on a GitHub macOS arm64 runner; Xcode SDK covers both arches)
MACOSX_DEPLOYMENT_TARGET=13.0 cargo build --release --target aarch64-apple-darwin
MACOSX_DEPLOYMENT_TARGET=13.0 cargo build --release --target x86_64-apple-darwin   # optional; or universal2 via lipo

# Guest toolchains (what `ostro build` runs hermetically)
uv tool run --from componentize-py==0.25.1 componentize-py -d wit -w ostro-workflow componentize --stub-wasi app -o app.wasm
npx -y @bytecodealliance/jco@1.42.0 componentize src/index.ts --wit wit --world-name ostro-workflow --disable all -o app.wasm
cargo build --release --target wasm32-wasip2      # Rust SDK, wit-bindgen 0.63
```

---

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|---|---|---|
| redb 4.6 | **fjall 3.1.13** (pure-Rust LSM; deps include lsm-tree 3.1, lz4_flex. No C) | If state-apply write throughput becomes the bottleneck (LSM beats CoW B-tree on random writes), or you need compression. fjall keeps its own journal, which duplicates the chronik-wal journal, so run it with relaxed persistence and checkpoint `applied_offset` the same way. Keep it behind the `StateStore` trait as already designed. |
| Wasmtime 50.x, tracking monthly to 60 LTS | Start on **48 LTS** | If you want security backports without monthly API churn right now. 48 already has WASI 0.3 and CM-async on by default (both since v46), but lacks 50's `Engine::release_idle_pool_memory` and CM-async perf work. Reasonable to freeze on 48 for a pre-release if the monthly bumps hurt. |
| StarlingMonkey (jco default backend) | jco `--backend quickjs` (componentize-qjs) | Much smaller (1.47 MB vs 12 MB) and faster to compile. Use it if JS fan-out density matters more than performance and spec completeness. Evaluate it in the TS SDK phase; don't commit up front. |
| jco per-module components | jco `--script-only` + shared engine component | Ostro stores one engine per release and user modules are ~KBs, which gives AOT reuse across modules. Costs: user code isn't pre-initialised, and the host must instantiate the engine and `load()` the script on each activation. Worth a spike. |
| `metrics` facade + exporter | `prometheus` crate 0.14.0 | Don't. Its last release was 2025-03, it uses a global registry, and its default `protobuf` feature pulls in the protobuf crate. Chronik uses 0.13, which pulls protobuf 2.x. |
| `object_store` 0.14 | `opendal` 0.59.5 | If you need exotic backends (HDFS, WebDAV, etc.). Ostro needs S3, GCS, Azure and local only. |
| chronik-raft (vendored raft-rs 0.7) | `openraft` 0.10.0 | If extracting chronik-raft turns out costlier than adopting openraft (it's async and actively maintained). Not expected. |
| aws-lc-rs crypto provider | `ring` 0.17.14 | If aws-lc-sys cross-compilation becomes a problem on some target. ring is simpler to build, but then every dependency must use `*-ring` / `rustls-no-provider` features, which goes against reqwest 0.13's and object_store's defaults. |
| hyper-util auto builder + one router | `tonic::transport::Server::accept_http1(true)` + gRPC-web | Only if you want tonic to own the server. One axum router (REST + `Routes::into_axum_router()`) served by hyper-util auto is simpler and keeps TLS in one place. |

---

## What NOT to Use

| Avoid | Why | Use Instead |
|---|---|---|
| `Config::async_support(true)` (as in docs/01 §3.3) | **Removed in Wasmtime 42.** Async is simply available when the `async` feature is on. docs/01 is out of date here. | Nothing to configure. Use `call_async` / `instantiate_async`, and async imports in `bindgen!` |
| `ostro:runtime` WIT built on CM-async (`async func`, `stream<T>`, `future<T>`) in v1 | WASI 0.3 is **stable (0.3.0 ratified 2026-06-11, 0.3.1 2026-08-06)** and Wasmtime ≥46 runs it by default. But Rust `wasm32-wasip3` is Tier 3/nightly, jco's 0.3 support is "experimental", and componentize-js/StarlingMonkey async export support isn't documented. | Sync WIT functions with poll-style op handles (as in docs/03 §6). The host side is still fully async on fibers. Revisit for WIT 0.2 once all three guest toolchains support CM-async. |
| Linking `wasi:http`, `wasi:sockets`, `wasi:filesystem` | Breaks determinism, egress control and idempotency (already decided) | `--stub-wasi` / `--disable all` builds, plus Ostro effects |
| `cargo-component` | Stale (last commit 2025-07), self-described experimental, needs OpenSSL plus a C toolchain | `cargo build --target wasm32-wasip2` with `wit-bindgen` 0.63 |
| esbuild in the TS build path | jco 1.42 bundles with Rolldown itself and compiles `.ts` directly (verified) | `jco componentize --bundle` (automatic for TS) |
| Javy | Produces core modules with a stdin/stdout model, not WIT components | jco (StarlingMonkey or QuickJS backend) |
| `cross` (cross-rs) | Last release 2023, needs Docker images, Chronik needed protoc hacks | `cargo-zigbuild` |
| OpenSSL / `native-tls` / `openssl = { features=["vendored"] }` (in Chronik's workspace deps) | C dependency, harder static builds, two TLS stacks | rustls + aws-lc-rs everywhere |
| RocksDB, SQLite-bundled, `rdkafka` (`cmake-build`) | C/C++ builds break the single-static-binary moat (D1). Chronik's workspace declares rdkafka (tests only) | redb / fjall. Keep Chronik test-only C deps out of Ostro's graph |
| redb **5.0** (unreleased, "2026-XX-XX" in the changelog) | Breaking: `Option<&str/String/bool/char>` encoding changes, and **custom `Value` impls can silently misread old tables** | Stay on 4.x. When moving to 5.0, avoid those `Option` types in persisted schemas, or migrate deliberately |
| bincode for new formats | Upstream repo archived | postcard or prost |
| tokio-cron-scheduler | Ostro already owns timers (timer wheel rebuilt from state) | `croner` for parsing only |
| Wasmtime default features as-is | Pulls in `gc`, `threads`, `stack-switching`, `wat`, `component-model-async`, `wit-parser`, `debug`, … The 50.0.2 GHSAs were largely in GC paths, and less code means less attack surface for untrusted guests | `default-features = false` plus the explicit list above. Also set `wasm_threads(false)` and leave GC off |

---

## Stack Patterns by Variant

**Wasmtime engine configuration (Wasmtime 50 API names, verified on docs.rs):**
- `Config::consume_fuel(true)` for deterministic budgets, plus `epoch_interruption(true)` with a ticker thread calling `Engine::increment_epoch()` for wall-clock slicing. Note: epoch-deadline callbacks now receive `StoreHookState`, not `StoreContextMut` (changed in 50).
- `allocation_strategy(InstanceAllocationStrategy::Pooling(PoolingAllocationConfig::default()...))` with `memory_init_cow(true)` (default). Call `Engine::release_idle_pool_memory()` (new in 50) under memory pressure.
- `signals_based_traps(true)`, the default. Disabling it costs up to 2x according to the docs. On macOS, `macos_use_mach_ports` defaults to true. Keep it.
- `cranelift_nan_canonicalization(true)`, `relaxed_simd_deterministic(true)` (default false!), `wasm_threads(false)`.
- `async_stack_size`: default 2 MiB. Python and JS guests may need it raised. Measure.
- AOT: `Engine::precompile_component` at deploy time, store the `.cwasm` keyed by `(sha256, wasmtime version, target, config hash)`, and load it with `unsafe Component::deserialize_file` (mmap). **Compile memory is significant: about 460–530 MB peak RSS for an 18 MB Python or 12 MB JS component** (measured with the CLI and parallel compilation). On small nodes, compile in a bounded queue with `parallel_compilation(false)`, or compile on `ostro deploy` on the client side and upload the artefact (HMAC-protected).
- Ship one shared engine per Ostro release for each language runtime where possible: AOT artefacts are about 33 MB each for Python and JS.

**Guest determinism by construction (verified flags):**
- Python: `componentize-py ... componentize --stub-wasi`. JS: `jco componentize --disable all` (verified: the output component has no imports). Rust: don't call `std` time/random APIs (lint in the SDK). If a WASI import does slip through, the host refuses the component at deploy time, using the import list from `Component::component_type()`.
- Research flag: confirm what CPython's hash seed, `time`, and `random` do under `--stub-wasi` (stubs trap or return zero?) and route them through Ostro's context API.

**One port for HTTP/1.1 + HTTP/2 + gRPC:**
```rust
let grpc = tonic::service::Routes::new(control_plane_svc)
    .add_service(worker_execute_svc)
    .prepare()
    .into_axum_router();                 // tonic 0.14, feature "router"
let app = axum::Router::new()
    .nest("/v1", rest_api())
    .merge(grpc)                         // gRPC paths are /package.Service/Method
    .fallback_service(ui_assets());
// Plaintext: axum::serve(listener, app) handles HTTP/1 + h2c when axum "http2" is on.
// TLS: accept with tokio-rustls (ALPN ["h2","http/1.1"]), then
// hyper_util::server::conn::auto::Builder::new(TokioExecutor::new()).serve_connection(io, svc)
```
The internal cluster port (Raft and replication) is a second listener using the same stack.

**If Python per-instance memory turns out to be too high (10–50 MB expected under real workloads):**
- Prefer evict-and-replay over hot-wait for Python activations, and cap concurrent Python instances per node from a resident-memory budget.

---

## Version Compatibility

### Chronik vs Ostro dependency conflicts (critical)

Measured from `chronik-stream` `main` @ `08715f4` (2026-11-18), workspace `Cargo.toml` and crate manifests:

| Chronik pins | Pulled into Ostro via | Ostro wants | Effect if left alone | Resolution |
|---|---|---|---|---|
| `tonic 0.11`, `prost 0.12`, `prost-types 0.12`, `tonic-build 0.11` | `chronik-raft` (transport), `chronik-raft-bridge`, vendored `raft`/`raft-proto` (prost 0.12, `prost-build 0.12` → needs system `protoc`) | tonic/prost **0.14** | It compiles (Cargo allows semver-incompatible duplicates), but you get **two gRPC stacks** (tonic 0.11 runs on hyper 0.14 / http 0.2), so the Raft gRPC service **cannot be mounted on Ostro's tonic 0.14 server or internal port**, and binary size grows. | **In Chronik:** upgrade the workspace to tonic 0.14 + tonic-prost, prost 0.14, regenerate vendored `raft-proto` with prost 0.14 (build with `protox`, not `protoc`). Day-1 single-member Raft can use the in-memory transport, so this blocks only Phase 4, but do it during extraction. |
| `[patch.crates-io] raft/raft-proto = { path = "crates/raft*" }` | Chronik root manifest | n/a | **Cargo applies `[patch]` only from the root workspace being built.** As a git dependency, Chronik's patch is ignored: `raft = "0.7"` resolves to crates.io raft 0.7.0 (prost 0.11 / protobuf 2) and `chronik-raft-bridge` fails to compile. | **Best:** rename the vendored crates (e.g. `chronik-raftrs`, `chronik-raftrs-proto`) and depend on them by path inside Chronik, so no patch is needed. **Stopgap:** in Ostro's root `Cargo.toml` add `[patch.crates-io] raft = { git = "…chronik-stream", rev = "<rev>" }` and the same for `raft-proto`. |
| `axum 0.6`, `hyper 0.14`, `tower 0.4`, `tower-http 0.4`, `opentelemetry 0.22`, `opentelemetry-otlp 0.15`, `opentelemetry_sdk 0.22`, `tracing-opentelemetry 0.23` | `chronik-monitoring`, a dependency of **both** `chronik-raft` and `chronik-wal` | axum 0.8 / hyper 1 / OTel 0.33 | Duplicate HTTP and OTel stacks. The OTel 0.22 global is separate from OTel 0.33, so anything Chronik exports through it bypasses Ostro's pipeline. Slower builds, bigger binary. | `chronik-raft` declares `chronik-monitoring` and `chronik-common` but **doesn't use them** (grep: no `use chronik_*` in `chronik-raft/src`), so delete the dependencies. `chronik-wal` uses only `chronik_monitoring::MetricsRecorder` (in `group_commit.rs`) and something from `chronik_common` (in `fsync.rs`): replace with the `metrics` facade or a small trait. |
| `prometheus 0.13` | `chronik-common` (via chronik-wal) | `metrics` facade | Second metrics registry. protobuf 2.x through prometheus's default feature. | Same as above: drop `chronik-common` from the WAL path, or gate its metrics behind a feature. |
| `tokio-uring 0.4` (non-optional on Linux; pulls `io-uring 0.5`, `socket2 0.4`) | `chronik-wal` | Optional | Pure Rust, so it's static-OK, but it's old, and io_uring is often blocked in containers (seccomp). Code already defaults `use_io_uring=false`. | Make it an optional feature (`io-uring`) and keep it off for Ostro by default. |
| `bincode 1.3`, `dashmap 5.5`, `thiserror 1.0`, `rand 0.8` | chronik-wal/raft | dashmap 6, thiserror 2, rand 0.10 | Harmless duplicates. | Tolerate them. Upgrade opportunistically. Exempt them in cargo-deny `skip`. |
| `tokio 1.35`, `bytes 1.5`, `serde 1`, `tracing 0.1` | all | same majors | Unify cleanly. | none |
| `openssl` (vendored), `rdkafka` (cmake-build), `opendal 0.45`, `kube 0.87`, `tantivy`, `warp` | Chronik workspace deps, **not** used by chronik-raft or chronik-wal | n/a | None, as long as Ostro depends only on raft/wal. | Keep it that way. Extract object-store code onto `object_store` 0.14 rather than opendal 0.45. |

**Recommended sequencing:** a "chronik-core" preparation task at the start of Phase 1, in the Chronik repo (the author's own). In order: (1) drop the unused deps from chronik-raft, (2) swap MetricsRecorder for `metrics`, (3) rename the vendored raft crates so the `[patch]` problem goes away, (4) make tokio-uring optional, (5) bump tonic/prost to 0.14 with protox codegen. Then pin Ostro to that rev and add a `cargo deny` duplicate-ban on tonic, prost, hyper, axum and opentelemetry.

### Ostro-internal compatibility

| Package A | Compatible With | Notes |
|---|---|---|
| wasmtime 50.0.2 | Rust ≥1.97; wasmparser 0.261.x | wasm-tools 1.263 / wit-component 0.263 are newer. Fine as CLIs. As library deps, match 0.261 or let Wasmtime re-export (`reexport-wasmparser` feature). |
| tonic 0.14.9 | prost 0.14, axum 0.8, hyper 1.10+, tower 0.5, http 1.1+ | Codegen is now `tonic-prost-build`, not `tonic-build`'s prost path. |
| opentelemetry-otlp 0.33.1 | tonic 0.14.1+, prost 0.14, reqwest 0.13 | Same stack as Ostro. All `opentelemetry*` crates must share one minor version (0.33), and `tracing-opentelemetry` 0.34 targets 0.33. |
| axum 0.8.9 | tower-http 0.6.8+ (optional dep), works with 0.7.1 used directly | |
| reqwest 0.13.5 | rustls 0.23, hyper 1, tower-http 0.6 | 0.13's `rustls` feature = aws-lc-rs + platform verifier. |
| redb 4.6.0 | Rust ≥1.90 | 5.0 is breaking (see What NOT to Use). |
| componentize-py 0.25.1 | Wasmtime ≥46 for p3 components; p2 components on any recent Wasmtime | It embeds wasmtime 48 / wit-component 0.258 internally (irrelevant at runtime). |
| jco 1.42.0 | componentize-js 0.23.0, StarlingMonkey 0.5.x, preview2-shim 0.31.1, preview3-shim 0.14.1 | jco also ships a `componentize-js-0-19-3` fallback dependency. |
| WIT package versions | All tools on the same WASI version | wasi.dev warns that mixed 0.2.x/0.3.x WIT gives confusing `wrong type` errors at instantiation. Ostro's own package (`ostro:runtime@0.1.0`) mostly avoids this because guests import no `wasi:*`. |

---

## Sources

- crates.io API (`/api/v1/crates/<name>`, `/<version>`, `/<version>/dependencies`), queried 2026-11-18: versions, features, MSRVs, dependency lists for wasmtime, wasmtime-wasi, wit-bindgen, cargo-component, wasm-tools, wac-cli, redb, fjall, tokio, tonic, tonic-prost, prost, axum, hyper, hyper-util, rustls, reqwest, raft, openraft, opentelemetry*, tracing-opentelemetry, prometheus, prometheus-client, metrics*, object_store, opendal, sqlx, mimalloc, cargo-zigbuild, cargo-dist, cargo-deny, aws-lc-rs, wasip2, wasip3. **HIGH**
- PyPI JSON API (componentize-py 0.25.1, wheel sizes); npm registry (jco 1.42.0, componentize-js 0.23.0, preview2-shim 0.31.1, esbuild 0.28.2). **HIGH**
- GitHub releases/API: wasmtime v50.0.2, binaryen version_133, StarlingMonkey v0.5.1, javy v9.1.0, uv 0.13.9, wasi-sdk 34, cross v0.2.5; repo activity for cargo-component (last push 2025-07-14) and bincode (archived). **HIGH**
- Wasmtime RELEASES.md for release-40 through release-50 (async_support removal in 42; WASI 0.3 + CM-async default in 46; `wasmtime-wasi-http` p3 default in 49; MSRV 1.97, `StoreHookState`, `release_idle_pool_memory`, and 8 GHSAs in 50.x): https://github.com/bytecodealliance/wasmtime/blob/release-50.0.0/RELEASES.md **HIGH**
- Wasmtime stability/LTS policy: https://docs.wasmtime.dev/stability-release.html **HIGH**
- Wasmtime `Config` and `bindgen!` docs: https://docs.rs/wasmtime/50.0.2/wasmtime/struct.Config.html, https://docs.rs/wasmtime/50.0.2/wasmtime/component/macro.bindgen.html **HIGH**
- WASI releases and language support: https://wasi.dev/releases, https://wasi.dev/languages (WASI 0.3 stable; Rust wasip2 Tier 2, wasip3 Tier 3 nightly; jco 0.3 experimental). **HIGH**
- WASI 0.3 ratification date (2026-06-11), secondary coverage: https://www.youngju.dev/blog/2026-07-16-wasi-0-3-native-async.en, https://wasi.dev/releases/wasi-p3. **MEDIUM**
- componentize-py repo (README, `examples/cli-p3`, `examples/sandbox`, `bundled/componentize_py_async_support`, Cargo.toml): https://github.com/bytecodealliance/componentize-py **HIGH**
- componentize-py size discussion (35 MB report, strip advice): https://github.com/bytecodealliance/componentize-py/issues/98 **LOW**, superseded by the local measurement
- `jco componentize --help` (1.42.0) and the installed package manifests (Rolldown, componentize-qjs, script-only engine WIT), measured locally. **HIGH**
- tonic `Routes::into_axum_router`: https://docs.rs/tonic/0.14.9/tonic/service/struct.Routes.html **HIGH**
- redb CHANGELOG (4.0 to 4.6 fixes, 5.0 breaking notes) and `Durability` docs: https://github.com/cberner/redb/blob/master/CHANGELOG.md, https://docs.rs/redb/4.6.0/redb/enum.Durability.html **HIGH**
- OpenTelemetry Rust status table: https://github.com/open-telemetry/opentelemetry-rust/blob/main/README.md **HIGH**
- aws-lc-rs build requirements: https://aws.github.io/aws-lc-rs/requirements/linux.html **HIGH**
- cargo-zigbuild README (musl, macOS SDK, universal2, deployment target): https://github.com/rust-cross/cargo-zigbuild **HIGH**
- tikv/raft-rs master `Cargo.toml` and `proto/Cargo.toml` (still prost 0.11 / protobuf 2): https://github.com/tikv/raft-rs **HIGH**
- Chronik Stream source @ `08715f4`: workspace `Cargo.toml`, `crates/{chronik-raft,chronik-wal,chronik-raft-bridge,chronik-common,chronik-monitoring,raft,raft-proto}/Cargo.toml`, `Cross.toml`, grep of `use chronik_*`. **HIGH**
- Local measurements (2026-11-18, x86_64 WSL2): componentize-py 0.25.1, jco 1.42.0 (StarlingMonkey, QuickJS, script-only), Wasmtime CLI 50.0.2 `compile`/`run --invoke` with `/usr/bin/time`. **HIGH** for these builds; real workloads will differ.

---
*Stack research for: single-binary polyglot Wasm durable execution engine (Ostro)*
*Researched: 2026-11-18*
