---
phase: 07-workers-placement-flow-control
plan: 02
subsystem: dependencies, partition contracts, server wiring seam
tags: [deps, proto, worker, placement, flow-control, seam, config, tls, errors]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-01 gate (Phase 6 complete, F1-F4/F7/F8 in code, Phase 7 ordinals reserved)"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "BridgeServices (06-14), ostro-testkit mock-http/canary"
  - phase: 08-triggers-bottomless-observability
    provides: "services seam (P8-D21), injected clock (F5), ApiExtensions"
provides:
  - "Every Phase 7 dependency (rcgen, x509-parser, axum-server, instant-acme, arc-swap, libc, tokio-rustls, rustls-pki-types; tonic tls-aws-lc + tls-connect-info)"
  - "ostro-worker-proto: ostro.worker.v1 Execute service, PROTO_VERSION = 1, WireOutcome"
  - "ostro-worker skeleton: WorkerConfig contract, run() stub, failpoints feature"
  - "ostro-partition Placement/FlowControl/QueueGauges contracts + fair.rs stub"
  - "ostro-server p7.rs seam, P7Shared on ServerControlPlane, tls/ flow/ workers/ stubs"
  - "Config sections [workers] [cluster] [api.tls] [flow]; ErrorCode::RateLimited"
affects: [07-03, 07-04, 07-05, 07-06, 07-07, 07-08, 07-09, 07-10, 07-11, 07-12, 07-13, 07-14, 07-15, 07-16, 07-22]
tech-stack:
  added: [rcgen 0.14.10, x509-parser 0.18.1, axum-server 0.9.2, instant-acme 0.8.5]
  patterns: ["single wiring seam (p7.rs) with fixed-signature stub hooks", "late-bound OnceLock state for node/engine", "dependency anchors for cargo::unused_dependencies"]
key-files:
  created:
    - crates/ostro-worker-proto/proto/ostro/worker/v1/execute.proto
    - crates/ostro-worker-proto/src/outcome.rs
    - crates/ostro-worker/src/config.rs
    - crates/ostro-partition/src/leader/placement.rs
    - crates/ostro-partition/src/leader/flow.rs
    - crates/ostro-server/src/p7.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-api/src/error.rs
key-decisions:
  - "No DispatcherHooks field exists (05-11 kept its hooks on the executor, ExecutorHooks), so p7::dispatcher_config takes (executor, stats, ctx) and carries nothing else"
  - "P7Ctx holds MetaView + Weak<MetaHandle> (not Arc<MetaHandle>) and NodeRefs holds partition handles (not Arc<Node>): the server must stay the last owner of meta and the node at shutdown"
  - "SpillHandle::note_overflow takes now_ms: u64 (injected clock, D-W26) instead of the plan's tokio Instant"
  - "FlowControl::admit takes now_ms (D-W26); FlowPermit has an optional release hook for committed charges"
  - "RateLimited shares gRPC RESOURCE_EXHAUSTED with PayloadTooLarge; ErrorCode::from_grpc_code_and_details disambiguates by the retry_after_ms detail"
  - "[workers] spill thresholds live in a nested [workers.spill] table; AcmeChallenge serialises as tls-alpn-01 / http-01"
requirements-completed: []
duration: ~2h40m to the Task 3 commit + ~3h50m ci-local
completed: 2026-11-21
---

# Phase 7 Plan 02: Dependencies, partition contracts and the ostro-server seam Summary

**Every Phase 7 dependency is declared once (deny-clean, no ring/chrono, no banned duplicate), the `ostro.worker.v1` protocol crate round-trips every `ActivationOutcome` through `WireOutcome`, ostro-partition exposes the Placement/FlowControl/QueueGauges contracts, and ostro-server has its single Phase 7 seam (`p7.rs`) with stub hooks, config sections and `ErrorCode::RateLimited`. Server behaviour is unchanged.**

## Why this plan is wide (~58 files)

Most entries are one-line registrations (`pub mod` plus a `//! Phase 7: implemented in 07-NN` stub), dependency lines and config structs. This plan is the deliberate serialisation point (D-W17) for files that several later waves, or Phase 8, would otherwise edit at the same time: `Cargo.toml`, `Cargo.lock`, member manifests, `server.rs`, `config.rs`, `lib.rs` and the shared `mod.rs` files. After it, later Phase 7 plans fill only files they own.

## Task commits

1. `826548e` build(07-02): Phase 7 dependencies, ostro-worker-proto and ostro-worker skeleton
2. `9f7fb47` feat(07-02): partition placement/flow/gauge contracts
3. `343d276` feat(07-02): ostro-server Phase 7 seam, config sections, RateLimited error code

## Dependencies (resolved versions)

| Crate | Pin | Resolved | Features | Where |
|---|---|---|---|---|
| rcgen | 0.14.10 | 0.14.10 | `default-features = false`, `aws_lc_rs`, `pem`, `x509-parser` | ostro-server, ostro-worker, dev: ostro |
| x509-parser | 0.18.1 | 0.18.1 | default (no `verify`) | ostro-server, ostro-worker, dev: ostro |
| axum-server | 0.9.2 | 0.9.2 | `default-features = false`, `tls-rustls-no-provider` | ostro-server |
| instant-acme | 0.8.5 | 0.8.5 | `default-features = false`, `aws-lc-rs`, `hyper-rustls` | ostro-server |
| arc-swap | 1.9 | 1.9.5 (already locked) | | ostro-server |
| libc | 0.2 | 0.2.191 (already locked) | | ostro-server |
| tokio-rustls | 0.26 | 0.26.6 (already locked) | `default-features = false` | ostro-server |
| rustls-pki-types | 1 | 1.16.0 (already locked) | | ostro-server |
| tonic (member level) | 0.14.9 | 0.14.9 | `tls-aws-lc`, `tls-connect-info` | ostro-server, ostro-worker |

- No governor (D-W24). No toml crate: the worker's `[worker]` file goes through the existing `config` pin.
- New transitive crates: asn1-rs 0.7.2 (+ derive/impl), der-parser 10.0.0, oid-registry 0.8.1, nom 7.1.3, minimal-lexical, rusticata-macros 4.1.0, yasna 0.6.0, bit-vec 0.9.1, synstructure 0.13.2, untrusted 0.7.1, fs-err 3.3.2.
- **Licences:** `cargo deny check` passes on the stock deny.toml (`advisories ok, bans ok, licenses ok, sources ok`). No licence string was added and deny.toml is unchanged.
- **Fallbacks (D-W6/D-W7):** none needed. instant-acme and axum-server resolve cleanly.

Member manifests:
- **ostro-server:** the pins above plus `ostro-worker-proto`. Dev-deps gain `ostro-worker` and `reqwest` (rustls, json); `ostro-testkit` with `mock-http` + `canary` was already present from Phase 6.
- **ostro:** `ostro-worker`; `failpoints` forwards to `ostro-worker/failpoints`. Dev-deps: ostro-testkit (`mock-http`, `canary`), reqwest (rustls, json), rcgen, x509-parser. serde_json is already a normal dependency.
- **ostro-cli:** unchanged (the `ostro-api` dev-dependency already exists, 08-12).
- **ostro-worker-proto:** tonic, tonic-prost, prost, serde, postcard, ostro-engine, ostro-types; build: protox, tonic-prost-build.
- **ostro-worker:** ostro-engine, ostro-types, ostro-worker-proto, tonic (TLS), rustls, tokio, tokio-util, reqwest (rustls, json), rcgen, x509-parser, sha2, serde, serde_json, config, tracing, metrics, thiserror, async-trait, optional fail. Dev: tokio, tempfile, ostro-testkit.

### `cargo tree -i ring -e normal --target all --locked`
```
warning: nothing to print.
```
`cargo tree -i chrono` prints the same. The normal tree has no chrono, openssl-sys, webpki-roots, native-tls or ring.

### Duplicates (`cargo tree -d --depth 0 -e normal --workspace --locked`)
None of tonic, tonic-prost, prost, prost-types, hyper, http, axum or rustls is duplicated. This plan adds two tolerated duplicates, both under the `multiple-versions = "warn"` policy:
- synstructure 0.13.2, a proc-macro via asn1-rs-derive, next to 0.12.6 and 0.14.0.
- untrusted 0.7.1, pulled by aws-lc-rs features that rcgen and instant-acme enable, next to 0.9.0.

nom 7.1.3 (x509-parser) does not appear in the normal-edge duplicate list. The other duplicates were there before.

## ostro-worker-proto

- `execute.proto`: research Pattern 2 in full (`service Execute { Session, FetchJournal, FetchBlob, Append, StartEffect, Renew }`). It adds `EngineLimits`, `Member`, `BlobRef`, `Header`, `EffectRetry`, `CancelEffect`, `GuestLogs`/`GuestLogLine`, `Drain`, `GoAway`, `LeaseLost`, `RenewRequest{csr}`/`RenewResponse{client_cert, renew_after_ms}`, `FetchBlobRequest{session_id, run, token, sha, offset}`/`BlobChunk{data, last, total_len}`, `AppendResult{oneof next | fenced | conflict_expected | paused | rejected}` and `StartEffectResponse{oneof started | rejected}`.
- `pub const PROTO_VERSION: u32 = 1`.
- `WireOutcome`, `WireEvictReason`, `WireAttemptFailure`: `From<&ActivationOutcome>`, `into_engine()`, and `encode()`/`decode()` (postcard; decode returns an error and never panics). Every conversion is an exhaustive match with no wildcard.
- `tests/wire.rs` (5 tests) round-trips all 23 representative outcomes, including every variant and sub-variant.

## ostro-worker skeleton

- Nine stub modules, each `//! Phase 7: implemented in 07-07`.
- `pub async fn run(cfg: WorkerConfig, shutdown: CancellationToken) -> Result<(), WorkerError>` returns `Err(WorkerError::NotImplemented)`.
- `WorkerConfig { join, token: Option<JoinToken>, ca_hash, labels: BTreeMap, slots (available_parallelism), cache_dir, memory_bytes (1 GiB), tls_cert, tls_key, tls_ca, worker_id, reconnect_max_ms (30_000), rejoin_after_failures (5) }`. It loads with `WorkerConfig::load(file, env)`: a `[worker]` TOML table, then flat `OSTRO_WORKER_<KEY>` variables, with `LABELS` written as `k=v,k2=v2`. Unknown keys are rejected.
- `validate()` accepts either join (`join` + `token` + optional `ca_hash`) or the external certificate triple. `JoinToken` is redacted in Debug, Display and serde.
- 5 unit tests, plus `tests/failpoints.rs` (feature-gated).

## Partition contracts (`ostro_partition::*`)

```rust
pub struct EntryView<'a> { queue, workflow, tenant: Option<&'a str>, concurrency_key, priority: i32, first_activation: bool }
impl EntryView<'a> { pub fn from_row(row: &'a ReadyEntryRow) -> Self }
pub enum ReservationKind { Embedded, Worker }
pub struct Reservation { .. }  // new(kind, T), kind(), downcast_ref::<T>(), into_inner::<T>() -> Result<Box<T>, Self>
pub enum Reserve { Granted(Reservation), Blocked }
pub trait Placement: Send + Sync + 'static { fn reserve(&self, e: &EntryView<'_>) -> Reserve; fn changed(&self) -> watch::Receiver<u64>; }
pub struct SemaphorePlacement  // new(Arc<Semaphore>), slots(); inner guard leader::placement::EmbeddedPermit (release, then bump)
pub enum Admit { Yes(FlowPermit), QueueBlocked { until_ms: Option<u64> }, KeyBlocked { until_ms: Option<u64> }, Paused }
pub struct FlowPermit  // none(), new(rollback), with_release(release), commit(&mut self), is_committed()
pub trait FlowControl: Send + Sync + 'static { fn admit(&self, e: &EntryView<'_>, now_ms: u64) -> Admit; fn queue_weight(&self, q: &str) -> u32; fn changed(&self) -> watch::Receiver<u64>; }
pub struct NoFlow  // admits all, weight 1
pub struct QueueGauges  // set(PartitionId, &str, QueueGauge), remove_partition(PartitionId), snapshot() -> BTreeMap<String, QueueGauge>
pub struct QueueGauge { ready, ready_unblocked, oldest_ready_at_ms: Option<u64>, running_embedded, running_on_workers }
```
`leader/fair.rs` is a stub (07-05). The dispatcher is not wired to these yet. `tests/p7_contract.rs` has 6 tests. The `dispatcher` suite is unchanged: 17 passed.

## Stub-module map (file → owning plan, fixed signatures)

| File | Owner | Fixed surface |
|---|---|---|
| `tls/mod.rs` | 07-06 | `enum TlsMode { Off, SelfSigned, Files, Acme }`; `struct TlsState` (`off()`, `mode()`, `endpoint(addr) -> ClientEndpoint`); `struct ClientEndpoint { scheme: &'static str, addr: SocketAddr, fingerprint: Option<String> }` + `url()`; `enum TlsError { Config(String), Io { path, source } }`; `pub fn init(cfg: &ServerConfig, data_dir: &Path, cluster_id: &str) -> Result<Arc<TlsState>, TlsError>` (stub: Off) |
| `tls/listener.rs` | 07-06 | `pub fn serve(state: Arc<TlsState>, listener: TcpListener, app: Router, shutdown: CancellationToken) -> impl Future<Output = io::Result<()>> + Send + 'static` (stub: plaintext `axum::serve` + graceful shutdown) |
| `tls/{ca,dev_cert,resolver}.rs` | 07-06 | doc stubs |
| `tls/acme.rs` | 07-10 | doc stub |
| `flow/mod.rs` | 07-08 | `NodeFlow::new(meta: MetaView, gauges: Arc<QueueGauges>) -> Self` (implements `FlowControl` with NoFlow semantics); `pub fn install(ctx: &P7Ctx, cfg: &mut DispatcherConfig)` (no-op) |
| `flow/{policy_view,limits,rate_limit}.rs` | 07-08 | doc stubs |
| `workers/registry.rs` | 07-09 | `WorkerRegistry::new(cfg: &WorkersSection)`, `connected() -> usize`, `slots_free() -> u64`, `max_frame_bytes()` |
| `workers/service.rs` | 07-09 | `pub fn start(ctx: &P7Ctx) -> Result<Option<JoinHandle<()>>, P7Error>` (Ok(None)) |
| `workers/join.rs` | 07-09 | `JoinService::new(meta: MetaView, tls: Arc<TlsState>, cfg: &WorkersSection)`; `async fn join(&self, JoinRequest) -> Result<JoinResponse, ApiError>` and `async fn renew(&self, token_id, worker_id, csr)`, both returning Unavailable; `JoinRequest` (token redacted in Debug) and `JoinResponse { client_cert, ca_cert, endpoints, renew_after_ms }` |
| `workers/placement.rs` | 07-12 | `pub fn install(ctx: &P7Ctx, cfg: &mut DispatcherConfig)` (no-op) |
| `workers/{remote_executor,selector}.rs` | 07-12 | doc stubs |
| `workers/spill.rs` | 07-13 | `SpillHandle::new()`, `spilling(&self, queue: &str) -> bool` (false), `note_overflow(&self, queue: &str, now_ms: u64)` (no-op); `pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>>` (None) |
| `workers/demand.rs` | 07-16 | `pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>>` (None); `pub fn snapshot(shared: &P7Shared, queue: &str) -> DemandSnapshot` (zeros, placement "embedded"); `DemandSnapshot { queue, demand, backlog, running_on_workers, placement, spilling, workers, worker_slots_free }` (Serialize) |
| `ostro-partition leader/fair.rs` | 07-05 | doc stub |
| `ostro-worker src/{join,session,remote_sink,effect_proxy,journal_fetch,blob_fetch,runner,drain}.rs` | 07-07 | doc stubs |

## P7Ctx / P7Shared as built

```rust
pub struct P7Shared {
    pub gauges: Arc<QueueGauges>,
    pub registry: Arc<WorkerRegistry>,
    pub flow: Arc<NodeFlow>,
    pub spill: Arc<SpillHandle>,
    pub tls: Arc<TlsState>,
    pub join: Arc<JoinService>,
    pub embedded_slots: Arc<Semaphore>,            // [executor] slots: the dispatcher's semaphore
    pub engine: OnceLock<Arc<EngineExecutor>>,     // set by p7::executor
    pub bridge: Option<Arc<BridgeServices>>,       // the 06-14 instance the executor uses
    pub node: OnceLock<NodeRefs>,                  // set by p7::attach_node
}
pub struct NodeRefs { pub partitions: Vec<Option<PartitionHandle<Rf1Log>>>, pub blobs: Option<Arc<BlobStore>> }
pub struct P7Ctx {
    pub config: Arc<ServerConfig>,                 // the same Arc the server uses after step 4
    pub clock: Arc<dyn ostro_partition::Clock>,   // ServerOptions.clock = NodeConfig.clock (F5)
    pub data_dir: PathBuf,
    pub meta: MetaView,
    pub meta_handle: Weak<MetaHandle>,
    pub runtime: Arc<Runtime>,
    pub cluster_id: String,
    pub node_id: u64,
    pub lease_ttl: Duration,
    pub shutdown: CancellationToken,               // cancelled by ServerHandle::shutdown with the listener stop
    pub shared: Arc<P7Shared>,
}
pub struct P7Init { config, clock, data_dir, meta: Arc<MetaHandle>, runtime, bridge, cluster_id, node_id, lease_ttl }
pub enum P7Error { Tls(TlsError), Config(String), Io(io::Error) }   // ServerError::P7(#[from])
pub async fn init(i: P7Init) -> Result<P7Ctx, P7Error>;
pub fn executor(engine: EngineExecutor, ctx: &P7Ctx) -> Arc<dyn Executor>;
pub fn dispatcher_config(executor: Arc<dyn Executor>, stats: Option<Arc<DispatcherStats>>, ctx: &P7Ctx) -> DispatcherConfig;
pub fn attach_node(ctx: &P7Ctx, node: &Node);
pub async fn start_services(ctx: &P7Ctx) -> Result<P7Handles, P7Error>;   // P7Handles::join() at shutdown
pub fn serve_client(ctx: &P7Ctx, listener: TcpListener, app: Router) -> impl Future<Output = io::Result<()>> + Send + 'static;
pub fn client_endpoint(ctx: &P7Ctx, addr: SocketAddr) -> ClientEndpoint;
```
`ServerHandle` gains `client_endpoint() -> &ClientEndpoint`, for the 07-15 ready line, and `p7() -> &Arc<P7Shared>`.

**DispatcherHooks field carried:** none. At HEAD, `DispatcherConfig` is `{ executor, lease_ttl, slots, stats }` and 05-11's hooks live on the executor (`ExecutorHooks` through `EngineExecutor::with_hooks`). Those still pass through unchanged inside `p7::executor(EngineExecutor::...with_hooks(hooks)..., &p7)`. 07-05 adds the placement/flow fields to `DispatcherConfig`, and the `install` hooks fill them.

## Config sections (both profiles, documented defaults)

- `[workers]`: `enabled = true`, `max_frame_bytes = 1048576` (validated 1..=4 MiB), `max_append_bytes = 67108864` (≥ frame), `heartbeat = "10s"` (`effective_heartbeat(lease_ttl) = min(heartbeat, lease_ttl/3)`), `keepalive = "5s"`, `require_same_version = true`, `client_cert_ttl = "24h"`, `join_token_ttl = "24h"`, `default_placement = "embedded"` (`embedded|workers|auto`).
- `[workers.spill]`: `cpu_pct = 70`, `slots_pct = 90`, `mem_pct = 85`, `lag = "5s"`, `sustain = "10s"`, `cooldown = "30s"`, `hysteresis_pct = 85`.
- `[cluster]`: `listen` (dev `127.0.0.1:0`, server `0.0.0.0:7701`), `advertise = []`. `[cluster.tls]`: `client_ca` (unset).
- `[api.tls]`: `mode = "auto"` (`auto|off|self-signed|files|acme`), `cert_file`, `key_file`, `allow_self_signed = false`, `reload_poll = "30s"`. `[api.tls.acme]`: `domains = []`, `email`, `directory` (Let's Encrypt production), `directory_ca_file`, `cache_dir` (unset = `<data_dir>/tls/acme`), `challenge = "tls-alpn-01"` (`|"http-01"`), `http01_listen = "0.0.0.0:80"`. `files` mode requires both files; `acme` requires at least one domain.
- `[flow]`: `max_tracked_keys = 100000`, `idle_key_evict = "10m"`, `default_queue_weight = 1`.
- `workers`, `cluster` and `flow` were added to `SECTIONS`, so `OSTRO_WORKERS__*` etc. are read. `[api.tls]` comes through `OSTRO_API__TLS__*`. `--print-config` renders nested tables as recursive inline tables (`tls = { "acme" = { ... }, ... }`), and the output round-trips.
- Tests: 5 new tests in `tests/config.rs` (38 total): defaults per profile, TOML parse, env overrides, invalid values/modes rejected with the key named, round-trip of the printed config.

## ErrorCode::RateLimited (D-W21)

No Phase 5/6 equivalent existed, so it is appended: `rate_limited`, HTTP 429, gRPC `ResourceExhausted`. `ApiError::rate_limited(msg, retry_after_ms)` sets `details.retry_after_ms`. `IntoResponse` adds `Retry-After: ceil(ms/1000)` for this code only. `ErrorCode::ALL` now has 9 entries. `RESOURCE_EXHAUSTED` is shared with `PayloadTooLarge`: `from_grpc_code` keeps returning PayloadTooLarge, and `ErrorCode::from_grpc_code_and_details(code, details)` returns RateLimited when `retry_after_ms` is present. 07-08/07-11 parity tests should use it. The `ErrorDetail` doc comment, which is in the OpenAPI snapshot, was **not** touched; 07-11 owns the snapshot.

## server.rs + control/mod.rs diff hunks (343d276, test module omitted)

```diff
diff --git a/crates/ostro-server/src/control/mod.rs b/crates/ostro-server/src/control/mod.rs
--- a/crates/ostro-server/src/control/mod.rs
+++ b/crates/ostro-server/src/control/mod.rs
@@ -53,6 +53,7 @@ use ostro_types::{PartitionCount, PartitionId};
 
 use crate::bridge_services::BridgeServices;
 use crate::config::ServerConfig;
+use crate::p7::P7Shared;
 use crate::services::guest_logs::GuestLogSink;
 pub use watch::RunWatchers;
 
@@ -89,6 +90,9 @@ pub struct ServerControlPlane {
     /// The effects bridge (06-14): sealing key, secret resolver, object connector. 06-16
     /// implements the Phase 6 admin methods over it.
     pub(crate) bridge: Arc<BridgeServices>,
+    /// Phase 7 shared state (07-02 seam, B3): worker registry, flow control, gauges, TLS,
+    /// join. 07-14 implements the Phase 7 control-plane methods over it.
+    pub(crate) p7: Arc<P7Shared>,
 }
 
 impl std::fmt::Debug for ServerControlPlane {
@@ -108,6 +112,7 @@ impl ServerControlPlane {
         runtime: Arc<Runtime>,
         cfg: Arc<ServerConfig>,
         bridge: Arc<BridgeServices>,
+        p7: Arc<P7Shared>,
     ) -> Self {
         let blobs = partitions
             .iter()
@@ -129,6 +134,7 @@ impl ServerControlPlane {
             status_rows_read: Arc::new(AtomicU64::new(0)),
             watchers,
             bridge,
+            p7,
         }
     }
 
diff --git a/crates/ostro-server/src/server.rs b/crates/ostro-server/src/server.rs
--- a/crates/ostro-server/src/server.rs
+++ b/crates/ostro-server/src/server.rs
@@ -53,6 +53,13 @@
 //! P8-D21): `Node::open` has returned (every partition's readiness barrier is open and
 //! its leader services run) and the leaders are recorded in meta.
 //!
+//! Phase 7 (07-02) has one seam, [`crate::p7`] (D-W17): [`p7::init`] after the engine
+//! runtime (TLS state, worker registry, flow control, spill, join), [`p7::executor`] and
+//! [`p7::dispatcher_config`] build the dispatcher, [`p7::attach_node`] runs right after
+//! `Node::open`, [`p7::start_services`] after the services seam, and the client listener is
+//! served by [`p7::serve_client`] (plaintext until 07-06). The control plane holds
+//! [`p7::P7Shared`]; the ready line reads [`ServerHandle::client_endpoint`].
+//!
 //! One injectable clock (Phase 10 F5): [`ServerOptions::clock`] (the system clock by
 //! default) is the single `Arc<dyn Clock>` handed to `InitCluster`'s timestamp,
 //! `NodeConfig.clock` and the services' [`crate::services::ServiceCtx::clock`].
@@ -76,12 +83,13 @@ use ostro_engine::error::EngineError;
 use ostro_engine::runtime::{Runtime, RuntimeOptions};
 use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaResult};
 use ostro_partition::{
-    Clock, DispatcherConfig, DispatcherStats, Executor, Node, NodeConfig, NodeError, PartitionSlot,
+    Clock, DispatcherStats, Executor, Node, NodeConfig, NodeError, PartitionSlot,
 };
 use ostro_types::PartitionCount;
 use tempfile::TempDir;
-use tokio::sync::{Semaphore, watch};
+use tokio::sync::watch;
 use tokio::task::JoinHandle;
+use tokio_util::sync::CancellationToken;
 use tracing::{error, info, warn};
 
 use crate::bridge_services::{BridgeEnv, BridgeServices};
@@ -90,10 +98,12 @@ use crate::control::{ControlCtx, ServerControlPlane, phase8_extensions};
 use crate::derive::MetaModules;
 use crate::executor::{EngineExecutor, ExecutorConfig, ExecutorHooks};
 use crate::leadership::MetaLeadership;
+use crate::p7::{self, P7Handles, P7Shared};
 use crate::retention::MetaPolicies;
 use crate::services::guest_logs::GuestLogSink;
 use crate::services::{ServiceCtx, Services};
 use crate::telemetry::{Telemetry, TelemetryError, TelemetryGuard};
+use crate::tls::ClientEndpoint;
 
 /// How long shutdown waits for in-flight HTTP/gRPC requests before closing them.
 const SERVE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
@@ -139,6 +149,8 @@ pub enum ServerError {
     Invariant(String),
     #[error("telemetry: {0}")]
     Telemetry(#[from] TelemetryError),
+    #[error("workers / TLS: {0}")]
+    P7(#[from] p7::P7Error),
 }
 
 /// Wall-clock durations of the startup steps (logged at info; the 04-19 startup test
@@ -346,6 +358,28 @@ async fn start_inner(
     };
     timings.runtime = t.elapsed();
 
+    // 3b. Phase 7 seam (D-W17): TLS state, worker registry, flow control, spill, join.
+    let p7 = match p7::init(p7::P7Init {
+        config: Arc::new(cfg.clone()),
+        clock: Arc::clone(&clock),
+        data_dir: data_dir.clone(),
+        meta: Arc::clone(&meta),
+        runtime: Arc::clone(&runtime),
+        bridge: Some(Arc::clone(&bridge)),
+        cluster_id: cluster_id.clone(),
+        node_id,
+        lease_ttl: cfg.executor.lease_ttl.get(),
+    })
+    .await
+    {
+        Ok(p) => p,
+        Err(e) => {
+            bridge.shutdown().await;
+            stop_meta(meta).await;
+            return Err(e.into());
+        }
+    };
+
     // 4. node + dispatcher(EngineExecutor).
     let t = Instant::now();
     let lease_ttl = cfg.executor.lease_ttl.get();
@@ -363,7 +397,7 @@ async fn start_inner(
             .with_clock(Arc::clone(&clock))
             .with_logger_provider(telemetry.as_ref().and_then(TelemetryGuard::logger_provider)),
     );
-    let executor: Arc<dyn Executor> = Arc::new(
+    let executor: Arc<dyn Executor> = p7::executor(
         EngineExecutor::new(Arc::clone(&runtime), exec_cfg)
             .with_hooks(hooks)
             .with_guest_log_sink(Arc::clone(&guest_logs))
@@ -375,6 +409,7 @@ async fn start_inner(
                     defaults: cfg.runs.clone(),
                 }),
             ),
+        &p7,
     );
     let stats = Arc::new(DispatcherStats::default());
     let mut node_cfg = NodeConfig::new(data_dir.clone(), count);
@@ -391,14 +426,11 @@ async fn start_inner(
         cfg.runs.clone(),
     ));
     node_cfg.backup_enabled = cfg.backup.enabled();
-    node_cfg.leader.dispatcher = Some(DispatcherConfig {
+    node_cfg.leader.dispatcher = Some(p7::dispatcher_config(
         executor,
-        lease_ttl,
-        slots: Arc::new(Semaphore::new(
-            usize::try_from(cfg.executor.slots).unwrap_or(usize::MAX),
-        )),
-        stats: Some(Arc::clone(&stats)),
-    });
+        Some(Arc::clone(&stats)),
+        &p7,
+    ));
     let node = match Node::open(node_cfg).await {
         Ok(n) => Arc::new(n),
         Err(e) => {
@@ -408,6 +440,7 @@ async fn start_inner(
         }
     };
     timings.node_open = t.elapsed();
+    p7::attach_node(&p7, &node);
     for slot in node.slots() {
         if let PartitionSlot::Ready(h) = slot {
             bridge.partitions().insert(h.clone());
@@ -437,7 +470,7 @@ async fn start_inner(
         .next()
         .map(|h| Arc::clone(h.blobs()));
     let ctl_blobs = blobs.clone();
-    let cfg = Arc::new(cfg);
+    let cfg = Arc::clone(&p7.config);
 
     // 4b. Services seam (P8-D21): every barrier is open and the leaders are recorded.
     let services = match blobs {
@@ -459,6 +492,20 @@ async fn start_inner(
         }
     };
 
+    // 4c. Phase 7 services (cluster listener / Execute service, spill, demand).
+    let p7_handles = match p7::start_services(&p7).await {
+        Ok(h) => h,
+        Err(e) => {
+            drop(p7);
+            drop(partitions);
+            services.shutdown().await;
+            bridge.shutdown().await;
+            shutdown_node(node).await;
+            stop_meta(meta).await;
+            return Err(e.into());
+        }
+    };
+
     // 5. API.
     let control = Arc::new(ServerControlPlane::new(
         Arc::clone(&meta),
@@ -467,6 +514,7 @@ async fn start_inner(
         Arc::clone(&runtime),
         Arc::clone(&cfg),
         Arc::clone(&bridge),
+        Arc::clone(&p7.shared),
     ));
     let api_cfg = ApiConfig {
         max_module_bytes: usize::try_from(cfg.api.max_module_bytes).unwrap_or(usize::MAX),
@@ -492,14 +540,11 @@ async fn start_inner(
     if cfg.telemetry.metrics {
         app = app.merge(crate::telemetry::metrics_router());
     }
-    let (stop_tx, mut stop_rx) = watch::channel(false);
     let (done_tx, done_rx) = watch::channel(false);
+    let client_endpoint = p7::client_endpoint(&p7, local_addr);
+    let serve_client = p7::serve_client(&p7, listener, app);
     let serve = tokio::spawn(async move {
-        let res = axum::serve(listener, app)
-            .with_graceful_shutdown(async move {
-                let _ = stop_rx.wait_for(|stop| *stop).await;
-            })
-            .await;
+        let res = serve_client.await;
         if let Err(e) = &res {
             error!(error = %e, "API server stopped with an error");
         }
@@ -534,7 +579,9 @@ async fn start_inner(
         clock,
         telemetry: telemetry.take().unwrap_or_else(TelemetryGuard::disabled),
         stats,
-        stop_tx,
+        p7_shutdown: p7.shutdown.clone(),
+        p7_handles: Some(p7_handles),
+        client_endpoint,
         serve: Some(serve),
         done_rx,
         data_lock,
@@ -758,7 +805,12 @@ pub struct ServerHandle {
     /// OTel providers + Prometheus upkeep; shut down last.
     telemetry: TelemetryGuard,
     stats: Arc<DispatcherStats>,
-    stop_tx: watch::Sender<bool>,
+    /// Phase 7 shutdown (stops the client listener and the Phase 7 tasks).
+    p7_shutdown: CancellationToken,
+    /// Phase 7 tasks (07-02 seam); joined after the listener drained, before the services.
+    p7_handles: Option<P7Handles>,
+    /// How clients reach the client listener (scheme, address, fingerprint).
+    client_endpoint: ClientEndpoint,
     serve: Option<JoinHandle<std::io::Result<()>>>,
     done_rx: watch::Receiver<bool>,
     /// The exclusive `<data>/LOCK` descriptor, held until shutdown finishes (or drop).
@@ -783,6 +835,17 @@ impl ServerHandle {
         self.local_addr
     }
 
+    /// How clients reach the client listener: `http`/`https`, the bound address and the
+    /// certificate fingerprint (Phase 7 seam; plaintext until 07-06).
+    pub fn client_endpoint(&self) -> &ClientEndpoint {
+        &self.client_endpoint
+    }
+
+    /// Phase 7 shared state (worker registry, flow control, gauges, TLS, join).
+    pub fn p7(&self) -> &Arc<P7Shared> {
+        &self.control.p7
+    }
+
     /// The data directory in use (a temp dir in ephemeral mode).
     pub fn data_dir(&self) -> &Path {
         &self.data_dir
@@ -876,14 +939,15 @@ impl ServerHandle {
             node,
             services,
             mut telemetry,
-            stop_tx,
+            p7_shutdown,
+            p7_handles,
             serve,
             data_lock,
             tempdir,
             ..
         } = self;
         control.stop_serving();
-        stop_tx.send_replace(true);
+        p7_shutdown.cancel();
         if let Some(mut serve) = serve {
             match tokio::time::timeout(SERVE_DRAIN_TIMEOUT, &mut serve).await {
                 Ok(Ok(Ok(()))) => {}
@@ -895,6 +959,10 @@ impl ServerHandle {
                 }
             }
         }
+        // Phase 7 tasks (stopped by `p7_shutdown` above).
+        if let Some(h) = p7_handles {
+            h.join().await;
+        }
         // Services first: they hold node / meta clones until joined.
         if let Some(services) = services {
             services.shutdown().await;
```

## Deviations from Plan

1. **[Rule 3 - blocking] `crates/ostro/src/main.rs` anchor (outside files_modified).** Cargo 1.100's `cargo::unused_dependencies` lint warns for the new `ostro-worker` dependency of the `ostro` binary, and ci-local requires a warning-free build. I added one line, `use ostro_worker as _;`, with a comment saying 07-15 consumes it, to main.rs (commit 826548e). The same anchor pattern (`mod phase7_dep_anchors`, `mod dep_anchors`) is used in ostro-server and ostro-worker lib.rs, as Phases 6/8 did.
2. **[Plan adaptation] No DispatcherHooks.** `p7::dispatcher_config` has no `hooks` parameter because `DispatcherConfig` has no 05-11 hooks field. See above.
3. **[Rule 1 / F5] Clock instead of Instant.** `SpillHandle::note_overflow(queue, now_ms: u64)`, not `now: Instant`, and `FlowControl::admit(e, now_ms)`, per D-W26 / Phase 10 F5.
4. **[Rule 2 - correctness] Meta/node ownership.** P7Ctx holds `MetaView` + `Weak<MetaHandle>`, and `NodeRefs` holds partition handles, never `Arc<Node>`. A strong reference would break `stop_meta` / `shutdown_shared_node`, which both require the server to be the last owner.
5. **[Rule 2] ServerHandle shutdown.** The `watch` stop channel was replaced by `P7Ctx::shutdown` (`CancellationToken`) driving `tls::listener::serve`. Phase 7 tasks are joined after the listener drains and before the services stop. The observable shutdown order is unchanged.
6. **[Rule 1 - plan wording] Target dir.** The orchestrator brief's `target/wt-07-02` was used instead of the plan's `target/p7-02`.
7. **`toml_render`** in config.rs now renders nested objects recursively. Phase 7's two-level `[api.tls.acme]` would otherwise print invalid TOML. Earlier sections render byte-identically.
8. **`--print-config` binary test** (crates/ostro/tests/server_config.rs) is outside files_modified. The `to_toml_string` round-trip test in `tests/config.rs` covers the sections `--print-config` prints.
9. **Commits are not TDD-split.** The plan asks for one `feat(07-02)` commit for Task 2. RED was observed (p7_contract failed to compile: unresolved imports) before the implementation; test and code landed together.

## Verification

- Task 1: `cargo deny check` ok. The banned-crate and duplicate checks pass (above). `cargo test -p ostro-worker-proto`: 5 passed. `cargo build -p ostro-worker --features failpoints` ok. `cargo test -p ostro-worker --features failpoints -- --test-threads=1`: 5 + 1 passed. Workspace `--all-targets` build is warning-free. fmt and clippy are clean.
- Task 2: `p7_contract` 6 passed, `dispatcher` 17 passed, clippy `-D warnings` clean.
- Task 3: `config` 38, `executor` 8, `leases` 4, ostro-api (all targets) green, `dispatcher` 17, `ostro-server --lib` 84 (including the new `p7_seam_is_wired_and_behaviour_preserving` and the F5 clock test). Workspace clippy `-D warnings` is clean, the workspace build is warning-free, and the plan's grep checks pass.
- `bash scripts/ci-local.sh --no-musl`: **all steps passed** at 343d276 (worktree, `CARGO_TARGET_DIR=target/wt-07-02`, alongside the Phase 8 10k campaign at load 17-27). Results: fmt, clippy, warning-free build, sdk workspace fmt/clippy/test, guests, order-saga all PASS; `cargo test` PASS (5906 s, 0 failures, including `semantics_e2e::continue_as_new`); semantics sleepers PASS; failpoint list sync PASS (ostro-worker in both lists); failpoint suites PASS (6206 s, including `ostro-worker --features failpoints`); server campaign smoke (20 kills) PASS; cargo deny, check-vendored, check-deny-fixtures, forbidden crates, workflow validation PASS. The external PG/MySQL/S3 legs SKIP (env unset, human_needed), and musl SKIP (`--no-musl`).

## Issues / follow-ups

- 07-05 adds placement/flow fields to `DispatcherConfig` (Pitfall 1: `SemaphorePlacement` must wrap the same `embedded_slots` semaphore that `p7::dispatcher_config` puts in `cfg.slots`).
- 07-08/07-11: use `ErrorCode::from_grpc_code_and_details` in gRPC parity tests for `rate_limited`.
- 07-15: replace the `use ostro_worker as _;` anchor in main.rs with the real `ostro worker` command and use `ServerHandle::client_endpoint()` for the ready line.
- While this plan ran, 07-03 and 07-04 (`346248a`, `9928aaf`, `0c9840c`) landed on main with no file overlap. ci-local covered 343d276.

## Self-Check: PASSED
- FOUND: 826548e, 9f7fb47, 343d276 on main
- FOUND: crates/ostro-worker-proto/proto/ostro/worker/v1/execute.proto (`service Execute`), crates/ostro-partition/src/leader/placement.rs (`trait Placement`), crates/ostro-partition/src/leader/flow.rs (`trait FlowControl`), crates/ostro-server/src/p7.rs (`pub struct P7Shared`), `p7::attach_node` in server.rs, `P7Shared` in control/mod.rs

