---
phase: 06-effects-bridge-connectors-secrets
plan: 14
subsystem: server-effect-seam
tags: [bridge, effects, server, config, secrets, presign, completion, send-gate, b1]
requires:
  - "06-03 effects rows + CompleteEffect apply + query::effect_row"
  - "06-05 secrets core (SealingKeyRing, SealedStore, providers, SecretResolver::with_sealed_store)"
  - "06-06 EffectDriver + ActivationSink::effect_driver (P-1)"
  - "06-07 Bridge / RateLimiter / ConnectorsConfig"
  - "06-10 HttpConnector, 06-11 SqlConnector + spawn_gc, 06-12 ObjectConnector + PresignKeyStore, 06-13 Vault/K8s providers"
provides:
  - "ostro_server::bridge_services::{BridgeServices, BridgeEnv, MetaPresignKeyStore, TenantHttp, SEALING_KEY_MISSING}"
  - "ostro_server::effects::{ServerEffectDriver, PartitionSendGate, PartitionCompletionSink, PartitionTable, EffectSeam, EffectHooks, NoopEffectHooks}"
  - "ostro_server::meta_view::MetaHandleView"
  - "config [secrets]/[egress]/[connectors] (SecretsSection, EgressSection, ConnectorsSection, ByteSize, ServerConfig::public_base_url)"
  - "ServerHandle::bridge(), ServerControlPlane::bridge(), EngineExecutor::with_bridge, PartitionSink::with_bridge / unsupported_input"
  - "test helpers (tests/common): start_server_with_bridge, BridgeTestOpts, bridge_server_config, TEST_SEALING_KEY, put_meta, put_egress, put_grant, put_sealed_secret, put_connection, fx_guest, TestServer::crash, TestServer::deploy_named"
affects: [06-15, 06-16, 06-17, 06-18, 06-19, 06-20, 08-21]
tech-stack:
  added: []
  patterns:
    - "Late-bound partition table: the bridge exists before Node::open; sinks and the server register handles; closed at shutdown"
    - "Mirror config sections (Serialize + unknown keys ignored) converted to the bridge's Deserialize-only types with serde"
    - "Per-tenant HTTP connector routing (SealedProvider is single-tenant; EffectCtx carries the tenant)"
key-files:
  created:
    - crates/ostro-server/src/bridge_services.rs
    - crates/ostro-server/src/effects.rs
    - crates/ostro-server/src/meta_view.rs
  modified:
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/derive.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/control/webhooks.rs
    - crates/ostro-meta/src/state.rs
    - crates/ostro-server/tests/config.rs
    - crates/ostro-server/tests/sink.rs
    - crates/ostro-server/tests/common/mod.rs
    - crates/ostro-server/tests/derive_phase5.rs
key-decisions:
  - "The bridge is built after InitCluster and before Node::open; its gate and completion sink resolve partitions through a late-bound PartitionTable (filled by every bridge-enabled sink and by the server after Node::open, closed at shutdown)"
  - "Leadership in the gate = apply machine healthy AND meta's assignment names this node with epoch <= the handle's epoch (so a re-drive during Node::open, before SetPartitionLeader is recorded, is not abandoned)"
  - "Without a bridge the sink strips RegisterEffect/CancelEffect (no orphan effects rows) and keeps the Phase 4 unsupported path; with one, unsupported_input is None for EffectScheduled (B1)"
  - "CancelEffect is derived only for an op identified as EffectScheduled (not for every non-timer op); RegisterEffect is left out of terminal batches like RegisterWait"
  - "HTTP effects resolve secrets through a per-tenant chain (TenantHttp); SQL/object connection configs are operator-owned and use the default-tenant resolver"
  - "Phase 6 config sections are server-side mirrors (the bridge types are Deserialize-only and deny unknown keys); [secrets] gains auto_sealing_key (true in the dev profile) to carry the profile rule"
  - "The bridge holds meta only weakly (presign writes), so meta still stops through Arc::try_unwrap"
metrics:
  duration: "~2h30m"
  completed: 2026-11-20
---

# Phase 6 Plan 14: Server Effect Seam Summary

**The server now routes every effect through the bridge. `BridgeServices` is built once per server: the sealing key ring, the provider chains, the resolver, egress, the rate limiter, the http/sql/object connectors, the `Bridge` and the SQL GC task. `PartitionSink::effect_driver` hands the engine a `ServerEffectDriver`. Completions are submitted as `Cmd::CompleteEffect` carrying the encoded `EffectCompleted` bytes (blob above 256 KiB). A `SendGate` abandons a send when the run is terminal, the effects row is gone or the node no longer leads the partition. In-flight effects are never repaired as `unsupported` (B1); children still are.**

## BridgeServices API (06-15..06-19)

```rust
// crates/ostro-server/src/bridge_services.rs
pub struct BridgeEnv {
    pub meta: Weak<MetaHandle>,              // presign key writes (weak: meta still stops cleanly)
    pub view: ostro_meta::MetaView,          // live meta reads
    pub data_dir: PathBuf,                   // <data_dir>/sealing.key
    pub partitions: Arc<PartitionTable>,     // late-bound partition handles
    pub cluster_id: String,
    pub node_id: u64,
    pub clock: Arc<dyn Clock>,               // completed_at
    pub public_base_url: String,             // ServerConfig::public_base_url(bound addr)
}
impl BridgeServices {
    pub async fn build(cfg: &ServerConfig, env: BridgeEnv) -> Result<Arc<Self>, BridgeBuildError>;
    pub fn sealed_store(&self) -> Option<&SealedStore>;       // None = no key: secrets refused
    pub fn resolver(&self) -> &SecretResolver;                 // default tenant (+ with_sealed_store)
    pub fn object_connector(&self) -> &Arc<ObjectConnector>;   // 06-16: serve_local
    pub fn bridge(&self) -> &Arc<Bridge>;
    pub fn driver(&self) -> &Arc<ServerEffectDriver>;
    pub fn partitions(&self) -> &Arc<PartitionTable>;
    pub async fn shutdown(&self);                              // grace 2 s, GC stop, table close
}
pub struct MetaPresignKeyStore;   // impl PresignKeyStore; new(Weak<MetaHandle>, MetaView, Option<SealedStore>)
pub const SEALING_KEY_MISSING: &str = "sealing_key_missing";
pub struct TenantHttp;            // impl Connector (Http), one HttpConnector per tenant

// crates/ostro-server/src/effects.rs
pub trait EffectHooks: Send + Sync {   // 08-21 fills these in; defaults are no-ops
    fn decorate_live_request(&self, _run: &RunRecord, _op: u64, _req: &mut HttpRequest) {}
    fn on_effect_complete(&self, _run: &RunRef, _op: u64, _kind: EffectKind,
                          _started: SystemTime, _ended: SystemTime, _outcome: &RecordedResult) {}
}
pub struct NoopEffectHooks;
impl ServerEffectDriver { pub fn set_hooks(&self, hooks: Arc<dyn EffectHooks>); pub fn bridge(&self); pub fn seam(&self); }
impl EffectDriver for ServerEffectDriver { start, cancel }
pub struct PartitionSendGate;        // impl SendGate
pub struct PartitionCompletionSink;  // impl CompletionSink
pub struct PartitionTable { new, insert, get, close }

// access from a running server
ServerHandle::bridge() -> &Arc<BridgeServices>;  ServerControlPlane::bridge() -> &Arc<BridgeServices>
```

`build` order: key ring (dev may create `<data_dir>/sealing.key`, path logged, key never) → `SealedStore` (Option) → providers in `[secrets] providers` order (`sealed` only with a key, `file` only with `file_dir`, `vault`/`k8s` only when configured; remote providers shared) → resolver → `Egress` (`HickoryLookup`) → `RateLimiter(default_rate)` → connectors (`TenantHttp`, `SqlConnector`, `ObjectConnector` + `MetaPresignKeyStore`, `public_base_url` defaulted) → `Bridge::new(.., rate_max_wait, gate, sink, limiter)` → `spawn_gc(sql_effects_retention)`.

## B1 change

`crates/ostro-server/src/sink.rs:151` `unsupported_completion(ev, bridge)`: `EffectScheduled(_) if bridge => None` (line 154). `PartitionSink::unsupported_input(&self, ev)` (sink.rs:221) passes `self.bridge.is_some()`. Both callers use it: the post-append path in `append` and `repair_unsupported`, which runs on every journal load. `ChildStarted` keeps its unsupported completion on both paths.

## Completion and gate

- `PartitionCompletionSink::complete`: `completed_at_ms = clock.now_ms()`, `complete_effect_payload(op, &outcome, ms * 1e6)` → `prepare_payload` → `Cmd::CompleteEffect{run_id, op, outcome: Some(ref), completed_at_ms}`. `Accepted`/`Duplicate` → Ok (then `on_effect_complete`); `RunTerminal`/`RunNotFound` → `Dropped`; other rejections → `Dropped` (warn); unregistered partition, payload error, `submit_one` error → `Unavailable`.
- `PartitionSendGate`: `Proceed` only if the partition is registered, the machine is healthy, meta's assignment is (this node, epoch ≤ handle epoch), the run exists and is not terminal, and `query::effect_row(run, op)` is `Some`.
- Leader/epoch accessor used: `PartitionHandle::epoch()` + `machine().check_healthy()` against `MetaState::partition(p)` (`leader`, `epoch`).
- `ServerEffectDriver::start` reads the run record, takes the module from `WorkflowVersion.module` (cached per `(workflow, version)`, never from `ModuleRecord`), applies `decorate_live_request` to the live HTTP copy, records `(partition, run, op) → (kind, SystemTime)` for the hooks and calls `Bridge::start` with `EffectBase{cluster_id, partition, run_id, attempt, module, tenant}`.

## Derive mappings

| event | intent |
|---|---|
| `EffectScheduled` at seq s, ts t (not in a terminal batch) | `register_effect(s, EffectKind::from_request as u32, ceil_ms(t))` |
| `OpCancelled{op}`, op an `EffectScheduled` (batch or `prior`) | `cancel_effect(op)` (blind delete in apply) |

After an accepted append that carried `CancelEffect`, the sink calls `Bridge::cancel` (best effort). `cancel_run` on terminate is not wired; the gate's terminal/row check is the guarantee.

## Config

`[secrets]`, `[egress]` and `[connectors]` parse into `SecretsSection` / `EgressSection` / `ConnectorsSection`, with the 06-DECISIONS defaults. Unknown keys under them are ignored (D18). Each converts with `to_bridge()` (validated at load). `[secrets] auto_sealing_key` is true in the dev profile and false in the server profile. `ServerConfig::public_base_url(bound)` returns `[connectors] public_base_url` or `http://<bound addr>`, with an unspecified IP mapped to loopback. The new sections are in `SECTIONS` (env `OSTRO_EGRESS__*` etc.), and `--print-config` renders arrays and inline tables.

## Test helpers (tests/common/mod.rs)

- `start_server_with_bridge(dir: TempDir, BridgeTestOpts{allow_private_loopback, sealing_key, extra_toml}) -> TestServer`. `BridgeTestOpts::default()` = loopback allowed, `TEST_SEALING_KEY`, no extra TOML. The server runs on **its own tokio runtime**, registered per data dir; `boot`/`shutdown`/`restart` use it automatically. `bridge_server_config(dir, &opts)` gives the config alone.
- `TestServer::crash()` (W10): `Runtime::shutdown_background()` on the server's runtime, then drops the handle on a plain thread. `restart()` boots on the same dir with a fresh runtime. **The test must not hold a partition handle (`handle_of`/`partitions`) across `crash()`.** A held handle keeps that partition's fjall store open, and the restart fails with `FjallError::Locked`. An apply thread may print `A Tokio 1.x context was found, but it is being shutdown` during the crash; that is expected and harmless.
- `put_meta(server, key, record)` creates or replaces by CAS. `put_egress(server, module, &[rules])`, `put_grant(server, GrantSpec)`, `put_sealed_secret(server, record_name, value)` (full record name such as `_/tok`, sealed with the server's key) and `put_connection(server, name, kind, serde_json::Value)`.
- `fx_guest(name)` wraps `guest_wasm`. `TestServer::deploy_named(guest, name, tenant) -> String` returns the deployed module name, which egress policies and grants are keyed by. `deploy_guest` uploads without a name, so its module name is `""` and no egress policy can match it.

## Verification

- `cargo test -p ostro-server --test config --locked`: 33 passed (4 new Phase 6 cases: per-profile defaults, full example, unknown keys ignored, bad values named).
- `cargo test -p ostro-server --lib --locked`: 71 passed (bridge_services presign store creates/reuses/refuses; effects no-op hooks byte-identical; 3 new derive cases plus the updated effect-cancel case).
- `cargo test -p ostro-server --test sink --locked`: 19 passed. The B1 cases: an uncompleted effect is not repaired; a child is still repaired; append registers and cancels the effects row with no unsupported completion; without a bridge no row and the old completion.
- Ad-hoc end-to-end smoke (scratch only, not committed; 06-15 owns the seam e2e). fx-http through a full server against `MockProvider`: the POST `/pay` and a templated `/echo` (secret `[REDACTED]` in the echo) complete Ok, and the effects rows are cleaned up. After `crash()` + `restart()`, a run whose `/pay` was in flight is re-driven (2 requests, same key), completes Ok, and has no `unsupported` completion in the journal.
- `cargo clippy -p ostro-server -p ostro-meta --all-targets --all-features --locked -- -D warnings` and `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. `cargo build --workspace --all-targets --locked`: 0 warnings. `cargo fmt`: clean.
- Full `cargo test -p ostro-server --locked --no-fail-fast` (pre-rebase, base 45cd563): every binary ok except `derive_phase5::wait_token_cancel_and_status_mappings`, whose case "cancel of an effect is no intent" asserted the old mapping. It was updated to `[register_effect(3, 0, 3), cancel_effect(3)]`, after which `--test derive_phase5` passed 16. `cargo test -p ostro-server --features failpoints --test failpoints_phase5 --locked -- --test-threads=1`: 5 passed.
- After rebasing onto main 57e658c: `cargo build -p ostro-server --all-targets --locked` is warning-free.
- `bash scripts/ci-local.sh` was **not** run (about 40 min with 4 other executors building). The crate tests, clippy (with and without failpoints) and the warning-free workspace build were run instead.

## Deviations from Plan

### Auto-fixed issues

**0. [Rule 1 - Expected consequence] `tests/derive_phase5.rs` (outside files_modified)**
- One table case asserted that cancelling an effect derives no intent, which is the mapping this plan replaces. It now expects `register_effect` + `cancel_effect`.

**1. [Rule 3 - Blocking] `executor.rs` edited (outside files_modified)**
- The engine sees `EngineSink` (executor.rs), not `PartitionSink`, so without forwarding `effect_driver()` no effect would ever start. The `PartitionSink` is also constructed in executor.rs.
- Change: `EngineExecutor::with_bridge`, `sink.with_bridge(..)` when set, and `EngineSink::effect_driver` forwarding (+17 lines). No leadership code was needed.
- The phase rule said to STOP in this case. The edit is mechanical, no parallel plan touches executor.rs, and every later Phase 6 plan depends on it, so it was made and is recorded here instead.

**2. [Rule 3 - Blocking] `ostro-meta/src/state.rs`: `MetaState::grants()` iterator**
- `MetaView::grants_for(module)` needs to enumerate grants, and MetaState had no grants accessor. Added a read-only iterator (+6 lines, additive, no format change).

**3. [Rule 3] Config mirror sections instead of embedding the bridge types**
- The layered loader needs `Serialize` (the profile is the base layer). The bridge's `SecretsConfig`/`EgressConfig`/`ConnectorsConfig` are `Deserialize`-only and `deny_unknown_fields`, while D18 wants unknown keys ignored.
- Fix: server-side mirrors with identical keys and defaults, converted through serde, so the bridge parsers and `validate()` still decide.
- `[secrets] auto_sealing_key` was added to carry the dev/server profile rule; `SealingKeyRing::load` takes a `dev` flag and ServerConfig has no profile marker.

**4. [Rule 2] Per-tenant secret chains**
- `SecretResolver` has one chain and ignores `ResolveCtx.tenant` when fetching, but `SealedProvider` serves one tenant.
- Fix: `TenantHttp` routes each HTTP effect to an `HttpConnector` built for the run's tenant, with the per-tenant `SealedProvider` in the configured position.
- SQL/object connections are operator-owned and use the default-tenant chain.

**5. [Rule 1] No orphan effects rows without a bridge**
- `derive` now always emits `RegisterEffect`. A sink without a bridge strips `RegisterEffect`/`CancelEffect`, because nothing would ever delete those rows: the unsupported completion is an input, not a `CompleteEffect`. Leftover rows would also cause spurious P-9 wakes on every restart.

**6. [Plan detail] `CancelEffect` only for effect ops**
- The plan said "every op not identified as a timer". The narrower mapping avoids adding a `CancelEffect` next to every `CancelWait`.
- The must-have ("OpCancelled of an effect derives CancelEffect") holds. The existing test `op_cancelled_effect_is_no_intent` became `op_cancelled_effect_cancels_effect`, and `no_intent_events` no longer lists `EffectScheduled`.

**7. [Plan detail] `BridgeServices::build(cfg, env)` signature**
- `build` takes a `BridgeEnv` instead of `(cfg, meta, blobs, partitions, cluster_id)`.
- No blob store is needed: `prepare_payload` goes through the partition handle. The node id, clock, data dir and public base URL were needed in addition.

**8. [Rule 3] Meta wrapped in `Arc` right after InitCluster**
- The presign store needs `MetaHandle::propose`. The bridge holds it as `Weak`, and error paths after that point use `stop_meta`.

### Integration requested by the orchestrator (08-15)

`control/webhooks.rs` now opens `aes256gcm` verifier secrets with the **node's `SealedStore` from `BridgeServices`**. It is the same ring as every other sealed secret and honours `OSTRO_SEALING_KEY`, `[secrets] sealing_key_file` and the dev auto key. It falls back to the old lazy discovery (`OSTRO_SEALING_KEY` / `<data_dir>/sealing.key`) only when the node started without a key. The existing webhooks test writes `sealing.key` after start and relies on that fallback.

The bridge `SecretResolver` itself is **not** used for webhooks. Its chain resolves `<tenant|_>/<name>` and adds env/file/vault lookups and the 8-byte template minimum, none of which fit an exactly named verifier secret.

### Notes for later plans

- **06-15:** use `start_server_with_bridge` + `deploy_named` (not `deploy_guest`) + `put_egress(module, &[&mock.base_url()])`. Drop partition handles before `crash()`.
- **06-16:** `ServerControlPlane::bridge()` gives you `sealed_store()` (None → refuse `set_secret`), `resolver()` and `object_connector().serve_local(..)` for the `/v1/objects` route. Seal user secrets as `<tenant|_>/<name>`; the presign keys are `__ostro/presign/<conn>` and are written only by `MetaPresignKeyStore`.
- **08-21:** call `ServerHandle::bridge().driver().set_hooks(Arc<dyn EffectHooks>)`; the hook signatures above are stable.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1+2 | 453bd42 | feat(06-14): server effect seam: BridgeServices, EffectDriver adapter, CompleteEffect, SendGate |

Tasks 1 and 2 share `bridge_services.rs` (the Bridge needs Task 2's gate and sink), so they went into one commit, like 06-03/06-06.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/{bridge_services,effects,meta_view}.rs
- FOUND: feat commit 453bd42 on main (16 files, 2506+/60-)
