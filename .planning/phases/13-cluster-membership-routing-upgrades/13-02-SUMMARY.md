---
phase: 13-cluster-membership-routing-upgrades
plan: 02
subsystem: server seam + config + client/CLI stubs + conformance harness API
tags: [seam, p13, cluster, boot, voter-check, join-marker, routing, config, cli, harness]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 contracts and stubs (compat, contracts, handoff, shutdown, placement/membership stubs, ostro_api::phase13, RequestMeta, ApiExtensions slots); 13-04 compat/upgrade (landed on main during this plan, consumed via start_advertiser_with / start_finalizer_with / upgrade_view)"
provides:
  - "crates/ostro-server/src/p13/mod.rs: the single Phase 13 seam (final after 13-02)"
  - "Fixed call sites in cluster.rs (join marker first, boot_mode, both voter checks, joined TLS, boot fence, late-bound listener services) and server.rs (boot fence before Node::open, wrap_control_plane, fill_extensions, cluster_listener_services, start, before_shutdown first)"
  - "D-P13-23 config keys with D-P13-26 defaults ([cluster], [api], [modules], [workers]) + validate_phase13"
  - "ControlCtx::routed: every in-process ControlPlane consumer goes through the wrapped control plane"
  - "Stub hook files with fixed signatures for 13-04/09/10/11/12/13/14/15/16/17; ostro-client cluster.rs, ostro-cli cluster/migrate_runs/modules_lc, main.rs `cluster join` interception"
  - "P13Harness trait + NodeOpts; stub oracle/scenario/mutant modules registered in the conformance lists"
affects: [13-03, 13-04, 13-05, 13-08, 13-09, 13-10, 13-11, 13-12, 13-13, 13-14, 13-15, 13-16, 13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "one hook per later plan, called from the final seam; stubs keep Phase 12 behaviour exactly"
    - "late-bound gRPC fallback (ArcSwapOption<Routes>) behind the Phase 12 cluster-listener services, detached in stop_tasks"
    - "Phase 13 context holds the node weakly; server drops it (and joins P13Services) before Node::shutdown"
key-files:
  created:
    - crates/ostro-server/src/p13/{mod,cluster_api,module_lifecycle,node_ops,joiner,join,membership,status,promote,prewarm,module_gc,module_refs}.rs
    - crates/ostro-server/src/route/{mod,table,peer,fanout,consistency}.rs
    - crates/ostro-server/src/control/{migrate,module_retire}.rs
    - crates/ostro-client/src/cluster.rs
    - crates/ostro-cli/src/commands/{cluster,migrate_runs,modules_lc}.rs
    - crates/ostro-repl-conformance/src/harness/p13.rs
    - crates/ostro-repl-conformance/src/mutants_p13.rs
    - crates/ostro-repl-conformance/src/oracle/{p13_ryw,p13_membership,p13_upgrade,p13_gc}.rs
    - crates/ostro-repl-conformance/src/scenarios/{membership,reassign,handoff,routing,ryw,version_gate,module_gc,promotion,migrate,integration_p13,process_p13}.rs
    - crates/ostro-repl-conformance/tests/p13_skeleton.rs
  modified:
    - crates/ostro-server/src/{lib,server,cluster,config,p11}.rs
    - crates/ostro-server/src/control/{mod,runs,modules,webhooks,p11_dlq}.rs
    - crates/ostro-server/src/services/{bulk,dlq_hook}.rs
    - crates/ostro-server/tests/config.rs
    - crates/ostro/src/main.rs
    - crates/ostro-client/src/lib.rs
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-cluster/src/placement/controller.rs
    - crates/ostro-partition/src/handoff.rs
    - crates/ostro-repl-conformance/src/{lib,harness/mod,oracle/mod,scenarios/mod}.rs
key-decisions:
  - "[cluster] advertise NOT changed: the key already exists as the Phase 7 Vec<String> (worker-advertised names + node cert SANs) used by tls/mod.rs, workers/service.rs, cluster.rs, the tier-C harness and config tests. The plan's Option<{api, cluster, repl}> shape would break that frozen user-facing key; left for an orchestrator/user decision (see Open issues)"
  - "Boot fence call: ClusterServices::boot_fence (cluster.rs, calls p13::boot_fence) invoked by server.rs immediately before Node::open (Node::open lives in server.rs, not cluster.rs)"
  - "Listener services attach late (the cluster listener binds before the node and control plane exist): a fallback service behind the Phase 12 routes; unknown services still answer UNIMPLEMENTED"
  - "members_known is computed by a hook (p13::members_known → joiner::members_known, stub false) so 13-09 decides it for both PreStart (no meta view) and PostReady"
  - "joined_tls / tls_complete hooks: a joined node has no CA key, so the TLS step takes the joined certificate set from joiner::joined_tls when present (stub None = Phase 12 CA path)"
  - "The effective node id (marker → config) is written back into the server config, so every later consumer (p7/TLS, bridge, node) sees it"
  - "Consistency hooks are async and take &P13Ctx (in-process multi-node tests make process globals unusable)"
  - "13-01 had not created placement::controller::{request_reassign, request_rebalance} that 13-10 expects; added as stubs with PlacementTrigger, PlacementError and PlacementConfig::level_max so the final cluster_api.rs can delegate"
  - "impl HandoffControl for Node stub added in ostro-partition handoff.rs (13-11 replaces the body) so the final seam can build LocalHandoff over the node"
  - "upgrade_status reads 13-04's upgrade::upgrade_view (landed on main mid-plan); advertiser/finalizer started through the *_with variants with the one injectable clock"
requirements-completed: []
duration: "about 4 h 50 min (incl. a 3 h 15 min ci-local)"
completed: 2026-11-24
---

# Phase 13 Plan 02: Phase 13 server seam, config, client/CLI stubs, P13Harness Summary

**The Phase 13 seam is in place and final: `p13/mod.rs` with its call sites in `cluster.rs` (join marker first, boot rule, both voter checks, joined TLS, boot fence, late-bound listener services) and `server.rs` (fence before `Node::open`, routed control plane for every consumer, Phase 13 API slots, background tasks, controlled shutdown first); the D-P13-23 config keys with D-P13-26 defaults; client/CLI stubs and the `ostro cluster join` interception; the `P13Harness` API with stub oracle/scenario/mutant modules. Every hook is a stub that keeps the Phase 12 behaviour exactly; all targeted suites are green.**

## Seam signatures as landed (`crates/ostro-server/src/p13/mod.rs`)

```rust
pub struct JoinMarker { pub node_id: u64, pub cluster_id: String, pub seeds: Vec<ostro_cluster::address_book::Seed> }
pub struct DataDirProbe { pub data_dir: PathBuf, pub meta_dir: PathBuf, pub meta_blank: bool,
    pub has_partition_data: bool, pub join_marker: Option<JoinMarker>, pub tls_complete: bool }
pub enum BootMode { Fresh, Existing, Joiner }
pub enum VoterCheckPhase { PreStart, PostReady }
pub type ConfView = ostro_cluster::identity::ConfStateView;            // { voters, learners }
pub struct JoinedTls { pub ca_pem: String, pub cert_chain_pem: String, pub key_pem: String }
pub struct JoinArgs { seed, token, ca_hash, data_dir: Option<PathBuf>, config: Option<PathBuf>,
    advertise_api / advertise_cluster / advertise_repl: Option<String>, no_start: bool }
pub struct JoinPrepared { pub node_id: u64, pub cluster_id: String, pub data_dir: PathBuf }

pub fn join_identity(data_dir: &Path) -> Result<Option<JoinMarker>, BootError>      // -> joiner::join_identity (13-09)
pub fn tls_complete(data_dir: &Path) -> bool                                        // -> joiner::tls_complete (13-09)
pub fn joined_tls(data_dir: &Path) -> Result<Option<JoinedTls>, BootError>          // -> joiner::joined_tls (13-09)
pub fn boot_mode(cfg: &ServerConfig, probe: &DataDirProbe) -> Result<BootMode, BootError> // -> joiner::boot_mode (13-09)
pub fn members_known(data_dir: &Path, view: Option<&MetaView>) -> bool              // -> joiner::members_known (13-09)
pub fn voter_check(view: &ConfView, cfg: &ServerConfig, members_known: bool, phase: VoterCheckPhase)
    -> Result<(), BootError>                                                        // -> joiner::voter_check (13-09)
pub async fn boot_fence(meta: &MetaHandle, level_max: u32) -> Result<(), CompatError> // -> ostro_cluster::compat::boot_fence (13-04)
pub fn wrap_control_plane(ctx: &Arc<P13Ctx>, local: Arc<ServerControlPlane>) -> Arc<dyn ControlPlane> // -> route::wrap_control_plane (13-12)
pub fn fill_extensions(ctx: &Arc<P13Ctx>, ext: &mut ApiExtensions)   // cluster = P13ClusterApi, module_lifecycle = P13ModuleLifecycle, then route::wrap_extensions (13-12)
pub fn cluster_listener_services(ctx: &Arc<P13Ctx>) -> Vec<Routes>  // membership::service (13-09) + NodeOpsService::routes (13-03) + route::node_api_services (13-12)
pub async fn start(ctx: Arc<P13Ctx>) -> P13Services                 // see "start" below
pub async fn before_shutdown(ctx: &P13Ctx)                          // -> ostro_cluster::shutdown::controlled_shutdown (13-11)

pub struct P13Ctx { pub cfg: Arc<ServerConfig>, pub meta: Arc<MetaHandle>, pub cluster: Arc<ClusterServices>,
    node: Weak<Node>, pub runtime: Arc<Runtime>, pub local_cp: Arc<ServerControlPlane>, pub level_max: u32,
    pub clock: Arc<dyn Clock>, pub handoff: Arc<LocalHandoff>, pub handoff_client: Arc<dyn HandoffClient>,
    pub module_refs: Arc<LeaderModuleRefs>, pub placement_trigger: PlacementTrigger, fatal: watch::Sender<Option<String>> }
impl P13Ctx { fn new(P13Init) -> Arc<Self>; fn node_id(); fn node() -> Option<Arc<Node>>; fn fatal(String);
    fn fatal_watch(); fn node_version() -> NodeVersion }
```

`start` runs, on every node: `compat::start_advertiser_with(meta, node_id, ctx.node_version(), ctx.clock, AdvertiserConfig::default())` and `upgrade::start_finalizer_with(meta, FinalizerConfig{auto_finalize, ..}, ctx.clock)` (13-04, landed), `compat::start_running_fence(meta, level_max, on_fatal → ctx.fatal)` (13-04), `membership::start(&ctx, stop)` (13-09; stub = the 13-01 `MembershipService` idle loop with `promote_lag` / `join_orphan_timeout`), `PlacementController::new(meta, handoff_client, PlacementConfig{max_concurrent_moves, leader_rebalance_interval, level_max}).with_trigger(ctx.placement_trigger).start(stop)` (13-10), `module_gc::start` (13-15), `prewarm::start` (13-14).

`ctx.node_version()` = `{ostro_version: CARGO_PKG_VERSION, level_min: LEVEL_MIN, level_max: ctx.level_max, wasmtime: ostro_engine::cache::WASMTIME_VERSION, event_codec_ver: EVENT_CODEC_VER, wit_lines: LinkerSet::default().lines() as "0.1", reported_at_ms: clock}`.

`ctx.fatal(msg)` makes `ServerHandle::wait()` return and `ServerHandle::fatal()` report the reason; the binary then shuts down gracefully and exits 1 with the reason.

### Both voter-check call sites and the boot rule (`cluster.rs`, `ClusterServices::start`)

0. `let marker = p13::join_identity(data_dir)?` (first, before `bind_node_id` and any TLS load). With a marker the effective config gets `node.node_id = marker.node_id` (node.toml → marker → config: `bind_node_id` refuses a node.toml that disagrees), and every later step sees it.
1. `bind_node_id`.
2. `identity::resolve_cluster_id(configured, node.toml, CA, ClusterInfo, probe.blank || marker.is_some())`; `resolved_cluster_id(&source, marker)`: `GenerateFresh` + marker → the marker's id (never generated while a marker exists), a present source that disagrees with the marker → `BootError::ClusterIdConflict`.
3. **Boot rule:** `p13::boot_mode(cfg, &DataDirProbe{data_dir, meta_dir, meta_blank: probe.blank, has_partition_data, join_marker, tls_complete: p13::tls_complete(data_dir)})`. Stub = Phase 12: blank meta + partition data → `joiner_error` (wiped-voter with peers / meta dir missing); blank → `Fresh`; else `Existing`.
4. **PreStart voter check:** `p13::voter_check(&probe.conf.unwrap_or_default(), cfg, p13::members_known(data_dir, None), PreStart)`. Stub = `identity::check_meta_voters` (empty persisted voters = blank = Ok).
5. `bind_cluster_id`. `BootMode::Joiner` (13-09 only): `voters = []`, meta peers += marker seeds.
6. TLS: `p13::joined_tls(data_dir)?` → the joined certificate set, else the Phase 12 `[cluster.tls]` import / own CA + `load_or_issue_node_cert`.
7. Transport: the TCP `AddressBook` from `[[cluster.peers]]` + marker seeds (kept in `ClusterServices::address_book()` for 13-09). Listener: the Phase 12 services, MetaPeer limit = `[cluster] meta_max_message_bytes` (server and outbox), then a fallback to the late-bound Phase 13 services.
8. Meta start; Joiner waits `wait_ready` within `[cluster] join_timeout` (error: "joining: no metadata leader reached this node within [cluster] join_timeout").
9. **PostReady voter check:** `p13::voter_check(&ConfView{voters, learners} from meta.raft().status(), cfg, p13::members_known(data_dir, Some(meta.view())), PostReady)`. Stub = `identity::check_voters_after_ready`.
10. `InitCluster` unless `BootMode::Joiner` (a joiner waits, within `join_timeout`, for the seed's `ClusterInfo` + token key to apply locally, then checks the partition count), `check_cluster_info`, own Member upsert, controller (a joiner's voter set = the group's).

`ClusterServices` gains `node_id()`, `boot_mode()`, `address_book()`, `boot_fence(level_max)` (calls `p13::boot_fence`; server.rs calls it immediately before `Node::open` and aborts the start on error), `attach_p13_services(Vec<Routes>)`; `stop_tasks()` detaches the Phase 13 listener services first.

### Other fixed call sites

- `control/runs.rs` start path: `module_retire::check_not_retired(view, &wf.module_sha256)?` after the version is resolved; `route::table::pinned_partition()` (honoured only for an unkeyed start, bounded by the partition count).
- `control/modules.rs`: upload (fresh module) calls the local `fn upload_gate(view: &MetaView, selection: &WitSelection) -> Result<(), ApiError>` (stub Ok; the selection is the 0.1 / LEVEL_MIN placeholder until 13-14 takes it from `linker_set::select`); activation calls `check_not_retired` for the version's module.
- `route::consistency` (13-13): `async fn before_partition_read(ctx: &P13Ctx, p) -> Result<(), ApiError>`, `async fn before_meta_read(ctx) -> Result<(), ApiError>`, `fn token_after_partition_write(ctx, p) -> Option<String>`, `fn token_after_meta_write(ctx) -> Option<String>` (async + `&P13Ctx`: the hooks need roles, meta and the clock, and tier-B nodes share one process). 13-12's router calls them.
- `main.rs`: `ClientCommand::Cluster{command: ClusterCmd::Join(a)}` → `p13::joiner::run_join(JoinArgs)` (stub: "`ostro cluster join` is implemented in 13-09", exit 1); on success the server profile starts on the prepared data dir (`--config`, `--data`) unless `--no-start`.

## In-process ControlPlane consumers switched to the wrapped instance

`ControlCtx` gains `pub routed: Arc<dyn ControlPlane>` (defaults to `partitions`; `server.rs` sets it with `with_routed(p13::wrap_control_plane(..))`). Switched (`ControlPlane` calls only; partition handles still come from `ControlCtx::partitions`):

| Consumer | File | Calls now through `routed` |
|----------|------|-----------------------------|
| REST + gRPC API router | `p11.rs` (`wire`) | the whole `ostro_api::router_with` control plane |
| Embedded UI | `p11.rs` (`wire`) | `UiDeps.cp` |
| Webhook triggers (`HookIngress`, `WebhookApi`) | `control/webhooks.rs` (`cp()`) | start_run, signal_run, complete_token, get_run |
| on_failure hooks | `services/dlq_hook.rs` | start_run of the hook run |
| DLQ requeue (resume) | `control/p11_dlq.rs` | retry_run |
| Bulk jobs | `services/bulk.rs` (`act`) | cancel_run, terminate_run, retry_run, pause_run, resume_run, signal_run |

Cron fires do not go through the control plane (the schedule owner partition submits `ScheduleFire`, Phase 8), and `trigger_schedule` is a `ScheduleApi` method routed by 13-12's `wrap_extensions`. Rerun / DLQ restart / bulk restart build commands on partition handles (`p11_rerun`, `restart_with_key`); 13-12 routes them at the `OpsApi` layer.

## Stub-module map (file → owning plan)

| File | Owner | Stub behaviour |
|------|-------|----------------|
| `ostro-server/src/p13/mod.rs`, `p13/cluster_api.rs`, `p13/module_lifecycle.rs` | 13-02 (final) | delegation |
| `p13/node_ops.rs` | 13-02 delegation, 13-03 (generated types; `routes()` → `Some`) | local message shapes, `routes() = None` |
| `p13/joiner.rs` | 13-09 | no marker; Phase 12 boot rule + voter checks; `run_join` Err |
| `p13/join.rs` (`handle_join`, `create_node_token`, `ensure_cluster_ca`) | 13-09 | Unimplemented / Ok(()) |
| `p13/membership.rs` (`start`, `service`, `request_remove`, `request_drain`) | 13-09 | idle loop / None / Unimplemented |
| `p13/status.rs` | 13-09 | Unimplemented |
| `p13/promote.rs` (`init`) | 13-16 | Unimplemented |
| `p13/prewarm.rs` (`start`) | 13-14 | no task |
| `p13/module_gc.rs` (`start`, `run_now`) | 13-15 | no task / Unimplemented |
| `p13/module_refs.rs` (`LeaderModuleRefs::new(Weak<Node>, Arc<dyn PartitionRoles>)`) | 13-15 | every sha referenced, every partition uncovered |
| `route/{mod,table,peer,fanout}.rs` | 13-12 | pass-through, no services, no pin |
| `route/consistency.rs` | 13-13 | Ok / None |
| `control/module_retire.rs` (`retire`, `is_retired`, `check_not_retired`) | 13-15 | Unimplemented / false / Ok |
| `control/migrate.rs` (`migrate_runs`) | 13-17 | Unimplemented |
| `control/modules.rs` `upload_gate` | 13-14 | Ok |
| `ostro-cluster/src/placement/controller.rs` `request_reassign`, `request_rebalance`, `PlacementTrigger`, `PlacementError`, `PlacementConfig::level_max` | 13-10 | NotImplemented |
| `ostro-partition/src/handoff.rs` `impl HandoffControl for Node` | 13-11 | prepare = Failed("implemented in 13-11"), in_handoff = false |
| `ostro-client/src/cluster.rs` | 13-16 | 501 `unimplemented` |
| `ostro-cli/src/commands/{cluster,migrate_runs,modules_lc}.rs` | 13-16 | clap shape final; verbs print the JSON body (stub client → error) |
| `ostro-repl-conformance/src/harness/p13.rs` (trait) | 13-02 (final), impls 13-08 | — |
| `oracle/p13_{ryw,membership,upgrade,gc}.rs` | 13-08 (fixes 13-18) | empty |
| `scenarios/{membership,reassign,handoff,routing,ryw,version_gate,module_gc,promotion,migrate}.rs` | 13-09, 13-10, 13-11, 13-12, 13-13, 13-14, 13-15, 13-16, 13-17 | `all() = vec![]` |
| `scenarios/integration_p13.rs`, `mutants_p13.rs` | 13-18 | empty |
| `scenarios/process_p13.rs` | 13-19 | empty (`Vec<Scenario<ProcCluster>>`, in `all_process()`) |

## Config (D-P13-23, defaults D-P13-26; `config.rs`, append only, `deny_unknown_fields` kept)

`[cluster]` `auto_finalize = true`, `join_timeout = "120s"`, `join_orphan_timeout = "25h"` (validated ≥ `[workers] join_token_ttl`), `promote_lag = 64`, `handoff_grace = "2s"`, `handoff_catchup_timeout = "10s"`, `shutdown_handoff_timeout = "30s"`, `leader_rebalance_interval = "5m"`, `max_concurrent_moves = 2` (≥ 1), `meta_max_message_bytes = 268435456` (≥ 4 MiB); `[api]` `unkeyed_start_placement = "local" | "uniform"`, `strict_reads = false`, `lease_margin = 0.2` (0 < m < 0.5); `[modules]` (new section in `SECTIONS`) `gc_interval = "10m"` (non-zero), `gc_grace = "10m"`, `gc_protect_resumable = true`; `[workers]` `version_policy = "compatible" | "exact"`. Same defaults in both profiles. `ServerConfig::validate_phase13()` runs from `resolve_and_validate`. `tests/config.rs`: `phase13_defaults_match_d_p13_26_in_both_profiles`, `phase13_keys_parse_and_round_trip_in_both_profiles`, `phase13_invalid_values_are_rejected_and_named`, `phase13_env_overrides`.

## Task Commits

1. `048ec0d` feat(13-02): Phase 13 server seam + config — **landed empty** (zsh passed the file list as one word; main is never rewritten, so it stays as a no-op commit, like 13-01's f006010)
2. `1f46a38` feat(13-02): Phase 13 server seam + config (content; 048ec0d landed empty) — 41 files (server seam, config, CLI/client stubs, controller and handoff stubs)
3. `6a509c1` feat(13-02): client/CLI stubs + P13Harness API — 22 conformance files

## Verification

- `cargo build --workspace --all-targets --locked`: warning-free (at the 489ab0d base and again at 6a509c1). `cargo clippy -p ostro-server -p ostro-cli -p ostro-client -p ostro -p ostro-repl-conformance -p ostro-cluster -p ostro-partition --all-targets --locked -- -D warnings`: clean. `cargo fmt --all -- --check`: clean.
- Plan grep checks: `p13::boot_fence`, `p13::join_identity`, `PostReady` in cluster.rs; `p13::voter_check` twice in cluster.rs; `p13::wrap_control_plane` and `p13::before_shutdown` in server.rs; `trait P13Harness` in harness/p13.rs: all present.
- Targeted tests (every run ≥ 1 passed): `-p ostro-server --test config` 48 (4 new Phase 13 tests), `-p ostro-server --lib` 175 (incl. the real-server `injected_clock_reaches_cluster_init_and_services` / `p7_seam_is_wired_and_behaviour_preserving` and the new `node_ops` tests), `-p ostro-server --test cluster_boot / cluster_api / bootstrap / webhooks / dlq_api / bulk_ops / start_run_api / modules_api / p11_contract / triggers_e2e` all green, `-p ostro` (every binary incl. ready_line, worker_e2e, worker_failover, ui_e2e, server_config) green, `-p ostro-cli` 80 + 10 + 6, `-p ostro-client --lib` 11 (new `every_stub_is_unimplemented`), `-p ostro-repl-conformance --test p13_skeleton` 3, `--test skeleton` 2, `--test harness_smoke` 2, `--test placement` 1 (3-node in-process cluster through the new listener fallback), `-p ostro-partition --lib handoff` 1.
- **Plan-level `bash scripts/ci-local.sh --no-musl --no-polyglot`** at `6a509c1` (worktree under the session scratchpad, `nice -n 15`, `CARGO_BUILD_JOBS=4`, slot-6, started 15:23 UTC): every step PASS except `one log path + P12 grep gates` FAIL, whose only hits were `SystemClock` defaults in 13-04's `ostro-cluster/src/{compat,upgrade}.rs` (not 13-02 files). 13-04 fixed them on main in `20ddfb0` (signatures of the `*_with` functions this seam calls unchanged); at `20ddfb0` `scripts/p12-grep-gates.sh` exits 0 and `cargo check -p ostro-server -p ostro -p ostro-repl-conformance --all-targets --locked` is warning-free. Step times: fmt 3 s, clippy 79 s, build 73 s, sdk 28 + 30 s, order-saga 12 s, cargo test 5643 s (642 test binaries, 4884 passed, 0 failed), clients go 5 s, sleepers 106 s, failpoint list sync PASS, failpoint suites 6617 s, replication mutant matrix 239 s, server campaign smoke (20 kills) 829 s, phase 7 failpoint legs 65 s, deny / vendored / fixtures / forbidden / workflow PASS; external PG/MySQL/S3 SKIP (no env), polyglot SKIP (flag), musl SKIP (flag).
- No ci.yml change is needed (no new failpoint crate or target); no hunk for the orchestrator.

## Deviations from Plan

### Auto-fixed / adapted

1. **[Rule 3] Task split:** the client/CLI stubs went into the Task 1 commit, because the `cluster join` interception in `main.rs` (Task 1) needs the CLI variant to compile; the Task 2 commit holds the conformance seam only.
2. **[Rule 3] Files outside `files_modified`:** `p11.rs` (`wire` takes the Phase 13 extension filler and uses `ControlCtx::routed` for the API and UI; `#[allow(clippy::too_many_arguments)]`), `control/webhooks.rs`, `control/p11_dlq.rs`, `services/dlq_hook.rs`, `services/bulk.rs` (consumers switched to the routed instance), `ostro-cluster/src/placement/controller.rs` (13-01 had not created the `request_reassign` / `request_rebalance` stubs 13-10 expects; `PlacementConfig::level_max` for its `compat::require`; `PlacementTrigger` / `PlacementError`), `ostro-partition/src/handoff.rs` (stub `impl HandoffControl for Node`, needed to build `LocalHandoff` from the final seam). No Cargo change.
3. **[Adaptation] Signatures vs the plan text:** `LeaderModuleRefs::new` takes `Weak<Node>` (the cluster listener outlives the node; a strong reference would break `Node::shutdown`); `boot_fence` is reached through `ClusterServices::boot_fence` because `Node::open` is in `server.rs`; the listener services attach late through a fallback route (the listener binds before the node and control plane exist); added hooks `members_known`, `tls_complete`, `joined_tls` (13-09 needs them to lift the checks and to boot a joined node that holds no CA key); `controlled_shutdown` is the 13-01 name of the plan's `shutdown::controlled`; `run_join` returns `JoinPrepared` and the binary starts the server (signal handling lives in `main.rs`); consistency hooks take `&P13Ctx` and the two read hooks are async.
4. **[Adaptation] 13-04 landed during this plan:** `start` uses `start_advertiser_with` / `start_finalizer_with` with the one injectable clock, and `upgrade_status` uses `upgrade::upgrade_view`.
5. **[Rule 3] Environment:** `target/slot-6` had no `CACHEDIR.TAG`, so `triggers_e2e::cron_cli_policies` read `slot-6` as a target triple (the 12-18 symptom); copied cargo's standard tag file into the slot (no repo change), re-run green.
6. **Worktree / target / ci-local flags:** scratchpad worktree, `CARGO_TARGET_DIR=target/slot-6`, `nice -n 15`, `CARGO_BUILD_JOBS=4`, `CARGO_INCREMENTAL=0`; plan-level ci-local with `--no-musl --no-polyglot` (campaign running on the host).

### Not done (needs a decision)

- **`[cluster] advertise` shape.** D-P13-23 / the plan want `advertise: Option<{api, cluster, repl}>`, but `[cluster] advertise` already exists as the Phase 7 `Vec<String>` (names advertised to workers and the node-certificate SANs; `tls/mod.rs`, `workers/service.rs`, `cluster.rs`, the tier-C harness and three config tests use it). Changing its type breaks a shipped user-facing key; adding a different key would use a name outside RESERVATIONS §11. Left unchanged. Options: (a) keep the Vec and let 13-16 / 13-08 take the cluster address from it (`InitRequest.advertise` is already a single string); (b) accept both forms (array = Phase 7, table = `{api, cluster, repl}` + SANs derived) with a custom deserializer in a follow-up 13-02 patch; (c) reserve a new key (e.g. `[cluster.advertise_addrs]`) in RESERVATIONS.

## Next Phase Readiness / handover

- **13-03:** swap the message shapes in `p13/node_ops.rs` for the generated `ostro.cluster.v1.NodeOps` types and make `NodeOpsService::routes()` return `Some(Routes::new(NodeOpsServer::new(self)))` (the service is already mounted through `cluster_listener_services` → `attach_p13_services`); keep `authz::require_node` first. `membership::service` (13-09) and `route::node_api_services` (13-12) are mounted the same way. Unknown services on the cluster listener still answer UNIMPLEMENTED (the late fallback forwards to tonic's default).
- **13-08:** `P13Harness` and `NodeOpts` are in `harness/p13.rs`; `ServerOptions::with_level_max_override(Option<u32>)` exists (`level_max_override`, doc(hidden)); `ServerHandle::p13()` exposes the context and `ServerHandle::fatal()` the fail-stop; `ClusterServices::node_id()` is the effective id. `add_node` / `cluster_status` hit the stubs (501 `unimplemented` once 13-03 mounts the REST routes; 404 before). The `[cluster] advertise` decision above affects `start_single`'s config (today: the Phase 7 `Vec<String>`).
- **13-09:** implement only `p13/{joiner,join,membership,status}.rs`. The boot path already does everything else: marker → effective node id, marker as cluster-id source, Joiner meta start with seeds and `join_timeout`, no `InitCluster`, seed `ClusterInfo` wait, PostReady with learners. `members_known(data_dir, None)` is the PreStart input (no meta view yet). `ClusterServices::address_book()` is the TCP transport's book. `p13::P13Ctx::fatal` is the removed-node stop. p7's worker CA (`tls/mod.rs`, `p7::init`) still uses `ClusterCa::load_or_create` and `cfg.node.node_id` (now the effective id): a joined node without the CA key needs a 13-09 deviation there for worker joins.
- **13-10 / 13-11 / 13-12 / 13-13 / 13-14 / 13-15 / 13-16 / 13-17:** the hook files in the map above; `cluster_api.rs` and `module_lifecycle.rs` are final and delegate to them. 13-07 must keep `LinkerSet: Default` + `lines()` (used by `P13Ctx::node_version`).
- Ownership rule for later plans: anything stored in the cluster-listener services may hold `Arc<P13Ctx>` (it is detached in `stop_tasks`); background tasks must stop on the `P13Services` token; nothing may hold `Arc<Node>` strongly.

## Metrics

- Started about 15:25 CET, finished about 20:15 CET (2026-11-24); 2 tasks, 63 code files (41 + 22) plus this SUMMARY, ROADMAP and STATE.

## Self-Check

PASSED: every created file listed above exists at `6a509c1`; commits `048ec0d` (empty), `1f46a38`, `6a509c1` are on main.
