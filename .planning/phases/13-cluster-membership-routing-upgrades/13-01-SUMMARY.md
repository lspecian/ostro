---
phase: 13-cluster-membership-routing-upgrades
plan: 01
subsystem: planning-gate + library contracts
tags: [gate, reservations, decisions, cargo, features, contracts, stubs, compat, membership, handoff, routing]
requires:
  - phase: 12-replicated-partition-logs
    provides: "18/18 SUMMARYs; 12-VERIFICATION human_needed (104/105 truths, 4/4 SC, no gaps; QA-01 10k campaign re-run pending under D-P12-28); ClusterServices, MetaHandle, PartitionRoles, AssignmentSource, AddressBook, identity checks"
  - phase: 11-run-operations-embedded-ui
    provides: "18/18 SUMMARYs, 11-VERIFICATION human_needed (browser/UI + remote CI + user review only); OpsApi/InventoryApi, ApiExtensions slots"
  - phase: 10-developer-cli-test-kits
    provides: "20/20 SUMMARYs, 10-VERIFICATION human_needed (user decisions, macOS, remote CI, publishing); replay_check, ostro-client"
  - phase: 09-polyglot-sdks-conformance
    provides: "20/20 SUMMARYs, 09-VERIFICATION human_needed (publishing, remote CI, upstream toolchain, native perf)"
provides:
  - "PHASE-13 GATE: accepted by orchestrator, Phase 12 campaign pending (D-P12-28); Phases 9-11 complete"
  - "Phase 7/10/11/12 seam signatures and the D-P13-14 route classification table (below) for 13-02..13-20"
  - "RESERVATIONS.md P13 rows (Cmd 43, RejectReason 23/24 + placeholders, MetaCommand 7 placeholder + 8, ext 96-102, §11 names)"
  - "STATE.md: D-P13-1..29, [USER REVIEW] D-P13-4 / D-P13-15, [HUMAN] Phase 13 items, Phase 13 shared-file rule"
  - "Every Phase 13 Cargo feature/dependency pinned (manifests first): ostro-types/level-emulation, ostro-engine/wit-next-fixture, failpoints chains, repl-conformance -> ostro-api/ostro-client"
  - "Library contracts + stubs in ostro-types, ostro-meta, ostro-partition, ostro-cluster, ostro-engine, ostro-api"
affects: [13-02, 13-03, 13-04, 13-05, 13-06, 13-07, 13-08, 13-09, 13-10, 13-11, 13-12, 13-13, 13-14, 13-15, 13-16, 13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "one stub file per later module (`//! Phase 13: implemented in 13-NN` + fixed signatures); stubs keep Phase 12 behaviour (gates allow, fences pass, loops idle, handoffs Aborted)"
    - "P-15 placeholders (`#[doc(hidden)] Reserved<Phase>_<n>`) below a newly claimed declaration-indexed ordinal"
    - "commits through a private index built on the current main + CAS update-ref with a merge-base ancestor assert"
key-files:
  created:
    - crates/ostro-types/src/compat.rs
    - crates/ostro-partition/src/handoff.rs
    - crates/ostro-partition/src/apply/repin.rs
    - crates/ostro-partition/tests/p13_format.rs
    - crates/ostro-cluster/src/contracts.rs
    - crates/ostro-cluster/src/compat.rs
    - crates/ostro-cluster/src/upgrade.rs
    - crates/ostro-cluster/src/handoff.rs
    - crates/ostro-cluster/src/shutdown.rs
    - crates/ostro-cluster/src/membership/{mod,reconcile,service,remove}.rs
    - crates/ostro-cluster/src/placement/{mod,plan,reassign,leaders,controller}.rs
    - crates/ostro-engine/src/host/linker_set.rs
    - crates/ostro-api/src/phase13.rs
    - crates/ostro-api/src/request_meta.rs
    - crates/ostro-api/src/grpc_client.rs
  modified:
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
    - Cargo.lock
    - crates/{ostro-types,ostro-engine,ostro-server,ostro,ostro-repl-conformance}/Cargo.toml
    - crates/ostro-types/src/{lib,command}.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/golden/commands.txt
    - crates/ostro-meta/src/{command,ext,state}.rs
    - crates/ostro-meta/tests/meta_commands_golden.rs
    - crates/ostro-meta/tests/golden/meta_commands.txt
    - crates/ostro-partition/src/{lib.rs,apply/mod.rs,apply/outcome.rs,store/mod.rs}
    - crates/ostro-cluster/src/{lib,address_book,blob}.rs
    - crates/ostro-engine/src/host/mod.rs
    - crates/ostro-api/src/{lib,phase8}.rs
    - crates/ostro-repl-conformance/src/lib.rs
key-decisions:
  - "Gate recorded as accepted by orchestrator with the Phase 12 QA-01 10k campaign re-run pending (D-P12-28); Phase 12 NOT marked complete by 13-01"
  - "MetaCommand 7 = #[doc(hidden)] ReservedP12_7 {} (apply Invalid(\"reserved\")) so AlterReplicas is postcard index 8"
  - "RejectReason placeholders ReservedP7_16, ReservedP7_17, ReservedP8_18, ReservedP8_19, ReservedP9_20..22 declared below ModuleMismatch (23) / RunBusy (24)"
  - "HandoffControl gains in_handoff(p) (D-P13-13 W2/W11 read fence) next to prepare_handoff; HandoffGuard releases exactly once and aborts on drop"
  - "Adapted plan signatures to the real Phase 12 types: LocalHandoff::new takes Arc<MetaHandle> and a LogLookup closure; compat::require returns FeatureNotEnabled; select() returns EngineError (no ValidateError type exists)"
  - "No ci-local.sh / ci.yml change: FAILPOINT_CRATES and the ci.yml Failpoint suites step are already in sync under the stricter check_failpoint_sync"
requirements-completed: []
duration: "see Metrics"
completed: 2026-11-24
---

# Phase 13 Plan 01: Phase 13 gate, contracts and stubs Summary

**Gate accepted (Phase 12 campaign pending, D-P12-28); P13 ordinals registered and in code (Cmd 43 RepinRun, MetaCommand 7 placeholder + 8 AlterReplicas, RejectReason 23/24, ext 96-102); every Phase 13 feature/dep pinned; compat registry, ostro-cluster contracts and per-plan stubs, LinkerSet stub, ostro_api::phase13 traits + RequestMeta + GrpcControlPlane stub, all warning-free with goldens append-only.**

## Why this plan is wide

About 60 paths change, most of them mechanically: one stub file per later module (`//! Phase 13: implemented in 13-NN` + fixed signatures), one feature line per manifest, one arm per exhaustive match, one module line per declaration list. Concentrating them here lets the later waves own disjoint files and never touch Cargo files, module lists or shared enums concurrently (D-P13-24).

## Gate report (Task 1)

Checks run at main `9445534` (worktree base), classification:

```
.planning/phases/12-replicated-partition-logs plans=18 missing=0 status: human_needed
.planning/phases/11-run-operations-embedded-ui plans=18 missing=0 status: human_needed
.planning/phases/10-developer-cli-test-kits plans=20 missing=0 status: human_needed
.planning/phases/09-polyglot-sdks-conformance plans=20 missing=0 status: human_needed
```

| Phase | human_needed items | Class |
|-------|--------------------|-------|
| 12 | (1) QA-01 10,000-fault tier-C campaign re-run with the D-P12-27 trimmed-log rule (fault-1030 recheck: all 133,443 flagged runs on 3/3 nodes) | **open, running now under D-P12-28; accepted by the orchestrator, Phase 12 campaign pending** |
| 12 | (2) failover p99 / RF=3 bench on native Linux (D15) | by-design (a) |
| 12 | (3) SeaweedFS/S3 legs (P8-D18) | by-design (c) |
| 12 | (4) remote CI legs incl. aarch64 (FOUND-05) | by-design (d) |
| 12 | (5) macOS CI legs | user's Linux-first decision (deferred) |
| 12 | (6) user review D-P12-2/3/4 | by-design (e) |
| 11 | browser render / console / htmx / confirm / no-JS / keyboard / foreign-origin CSRF / non-loopback host; remote CI; user review D-P11-3/8/13/14/17 | 11-VALIDATION Manual-Only + (d) + user review |
| 10 | user decisions D-P10-13/14 + ephemeral budget; user review D-P10-6/11; macOS clean run; remote CI; publishing | user review + (d) + manual-only |
| 9 | publishing; remote CI polyglot job; upstream toolchain PRs + user review D-P9-3; native SC5 perf (D15) | manual-only + (a) + (d) + user review |

**Verdict:** checks 1–4 pass except Phase 12 item (1), which the orchestrator accepted for the Phase 13 gate ("accepted by orchestrator, Phase 12 campaign pending (D-P12-28)"). Phase 12 is **not** marked complete here; the orchestrator does that when the campaign finishes.

Check 3 (code presence), all ok: `struct ClusterServices` (ostro-server/src/cluster.rs), `ElectLeader` (ostro-meta/src/command.rs), no `Rf1Log` in crates/, `fn sample_live` (vendor/ostro-raft-driver/src/driver.rs), `pub mod replay_check` (ostro-engine/src/lib.rs), `trait OpsApi` (ostro-api/src/phase11.rs), harness `trait ClusterHarness`, `struct InProcCluster`, `struct ProcCluster`, `struct ChaosProxy` (ostro-repl-conformance/src).

Check 4 (contention, stale-blob rule against the shared checkout): every listed path either matches a commit on main or is absent in the shared checkout; `.planning/STATE.md` there is a stale blob (matches 31b992f, 167 commits back), not contention.

Check 5 (baseline ci-local): not re-run at the start (deviation 1); the baseline evidence is 12-VERIFICATION's independent runs at 151a6cf plus the 12-18 full ci-local. The plan-level ci-local result at the final code commit is under "Verification".

## Phase 7/10/11/12 seams (recorded verbatim; later plans read them from here)

**ClusterServices** (`crates/ostro-server/src/cluster.rs`): fields `node_id: u64, cluster_id: String, token_key: [u8; 32], meta: Arc<MetaHandle>, voters: Vec<u64>, count: PartitionCount, clock: Arc<dyn Clock>, tuning: ReplicationTuning, data_dir: PathBuf, transport: Arc<dyn ClusterTransport>, assignments: Arc<MetaAssignments>, follower: Arc<LateFollower>, replication: ReplicationService, execute: LateExecute, listener_addr: SocketAddr, liveness: LivenessHandle, tasks: Mutex<Vec<JoinHandle<()>>>, tasks_stop: CancellationToken, listener_tasks: Mutex<Vec<JoinHandle<()>>>, listener_stop: CancellationToken, pub timings: ClusterTimings`. `pub async fn start(s: ClusterStart<'_>) -> Result<ClusterServices, ServerError>` with `ClusterStart { cfg, data_dir, count, clock, transport, api_addr }`. Start order (module doc + body): 1 `ostro_partition::bind_node_id(data_dir, node_id)`; 2 `identity::probe_meta(&meta_dir)` → `identity::resolve_cluster_id(configured, node_toml, stored_ca, probe.cluster_id, probe.blank)`; 3 joiner/wiped-voter site: `if identity::is_joiner(probe.blank, partition_data_exists(data_dir)) { return Err(boot(identity::joiner_error(!peers.is_empty(), &meta_dir))) }` (cluster.rs ~line 222); 4 **PreStart voter check** `identity::check_meta_voters(probe.conf.clone(), &voters)` (~line 225); cluster_id = generated (`Uuid::now_v7`) only for `ClusterIdSource::GenerateFresh`; `bind_cluster_id`; 5 TLS (`ClusterCa::import_pem` with `[cluster.tls]` files else `ClusterCa::load_or_create`; `ca.load_or_issue_node_cert(node_id, &cfg.cluster.advertise)`); 6 transport (`AddressBook::from_peers(cfg.cluster.peers…)` → `TcpTransport::new(&cluster_id, node_id, NodeTls{..}, book)` unless injected) + always-bound listener (`Replication` + `MetaPeerService` + `LateExecute`); 7 `MetaHandle::start_with(dir, MetaStart{node_id, voters, outbox: Box<dyn Outbox>, forwarder: Option<Arc<dyn ProposeForwarder>>, tick: TickConfig})`, `RaftSlot::set`, `meta.wait_ready()`, then **PostReady voter check** `identity::check_voters_after_ready(&meta.raft().status().voters, &voters)` (~line 389); 8 `crate::server::init_cluster(&meta, &cluster_id, node_id, count, now_ms)` + `identity::check_cluster_info` + `ostro_cluster::members::upsert_self(&meta, me)` (~line 421); 9 controller, `MetaAssignments`, self-demotion watchdog. Then the server opens the node and calls `attach_node(&Arc<Node>)`. Accessors: `cluster_id()`, `token_key()`, `meta() -> &Arc<MetaHandle>`, `voters()`, `transport()`, `liveness()`, `assignments() -> Arc<dyn AssignmentSource>`, `assignment_watch()`, `follower_driver()`, `execute_slot()`, `self_demoted()`, `status_document(&Node)`, `status_router`, `stop_tasks`, `stop_listener`, `into_meta`, `stop`, `leader_hints(&Arc<Node>)`, `module_push`, `module_push_plan`; free fn `role_of(node, p)`.

**cluster_id resolution** (`ostro_cluster::identity`): `pub fn resolve_cluster_id(configured: Option<&str>, node_toml: Option<&str>, stored_ca: Option<&str>, persisted_cluster_info: Option<&str>, meta_blank: bool) -> Result<ClusterIdSource, BootError>`; `is_joiner(meta_blank, partition_data_exists) -> bool`; `joiner_error(has_peers, meta_dir) -> BootError` (WipedVoter / MetaDirMissing); `check_meta_voters(persisted: Option<ConfStateView>, expected: &[u64])`; `check_voters_after_ready(actual, expected)`; `check_cluster_info(stored, bound)`; `stored_ca_cluster_id(tls_dir)`; `probe_meta(meta_dir) -> Result<MetaProbe, BootError>`.

**MetaHandle** (`ostro_meta::handle`): `start(dir, node_id)`, `start_with(dir, MetaStart)`, `start_with_unsynced_log`, `wait_ready(&self)`, `bootstrap_from_snapshot(dir, node_id, index, snapshot)`, `propose(&self, MetaCommand) -> Result<MetaResult, MetaError>` (forwards to the leader with >1 voter), `failure() -> Option<String>`, `init_cluster(..)`, `view() -> &MetaView`, `is_leader()`, `raft_status() -> MetaRaftStatus {term, commit_index, applied_index, is_leader}`, `raft() -> &RaftHandle`, `node_id()`, `stop(self)`. `wait_applied` is on `RaftHandle` (vendor driver.rs:221 `pub async fn wait_applied(&self, index) -> Result<(), ProposeError>`); MetaHandle has a private `wait_applied_until`. No `read_index` yet (13-05). `MetaView` (sm.rs, Clone): `read(|&MetaState| ..)`, `applied()`, `subscribe() -> watch::Receiver<u64>`, `pre_freeze_format()`.

**Meta-contact age** (self-demotion): `RaftHandle::leader_contact_age(&self) -> Option<Duration>` (vendor driver.rs:205); `RaftHandle::status() -> RaftStatus`.

**Address book** (`ostro_cluster::address_book`): `AddressBook { inner: RwLock<BTreeMap<u64, PeerAddr>> }`, `PeerAddr {cluster_addr, repl_addr: Option<String>}`, `PeerEntry {id, addr, repl_addr}`; `new()`, `from_peers(impl IntoIterator<Item = PeerEntry>)`, `get(id)`, `cluster_addr(id)`, `repl_addr(id)`, `ids()`, `upsert(id, cluster_addr, repl_addr) -> bool`, `upsert_member(&Member) -> bool`; **new** `from_meta(view: MetaView, seeds: Vec<Seed>) -> Self` (`Seed = PeerEntry`). The outbox resolves addresses via `TcpTransport` holding `Arc<AddressBook>` and re-dialing a node whose address changed (`ClusterTransport::channel(node) -> Result<Channel, TransportError>`).

**members.rs self-upsert**: `pub async fn upsert_self(meta: &MetaHandle, me: ostro_meta::Member) -> Result<bool, MetaError>` (CAS loop on `s.member(node_id)`).

**Roles** (`ostro_partition::roles`): `PartitionAssignmentView {partition, leader, epoch, replicas, isr, isr_version, min_insync}`; `trait AssignmentSource { subscribe() -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>>; local_node() -> u64; request_term(p, min_epoch) -> BoxFuture<'_, ()>; resign(p, epoch) -> BoxFuture<'_, ()> }`; `trait PartitionRoles { led() -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>> }` (where "LeaderAnnounced applied" becomes observable: the led set); `trait PartitionControl { quiesce_for_install(p) -> BoxFuture<'_, Result<InstallGuard, PartitionError>> }`; `trait FollowerDriver`, `FollowerTask`; `pub type PartitionLogHandle = ostro_log::PartitionLog`; `decide(..) -> RoleDecision`, `RoleConfig`, `NodeTestHooks::fail_partition`.

**Dispatcher counters** (`ostro_partition::leader::dispatcher::DispatcherStats`): `dispatched, rescans, lease_expired, reactivated, leases, grants_rejected: AtomicU64` (in-flight leases = `leases`; admission is the queue/flow path `Admit`, `FlowControl`).

**ServerOptions** (`ostro-server/src/server.rs:202`): `clock: Arc<dyn Clock>, hooks: Arc<ExecutorHooks>, tls_profile: Option<TlsProfile>, cluster_transport: Option<Arc<dyn ClusterTransport>>, wal_rotation_bytes: Option<u64>` (13-02 adds `level_max_override`). **ExecutorHooks** (executor.rs:263): `activations, self_fences: AtomicU64, pause_before_append: Mutex<Option<Arc<Semaphore>>>, suppress_heartbeat: AtomicBool, only_attempt: AtomicU32, jobs: Mutex<Vec<JobRecord>>, module_override: ModuleOverrideSlot`.

**ClusterCa** (`ostro-server/src/tls/ca.rs`): `load_or_create(dir, cluster_id)`, `import_pem(dir, cluster_id, ca_crt_pem, ca_key_pem) -> Result<Self, TlsError>`, `issue_node_cert(&self, node_id, sans: &[String])`, `load_or_issue_node_cert(&self, node_id, advertise: &[String])`, `cert_pem()`. Hot swap: `tls::resolver::ReloadableResolver::swap(&self, impl Into<Arc<CertifiedKey>>)` (ArcSwap).

**Worker Hello** (`ostro-worker-proto/proto/ostro/worker/v1/execute.proto`): `worker_id = 1, proto_version = 2, ostro_version = 3, wasmtime_version = 4, event_codec_ver = 5, repeated wit_packages = 6, map labels = 7, slots = 8, memory_bytes = 9` → next free field number **10** (`feature_level`, 13-03). Placement predicate site: `WorkerRegistry::pick(&self, pred: impl Fn(&WorkerView<'_>) -> bool) -> Option<CreditGuard>` (workers/registry.rs:673).

**replay_check** (`ostro_engine::replay_check`): `enum ReplayVerdict { Ok { appended }, OkFrontier { appended }, Diverged { seq, detail }, Error(..) }` (see replay_check.rs:46).

**ApiExtensions**: defined in `crates/ostro-api/src/phase8.rs` (not lib.rs; re-exported as `ostro_api::ApiExtensions`); slots `schedules, webhooks, hooks, run_logs, admin, hook_body_limit, ops, inventory` + **new** `cluster`, `module_lifecycle`. Every construction uses `..Default::default()` (`control/mod.rs::phase8_extensions`, `p11.rs::extensions`, `ostro-ui/tests/actions.rs`, the phase8 fake).

**In-process ControlPlane consumers**: `ServerControlPlane` appears in ostro-server `server.rs`, `lib.rs`, `p7.rs`, `p11.rs`, `devtime.rs`, `flow/rate_limit.rs`, `control/{mod,runs,list,history,lifecycle,watch,modules,bridge,webhooks,dev,p7_pause,p7_workers,p11_search}.rs`; `Arc<dyn ControlPlane>` built in `p11.rs:242` (`Arc::clone(&cc.partitions) as Arc<dyn ControlPlane>`).

**main.rs dispatch** (`crates/ostro/src/main.rs`): clap `Command::{Dev{..}, Server{..}, Backup, Restore, Worker, Completions, BuildInfo, Openapi, Client(Box<ostro_cli::ClientCommand>)}`; `Dev`/`Server` → `run_server(name, cfg, TlsProfile, started, watch)`; client verbs → `ostro_cli::run(*cmd)`.

**WIT packages**: `wit/ostro-runtime-0.1.0` only (no `0.1.N > 0`); `WIT_0_1_LATEST = "0.1.0"`. `JOURNAL_IFACE` now at `host/state.rs:565` (plan said :493).

## Route classification (D-P13-14; 13-12 implements verbatim)

| Trait | Method | Class |
|-------|--------|-------|
| ControlPlane | upload_module | local (blob upload + meta write; module push is cluster-wide via ModulePush) |
| ControlPlane | get_module, list_workflows | meta_read |
| ControlPlane | activate_workflow | meta_write_local |
| ControlPlane | start_run | partition(key) when an idempotency key / run id is given, else unkeyed_start |
| ControlPlane | get_run, run_history, signal_run, cancel_run, terminate_run, retry_run, await_result, pause_run, resume_run | partition(run_id) |
| ControlPlane | list_runs | fanout_partitions |
| ControlPlane | complete_token | token_partition |
| ControlPlane | set_workflow_policy, set_queue_policy, put_secret, delete_secret, put_grant, delete_grant, put_connection, delete_connection, put_egress, delete_egress, put_queue_flow, pause_queue, resume_queue, pause_workflow, resume_workflow, create_worker_token, revoke_worker_token | meta_write_local |
| ControlPlane | get_workflow_policy, get_queue_policy, list_secrets, list_grants, get_connection, list_connections, get_egress, list_egress, get_queue_flow, list_worker_tokens | meta_read |
| ControlPlane | presigned_object | local |
| ControlPlane | queue_demand | fanout_partitions |
| ControlPlane | list_workers, drain_worker | fanout_nodes |
| ControlPlane | join_worker | local (token check is a meta read; the session lands on this node) |
| ControlPlane | dev_time, dev_skip, replay_module | local (dev / tooling, server-local) |
| ScheduleApi | list_schedules, get_schedule | meta_read |
| ScheduleApi | create_schedule, update_schedule, delete_schedule, pause_schedule, resume_schedule | meta_write_local |
| ScheduleApi | trigger_schedule | partition(key) (the schedule's owner partition) |
| WebhookApi | list_webhooks, get_webhook | meta_read |
| WebhookApi | create_webhook, update_webhook, delete_webhook | meta_write_local |
| HookIngress | deliver | partition(key) (start with the derived idempotency key) / unkeyed_start |
| RunLogsApi | run_logs | partition(run_id) |
| AdminApi | backup | local |
| OpsApi | rerun_run, analyze_rerun | partition(run_id) (source journal read; the new run is an unkeyed/keyed start) |
| OpsApi | list_dlq | fanout_partitions |
| OpsApi | requeue_dlq, dismiss_dlq | partition(run_id) |
| OpsApi | create_bulk, list_bulk, get_bulk, cancel_bulk | fanout_nodes (bulk jobs are node-local; create = local, lookups fan out) |
| OpsApi | limits | local |
| OpsApi | get_failure_policy | meta_read |
| OpsApi | set_failure_policy | meta_write_local |
| InventoryApi | list_modules, list_queues | meta_read |
| InventoryApi | list_partitions | meta_read (assignments) + fanout_nodes for per-node stats |
| ClusterApi (P13) | status, upgrade_status | meta_read |
| ClusterApi (P13) | init, join, create_node_token, remove, drain, reassign, rebalance, upgrade_finalize/hold/release | meta_write_local |
| ModuleLifecycleApi (P13) | retire_module | meta_write_local |
| ModuleLifecycleApi (P13) | run_module_gc | meta_write_local (runs on the meta leader) |
| ModuleLifecycleApi (P13) | migrate_runs | fanout_partitions |

## Reservations check (script `13-01-rescheck.py`, scratch; extends 12-01's)

Pre (main 9445534):
```
Cmd tags: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21, 27, 45]
RejectReason: 0 StaleEpoch .. 14 ReservedP6_14, 15 RunPaused
MetaCommand: 0 InitCluster, 1 Put, 2 Delete, 3 SetPartitionLeader, 4 ElectLeader, 5 AlterIsr, 6 ResignLeader
ext tags: 15 EXT_TAG_TEST, 80 EXT_ELECTION_INFO
RESERVATIONS CHECK: PASS
```
Post (Task 2 tree):
```
Cmd tags: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21, 27, 43, 45]
Cmd variants: ... 27 ScheduleFire, 43 RepinRun, 45 DlqUpdate
RejectReason: ... 15 RunPaused, 16 ReservedP7_16, 17 ReservedP7_17, 18 ReservedP8_18, 19 ReservedP8_19, 20 ReservedP9_20, 21 ReservedP9_21, 22 ReservedP9_22, 23 ModuleMismatch, 24 RunBusy
MetaCommand: 0 InitCluster, 1 Put, 2 Delete, 3 SetPartitionLeader, 4 ElectLeader, 5 AlterIsr, 6 ResignLeader, 7 ReservedP12_7, 8 AlterReplicas
ext tags: 15 EXT_TAG_TEST, 80 EXT_ELECTION_INFO, 96 EXT_NODE_VERSION, 97 EXT_MEMBER_STATE, 98 EXT_CLUSTER_VERSION, 99 EXT_REPLICATION_POLICY, 100 EXT_REASSIGNMENT, 101 EXT_WIT_REQUIREMENT, 102 EXT_RETIRED
RESERVATIONS CHECK: PASS
```
The script extracts the `Cmd` oneof `tags` attribute and the tagged variants (and asserts they agree), `RejectReason` / `MetaCommand` declaration order (doc comments stripped) and every `pub const EXT_*: u32 = N`. Pre mode asserts the empty P13 block; post mode asserts exactly the allocations below.

### Final P13 ordinal table

| Space | Ordinal | Name | Status |
|-------|---------|------|--------|
| Cmd | 43 | RepinRun `{run_id 1, from_sha 2, to_sha 3, expected_next_seq 4, requested_at_ms 5}` | shape 13-01, apply 13-17 |
| Cmd | 44 | spare | reserved P13 |
| RejectReason | 16–22 | ReservedP7_16, ReservedP7_17, ReservedP8_18, ReservedP8_19, ReservedP9_20..22 (codes 32–38, never emitted) | placeholders |
| RejectReason | 23 / 24 | ModuleMismatch (code 39) / RunBusy (code 40) | declared 13-01, emitted 13-17 |
| MetaCommand | 7 | ReservedP12_7 {} (apply Invalid("reserved")) | placeholder |
| MetaCommand | 8 | AlterReplicas | shape 13-01, apply 13-10 (stub: Invalid("Phase 13: implemented in 13-10")) |
| ext | 96–102 | NodeVersion, MemberState, ClusterVersion, ReplicationPolicy, Reassignment, WitRequirement, Retired | registered 13-01 |
| ext | 103–111 | spare | reserved P13 |

## Stub-module map (file → owning plan)

| File | Owner |
|------|-------|
| ostro-types/src/compat.rs | 13-01 (final registry; levels, Feature, MUTANTS, level_max) |
| ostro-partition/src/handoff.rs | 13-11 (Node impl) |
| ostro-partition/src/apply/repin.rs | 13-17 |
| ostro-partition/src/store/mod.rs `StoreError::NewerFormat` | emitted by 13-04 |
| ostro-meta/src/state.rs AlterReplicas arm | 13-10 |
| ostro-cluster/src/contracts.rs | 13-01 (final) |
| ostro-cluster/src/{compat,upgrade}.rs | 13-04 |
| ostro-cluster/src/membership/reconcile.rs, placement/{plan,reassign,leaders}.rs | 13-06 |
| ostro-cluster/src/membership/{service,remove}.rs, address_book.rs `from_meta` | 13-09 |
| ostro-cluster/src/placement/controller.rs | 13-10 |
| ostro-cluster/src/{handoff,shutdown}.rs | 13-11 |
| ostro-engine/src/host/linker_set.rs | 13-07 |
| ostro-api/src/phase13.rs (default bodies name 13-04/09/10/11/15/16/17) | traits final; impls in ostro-server p13/ (13-02 delegation) |
| ostro-api/src/request_meta.rs | 13-01 (final contract); filled/emitted by the 13-03 adapters |
| ostro-api/src/grpc_client.rs | 13-03 |

## Contract signatures as landed

```rust
// ostro_types::compat
pub const LEVEL_MIN: u32 = 1; pub const LEVEL: u32 = 2; pub const TEST_LEVEL_MAX_ENV: &str = "OSTRO_TEST_LEVEL_MAX";
pub enum Feature { AlterReplicas, RepinRun, ReassignmentExt, WitRequirementExt, RetiredExt }
impl Feature { pub const ALL: [Feature; 5]; pub const fn introduced_at(self) -> u32 /* 2 */; pub const fn name(self) -> &'static str }
pub const fn feature_for_cmd_tag(tag: u32) -> Option<Feature>;        // 43 -> RepinRun
pub const fn feature_for_meta_variant(index: u32) -> Option<Feature>; // 8 -> AlterReplicas
pub const MUTANTS: [&str; 8]; pub const FAILPOINT_PREFIXES: [&str; 7];
pub fn parse_level_override(v: Option<&str>) -> Option<u32>; pub fn level_max() -> u32; pub const fn supports(finalized: u32, level_max: u32) -> bool;

// ostro_types::command
pub struct RepinRun { run_id: Vec<u8> /*1*/, from_sha: Vec<u8> /*2*/, to_sha: Vec<u8> /*3*/, expected_next_seq: u64 /*4*/, requested_at_ms: u64 /*5*/ }
Cmd::RepinRun(RepinRun) /* tag 43 */; Command::repin_run(r); kind_name() == "RepinRun"

// ostro_meta
MetaCommand::ReservedP12_7 {} /* 7 */
MetaCommand::AlterReplicas { partition: u16, epoch: u32, expected_isr_version: u64, replicas: Vec<u64>, isr: Vec<u64>, min_insync: u8, reassignment: Option<ext::Reassignment> } /* 8 */
ext::{EXT_NODE_VERSION=96, EXT_MEMBER_STATE=97, EXT_CLUSTER_VERSION=98, EXT_REPLICATION_POLICY=99, EXT_REASSIGNMENT=100, EXT_WIT_REQUIREMENT=101, EXT_RETIRED=102, P13_EXT_TAGS}
ext::{MEMBER_JOINING=0, MEMBER_ACTIVE=1, MEMBER_DRAINING=2, MEMBER_REMOVED=3}
ext::NodeVersion { ostro_version: String, level_min: u32, level_max: u32, wasmtime: String, event_codec_ver: u32, wit_lines: Vec<String>, reported_at_ms: u64 }
ext::MemberState { state: u8, changed_at_ms: u64 }
ext::ClusterVersion { finalized: u32, hold: Option<u32>, finalized_at_ms: u64 }
ext::ReplicationPolicy { rf: u8, min_insync: u8, max_voters: u8 }
ext::Reassignment { target: Vec<u64>, adding: Vec<u64>, removing: Vec<u64>, started_at_ms: u64 }
ext::WitRequirement { line: String, min_level: u32 }
ext::Retired { retired_at_ms: u64 }
ext::{get,put}_{node_version, member_state, cluster_version, replication_policy, reassignment, wit_requirement, retired}(ext: &[u8] [, &T]) -> Result<.., ExtError>

// ostro_partition
RejectReason::{ReservedP7_16, ReservedP7_17, ReservedP8_18, ReservedP8_19, ReservedP9_20, ReservedP9_21, ReservedP9_22, ModuleMismatch /*23, code 39*/, RunBusy /*24, code 40*/}
StoreError::NewerFormat(String) // "written by a newer Ostro; upgrade this binary: {0}"
pub trait HandoffControl: Send + Sync + 'static {
    fn prepare_handoff(&self, p: PartitionId, grace: Duration) -> BoxFuture<'_, Result<HandoffGuard, PartitionError>>;
    fn in_handoff(&self, p: PartitionId) -> bool;
}
pub enum HandoffRelease { Aborted, Completed }
impl HandoffGuard { pub fn new(partition: PartitionId, release: impl FnOnce(HandoffRelease) + Send + 'static) -> Self; pub fn partition(&self) -> PartitionId; pub fn abort(self); pub fn complete(self) } // Drop = abort
// re-exported: ostro_partition::{HandoffControl, HandoffGuard, HandoffRelease}

// ostro_cluster::contracts (final)
pub trait LevelSource: Send + Sync { fn level_max(&self) -> u32; fn finalized(&self) -> u32; }
pub enum HandoffOutcome { Moved { new_epoch: Epoch }, Aborted { reason: String } }
pub enum HandoffError { NotLeader { partition: u16, epoch: u32 }, TargetNotInIsr { partition: u16, target: u64 }, Transport(String), Meta(String) }
pub trait HandoffClient: Send + Sync { fn hand_off(&self, leader: u64, p: PartitionId, to: u64, epoch: Epoch) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>>; }
pub trait HandoffExecutor: Send + Sync { fn execute(&self, p: PartitionId, to: u64, epoch: Epoch) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>>; }
pub struct RefsAnswer { pub referenced: Vec<[u8; 32]>, pub uncovered: Vec<(PartitionId, Epoch)> }
pub trait ModuleRefsSource: Send + Sync { fn referenced(&self, shas: Vec<[u8; 32]>, partitions: Vec<(PartitionId, Epoch)>, protect_resumable: bool) -> BoxFuture<'_, Result<RefsAnswer, String>>; }
pub struct MemberView { node_id, addr, cluster_addr: Option<String>, state: u8, version: Option<NodeVersion>, live: bool }
pub struct ClusterStatusView { members: Vec<MemberView>, voters, learners, voters_outgoing: Vec<u64>, meta_leader: u64, finalized: u32, hold: Option<u32>, level_range: (u32, u32), policy: Option<ReplicationPolicy>, reassignments: Vec<(PartitionId, Reassignment)> }

// ostro_cluster::compat (13-04)
pub enum CompatError { FinalizedAbove { finalized, level_max }, FinalizedBelow { finalized, level_min, through: String }, NewerMetaFormat { found: u8 } }
pub struct FeatureNotEnabled { feature: Feature, needed: u32, finalized: u32 } // -> 409 feature_not_enabled
pub async fn boot_fence(meta: &MetaHandle, level_max: u32) -> Result<(), CompatError>;            // stub Ok
pub fn require(view: &MetaView, f: Feature, level_max: u32) -> Result<(), FeatureNotEnabled>;    // stub Ok
pub fn start_advertiser(meta: Arc<MetaHandle>, node_id: u64, version: NodeVersion, stop: CancellationToken) -> JoinHandle<()>;
pub fn start_running_fence(meta: Arc<MetaHandle>, level_max: u32, on_fatal: Arc<dyn Fn(String) + Send + Sync>, stop: CancellationToken) -> JoinHandle<()>;

// ostro_cluster::upgrade (13-04)
pub enum FinalizeDecision { Stay, Finalize { to: u32 }, Held { at: u32 }, Waiting { reason: String } }
pub enum UpgradeError { NotReady(String), Meta(String), NotImplemented }
pub fn finalize_decision(status: &ClusterStatusView, auto_finalize: bool) -> FinalizeDecision;
pub fn start_finalizer(meta: Arc<MetaHandle>, auto_finalize: bool, interval: Duration, stop: CancellationToken) -> JoinHandle<()>;
pub async fn finalize(meta: &MetaHandle, now_ms: u64) -> Result<ClusterVersion, UpgradeError>; // + hold, release

// ostro_cluster::membership (13-06 / 13-09)
reconcile::{MemberObs { id, state: u8, live, matched: u64, pending_snapshot }, ReconcileInput { leader, voters, voters_outgoing, learners, members, commit, pending_conf_change, promote_lag, max_voters: u8 },
  Step { LeaveJoint, AddLearner(u64), Joint { add_voters, remove_voters, demote: Vec<u64> }, RemoveLearner(u64), TransferLeader(u64), None }, fn next(&ReconcileInput) -> Step}
service::{MembershipConfig { promote_lag: 64, join_orphan_timeout: 25h, interval: 500ms }, MembershipService::new(Arc<MetaHandle>, MembershipConfig), start(self, CancellationToken) -> JoinHandle<()>}
remove::{RemoveError { NoQuorum{id}, NotMember{id}, NotImplemented }, fn check_remove(id, voters: &[u64], live: &[u64], force: bool) -> Result<(), RemoveError>}

// ostro_cluster::placement (13-06 / 13-10)
plan::target_replicas(current: &BTreeMap<u16, Vec<u64>>, members: &[u64], rf: u8) -> BTreeMap<u16, Vec<u64>>
reassign::{PartitionObs { partition, epoch, leader, replicas, isr, isr_version, min_insync, reassignment: Option<Reassignment> },
  ReassignStep { Expand { replicas, reassignment }, CatchUp, LeaderMove { to }, Shrink { replicas, isr, min_insync }, None }, fn next_step(&PartitionObs, target: &[u64], policy_min_insync: u8) -> ReassignStep}
leaders::preferred_leader_moves(partitions: &[(u16, Vec<u64>, u64)], live: &[u64], max_moves: usize) -> Vec<(u16, u64)>
controller::{PlacementConfig { max_concurrent_moves: 2, leader_rebalance_interval: 300s, interval: 500ms }, PlacementController::new(Arc<MetaHandle>, Arc<dyn HandoffClient>, PlacementConfig), start(self, CancellationToken) -> JoinHandle<()>}

// ostro_cluster::handoff / shutdown (13-11)
pub type LogLookup = Arc<dyn Fn(PartitionId) -> Option<Arc<PartitionLogHandle>> + Send + Sync>;
pub struct HandoffConfig { grace: 2s, catchup_timeout: 10s }
LocalHandoff::new(control: Arc<dyn HandoffControl>, meta: Arc<MetaHandle>, logs: LogLookup, cfg: HandoffConfig) // impl HandoffExecutor (stub Aborted{"not implemented"})
NodeOpsHandoffClient::new(transport: Arc<dyn ClusterTransport>, local: Arc<LocalHandoff>)             // impl HandoffClient  (stub Aborted{"not implemented"})
shutdown::{ShutdownReport { moved: Vec<u16>, kept: Vec<u16>, meta_transferred: bool }, async fn controlled_shutdown(node_id, meta: Arc<MetaHandle>, handoff: Arc<dyn HandoffClient>, timeout: Duration) -> ShutdownReport}
address_book::{pub type Seed = PeerEntry; AddressBook::from_meta(view: MetaView, seeds: Vec<Seed>) -> Self}

// ostro_engine::host::linker_set (13-07)
pub const WIT_0_1_LATEST: &str = "0.1.0";
pub struct WitLine { major: u32, minor: u32 } (WitLine::V0_1, Display "0.1"); pub struct WitSelection { line: WitLine, min_level: u32 }
pub struct LinkerSet; fn lines(&self) -> Vec<WitLine>;
pub fn select(component: &Component, engine: &Engine) -> Result<WitSelection, EngineError>; // stub: 0.1 / 1

// ostro_api::phase13 (traits with default bodies naming the plan)
ClusterApi { init(InitRequest) -> ClusterStatus; join(JoinRequest) -> JoinResponse; status() -> ClusterStatus; create_node_token(NodeTokenRequest) -> NodeTokenResponse;
  remove(id: u64, force: bool) -> ClusterStatus; drain(id) -> ClusterStatus; reassign(ReassignRequest) -> ClusterStatus; rebalance() -> ClusterStatus;
  upgrade_status/upgrade_finalize/upgrade_hold/upgrade_release() -> UpgradeStatus }
ModuleLifecycleApi { retire_module(sha: String) -> RetireModuleResponse; run_module_gc() -> GcRunReport; migrate_runs(MigrateRunsRequest) -> MigrateRunsReport }
DTOs: InitRequest{advertise, rf, min_insync}, Advertise{api, cluster, repl}, NodeVersionDto, JoinRequest{token, csr_pem, advertise, node_version, resume_node_id},
  SeedDto{id, addr, repl_addr}, JoinResponse{node_id, cluster_id, partitions, ca_cert_pem, node_cert_pem, seeds, finalized_level}, NodeTokenRequest{ttl_secs},
  NodeTokenResponse{id, token, expires_at_ms, ca_hash}, ClusterMember, ReplicationPolicyDto, ReassignmentDto, ClusterStatus, ReassignRequest{partition, replicas, cancel},
  MemberLevel, UpgradeStatus{finalized, hold, common_level_max, auto_finalize, members}, VersionOrSha{Version(u32) | Sha(String)} (snake_case externally tagged),
  MigrateRunsRequest{workflow, from, to_version, filter, limit, dry_run}, MigrateFailure{run_id, seq, detail},
  MigrateRunsReport{passed, repinned: Vec<String>, failed: Vec<MigrateFailure>, busy, seq_mismatch: Vec<String>}, WorkflowVersionRef, RetireModuleResponse, GcRunReport
ApiExtensions { .., cluster: Option<Arc<dyn ClusterApi>>, module_lifecycle: Option<Arc<dyn ModuleLifecycleApi>> }

// ostro_api::request_meta
pub const HEADER_{HOP, PARTITION, PARTITIONS, PRINCIPAL, CONSISTENCY, READ_AFTER}: &str;
pub struct RequestMeta { hop: u8, partition_pin: Option<u16>, partitions: Option<Vec<u16>>, principal: Option<String>, read_after: Option<String>, from_node: Option<u64>, deadline: Option<Instant> }
RequestMeta::current() -> Option<RequestMeta>; is_forwarded(&self) -> bool
pub async fn scope<F: Future>(meta, fut) -> F::Output; pub async fn scope_with_consistency(meta, fut) -> (F::Output, Option<String>)
pub fn set_consistency(token: String); pub fn take_consistency() -> Option<String>

// ostro_api::grpc_client (13-03)
pub struct GrpcControlPlane { channel: Channel }; GrpcControlPlane::new(channel); channel(&self) -> &Channel
```

## Dependencies and features (resolved)

No new third-party crate; `cargo deny check` clean (advisories, bans, licenses, sources ok).

| Manifest | Change |
|----------|--------|
| ostro-types | `[features] level-emulation = []` |
| ostro-engine | `wit-next-fixture = []` |
| ostro-server | failpoints += `ostro-types/level-emulation` |
| ostro | failpoints += `ostro-types/level-emulation`; dep `ostro-types.workspace = true` |
| ostro-repl-conformance | failpoints += `ostro-types/level-emulation`; deps `ostro-api`, `ostro-client` (path, workspace crates) |
| Cargo.lock | +3 dependency edges (ostro → ostro-types; ostro-repl-conformance → ostro-api, ostro-client); no version changes |

Existing workspace pins the contracts use: tokio 1.54 (task_local, full), tonic 0.14.9 (transport Channel), futures-util 0.3.34 (BoxFuture), async-trait 0.1.92, tokio-util 0.7.20 (CancellationToken), utoipa (ToSchema), postcard 1.1.3, prost.

FAILPOINT_CRATES vs ci.yml: already in sync under ci-local's `check_failpoint_sync` (exact command list incl. `crate:target` entries); no ci-local.sh or ci.yml change and no ci.yml hunk for the orchestrator.

## Task Commits

1. `7789c4c` docs(13-01): RESERVATIONS P13 rows (gate check passed)
2. `28730be` docs(13-01): STATE Phase 13 decisions, review/human todos, shared-file rule
3. `766ec47` chore(13-01): pin Phase 13 features and deps (manifests first) — base recorded in `target/13-01-base.sha`
4. `f006010` feat(13-01): Phase 13 library contracts and stubs — **landed empty** (a zsh word-splitting slip passed the file list as one argument; main is never rewritten, so it stays as a no-op commit)
5. `89abe7f` feat(13-01): Phase 13 library contracts and stubs (content; f006010 landed empty) — the 42 code/test files

## Verification

- `cargo build --workspace --all-targets --locked`: warning-free (`target/13-01-build.log`).
- `cargo clippy -p ostro-types -p ostro-meta -p ostro-partition -p ostro-cluster -p ostro-engine -p ostro-api -p ostro-repl-conformance --all-targets --locked -- -D warnings`: clean. `cargo fmt --all -- --check`: clean.
- Tests (all ok, each run proved ≥ 1 passed): `-p ostro-types` (lib 19, command 52, golden_frozen etc.), `-p ostro-types --features level-emulation --lib compat` (6), `-p ostro-meta` (all binaries; new `phase13_tests` 5, meta_commands_golden incl. `phase13_appends_alter_replicas_at_8`), `-p ostro-cluster --lib` (23), `-p ostro-api --lib` (59), `-p ostro-engine --lib host::linker_set` (1), `-p ostro-partition --lib --test p13_format --test p7_format --test p11_format` (46/3/2/4), `PROPTEST_CASES=64 -p ostro-partition --test rebuild_digest` (8 passed, 528 s).
- Goldens append-only: `git diff 766ec47 -- crates/ostro-meta/tests/golden crates/ostro-types/tests/golden` has no removed line (one line appended to each file).
- Reservations check post: PASS (above).
- `cargo build -p ostro --features failpoints --locked`: ok, warning-free.
- Plan-level `bash scripts/ci-local.sh --no-musl`: **all steps passed** at `89abe7f` in a separate detached worktree (`--no-musl --no-polyglot`, nice 15, slot-6): fmt 3 s, clippy 53 s, build 52 s, sdk 22+25 s, guests 16 s, order-saga 22 s, cargo test 3956 s, clients go 5 s, sleepers 56 s, failpoint list sync PASS, failpoint suites 4220 s, replication mutant matrix 237 s, server campaign smoke (20 kills) 617 s, phase 7 failpoint legs 51 s, deny/vendored/fixtures/forbidden/P12 grep gates/workflow PASS; external PG/MySQL/S3 SKIP (no env), polyglot SKIP (flag), musl SKIP (flag).

## Deviations from Plan

### Auto-fixed / adapted

0. **`--no-polyglot` on the plan-level ci-local** (plan says `--no-musl` only): kept CPU headroom for the running campaign; Phase 13 changes touch no polyglot path.
1. **[Rule 3 - Blocking/scope] Baseline ci-local not re-run at the start.** The campaign (D-P12-28) is running and the orchestrator asked for targeted runs; baseline evidence is 12-VERIFICATION (151a6cf) + 12-18's full ci-local. The plan-level ci-local ran once at the final code commit (Verification).
2. **[Rule 3] Files outside `files_modified` (tests/arms/anchors):** `crates/ostro-types/tests/command.rs` (repin_run golden vector + tag test; the Phase 11 ordering test now pins `names[30..35]` and adds `names[35..] == ["repin_run"]`), `crates/ostro-meta/tests/meta_commands_golden.rs` (AlterReplicas golden at ordinal 8; the Phase 12 test now pins the 0..=6 prefix and a new test pins `[0..6, 8]`), `crates/ostro-cluster/src/blob.rs` (exhaustive `Cmd` arm: RepinRun carries no payload blob), `crates/ostro-repl-conformance/src/lib.rs` (dep anchors for ostro-api/ostro-client: cargo's `unused_dependencies` lint warned), `crates/ostro-partition/tests/p13_format.rs` (new format-contract test), `crates/ostro-api/src/phase8.rs` (ApiExtensions lives there, not in lib.rs).
3. **[Adaptation] Signatures mapped to real Phase 12 types:** `LocalHandoff::new` takes `Arc<MetaHandle>` (MetaHandle is not Clone; ClusterServices holds `Arc<MetaHandle>`) and `logs: LogLookup`; `compat::require` returns `FeatureNotEnabled` (ostro-cluster has no ostro-api dep); `linker_set::select` returns `EngineError` (no `ValidateError` type); `WitLine` defined in linker_set.rs; `Seed = PeerEntry`.
4. **[Rule 2] `HandoffControl::in_handoff(p)`** added to the trait (D-P13-13 W2/W11 needs it; contracts are final after 13-01). HandoffGuard aborts on drop so an error path never leaves a partition fenced.
5. **Plan verify defect:** the plan's naive `diff` of FAILPOINT_CRATES against `cargo test -p X --features failpoints --locked` lines cannot pass because FAILPOINT_CRATES holds `crate:target` entries (`--test target`); the authoritative `check_failpoint_sync` (exact commands, in order) passes. No drift to fix.
6. **Worktree/target paths:** worktree under the session scratchpad and `CARGO_TARGET_DIR=target/slot-6` per the executor brief, instead of `/tmp/ostro-wt-13-01` / `/tmp/ostro-target-13-01`.

### Deferred Issues

None.

## Next Phase Readiness (13-02 / 13-04 and the rest)

- 13-02 owns `server.rs`, `cluster.rs`, `config.rs`, `main.rs`, `p13/mod.rs` and the repl-conformance harness seam. The two voter-check sites and the joiner site are recorded above with line anchors; `ServerOptions` has no `level_max_override` yet (13-02 adds it). `ApiExtensions.cluster` / `.module_lifecycle` exist (None by default).
- 13-04 fills `ostro-cluster/src/{compat,upgrade}.rs` (signatures above; `FeatureNotEnabled` → 409 `feature_not_enabled`), emits `StoreError::NewerFormat` from `apply/mod.rs`, and uses `ostro_types::compat::{level_max, supports, feature_for_cmd_tag, feature_for_meta_variant}`. `level-emulation` is reachable only through the failpoints chains.
- 13-05: `MetaHandle` has no `read_index` / transfer yet; `wait_applied` lives on `RaftHandle`.
- 13-07: `JOURNAL_IFACE` is at `host/state.rs:565`; `WIT_0_1_LATEST = "0.1.0"` with a test tying it to `wit/`.
- 13-03: Hello next free field = 10.

## Self-Check

PASSED: every created file listed above exists at 89abe7f; commits 7789c4c, 28730be, 766ec47, f006010 (empty), 89abe7f are on main.
