---
phase: 13-cluster-membership-routing-upgrades
plan: 03
subsystem: wire surface (ostro-proto, ostro-worker-proto, ostro-api adapters + gRPC client, p13/node_ops)
tags: [proto, grpc, rest, openapi, request-meta, grpc-client, parity, node-ops, worker-hello]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 phase13 traits + DTOs, RequestMeta contract, GrpcControlPlane stub, Hello next free field 10; 13-02 p13/node_ops.rs delegation + cluster_listener_services mount"
provides:
  - "ostro/api/v1/cluster.proto: ostro.api.v1.Cluster (12 rpcs) + ostro.api.v1.ModuleLifecycle (3 rpcs)"
  - "ostro/cluster/v1/membership.proto: ostro.cluster.v1.Membership {Removed}, NodeOps {HandOff, ModuleRefs}"
  - "worker Hello.feature_level = 10 (additive)"
  - "REST + gRPC adapters for ClusterApi / ModuleLifecycleApi (phase13_adapters.rs), join outside the API middleware"
  - "request_meta::{from_headers, to_metadata, middleware, TrustedNode, HEADER_DEADLINE_MS, parse/format_grpc_timeout}"
  - "FakeCluster + FakeModuleLifecycle (+ FakeMigrateOutcome, FAKE_P13_CLUSTER_ID)"
  - "GrpcControlPlane: ControlPlane + all 9 extension traits, status_to_error, DEFAULT_MAX_MESSAGE_BYTES"
  - "NodeOpsService::routes() = Some(NodeOpsServer)"
affects: [13-08, 13-09, 13-11, 13-12, 13-13, 13-14, 13-15, 13-16, 13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "Phase 11 adapter pattern for Phase 13 slots: absent slot = REST 404 / gRPC not mounted (UNIMPLEMENTED); stateless checks shared by both transports"
    - "one axum middleware over every REST + gRPC route parses ostro-* headers into the RequestMeta task-local and emits ostro-consistency on success"
    - "node identity only from a TrustedNode request extension (cluster listener), never from a header"
    - "client losslessness proven by a Debug-string transcript: direct fakes vs GrpcControlPlane -> tonic server -> fakes"
key-files:
  created:
    - crates/ostro-proto/proto/ostro/api/v1/cluster.proto
    - crates/ostro-proto/proto/ostro/cluster/v1/membership.proto
    - crates/ostro-proto/tests/p13_wire.rs
    - crates/ostro-api/src/phase13_adapters.rs
    - crates/ostro-api/tests/phase13_routes.rs
    - crates/ostro-api/tests/grpc_client_parity.rs
  modified:
    - crates/ostro-proto/{build.rs,src/lib.rs}
    - crates/ostro-worker-proto/proto/ostro/worker/v1/execute.proto
    - crates/ostro-worker-proto/tests/wire.rs
    - crates/ostro-api/src/{grpc_client,request_meta,fake,lib,openapi,phase13,rest}.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-server/src/p13/node_ops.rs
    - crates/ostro-worker/src/session.rs (feature_level: 0)
    - crates/ostro-server/tests/common/p7_workers.rs (feature_level: 0)
    - .planning/RESERVATIONS.md (ostro-deadline-ms header)
key-decisions:
  - "Orchestrator decision (recorded): `[cluster] advertise` stays the shipped Phase 7 Vec<String> (13-02 option a); the plan's Option<{api,cluster,repl}> is not adopted. 13-03's protos mirror the 13-01 DTOs unchanged (InitRequest.advertise: Option<String>, JoinRequest.advertise: {api, cluster, repl}); no config shape is implied by the wire."
  - "ostro-server has ostro-proto only as a dev-dependency and Cargo files are 13-01's: ostro-api re-exports `pub use ostro_proto as proto`, so p13/node_ops.rs names the generated types without a manifest change."
  - "Client headers never pin: ostro-partition / ostro-partitions / ostro-principal are parsed (malformed = 400) but honoured only with a TrustedNode(u64) request extension, which also sets from_node; hop, read-after and the deadline are accepted from anyone."
  - "GrpcControlPlane copies RequestMeta as is (13-12 sets hop = 1 in the scope it forwards from, per its plan); it never retries."
  - "REST deadline header `ostro-deadline-ms` (the plan's name) was not in RESERVATIONS §11; added to the Headers row."
  - "OpenAPI: the cluster join DTOs are published as ClusterJoinRequest / ClusterJoinResponse (`#[schema(as)]` on the 13-01 types) because Phase 7's worker JoinRequest / JoinResponse own those schema names; without it utoipa's merge silently dropped the cluster shapes."
  - "DELETE /v1/modules/{sha} is mounted as /v1/modules/{sha256}: axum cannot hold two parameter names on the same path; the OpenAPI path item merges with the existing GET/PUT."
  - "parity.rs keeps its raw tonic stubs (wire-level REST vs gRPC adapter parity, including malformed protos a typed client cannot send); the client's losslessness is the new grpc_client_parity.rs transcript over the domain traits."
requirements-completed: []
duration: "see Metrics"
completed: 2026-11-24
---

# Phase 13 Plan 03: Phase 13 wire surface (protos, adapters, RequestMeta, GrpcControlPlane) Summary

**The Phase 13 protos are fixed (public `Cluster` / `ModuleLifecycle`, node-only `Membership` / `NodeOps`, worker `Hello.feature_level = 10`), every D-P13-23 REST path and gRPC method reaches the `cluster` / `module_lifecycle` slots (404 / UNIMPLEMENTED when absent, 501 from the 13-02 stubs), one middleware fills `RequestMeta` from the `ostro-*` headers and emits `ostro-consistency`, and `GrpcControlPlane` forwards every trait method losslessly (139-step transcript identical to the direct fakes, errors and details included).**

## Proto field tables (fixed by 13-03; a later change needs a STOP + re-plan note)

### `ostro/api/v1/cluster.proto` (package `ostro.api.v1`)

`service Cluster`: `Init(InitClusterRequest) -> ClusterStatus`, `Join(JoinClusterRequest) -> JoinClusterResponse`, `Status(ClusterStatusRequest) -> ClusterStatus`, `CreateNodeToken(CreateNodeTokenRequest) -> NodeTokenCreated`, `Remove(RemoveMemberRequest) -> ClusterStatus`, `Drain(DrainMemberRequest) -> ClusterStatus`, `Reassign(ReassignPartitionRequest) -> ClusterStatus`, `Rebalance(RebalanceRequest) -> ClusterStatus`, `UpgradeStatus(UpgradeStatusRequest) -> UpgradeStatus`, `UpgradeFinalize(UpgradeFinalizeRequest) -> UpgradeStatus`, `UpgradeHold(UpgradeHoldRequest) -> UpgradeStatus`, `UpgradeRelease(UpgradeReleaseRequest) -> UpgradeStatus`.

`service ModuleLifecycle`: `RetireModule(RetireModuleRequest) -> RetireModuleResponse`, `RunModuleGc(RunModuleGcRequest) -> GcRunReport`, `MigrateRuns(MigrateRunsRequest) -> MigrateRunsReport`.

| Message | Fields |
|---|---|
| InitClusterRequest | 1 `optional string advertise`, 2 `optional uint32 rf`, 3 `optional uint32 min_insync` |
| NodeAdvertise | 1 `string api`, 2 `string cluster`, 3 `optional string repl` |
| NodeVersion | 1 `string ostro_version`, 2 `uint32 level_min`, 3 `uint32 level_max`, 4 `string wasmtime`, 5 `uint32 event_codec_ver`, 6 `repeated string wit_lines` |
| JoinClusterRequest (Debug redacts token) | 1 `string token`, 2 `string csr_pem`, 3 `NodeAdvertise advertise`, 4 `NodeVersion node_version`, 5 `optional uint64 resume_node_id` |
| SeedNode | 1 `uint64 id`, 2 `string addr`, 3 `optional string repl_addr` |
| JoinClusterResponse | 1 `uint64 node_id`, 2 `string cluster_id`, 3 `uint32 partitions`, 4 `string ca_cert_pem`, 5 `string node_cert_pem`, 6 `repeated SeedNode seeds`, 7 `uint32 finalized_level` |
| ClusterStatusRequest, RebalanceRequest, Upgrade{Status,Finalize,Hold,Release}Request, RunModuleGcRequest | empty |
| CreateNodeTokenRequest | 1 `optional uint64 ttl_secs` |
| NodeTokenCreated (Debug redacts token) | 1 `string id`, 2 `string token`, 3 `uint64 expires_at_ms`, 4 `string ca_hash` |
| ClusterMember | 1 `uint64 node_id`, 2 `string addr`, 3 `optional string cluster_addr`, 4 `string state`, 5 `NodeVersion version`, 6 `bool live`, 7 `string raft_role` |
| ClusterReplicationPolicy | 1 `uint32 rf`, 2 `uint32 min_insync`, 3 `uint32 max_voters` |
| PartitionReassignment | 1 `uint32 partition`, 2 `repeated uint64 target`, 3 `repeated uint64 adding`, 4 `repeated uint64 removing`, 5 `uint64 started_at_ms` |
| ClusterStatus | 1 `string cluster_id`, 2 `repeated ClusterMember members`, 3 `repeated uint64 voters`, 4 `repeated uint64 learners`, 5 `repeated uint64 voters_outgoing`, 6 `uint64 meta_leader`, 7 `uint32 finalized`, 8 `optional uint32 hold`, 9 `uint32 level_min`, 10 `uint32 level_max`, 11 `ClusterReplicationPolicy policy`, 12 `repeated PartitionReassignment reassignments` |
| RemoveMemberRequest | 1 `uint64 node_id`, 2 `bool force` |
| DrainMemberRequest | 1 `uint64 node_id` |
| ReassignPartitionRequest | 1 `uint32 partition`, 2 `repeated uint64 replicas`, 3 `bool cancel` |
| MemberLevel | 1 `uint64 node_id`, 2 `uint32 level_min`, 3 `uint32 level_max`, 4 `bool live` |
| UpgradeStatus | 1 `uint32 finalized`, 2 `optional uint32 hold`, 3 `uint32 common_level_max`, 4 `bool auto_finalize`, 5 `repeated MemberLevel members` |
| RetireModuleRequest | 1 `string sha256` |
| WorkflowVersionRef | 1 `string name`, 2 `uint32 version` |
| RetireModuleResponse | 1 `string sha256`, 2 `uint64 retired_at_ms`, 3 `repeated WorkflowVersionRef versions` |
| GcRunReport | 1 `repeated string candidates`, 2 `repeated string deleted`, 3 `repeated string referenced`, 4 `bool postponed`, 5 `optional string reason` |
| MigrateRunsRequest | 1 `string workflow`, `oneof from { 2 uint32 from_version; 3 string from_sha }` (required), 4 `uint32 to_version`, 5 `optional string filter`, 6 `optional uint32 limit`, 7 `bool dry_run` |
| MigrateFailure | 1 `string run_id`, 2 `uint64 seq`, 3 `string detail` |
| MigrateRunsReport | 1 `repeated string passed`, 2 `repeated string repinned`, 3 `repeated MigrateFailure failed`, 4 `repeated string busy`, 5 `repeated string seq_mismatch` |

u8 domain fields (`rf`, `min_insync`, `max_voters`) and u16 (`partition`, `partitions`) travel as uint32 and are range-checked (INVALID_ARGUMENT on the server, `internal` "malformed gRPC reply" in the client).

### `ostro/cluster/v1/membership.proto` (package `ostro.cluster.v1`, cluster listener only)

| Service / message | Fields |
|---|---|
| `service Membership { rpc Removed(RemovedNotice) returns (RemovedAck); }` | 13-09 serves it (`p13::membership::service`) |
| RemovedNotice | 1 `uint64 node_id`, 2 `string cluster_id`, 3 `string reason` |
| RemovedAck | empty |
| `service NodeOps { rpc HandOff(HandOffRequest) returns (HandOffResponse); rpc ModuleRefs(ModuleRefsRequest) returns (ModuleRefsResponse); }` | mounted by `NodeOpsService::routes()` |
| HandOffRequest | 1 `uint32 partition`, 2 `uint64 to`, 3 `uint32 epoch` |
| HandOffResponse | 1 `bool moved`, 2 `uint32 new_epoch`, 3 `string abort_reason` (the 13-02 stand-in called it `reason`) |
| PartitionEpoch | 1 `uint32 partition`, 2 `uint32 epoch` |
| ModuleRefsRequest | 1 `repeated bytes shas` (32 bytes each), 2 `bool protect_resumable`, 3 `repeated PartitionEpoch partitions` |
| ModuleRefsResponse | 1 `repeated bytes referenced`, 2 `repeated PartitionEpoch uncovered` |

No per-batch Raft ack was added (13-05's question): `MetaPeer.Raft` and its empty `RaftAck {}` are in the frozen 12-02 `cluster.proto` and untouched, so 13-05's follower lease anchor (the newest resolved ReadIndex round through the leader) stands as the mechanism.

### Worker `Hello`

`uint32 feature_level = 10` (next free number from 13-01). 0 = absent (an older worker): parse the level from `ostro_version` (D-P13-18). Old Hello bytes decode with `feature_level = 0`; the field encodes as the trailing `0x50 <varint>` (`hello_feature_level_is_additive`). The two existing Hello literals (`ostro-worker/src/session.rs`, `ostro-server/tests/common/p7_workers.rs`) send 0 until 13-14 fills the real level.

## REST routes (OpenAPI additive only: 15 operations, 23 schemas, tag `cluster`)

| Method / path | Trait method | Success |
|---|---|---|
| POST `/v1/cluster/init` (optional body) | `init` | 200 ClusterStatus |
| POST `/v1/cluster/join` (outside the API middleware; no client identity) | `join` | 200 ClusterJoinResponse, `Cache-Control: no-store` |
| GET `/v1/cluster/status` | `status` | 200 |
| POST `/v1/cluster/tokens` (optional body) | `create_node_token` | 201, `Cache-Control: no-store` |
| POST `/v1/cluster/members/{id}/remove` (optional `{force}`) | `remove` | 200 |
| POST `/v1/cluster/members/{id}/drain` | `drain` | 200 |
| POST `/v1/cluster/reassign` | `reassign` | 200 |
| POST `/v1/cluster/rebalance` | `rebalance` | 200 |
| GET `/v1/cluster/upgrade` | `upgrade_status` | 200 |
| POST `/v1/cluster/upgrade/{finalize,hold,release}` | `upgrade_*` | 200 |
| DELETE `/v1/modules/{sha256}` | `retire_module` | 200 |
| POST `/v1/modules/gc` | `run_module_gc` | 200 |
| POST `/v1/runs/migrate` | `migrate_runs` | 200 |

Stateless checks on both transports (`phase13_adapters::check`): node id > 0 (and numeric on REST), sha256 = 64 lowercase hex, reassign replicas non-empty / unique / non-zero unless `cancel`, migrate workflow non-empty, `from.sha` a sha, `limit` > 0. Slot absent: REST 404 `not_found`, gRPC service not mounted. Slot with the 13-01 default bodies (the 13-02 stubs): 501 `unimplemented` / UNIMPLEMENTED naming the plan.

## Header table (RequestMeta)

| Header | Parsed into | Honoured from | Malformed | Client sends |
|---|---|---|---|---|
| `ostro-hop` | `hop: u8` (absent = 0) | anyone | 400 / INVALID_ARGUMENT, `details.header` | when `hop > 0` |
| `ostro-partition` | `partition_pin: u16` | `TrustedNode` only | same | when set |
| `ostro-partitions` | `partitions: Vec<u16>` (comma list, ≤ 4096) | `TrustedNode` only | same | when set |
| `ostro-principal` | `principal` (≤ 256 bytes) | `TrustedNode` only | same | when set |
| `ostro-read-after` | `read_after` (≤ 256 bytes) | anyone | same | when set |
| `grpc-timeout` | `deadline = arrival + t` | anyone | same | remaining budget (tonic `set_timeout`) |
| `ostro-deadline-ms` (REST) | `deadline = arrival + ms` (min with grpc-timeout) | anyone | same | — |
| (none) | `from_node` = `TrustedNode(id)` request extension | cluster listener only | — | never |
| `ostro-consistency` (response) | emitted from the out-slot on success only | — | — | copied back into the caller's out-slot |

Repeated header = 400. A deadline above one year is capped at one year.

## Client trait coverage (`GrpcControlPlane`, every method explicit)

| Trait | Methods | Over gRPC | REST-only (explicit `unimplemented`, `details.method`) |
|---|---|---|---|
| ControlPlane | 52 | 48 (Modules, Workflows, Runs, Tokens, Secrets, Connectors, Queues, Workers) | `presigned_object`, `dev_time`, `dev_skip`, `replay_module` |
| ScheduleApi | 8 | 8 (Schedules) | — |
| WebhookApi | 5 | 5 (Webhooks) | — |
| HookIngress | 1 | — | `deliver` |
| RunLogsApi | 1 | 1 (RunLogs) | — |
| AdminApi | 1 | — | `backup` |
| OpsApi | 12 | 12 (RunOps) | — |
| InventoryApi | 3 | 3 (Inventory) | — |
| ClusterApi | 12 | 12 (Cluster) | — |
| ModuleLifecycleApi | 3 | 3 (ModuleLifecycle) | — |

`ready()` is `true` (lazy channel; an unready peer answers `unavailable`). `status_to_error` inverts `grpc::to_status` exactly (`rate_limited` vs `payload_too_large` by the `retry_after_ms` detail, as on the server table); transport DEADLINE_EXCEEDED / CANCELLED and connection failures are `unavailable`. `DEFAULT_MAX_MESSAGE_BYTES` = 80 MiB each way (`with_max_message_bytes`).

## Task Commits

1. `1b215f6` feat(13-03): Phase 13 protos (Cluster, ModuleLifecycle, Membership, NodeOps) + worker Hello feature_level
2. `87de634` feat(13-03): REST/gRPC adapters for cluster + module lifecycle, RequestMeta headers
3. `63944a4` feat(13-03): production GrpcControlPlane with metadata propagation

## Verification

- Targeted (each run proved >= 1 passed): `-p ostro-proto --test p13_wire` (6), `-p ostro-worker-proto --test wire` (9, incl. `hello_feature_level_is_additive`), `-p ostro-server --lib p13::node_ops` (2), `-p ostro-api` all binaries (lib 69 incl. request_meta 5 / phase13_adapters 3 / grpc_client 3; `phase13_routes` 9; `grpc_client_parity` 3; parity 9, openapi 2, phase7/8/11 surfaces, rest, lifecycle unchanged and green).
- `cargo clippy -p ostro-api --all-targets --locked -- -D warnings`: clean; `cargo build -p ostro-server -p ostro-worker --all-targets`: warning-free; fmt clean.
- OpenAPI snapshot: a JSON comparison shows every pre-existing path operation and schema unchanged; additions are only the 15 Phase 13 operations, 23 schemas and the `cluster` tag.
- Plan-level `nice -n 15 bash scripts/ci-local.sh --no-musl --no-polyglot` at `63944a4` (separate worktree, slot-6): **all steps passed** — fmt, clippy 115 s, build 88 s, sdk, guests, order-saga, cargo test 6089 s, clients go, sleepers, failpoint list sync, failpoint suites 6430 s, replication mutant matrix 243 s, server campaign smoke (20 kills) 745 s, phase 7 failpoint legs, deny, vendored, fixtures, forbidden crates, one log path + P12 grep gates (clock gate incl.), workflow validation; external PG/MySQL/S3 SKIP (no env), polyglot SKIP (flag), musl SKIP (flag).

## Deviations from Plan

### Auto-fixed / adapted

1. **[Rule 3 - Blocking] ostro-server could not name the generated types.** It has ostro-proto only as a dev-dependency and manifests are 13-01's: added `pub use ostro_proto as proto;` to ostro-api (`crates/ostro-api/src/lib.rs`), used by `p13/node_ops.rs`.
2. **[Rule 3] Hello literal sites outside `files_modified`:** `crates/ostro-worker/src/session.rs` and `crates/ostro-server/tests/common/p7_workers.rs` gained `feature_level: 0` (a new proto field breaks struct literals; the value stays 13-14's).
3. **[Rule 1 - Bug] OpenAPI schema collision:** `#[schema(as = ClusterJoinRequest/ClusterJoinResponse)]` on the 13-01 `JoinRequest` / `JoinResponse` (`phase13.rs`, doc attribute only) — utoipa merges keep the first schema of a name, so Phase 7's worker shapes would have been documented for the cluster join.
4. **[Layout] Adapters in a new `phase13_adapters.rs`** (plus wiring in `lib.rs`, `openapi.rs`, one tag line in `rest.rs`) instead of `rest.rs`/`grpc.rs`: the Phase 11 pattern keeps a phase's REST + gRPC adapters in one module over `ExtState`; `phase13.rs` stays 13-01's contract file.
5. **[Names] `ostro-deadline-ms`** (plan name, REST deadline) added to RESERVATIONS §11 Headers; `DELETE /v1/modules/{sha}` mounted as `{sha256}` (route-parameter name only).
6. **[Test design] parity.rs is not rewritten onto `GrpcControlPlane`.** Its raw stubs test the server adapters' wire handling (REST vs gRPC, malformed inputs); the client's losslessness is proven by `grpc_client_parity.rs` (direct fakes vs client → server → fakes, Debug-equal results incl. error messages and details), which covers the 04-11 core plus the Phase 6/7/8/11/13 traits. Two transcript inputs were adjusted to pass adapter-side validation the fakes do not repeat (webhook idempotency `json:/id`, no DLQ policy on a workflow scope); everything else is identical.
7. **`--no-polyglot` on the plan-level ci-local** (orchestrator instruction; no polyglot path touched).
8. **Worktree / target:** session-scratchpad worktree and `target/slot-6` per the executor brief instead of `/tmp/ostro-wt-13-03`.

### Interactions noted (no change made)

- **12-18 follow-up (stale plain meta reads from a cut-off old controller, about 2.6 s):** not in 13-03's plan. 13-03 only adds the `ostro-read-after` / `ostro-consistency` plumbing 13-13 needs; the read gate (13-13) uses 13-05's `read_index_barrier` / lease anchor. Nothing in 13-03 changes the window.
- **13-05:** no per-batch RaftAck (see the proto section).

### Deferred Issues

None.

## Next Phase Readiness / handover

- **13-12:** `PeerControlPlane` = `GrpcControlPlane::new(ClusterTransport::channel(node))` (cache per node); forward inside `request_meta::scope(RequestMeta { hop: 1, .. })` — the client copies the scope as is and sends the remaining deadline. `node_api_services()` must (a) put `TrustedNode(peer_id)` into the http request extensions after `authz::require_node`, and (b) run the `ostro.api.v1` services behind the same parsing: either reuse `ostro_api::router_with(...)` (which already layers `request_meta::middleware`) on the cluster listener, or apply `axum::middleware::from_fn(request_meta::middleware)` to its routes. Without (a) pins and `from_node` stay `None` by design. `status_to_error` maps transport failures to `unavailable` for the retry rule.
- **13-13:** read `RequestMeta::current().read_after`; publish write tokens with `request_meta::set_consistency`; the adapters emit `ostro-consistency` on success only and the client copies it back.
- **13-09:** serve `ostro_api::proto::cluster::v1::membership_server::MembershipServer` from `p13::membership::service`; the cluster join REST route is mounted outside the API middleware and both join responses are `no-store`.
- **13-11 / 13-15:** `NodeOps` is mounted on the cluster listener (`NodeOpsService::routes()` = `Some`); `HandOffResponse.abort_reason` carries the abort text.
- **13-14:** `Hello.feature_level` (field 10) exists; set it in `ostro-worker/src/session.rs::hello` (currently 0).
- **13-16:** `ostro_api::fake::{FakeCluster, FakeModuleLifecycle, FakeMigrateOutcome}` (`install(&mut ext)`) serve the CLI contract tests; behaviour is documented on the types (init → node 1 voter; tokens `njt_<n>` / `njt_<n>.fake-secret-<n>`, single-use; `promote`, `set_live`, `add_module`, `set_referenced`, `add_run`).
- **13-20:** no ci.yml hunk needed (no new failpoint crate or CI step; the new tests run under `cargo test --workspace`).

## Metrics

- 2026-11-24, about 20:10 CET to 00:45 CET (2026-11-25; ci-local took about 3.9 h); 3 tasks, 3 code commits, 24 files.

## Self-Check

PASSED: every created file listed above exists at `63944a4`; commits `1b215f6`, `87de634`, `63944a4` are on main.
