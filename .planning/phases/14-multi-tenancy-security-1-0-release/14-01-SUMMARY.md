---
phase: 14-multi-tenancy-security-1-0-release
plan: 01
subsystem: planning-gate + library contracts + manifests
tags: [gate, reservations, decisions, contracts, auth, audit, ext-tags, cargo, kafka, fuzz, jepsen]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "20/20 SUMMARYs; 13-VERIFICATION human_needed (3 by-design items); RoutedControlPlane, RequestMeta.principal, compat::Feature registry, tier-C ProcCluster/ChaosProxy"
  - phase: 05-12
    provides: "every PLAN has a SUMMARY; VERIFICATION passed (08) or human_needed with by-design items only (05, 06, 07, 09, 10, 11, 12)"
provides:
  - "PHASE-14 GATE part 1 passed (checks 1-4; baseline failures all pass alone, see Gate report)"
  - "Phase 5-13 seam inventory, method inventory, tenant-field inventory, lease clock source, non-active-version paths (below) for 14-01b..14-34"
  - "RESERVATIONS.md P14 rows: ext 112-117 planned, 118-143 spare, meta 24-25 unused, D-P14-29 + D-P14-51 names"
  - "STATE.md: D-P14-1..54, [USER REVIEW] D-P14-3/4, D-P14-12, D-P14-10, [HUMAN] Phase 14 items, Phase 14 shared-file rule"
  - "Contracts: ostro_types::audit, Phase 14 Feature entries, ext 112-117 structs, ostro_api::auth, ostro_api::phase14, ApiError 401/403, ostro_bridge Audited<R>"
  - "Every Phase 14 dependency declared (matrix below); new crates ostro-kafka, ostro-fuzz-targets, ostro-jepsen; fuzz/ workspace manifest + nightly"
affects: [14-01b, 14-02, 14-03, 14-04, 14-05, 14-06, 14-07, 14-08, 14-09, 14-10, 14-15, 14-16, 14-17, 14-18, 14-19, 14-21, 14-22, 14-23, 14-28, 14-29, 14-30, 14-31, 14-32, 14-33, 14-34, 14-27]
tech-stack:
  added: [minisign-verify 0.3.0, wasm-smith 0.261.0, kafka-protocol 0.18.0 (client), snap 1.1.2, ruzstd 0.9.1, derive_arbitrary 1.5.0, flagset 0.4.7 (via kafka-protocol)]
  patterns:
    - "contracts land with default-Unimplemented trait bodies naming the owner plan (Phase 8/11/13 pattern)"
    - "manifests first; declared-ahead dependencies anchored with `use <crate> as _;` (cargo 1.100's unused-dependency lint) until the owner plan uses them"
    - "meta golden extended append-only with `ext <name> <hex>` lines (one per Phase 14 value struct)"
key-files:
  created:
    - crates/ostro-types/src/audit.rs
    - crates/ostro-api/src/auth.rs
    - crates/ostro-api/src/phase14.rs
    - crates/ostro-bridge/src/secrets/audited.rs
    - crates/ostro-kafka/{Cargo.toml,src/*.rs,tests/skeleton.rs}
    - crates/ostro-fuzz-targets/{Cargo.toml,src/*.rs}
    - crates/ostro-jepsen/{Cargo.toml,src/**/*.rs,src/bin/ostro-soak.rs,tests/skeleton.rs}
    - fuzz/{Cargo.toml,rust-toolchain.toml,.gitignore}
  modified:
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
    - crates/ostro-types/src/{lib,compat}.rs
    - crates/ostro-meta/src/ext.rs
    - crates/ostro-meta/tests/{meta_golden.rs,golden/meta_v2.txt}
    - crates/ostro-api/src/{lib,error,phase8}.rs
    - crates/ostro-bridge/src/secrets/mod.rs
    - Cargo.toml, Cargo.lock
    - crates/{ostro-server,ostro,ostro-cli,ostro-api,ostro-client,ostro-triggers,ostro-repl-conformance}/Cargo.toml
    - crates/{ostro-server,ostro-cli,ostro-client,ostro-triggers}/src/lib.rs (dependency anchors only)
key-decisions:
  - "D-P14-20 negative control is inapplicable as things stand: the D-P13-15 lease reads Instant::now() and a raft-driver Instant anchor, never the injected Clock (which has no monotonic method)"
  - "x_run_index prefix 0x04 is already the Tenant row kind (tenant ‖ 0x00 ‖ !created_at ‖ !uuid): 14-16 can list tenant runs from it with no new rows"
  - "No Phase 12 commit-latency histogram exists: 14-23 measures commit p99 client-side (bench_rf3 method) or adds its own histogram"
  - "Remote worker Done carries no fuel: 14-06 takes the charge-the-limit fallback"
  - "Seeded RNG for ostro-jepsen = the existing workspace rand 0.9 pin (no SplitMix64)"
  - "fuzz/ nightly = nightly-2026-11-25 (rustc 1.102.0-nightly 739adecd4 2026-11-24); the path dependency on ostro-fuzz-targets resolves its workspace-inherited fields (no fallback needed)"
  - "Declared-ahead dependencies carry `use x as _;` anchors (cargo 1.100 warns on unused dependencies); owner plans delete the anchor line when they use the crate"
requirements-completed: []
duration: ~2h
completed: 2026-11-26
---

# Phase 14 Plan 01: PHASE-14 GATE part 1 + contracts + manifests Summary

**Gate passed (Phases 5-13 complete); P14 ordinals/names registered; D-P14-1..54 and the review/human items in STATE; Phase 14 contracts (audit sink, ext 112-117 with key fingerprints, Anonymous-default Principal + scopes + 116-row Method inventory, phase14 traits, 401/403 codes, audited resolver stub); every Phase 14 dependency declared with the new ostro-kafka / ostro-fuzz-targets / ostro-jepsen crates and the fuzz workspace.**

Requirement ids of the plan (SEC-02..05, DISP-06, QA-04/05, DIST-01..03, TRIG-04, BACK-05) are **not** ticked here: 14-01 lands contracts and manifests only; the ticks are 14-27's (D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| d3bfeb3 | 1 | docs: RESERVATIONS P14 rows (§10 24-25 unused, §10b 112-117 + 118-143 spare, §11 D-P14-29/51 names) |
| 4bacb4e | 1 | docs: STATE D-P14-1..54, [USER REVIEW]/[HUMAN] todos, Phase 14 shared-files rule |
| 83a4f4c | 2 | feat: Phase 14 contracts |
| d67ff84 | 2 | style: rustfmt of the contract files (found by `cargo fmt --check` before Task 3's commit) |
| e820d59 | 3 | chore: dependency matrix, every manifest, new-crate skeletons, fuzz workspace |
| (this) | — | docs: SUMMARY, ROADMAP tick, STATE line |

## Gate report (Task 1 checks 1-4)

**Check 1 — Phases 05-13 complete** (one script over all nine phases; base cd8dbde):

```
05 plans=24 summaries=24 status=human_needed gaps:(none listed) human_items=5
06 plans=20 summaries=20 status=human_needed gaps: [] human_items=6
07 plans=22 summaries=22 status=human_needed gaps: [] human_items=5
08 plans=22 summaries=22 status=passed gaps:(none listed) human_items=8
09 plans=20 summaries=20 status=human_needed gaps:(none listed) human_items=4
10 plans=20 summaries=20 status=human_needed gaps:(none listed) human_items=7
11 plans=18 summaries=18 status=human_needed gaps: [] human_items=10
12 plans=18 summaries=18 status=human_needed gaps:(none listed) human_items=6
13 plans=20 summaries=20 status=human_needed gaps:(none listed) human_items=3
```

Every human_needed item is by design (VALIDATION "Manual-Only" sets, remote CI legs, user reviews/decisions): 05 native perf / remote legs / docker quickstart / optional ENOSPC / user review; 06 MySQL + SeaweedFS legs / remote CI / GCS / user review / optional Vault; 07 real KEDA / ACME staging + Pebble / remote CI / user review; 09 publishing / remote polyglot job / upstream toolchain determinism PRs + user review / native SC5; 10 user decisions (D-P10-13/14, ephemeral fsync) / user reviews / macOS clean run / remote CI / publishing; 11 real-browser checks / CSRF from a foreign origin / non-loopback host / remote CI / user review (its `follow_ups` are non-blocking, `gaps: []`); 12 QA-01 10k campaign re-run (closed by user decision on r5 evidence) / native failover p99 + bench / S3 legs / remote CI / macOS / user review; 13 the >= 10,000-fault campaign, real previous-binary upgrade, quiet-node timings. No `gaps_found`. Accepted by the orchestrator (cd8dbde "Phase 13 complete").

**Check 2 — code presence:** all OK (`RoutedControlPlane` in route/, `pub mod p13`, `trait UiAuth`, `trait OpsApi`, `pub enum Feature`, `ProcCluster` + `ChaosProxy` in harness/, `SecretResolver` in ostro-bridge, `leader/fair.rs`).

**Check 3 — tree clean:** `git status --porcelain crates vendor scripts .github docs deploy fuzz Cargo.toml Cargo.lock deny.toml` empty in the worktree (the main checkout is stale by design).

**Check 4 — baseline `ostro-k8s-ci --parallel` at cd8dbde** (run p114619-28055, wall 44m48s): every step PASS except four, all of which **pass alone** on cluster slot 1:

| Step | Failure in the parallel run | Rerun alone (slot 1, cd8dbde) |
|---|---|---|
| musl zigbuild test (musl2) | `worker_failover::oscillation` (one 12.6 s on-phase vs 5 s sustain) — known timing-only flake (brief) | PASS (73.5 s) |
| musl zigbuild test (musl1) | `ostro-cluster isr_publisher::outcome_unknown_applied_is_adopted_from_meta_w4` (`isr.pending.is_none()`) — not in the brief's list | 7/7 PASS |
| cargo test (test2) | `ostro-server worker_session::registry_abandons_jobs_when_a_session_closes` (`elapsed < 100ms`) and `registry_heartbeat_renews_and_reports_unknown_leases` (0 vs 1) — not in the brief's list; timing under load | 18/18 PASS |
| Phase 13 tier C | `cluster_campaign_p13` (50 faults, seed 1919): oracle `p13_membership`: "node 4: removed node 4 in the persisted ConfState (voters [3, 4, 2], learners [1])"; workload itself clean (2014 acks, 0 lost, 0 duplicates) | 2/2 PASS (96 s, 92 s) |

No code changed between the last green gate (7942813) and cd8dbde (docs only), so the four are nondeterministic. **The campaign membership finding is reported for an orchestrator decision** (a rare removed-voter-still-in-ConfState outcome under the campaign's DrainRemove/Join faults; oracle untouched).

## Reservations check (check 9; script `rescheck14.py`, scratch, extends 13-01's)

Pre (cd8dbde):
```
Cmd tags: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21, 27, 43, 45]
RejectReason: 0 StaleEpoch, 1 Fenced, 2 SeqMismatch, 3 SeqGap, 4 RunNotFound, 5 RunTerminal, 6 RunExists, 7 IdempotencyConflict, 8 PayloadTooLarge, 9 WrongPartition, 10 Invalid, 11 RunCompleted, 12 ReservedP6_12, 13 ReservedP6_13, 14 ReservedP6_14, 15 RunPaused, 16 ReservedP7_16, 17 ReservedP7_17, 18 ReservedP8_18, 19 ReservedP8_19, 20 ReservedP9_20, 21 ReservedP9_21, 22 ReservedP9_22, 23 ModuleMismatch, 24 RunBusy
MetaRecord: 0 Cluster, 1 Member, 2 Partition, 3 Module, 4 Workflow, 5 Active, 6 Queue, 7 Schedule, 8 Token, 9 Tenant, 10 Secret, 11 Grant, 12 Connection, 13 Egress, 14 WorkflowPolicy, 15 QueuePolicy, 16 Webhook
MetaCommand: 0 InitCluster, 1 Put, 2 Delete, 3 SetPartitionLeader, 4 ElectLeader, 5 AlterIsr, 6 ResignLeader, 7 ReservedP12_7, 8 AlterReplicas
ext tags: 15 EXT_TAG_TEST, 80 EXT_ELECTION_INFO, 96 EXT_NODE_VERSION, 97 EXT_MEMBER_STATE, 98 EXT_CLUSTER_VERSION, 99 EXT_REPLICATION_POLICY, 100 EXT_REASSIGNMENT, 101 EXT_WIT_REQUIREMENT, 102 EXT_RETIRED, 144 EXT_ON_FAILURE, 145 EXT_DLQ_POLICY
RESERVATIONS CHECK: PASS
```
Post (e820d59):
```
Cmd tags: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21, 27, 43, 45]
RejectReason: 0 StaleEpoch, 1 Fenced, 2 SeqMismatch, 3 SeqGap, 4 RunNotFound, 5 RunTerminal, 6 RunExists, 7 IdempotencyConflict, 8 PayloadTooLarge, 9 WrongPartition, 10 Invalid, 11 RunCompleted, 12 ReservedP6_12, 13 ReservedP6_13, 14 ReservedP6_14, 15 RunPaused, 16 ReservedP7_16, 17 ReservedP7_17, 18 ReservedP8_18, 19 ReservedP8_19, 20 ReservedP9_20, 21 ReservedP9_21, 22 ReservedP9_22, 23 ModuleMismatch, 24 RunBusy
MetaRecord: 0 Cluster, 1 Member, 2 Partition, 3 Module, 4 Workflow, 5 Active, 6 Queue, 7 Schedule, 8 Token, 9 Tenant, 10 Secret, 11 Grant, 12 Connection, 13 Egress, 14 WorkflowPolicy, 15 QueuePolicy, 16 Webhook
MetaCommand: 0 InitCluster, 1 Put, 2 Delete, 3 SetPartitionLeader, 4 ElectLeader, 5 AlterIsr, 6 ResignLeader, 7 ReservedP12_7, 8 AlterReplicas
ext tags: 15 EXT_TAG_TEST, 80 EXT_ELECTION_INFO, 96 EXT_NODE_VERSION, 97 EXT_MEMBER_STATE, 98 EXT_CLUSTER_VERSION, 99 EXT_REPLICATION_POLICY, 100 EXT_REASSIGNMENT, 101 EXT_WIT_REQUIREMENT, 102 EXT_RETIRED, 112 EXT_API_TOKEN_META, 113 EXT_MODULE_SIGNATURES, 114 EXT_MODULE_UPLOADERS, 115 EXT_SECURITY_POLICY, 116 EXT_TENANT_SECURITY, 117 EXT_SECRET_TIMES, 144 EXT_ON_FAILURE, 145 EXT_DLQ_POLICY
RESERVATIONS CHECK: PASS
```
The script extracts the `Cmd` oneof tags, `RejectReason` / `MetaRecord` / `MetaCommand` declaration order and every `pub const EXT_*: u32 = N`; pre asserts an empty P14 ext block and MetaRecord ending at 16 (fallback 24-25 unused), post asserts exactly ext 112-117 and no other ordinal change.

## Phase 5–13 seams (Task 1 check 5)

Recorded from the code at cd8dbde (the plan's Phase-5-era `<interfaces>` adapted below).

| Seam | Where (file / function) | Notes for Phase 14 |
|---|---|---|
| `ApiExtensions` | `crates/ostro-api/src/phase8.rs:748` (re-exported `ostro_api::ApiExtensions`); slots `schedules, webhooks, hooks, run_logs, admin, hook_body_limit, ops, inventory, cluster, module_lifecycle` + **14-01: `tenants, api_tokens, audit, security`** | `impl Default` (all `None`); every literal in the tree uses `..ApiExtensions::default()` / `..phase8_extensions(cc)` / `..p8.extensions()`, so the four new slots needed no other edit. The definition file is phase8.rs (not lib.rs): edited as a recorded deviation. |
| `ApiConfig` | `crates/ostro-api/src/lib.rs:60` | unchanged |
| `RoutedControlPlane` | type `crates/ostro-server/src/route/mod.rs:701`; built by `route::wrap_control_plane(ctx, local)` (`route/mod.rs:1266`, re-exported as `p13::wrap_control_plane`), called once at `server.rs:648` (`let routed = p13::wrap_control_plane(&p13_ctx, Arc::clone(&control))`) and handed to `ControlCtx::with_routed(routed)` (`server.rs:679`, `control/mod.rs:737`). Extensions routed by `route::wrap_extensions` (`route/mod.rs:1281`, via `p13::fill_extensions`). | In-process consumers reading `ControlCtx.routed`: webhook deliveries (`control/webhooks.rs:445-448`, `HookIngress::deliver` → `start_run`/`signal_run`/`complete_token`), schedule manual trigger (`control/schedules.rs`), DLQ requeue (`control/p11_dlq.rs`), the `on_failure` hook service (`services/dlq_hook.rs:298-303`), bulk jobs (`services/bulk.rs`), and the API + UI routers (`p11.rs:251`). 14-01b's `wrap_ingress` must give REST/gRPC/UI the guarded instance and leave the others on `routed`. |
| REST/gRPC/UI router construction | `p11::wire` (`crates/ostro-server/src/p11.rs:210-262`): `ext = p11::extensions(cc, ctx)` (Phase 8 + 11 slots) → `p13_extensions(&mut ext)` (Phase 13 slots + routing) → `ostro_api::router_with(cp = cc.routed, api_cfg, ext.clone())` and `ostro_ui::router(UiDeps { cp, ext, cfg: ui, auth: Arc::new(ostro_ui::auth::AllowAll) })`; merged at `server.rs:684-693`. | Each router receives the routed `Arc<dyn ControlPlane>` and the routed `ApiExtensions`; the UI also gets `auth: Arc<dyn UiAuth>` (AllowAll today). |
| Node API `RequestMeta.principal` ingest | parsed in `crates/ostro-api/src/request_meta.rs:251-274` (`principal: principal.filter(\|_\| trusted_node)`); the node API router is `route::node_api_router` (`route/mod.rs:1345`) behind `trusted_node` (`route/mod.rs:1326`) which inserts `request_meta::TrustedNode`; served on the cluster listener via `LateApi` (`route/mod.rs:1293-1303`). Forwarders set the header in `route/peer.rs`. | 14-15 owns the ingest site (`route/peer.rs` + this router). |
| `ApiError` codes | `crates/ostro-api/src/error.rs`: before 14-01 no 401/403 code. **14-01 added `ErrorCode::Unauthenticated` (401, `WWW-Authenticate: Bearer`, gRPC UNAUTHENTICATED) and `ErrorCode::PermissionDenied` (403, PERMISSION_DENIED)** + the Phase 14 `reason::*` constants and `unauthenticated()/permission_denied()/missing_scope()` constructors. | No exhaustive match on `ErrorCode` outside error.rs needed changes. |
| D23 helpers | `crates/ostro-meta/src/validate.rs:142` `is_tenant_id`; `crates/ostro-server/src/control/names.rs:71` `qualify`, `:80` `split_qualified`, `:91` `qualify_request_name`; duplicate `qualify` in `crates/ostro-server/src/derive.rs:556`; API-side `is_tenant_id` copy in `crates/ostro-api/src/phase11.rs:919`. | — |
| `x_run_index` | `crates/ostro-partition/src/store/derived.rs` (keyspace `x_run_index`, version marker `0x00 'v'`, rebuild `PartitionStore::start_run_index_rebuild`), hooks in `crates/ostro-partition/src/apply/index.rs`, query in `crates/ostro-partition/src/query.rs`. Row kinds: Tag 0x01, Workflow 0x02, Status 0x03, **Tenant 0x04**, All 0x05; Tenant rows = `0x04 ‖ tenant ‖ 0x00 ‖ !created_at_ms ‖ !uuid`. | Prefix 0x04 is **already implemented as the tenant row** (written for every run with a tenant, rebuilt from `runs`): 14-16 can serve tenant run lists from it directly (no new rows); the D-P14-7 budgeted scan remains the fallback while the index is `Rebuilding`. |
| Phase 8 cron/webhook drivers | Cron: partition-internal leader service `crates/ostro-partition/src/leader/cron.rs` (fires a `ScheduleFire` command; apply `apply/schedule.rs` starts the run internally with the `__cron/` dedupe row, `CRON_KEY_PREFIX` at `apply/schedule.rs:83`) — started by `Node::open` from `node_cfg.cron = cfg.triggers.to_cron_config()` (`server.rs:531-535`); it never goes through a ControlPlane. Manual trigger: `control/schedules.rs` (`ScheduleApi::trigger_schedule`, key `__cron/<name>/manual/<hex>`). Webhooks: `control/webhooks.rs` (`HookIngress::deliver`, key `__hook/<hook>/<delivery>` = `HOOK_KEY_PREFIX` `:117`), through `ctx.routed`. | Trigger marker on start requests = the idempotency-key prefix only (`StartRunRequest` has no trigger field): `__cron/` (manual trigger) and `__hook/`; 14-01b derives `TriggerKind::{Cron, Webhook, Kafka}` from `__cron/`, `__hook/`, `__kafka/`. Cron fires themselves bypass the API (no ingress hook possible there; quotas apply at dispatch). |
| `[triggers]` config | `crates/ostro-server/src/config.rs:1141` `TriggersSection { webhook_body_limit, webhook_tolerance, cron_catchup_cap }` (+ `to_cron_config`) | 14-01b adds the raw `kafka` array. |
| `start_triggers` site (14-01b/14-32) | after `p13::wrap_control_plane` and `ControlCtx::with_routed` in `server.rs` (~`:648-681`), next to `p13::start`; it needs `routed` and the node's `PartitionRoles` (as the cron owner rule uses leadership). | — |
| Bridge resolver construction | `crates/ostro-server/src/bridge_services.rs:170` (`SecretResolver::new(meta, self.chain(tenant))` per tenant) and `:597` (operator resolver); `BridgeServices::build` called at `server.rs:405`. `SecretResolver` is a concrete struct (`crates/ostro-bridge/src/secrets/resolver.rs:101`), not a trait. | `Audited<R>` (14-01 stub) wraps it; 14-01b swaps the construction sites. |
| Phase 7 join/renew/revoke + demand | join: `control/p7_workers.rs:162` `join_worker` → `workers/join.rs`; renew: `workers/join.rs:369` (`renew`) and the worker stream `workers/service.rs:1324`; revoke: `control/p7_workers.rs:154` → `workers/join.rs:225` `revoke_token`; demand: `control/p7_workers.rs:183` `queue_demand` + `workers/demand.rs`. | `worker_event` hook sites (14-01b). |
| `fair.rs` / node-wide counters | `crates/ostro-partition/src/leader/fair.rs`: `FairQueues` (`new, insert, remove, next_candidate(now_ms), take, begin_round, skip_queue, hold, release_held, park_queue, park_key(queue, key, until_ms), unpark_due, gate_first(_key), ungate_all, set_weight(queue, weight), per_queue_*`), `FairEntry`, `Candidate`. Node-wide counters: `crates/ostro-server/src/flow/limits.rs` `Counters { try_admit(&AdmitReq, now_ms) -> Result<Charge, Block>, release, rollback, running, key_running }`, `RateCells` (GCRA `Gcra`), owned by `flow::NodeFlow` (`flow/mod.rs:87`). | 14-06 adds the tenant level above queue. |
| Activation end / remote `Done` fuel | embedded: `ostro-server/src/executor.rs` `done()` (`:731`) / outcome mapping `:1059-1071`; fuel metered in `ostro-engine/src/activation.rs:474` (`ostro_fuel_consumed_total`). Remote: `ostro-worker-proto` `Done { run, token, outcome (postcard WireOutcome), engine_end_seq }` — **no fuel field** (`WireOutcome` has none). | 14-06 takes the D-P14 fallback: charge the activation fuel limit for remote activations. |
| `Clock` | `crates/ostro-partition/src/clock.rs`: `trait Clock { now_ms(); subscribe_changes(); skip_barrier() }` (wall ms only; **no monotonic method**); `SystemClock`, `ManualClock`; constructed at `server.rs:355` (`dev_clock` or `SystemClock`), carried by `ControlCtx.clock`, `P13Ctx.clock`, `ServerConfig`-level `with_clock`. | 14-10's SkewClock wraps this trait. |
| Failpoints in spawned processes | `crates/ostro/src/main.rs:142-146`: a `--features failpoints` build calls `fail::FailScenario::setup()` (reads `FAILPOINTS` once at start). The tier-C harness sets it per process before (re)start: `ProcCluster::set_failpoints(i, spec)` / `failpoints(i)` (`harness/process.rs:779-784`). **No runtime reconfiguration endpoint exists.** | 14-10 adds `POST /v1/admin/test/failpoints` (failpoints builds only). |
| `ProcCluster` / `ChaosProxy` / oracles | `crates/ostro-repl-conformance/src/harness/process.rs` (`ProcCluster::start_with(spec, ProcOptions)`, `start_single_with`, `kill9`, `restart_many`, `stop_gracefully`, `set_failpoints`, `set_bin`, `set_level_max`, `proxy()`, `proxy_for(from,to)`, `config(i)`, `log_tail`, `pid`, `failpoint_bin()/ensure_failpoint_bin()`); `harness/chaos_proxy.rs` (`ChaosProxy::start(links)`, `cut/blackhole/delay/heal(from,to)`, `isolate(node)`, `heal_node`, `partition`, `heal_all`, `stats`); `harness/p13.rs` (`P13Harness`). Oracles: `src/oracle/{cluster_formed,disk,election,la_rule,objstore,replica,standby,status,p13_gc,p13_membership,p13_ryw,p13_upgrade}.rs`, result type `oracle::Check { new, violation, is_ok, mentions, into_result }`. | 14-09 builds on these; oracles never changed. |
| Phase 12 commit-latency histogram | **none exists.** The only histograms are `ostro_apply_batch_seconds` / `ostro_state_checkpoint_seconds` (`ostro-partition/src/machine.rs:162-163`). `tests/bench_rf3.rs` measures commit latency client-side (start request → ack, p50/p99). | 14-23 fallback: client-side p99 (bench_rf3 method), or adds a histogram in its own files; recorded for 14-23. |
| Upload path order | `crates/ostro-server/src/control/modules.rs::upload_module` (`:253`): sha parse + body hash check `:257-266` → **existing-sha lookup** `:278` (`s.module(&sha)`) → validate + AOT compile `:289` → binding checks (`binding_conflict` `:334`, `module_name_conflict` `:338`) → meta writes (module record, WorkflowVersion puts `:380`) → re-check `:393` → **auto-activation `Active` create-only put** `:414` → result `:429`. | `upload_gate` goes between `:266` and `:278` (14-01b). |
| Non-active version / sha selection paths | (1) explicit start version: `control/runs.rs::start_run` (`:277`) → `resolve_version(cp, &workflow, req.version)` (`:204`, `:288`); (2) migrate target: `control/migrate.rs::migrate_runs` (`:419`) → `version_sha(r.to_version, "to_version")` (`:185`) and `module_retire::check_not_retired`; (3) rerun version override: `control/p11_rerun.rs::rerun_run` (`:149`) → `target_version(ctx, &src.rec, &req.version)` (`:197`, `:460`); (4) retry: `control/lifecycle.rs::retry_run` (`:320`) — `RetryRequest` has **no version override** (resume/restart keeps the run's pinned version), so no gate is needed there; (5) restart through keyed starts `p13/keyed_start.rs` reuses the run's module pin. | `version_gate` call sites: (1)–(3). |
| Signal / await-token completion | `control/lifecycle.rs::signal_run` (`:160`), `::complete_token` (`:459`). **There is no `control/signals.rs` or `control/tokens.rs`** (D-P14-33 names them); the 14-01b hook lines go into `lifecycle.rs`. | — |
| ostro-proto descriptor export | **none**: `crates/ostro-proto/src/lib.rs` uses `tonic::include_proto!` only; no `FILE_DESCRIPTOR_SET`. | 14-02 (owns ostro-proto) adds it for 14-15's gRPC enumeration. |
| Ephemeral-dev marker (14-28) | `ServerControlPlane.cfg: Arc<ServerConfig>` (`control/mod.rs:120`) is visible to `control/modules.rs` (`upload_module(cp: &ServerControlPlane, …)`); dev profile = `crate::tls::TlsProfile::of(&cp.cfg) == TlsProfile::Dev` (`tls/mod.rs:46`, keyed on `secrets.auto_sealing_key`), ephemeral = `cp.cfg.node.ephemeral` (`config.rs:163`). Binding-conflict branch: `modules.rs:330-345` (and the re-check `:390-396`). | — |
| Uploader meta snapshot / meta leadership (14-29) | `crates/ostro-backup/src/uploader.rs:614-622`: the meta snapshot is taken only when `cl.is_none_or(\|c\| c.single)` and the applied index moved (`sess.meta_index != Some(index)`); key `meta_key(index)` (`:381`). `UploaderClusterDeps` `:319`, `with_cluster` `:429`. Meta leadership: `MetaHandle::is_leader()` (`ostro-meta/src/handle.rs:821`), `raft_status()` (`:826`). Server wiring: `crates/ostro-server/src/services/uploader.rs`. | — |
| `restore()` cluster-bucket refusal (14-30) | `crates/ostro-backup/src/restore.rs:406` `refuse_cluster_bucket` → `RestoreError::ClusterBucketUnsupported` (`:154`, raised `:422`); entry `restore()` `:729`. | — |
| Inspect-mode replay (14-29 observer) | `crates/ostro-partition/src/node.rs`: `StartMode::Inspect` (`:74-81`) → `TermMode::Inspect` (`:406`), no services (`:606-639`). | `replay_observer.rs` hooks here. |
| Controlled shutdown / ISR re-add (14-33) | `crates/ostro-cluster/src/shutdown.rs`: `controlled_shutdown` / `controlled_shutdown_with` (`:160/:170`): single-member skip → meta read barrier (`QUORUM_PROBE`) → partition handoffs (`targets(me, isr, replicas)` `:88`, `HANDOFF_PARALLELISM`) → meta leadership transfer → report; called from `p13::before_shutdown`. ISR: `ostro-log/src/replica/isr.rs` (`IsrState::{new, propose, commit, reject, set_published}`, `should_expand` `:106`, `should_shrink` `:129`, `next_change` `:137`), `tracker.rs` (`last_caught_up_ms` `:24`, set in `new/set_replicas` `:62/:97`, advanced in `on_fetch` `:170`); publisher `ostro-cluster/src/isr_publisher.rs` (re-add gate `:24-26`, `:320`). | — |

## Lease clock source (check 6, W2)

The D-P13-15 leader-lease check does **not** read the injected `Clock`. `route/consistency.rs::lease_holds(anchor, now, limit)` compares `std::time::Instant` values: `NodeReadEnv::now()` returns `Instant::now()` directly (`route/consistency.rs:497-500`, marked `// clock-exempt: the lease is measured on the monotonic clock (D-P13-15)`), and the anchor is `MetaHandle::lease_anchor() -> Option<Instant>` from the vendored raft driver. `ostro_partition::clock::Clock` has no monotonic method at all (wall `now_ms` only). Consequence for 14-10/14-22 (D-P14-20): a `SkewClock` over the injected `Clock` cannot reach the lease, so the monotonic-drift **negative control is inapplicable** (report it as such) unless a later plan routes the lease through an injectable monotonic source (not in 14-10's files).

## Method inventory (check 7)

`ostro_api::auth::Method` (landed in Task 2) is the inventory: 52 `ControlPlane` methods, `ScheduleApi` 8, `WebhookApi` 5, `HookIngress` 1, `RunLogsApi` 1, `AdminApi` 1, `OpsApi` 12, `InventoryApi` 3, `ClusterApi` 12, `ModuleLifecycleApi` 3, and the Phase 14 traits `TenantsApi` 7, `ApiTokensApi` 5, `AuditApi` 1, `SecurityApi` 5 = 116 rows, each with scope (D-P14-3), tenant class (D-P14-7: `run_id`, `qualified_name`, `list_runs`, `meta_list`, `system_only`, `tenant_admin`, `unscoped`), audited flag (D-P14-13) and wave (3 = reachable behind the 14-03 guard; 4 = Phase 14 traits from their owner plans 14-17/18/19). Rules used: reads → `read`; upload/activate/workflow & failure policies/retire/migrate/attach signatures → `deploy`; cancel/terminate/retry/pause/resume/rerun/bulk/DLQ/queue policy & flow/schedule pause-resume-trigger → `operate`; start/signal/complete token/hook delivery → `trigger`; secrets/grants/connections/egress/schedule & webhook CRUD/tokens → `admin` + `tenant_admin`; workers/cluster/backup/tenants/cluster policy/GC/dev skip → `admin` + `system_only` (status-style reads `read` + `system_only`). Audited = the D-P14-13 capture list (reads, starts, signals, token completions not audited; worker join/renew/revoke audited through `worker_event`). Phase 6 bridge admin methods live on `ControlPlane` (no separate trait); Phase 7 has no extension trait (its methods are on `ControlPlane` too); `phase7.rs` holds DTOs only.

## Tenant-field inventory (check 8, B7)

| Field | Type / where | Phase 14 rule (14-16 implements verbatim) |
|---|---|---|
| `UploadModule.tenant` | `Option<String>`, `ostro-api/src/domain.rs:315` | force/reject (D-P14-38) |
| `StartRunRequest.tenant` | `Option<String>`, `domain.rs:421` | force/reject |
| `PutSecret.tenant` | `Option<String>`, `domain.rs:965` | force/reject |
| `list_secrets(tenant)`, `delete_secret(name, tenant)` | method parameters, `domain.rs:114/119` | force/reject |
| token-create `tenant` | `CreateTokenRequest.tenant` (14-01, `phase14.rs`) | force/reject (tenant admin creates only its own tenant's tokens) |
| `AuditQuery.tenant` | 14-01, `phase14.rs` | force/reject |
| `ApiTokensApi::list_tokens(tenant)` | 14-01, `phase14.rs` | force/reject |
| `TrustedKeySpec.tenant` | 14-01, `phase14.rs` | force/reject (tenant keys only for the own tenant) |
| `ListRunsRequest.tenant` | **does not exist** (`domain.rs:611` has workflow/status/queue/tag/created_*/limit/page_token/tags) | tenant comes from the principal only; 14-16 filters with the x_run_index Tenant rows (no field to force) |
| Bulk filter tenant | `BulkCreateRequest` filter (phase11.rs) carries workflow/queue names, no tenant field | names qualified (`qualified_name`); the job's run selection filtered by the principal's tenant |
| Qualified-name fields (D23 `[<tenant>/]<name>`) | workflow names (`StartRunRequest.workflow`, `ActivateWorkflow`, policies), queue names (`StartRunRequest.queue`, queue policy/flow/pause/demand, inventory filters), grant `module`/`secret`, `ConnectionInfo.name`, `EgressInfo.name`, schedule names + their `workflow`/`queue`, webhook names + `workflow`, failure-policy targets, DLQ filters (`DlqListRequest` workflow/queue), migrate `workflow`, retire module name, `HookRequest.name` (`POST /hooks/{tenant}/{name}` → `<tenant>/<name>`) | qualify on input / strip on output; a qualified name from a tenant principal → 400 (D-P14-7) |
| Run-id fields | `get_run`, `run_history`, `signal_run`, `cancel_run`, `terminate_run`, `retry_run`, `await_result`, `pause_run`, `resume_run`, `run_logs`, `rerun_run`, `analyze_rerun`, `requeue_dlq`/`dismiss_dlq` (DLQ entry ids name runs), await-token completion (token encodes the run) | `RunRecord.tenant` must match → 404 |
| Worker / cluster / partition filters | `list_workers`, worker tokens, cluster ops, `list_partitions` | system-only (no tenant field) |

## Non-active-version paths

See the "Non-active version / sha selection paths" row above: `version_gate` call sites are `control/runs.rs::start_run` (explicit `req.version` via `resolve_version`), `control/migrate.rs::migrate_runs` (`to_version`), `control/p11_rerun.rs::rerun_run` (`target_version`). `retry_run` has no version override (no gate needed); keyed restarts reuse the run's pinned module.

## Contract signatures as landed (Task 2)

```rust
// ostro_types::audit  (crates/ostro-types/src/audit.rs)
pub enum ActorKind { Token, System, Worker, Node, Dev, Anonymous }           // as_str()
pub enum AuditOutcome { Ok, Denied(String), Failed(String) }
pub struct AuditEvent { action, actor, actor_kind: ActorKind, tenant: Option<String>, target,
                        outcome: AuditOutcome, request_id: Option<String>, details: BTreeMap<String,String> }
pub enum CoalesceKey { SecretAccess { run, module, secret, origin }, AuthFailure { token_id, client_ip } }
pub struct AuditUnavailable(pub String);       // 503 audit_unavailable (not executed)
pub struct AuditCommitError(pub String);       // 500 audit_commit_failed (executed = true)
pub type CommitFuture = Pin<Box<dyn Future<Output = Result<(), AuditCommitError>> + Send>>;
pub trait PermitSlot: Send { fn commit(self: Box<Self>, ev: AuditEvent) -> CommitFuture; }   // 14-04 writer side
pub struct AuditPermit;  // new(Box<dyn PermitSlot>), noop(), commit(self, ev) -> CommitFuture
pub trait AuditSink: Send + Sync {
    fn reserve(&self) -> Result<AuditPermit, AuditUnavailable>;
    fn record_coalesced(&self, ev: AuditEvent, key: CoalesceKey);
}
pub struct NoopAuditSink;

// ostro_types::compat::Feature  (+ Feature::P14)
ApiTokenMetaExt, ModuleSignaturesExt, ModuleUploadersExt, SecurityPolicyExt, TenantSecurityExt,
SecretTimesExt  // introduced_at() == LEVEL (2); names *_ext

// ostro_meta::ext  (tags 112-117; typed get_*/put_* keep unknown tags)
EXT_API_TOKEN_META = 112   ApiTokenMeta { name, description, created_by, rotated_from: Option<String> }
EXT_MODULE_SIGNATURES = 113 ModuleSignatures { sigs: Vec<ModuleSig { alg: u8, key_fingerprint: [u8; 32],
                                key_id: Vec<u8>, sig: Vec<u8>, added_at_ms: u64, added_by: String }> }
EXT_MODULE_UPLOADERS = 114 ModuleUploaders { tenants: Vec<String> }
EXT_SECURITY_POLICY = 115  SecurityPolicy { require_signed_modules: bool, trusted_keys: Vec<TrustedKey>, auth_mode: u8 }
EXT_TENANT_SECURITY = 116  TenantSecurity { require_signed_modules: Option<bool>, trusted_keys: Vec<TrustedKey> }
EXT_SECRET_TIMES = 117     SecretTimes { created_at_ms: u64, rotated_at_ms: Option<u64> }
TrustedKey { alg: u8, key_id: Vec<u8>, public_key: Vec<u8>, comment: String }
SIG_ALG_MINISIGN_ED = 1, SIG_ALG_MINISIGN_ED_PREHASHED = 2, SIG_ALG_COSIGN_P256 = 3
AUTH_MODE_UNSET = 0, AUTH_MODE_NONE = 1, AUTH_MODE_TOKEN = 2; P14_EXT_TAGS = [112..=117]
get_api_token_meta/put_api_token_meta, get_module_signatures/put_…, get_module_uploaders/…,
get_security_policy/…, get_tenant_security/…, get_secret_times/…

// ostro_api::auth  (crates/ostro-api/src/auth.rs)
pub enum Scope { Admin, Deploy, Operate, Trigger, Read }          // ALL, as_str, parse
pub struct ScopeSet;  // EMPTY, of(&[Scope]), admin(), contains, allows (admin allows all), is_empty, iter,
                      // to_strings, parse(&[S]) (non-API names reject the set), to_csv/from_csv
pub enum PrincipalKind { Anonymous, Token, Dev, System, Node }
pub struct Principal { token_id: String, tenant: Option<String>, scopes: ScopeSet, kind: PrincipalKind }
  // anonymous(), dev(), system(), token(id, tenant, scopes), is_anonymous, is_system_scope,
  // encode_forwarded() -> "p1;<token_id>;<tenant|*>;<scopes csv>", decode_forwarded(&str)
tokio::task_local! { pub static PRINCIPAL: Principal; }
pub fn current() -> Principal          // Principal::anonymous() outside any scope (B2)
pub async fn scope<F: Future>(p: Principal, fut: F) -> F::Output
pub enum AuthError { Missing, Malformed, Invalid, Expired, NotApiToken, Unavailable(String) } // to_api_error(): 401 / 503
#[async_trait] pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError>; }
pub struct AllowDev;                    // mode none: Principal::dev()
pub enum TenantClass { RunId, QualifiedName, ListRuns, MetaList, SystemOnly, TenantAdmin, Unscoped }
pub struct MethodSpec { api, name, scope: Scope, tenant: TenantClass, audited: bool, wave: u8 }
pub enum Method { /* 116 variants */ }  // ALL, all(), spec(), name() = "<api>.<method>", scope(), find(api, name)

// ostro_api::phase14  (slots ApiExtensions.{tenants, api_tokens, audit, security}; None -> 404 in 14-02)
TenantSpec, TenantInfo { #[flatten] spec, suspended, created_at_ms }, TenantPatch (Option<Option<_>>),
CreateTokenRequest { name, description, scopes, tenant, ttl_ms, no_expiry }, TokenInfo (never a hash),
CreatedToken { info, bearer } (Debug redacts), RotateTokenRequest { revoke_old, ttl_ms }, WhoAmI,
AuditQuery { tenant, actor, action, since_ms, until_ms, limit, cursor }, AuditRecordView,
AuditPage { records, cursor, partial_nodes }, TrustedKeySpec { alg, key_id, public_key, comment, tenant },
ModulePolicy { require_signed_modules, trusted_keys }, AttachSignaturesRequest { sha256, signatures },
AttachSignaturesResult
trait TenantsApi { create_tenant, get_tenant, list_tenants, update_tenant, suspend_tenant, resume_tenant, delete_tenant }
trait ApiTokensApi { create_token, list_tokens(tenant), revoke_token, rotate_token(id, req), whoami }
trait AuditApi { query(AuditQuery) -> AuditPage }
trait SecurityApi { get_module_policy, set_module_policy, add_trusted_key, remove_trusted_key, attach_signatures }
// every method has a default body: Unimplemented "… lands in 14-17/14-18/14-19"

// ostro_api::error
ErrorCode::Unauthenticated (401 + WWW-Authenticate: Bearer / UNAUTHENTICATED),
ErrorCode::PermissionDenied (403 / PERMISSION_DENIED); ApiError::{unauthenticated, permission_denied(reason, msg), missing_scope(method, scope)}
reason::{UNAUTHENTICATED, MISSING_SCOPE, FORBIDDEN_TENANT, TENANT_SUSPENDED, TENANT_EVENT_RATE, TENANT_NOT_EMPTY,
         SIGNATURE_REQUIRED, SIGNATURE_INVALID, AUDIT_UNAVAILABLE, AUDIT_COMMIT_FAILED}

// ostro_bridge::secrets::Audited<R>  (14-01 passthrough; 14-18 records CoalesceKey::SecretAccess)
Audited::new(inner, sink: Arc<dyn AuditSink>), inner(), sink(), into_inner();
impl Audited<SecretResolver> { resolve, resolve_operator, resolve_reserved }  // delegate
```

## Dependency matrix (Task 3; the verify also read it from `target/14-01-dep-matrix.md`, a worktree scratch copy)

Built from `grep` over all 35 `14-*-PLAN.md` (crate names, "declared by 14-01", per-plan
interfaces) plus the 14-01 Task 3 minimum table. 14-28's `notify` removal and its ostro-cli
`libc` line are excluded (14-28 owns them, D-P14-47). "existing" = already declared before
14-01 (no edit needed); "new" = added by 14-01.

| Crate | Dependency | Kind | Status | Needed by |
|---|---|---|---|---|
| root `Cargo.toml` | `minisign-verify = "=0.3.0"` | workspace pin | new | 14-05 |
| root | `arbitrary = { version = "1.5.0", features = ["derive"] }` | workspace pin | new (1.5.0 already in the lock) | 14-07, 14-08 |
| root | `wasm-smith = "=0.261.0"` | workspace pin | new | 14-08 |
| root | `wasm-encoder = "=0.261.0"` | workspace pin | new (0.261.0 already in the lock via wasmtime) | 14-08 |
| root | `kafka-protocol = { version = "=0.18.0", default-features = false, features = ["client"] }` | workspace pin | new | 14-31, 14-34 |
| root | `snap = "1.1.2"`, `ruzstd = "0.9.1"` | workspace pins | new | 14-34 |
| root | `rustls-native-certs = "0.8"`, `anyhow = "1"` | workspace pins | new (both already transitive in the lock) | 14-31, 14-34 |
| root | `[profile.release.package.kafka-protocol] opt-level = "z"` | profile | new | 14-34 (size) |
| root | `exclude += "fuzz"` | workspace | new | 14-07 |
| `ostro-server` | `minisign-verify` | normal | new | 14-05 (`p14/signing.rs`) |
| `ostro-server` | `aws-lc-rs` (direct) | normal | new | 14-03 (RNG, constant-time), 14-05 (ECDSA/Ed25519), 14-21 (HMAC) |
| `ostro-server` | `base64` | normal | existing | 14-02/14-19 (signature header), 14-21 (cookie) |
| `ostro-server` | `serde_json`, `sha2`, `hmac` | normal | existing | 14-04 (chain), 14-21 |
| `ostro-server` | `ostro-kafka = { path, optional = true }` + feature `kafka = ["dep:ostro-kafka"]` | normal (optional) | new | 14-32 |
| `ostro-server` | `proptest`, `tempfile` | dev | new (`tempfile` is a normal dep already; listed as dev too) | 14-04, 14-06, 14-10 |
| `ostro-server` | `ostro-api = { path, features = ["fake"] }` | dev | new | 14-03, 14-15 |
| `ostro-server` | `ostro-kafka = { path, features = ["testkit"] }` | dev | new | 14-32 (`tests/kafka_trigger.rs`) |
| `ostro-server` | feature `failpoints` includes `ostro-partition/failpoints` | feature | existing (verified) | 14-10 SkewClock |
| `ostro` | feature `kafka = ["ostro-server/kafka"]`, `default = ["kafka"]` | feature | new | 14-32, D-P14-42 |
| `ostro` | feature `failpoints` → `ostro-server/failpoints` (→ `ostro-partition/failpoints`) | feature | existing (verified; `ostro` has no direct ostro-partition dep, the chain goes through ostro-server) | 14-10 |
| `ostro` | `ostro-kafka = { path, features = ["testkit"] }` | dev | new | 14-32 (`tests/kafka_crash.rs`) |
| `ostro-partition` | `failpoints` exposes the SkewClock path (`dep:fail`) | feature | existing | 14-10 |
| `ostro-partition` | `proptest` | dev | existing | 14-06 (fair_tenant) |
| `ostro-cli` | `base64` | normal | new | 14-19 (`deploy --signature`) |
| `ostro-cli` | `insta`, `ostro-api` (fake), `axum` | dev | existing | 14-17/14-18/14-19 CLI snapshots |
| `ostro-api` | `base64` | normal | new | 14-02 (signature header decoding) |
| `ostro-client` | `base64 = "0.22"` (explicit version, like the crate's other deps) | normal | new | 14-17/14-19 (signature upload in `security.rs`) |
| `ostro-ui` | none | — | — | 14-21 (HMAC/cookies are server-side via aws-lc-rs) |
| `ostro-bridge` | `ostro-types` (audit sink) | normal | existing | 14-01 `secrets/audited.rs`, 14-18 |
| `ostro-triggers` | `base64` | normal | new | 14-32 (envelope `key_b64`/`value_b64`) |
| `ostro-repl-conformance` | `ostro-server` (failpoints via the `failpoints` feature chain of the spawned binary) | normal | existing | 14-26 `p14_integration` |
| `ostro-repl-conformance` | `ostro-kafka = { path, features = ["testkit"] }` | dev | new | 14-32 (`tests/kafka_failover.rs`) |
| `ostro-backup`, `ostro-log`, `ostro-cluster` | none | — | — | 14-29/14-30/14-33 use existing deps |
| `ostro-fuzz-targets` (new, `publish = false`) | `arbitrary` (derive), `wasm-smith`, `wit-component`, `wit-parser`, `wasmparser`, `wasm-encoder` (all `=0.261.0` family), path deps `ostro-types`, `ostro-meta`, `ostro-partition`, `ostro-log`, `ostro-engine`, `ostro-api`, `ostro-server`, `ostro-triggers`, `ostro-bridge`, plus `tokio`, `serde_json`, `tempfile` | normal | new | 14-07, 14-08 |
| `ostro-jepsen` (new, `publish = false`) | `ostro-repl-conformance`, `ostro-client`, `ostro-types`, `ostro-partition`, `ostro-log`, `ostro-server`, `tokio`, `serde`, `serde_json`, `clap`, `tempfile`, `rand` (existing workspace pin 0.9: the seeded RNG, no SplitMix64 needed) | normal | new | 14-09, 14-10, 14-22, 14-23 |
| `ostro-kafka` (new, `publish = false`) | `kafka-protocol` (client), `bytes`, `tokio`, `tokio-rustls`, `rustls`, `rustls-pki-types`, `rustls-native-certs`, `flate2` (rust_backend), `snap`, `lz4_flex` + `frame`, `ruzstd`, `hmac`, `sha2`, `aws-lc-rs`, `base64`, `zeroize`, `thiserror`, `tracing`, `anyhow`; feature `testkit` | normal | new | 14-31, 14-34 |
| `ostro-kafka` | `tokio`, `tempfile`, `rcgen`, `kafka-protocol` (client + broker) | dev | new | 14-31, 14-34 (fake broker, test CA) |
| `fuzz/` (separate workspace, package `ostro-fuzz`) | `libfuzzer-sys = "0.4.13"`, `arbitrary = "1.5.0"`, `ostro-fuzz-targets = { path }`; 15 `[[bin]]` | normal | new | 14-07, 14-08 (bins from 14-01b) |

**Checks (Task 3 step 5):** `cargo deny check` → `advisories ok, bans ok, licenses ok, sources ok`; duplicate-crate warnings identical before/after (no new duplicates of tonic/prost/hyper/axum/rustls/opentelemetry or anything else); `cargo tree --workspace -e normal` has none of chrono/bincode/object_store/openssl-sys/sigstore and no `lz4-sys`/`zstd-sys`. Licences: minisign-verify MIT; wasm-smith Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT; kafka-protocol MIT/Apache-2.0; snap BSD-3-Clause; ruzstd MIT; twox-hash MIT (already in the lock); flagset Apache-2.0 (new transitive of kafka-protocol); derive_arbitrary MIT/Apache-2.0; libfuzzer-sys (fuzz workspace only, not in the main lock) (MIT OR Apache-2.0) AND NCSA. Lock delta: 7 packages added, no existing version changed.

## Nightly chosen

`fuzz/rust-toolchain.toml`: `nightly-2026-11-25` → `rustc 1.102.0-nightly (739adecd4 2026-11-24)` (installed with `rustup toolchain install nightly-2026-11-25 --profile minimal`; ≥ the 1.101.0-nightly floor). `cargo +nightly-2026-11-25 metadata` over `fuzz/Cargo.toml` resolves (the path dependency's `version.workspace` etc. come from the main workspace), so the D-P14 fallback (explicit versions in ostro-fuzz) is not needed; the generated `fuzz/Cargo.lock` was deleted again (14-07 owns it). The 15 `fuzz_targets/*.rs` binaries are 14-01b's.

## Verification

- Task 1 verify: PASS (all nine phases, RoutedControlPlane, STATE/RESERVATIONS greps).
- Task 2 verify: PASS (`cargo build --workspace --all-targets --locked` warning-free; `ostro-types`, `ostro-meta`, `ostro-api --lib`, `ostro-bridge --lib` tests green; goldens append-only; full `cargo test -p ostro-api` green; clippy `-D warnings` on the four crates).
- Task 3 verify: PASS (deny, workspace build, `-p ostro --features failpoints`, `--no-default-features`, tree checks, manifest greps, fuzz manifest 15 bins, `ostro-jepsen --test skeleton`, `ostro-fuzz-targets`, `ostro-kafka --test skeleton`).
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean; `cargo fmt --all --check`: clean; CLOCK_GATE_SCOPED: no new clock reads.
- Reservations check post: PASS.
- Plan gate `ostro-k8s-ci --parallel` at e820d59: run p124337-24597 at e820d59, wall 90m05s (load was higher than the baseline: test2 26.4 GiB, musl2 23.7 GiB peak). Every step PASS (incl. clippy, warning-free build, deny, Phase 13 mutant matrix, **Phase 13 tier C with the 50-fault campaign**, replication mutant matrix, server campaign smoke, musl release + size < 60 MB) except three steps, each red on one load-sensitive test that **passes alone** on cluster slot 1 at the same worktree:

| Step | Failure under load | Rerun alone |
|---|---|---|
| cargo test (test2) | `ostro backup_cli::bucket_online_and_offline_round_trips` (`/readyz not 200 within 60s`) — known (brief) | PASS (7.1 s) |
| failpoint suites (fp1) | `ostro-partition rebuild_basic::deleted_state_rebuilds_identically` (`node open timed out` after 132 s) | 2/2 PASS (2.2 s) |
| failpoint suites (fp2) | `ostro-repl-conformance blobs` scenario (`placement: no partition led by a node other than the meta leader`, precondition) | 2/2 PASS (10.9 s, 10.2 s) |
| musl zigbuild test (musl2) | `ostro-repl-conformance reassign::rf_raised_on_join` (`writes stalled 6.9 s (> 2 s) during the reassignment`) | 2/2 PASS (8/8 each) |

None of these suites is touched by 14-01 (only new crates, manifests, contract types and dependency anchors changed); the baseline run had a different set of load failures. Per-group logs: `target/k8s-ci-logs/20261126-124337-par-e820d591-*.log`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] cargo 1.100 warns on unused dependencies**
- **Found during:** Task 3 (first warning-free workspace build)
- **Issue:** declaring dependencies ahead of their use (the point of D-P14-37) makes cargo's `unused_dependencies` lint warn, which fails the warning-free build gate.
- **Fix:** `use <crate> as _;` anchors (the existing Phase 8/10 precedent in ostro-cli/ostro-client/ostro-build/ostro-triggers): `dep_anchors` modules in the three new crates; one-line anchors in `crates/ostro-api/src/lib.rs` (base64), `crates/ostro-cli/src/lib.rs` (base64), `crates/ostro-client/src/lib.rs` (base64), `crates/ostro-triggers/src/lib.rs` (base64) and `crates/ostro-server/src/lib.rs` (`aws_lc_rs`, `minisign_verify`, `#[cfg(feature = "kafka")] ostro_kafka`). The ostro-server/cli/triggers `lib.rs` lines are outside the files the plan lists (ostro-server `lib.rs` is 14-01b's afterwards): minimal hunks, anchors only; owner plans delete an anchor when they use the crate.
- **Commit:** e820d59

**2. [Rule 3 - Blocking] `ApiExtensions` lives in `crates/ostro-api/src/phase8.rs`, the meta golden in `tests/golden/meta_v2.txt`**
- The four Phase 14 slots were added in phase8.rs (struct, `Default`, `Debug`); every struct literal in the tree already ends with `..Default`, so no other file changed. The golden lines were appended through `tests/meta_golden.rs::render()` (`ext <name> <hex>` after the snapshot lines; existing 24 lines byte-identical; `golden_frozen.rs` section pins unaffected).
- **Commit:** 83a4f4c

**3. [Rule 1 - Bug] rustfmt**
- The Task 2 commit had rustfmt drift in five files; fixed in d67ff84 before the Task 3 commit (no behaviour change).

### Plan text vs code (recorded, no change)
- `ostro`'s `failpoints` feature reaches `ostro-partition/failpoints` through `ostro-server/failpoints` (ostro has no direct ostro-partition dependency); the chain is verified, no edit.
- `control/signals.rs` / `control/tokens.rs` named by D-P14-33 do not exist: signal and await-token completion are in `control/lifecycle.rs`.
- `ListRunsRequest` has no `tenant` field (tenant comes from the principal; 14-16 filters by x_run_index Tenant rows).
- No Phase 12 commit-latency histogram and no ostro-proto descriptor-set export exist (14-23 / 14-02 respectively).

## Workflow hunks

None: 14-01 does not touch `.github/workflows` (no `wip/14-01-workflows` branch). Note for 14-27: ci-local's `cargo test` now also runs the three new crates' tests (skeleton/unit only).

## Next

14-01b (wave 2): p14 seam, stubs, call sites (upload_gate between `modules.rs:266` and `:278`; version_gate at the three sites above; `start_triggers` next to `p13::start` in `server.rs`), config, CLI/client stubs, fuzz bins, tools, CI sync.

## Self-Check

PASSED — all created files present (audit.rs, auth.rs, phase14.rs, audited.rs, the three new crates' lib.rs / skeleton tests / ostro-soak, fuzz/{Cargo.toml,rust-toolchain.toml,.gitignore}); commits d3bfeb3, 4bacb4e, 83a4f4c, d67ff84, e820d59 are ancestors of main (a concurrent p13-campaign commit f6881f1 landed on top of e820d59; it touches only vendor/ostro-raft-driver + VENDORED.md).
