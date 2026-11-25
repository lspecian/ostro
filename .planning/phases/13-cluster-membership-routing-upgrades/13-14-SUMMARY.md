---
phase: 13-cluster-membership-routing-upgrades
plan: 14
subsystem: workers, module upload, AOT pre-warm, version-gate scenarios (CLUS-06, ENG-10)
tags: [workers, placement, compat, feature-level, wit, upload-gate, ext-101, prewarm, aot, version-gate, tier-b]
requires:
  - phase: 13-03
    provides: "Hello.feature_level (field 10)"
  - phase: 13-04
    provides: "compat::{require, finalized_level}, finalizer, hold/release/finalize, boot fence"
  - phase: 13-07
    provides: "linker_set::{select, WitLine, WitSelection}, Prepared.wit, fixture guests p13-wit011(-lite), p13-wit-mixed"
  - phase: 13-08
    provides: "P13Harness (start_single_with, add_node, upgrade), oracle::p13_upgrade"
provides:
  - "ostro_server::workers::registry::{WorkerVersion, level_of_ostro_version, WorkerView.version, WorkerRegistry::{close_where_version, session_version}}"
  - "ostro_server::workers::placement::{compatible, session_current, module_requirement, default_requirement, ActivationReq, CompatSource, MetaCompat, NodePlacement::with_compat}"
  - "ostro_server::workers::service::{REASON_NODE_DRAINING, node_draining, min_session_level, welcome_members}"
  - "ostro_server::control::modules::{upload_gate, upload_gate_for, REASON_FEATURE_NOT_ENABLED}"
  - "ostro_server::p13::prewarm::{start, referenced_modules, warm, Warmed, PREWARM_CONCURRENCY, READY_SCAN_LIMIT, METRIC}"
  - "ostro_repl_conformance::scenarios::version_gate::{run, scenarios, start_level1_cluster, finalized, Workload, GateFn, NODES} + six scenario fns"
affects: [13-09, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "placement predicate as an optional CompatSource on NodePlacement (unit tests keep the Phase 7 behaviour)"
    - "meta-watch side effects in the cluster listener (token revocation, node drain, finalized floor)"
    - "scenario bodies over their own cluster lifecycle, parameterised by node count"
key-files:
  created:
    - crates/ostro-server/tests/worker_compat.rs
    - crates/ostro-server/tests/p13_upload_gate.rs
    - crates/ostro-server/tests/p13_prewarm.rs
    - crates/ostro-repl-conformance/tests/version_gate.rs
  modified:
    - crates/ostro-server/src/workers/{registry,service,placement,remote_executor}.rs
    - crates/ostro-worker/src/session.rs
    - crates/ostro-server/src/control/modules.rs
    - crates/ostro-server/src/p13/prewarm.rs
    - crates/ostro-repl-conformance/src/scenarios/version_gate.rs
    - crates/ostro-server/tests/{worker_session.rs,common/p7_workers.rs}
key-decisions:
  - "A ready row carries the workflow name, not the module: a reservation checks the requirement of every module registered for that workflow (strictest wins); a re-grant under the same lease checks the run's exact module (RunRecord.module_sha)"
  - "A Hello without feature_level is a pre-Phase-13 worker: 0.1.z -> LEVEL_MIN; 0.0.z / unparsable -> GoAway (\"version mismatch: ...\")"
  - "version_policy = compatible (default) no longer compares ostro_version / wasmtime_version; exact keeps the Phase 7 checks gated by require_same_version"
  - "ext 101 only for non-baseline modules ({0.1, 1} carries none), emitted through compat::require(WitRequirementExt)"
  - "409 feature_not_enabled is ApiError::conflict (HTTP 409, gRPC ALREADY_EXISTS) with details {reason, needed, finalized, wit_line}"
  - "Version-gate scenario bodies own their cluster lifecycle (single start at level 1 + joins), run on 3 nodes and as single-node variants"
requirements-completed: []
duration: "~9h (incl. CI waits)"
completed: 2026-11-25
---

# Phase 13 Plan 14: Version-aware execution Summary

**Workers advertise their feature level, the cluster listener admits them by `[workers] version_policy`, and worker placement only offers an activation to a session that has the module's WIT line and a high enough level. Draining a node sends its workers away. Module upload now gates on the WIT selection (409 `feature_not_enabled` above the finalized level, ext 101 for modules that need more than the baseline), and a pre-warm task compiles the modules of queued and running runs after a restart. The six tier-B version-gate scenarios pass on a 3-node cluster built by joins (once 13-09's restart fix 774e59d landed) and as single-node variants.**

## Compatibility predicate (D-P13-18)

```rust
// ostro_server::workers::placement
pub fn compatible(w: &WorkerView<'_>, req: &WitSelection) -> bool
    // req.line ∈ w.version.wit_lines && w.version.level >= req.min_level
pub fn session_current(v: &WorkerVersion, finalized: u32) -> bool
    // v.event_codec_ver == EVENT_CODEC_VER && v.level >= max(LEVEL_MIN, finalized - 1)
pub fn module_requirement(r: &ModuleRecord) -> Option<WitSelection>
    // ext 101; absent = {0.1, LEVEL_MIN}; undecodable ext or line = None (no worker)
pub struct ActivationReq { finalized: u32, modules: Vec<WitSelection> }   // admits(w) = current && all compatible
pub trait CompatSource { fn for_entry(workflow, tenant) -> Option<ActivationReq>; fn for_module(sha) -> Option<ActivationReq>; }
pub struct MetaCompat;  // over the local MetaView; installed by placement::install
```

- `NodePlacement::with_compat(MetaCompat)` filters every `WorkerRegistry::pick`. The ready row has only the workflow name (and tenant), so the requirement is that of **every** module registered under `workflow` or `qualify(tenant, workflow)`. With no record the default applies.
- `RemoteRunner::credit` (a re-grant under the same lease) filters with the run's exact `module_sha`.
- When no session qualifies, the queue behaves as with no worker credit: `auto` stays embedded, `workers` waits.

## Admission (`[workers] version_policy`)

| Policy | Checks | GoAway reason on failure |
|---|---|---|
| `compatible` (default) | `proto_version` and `event_codec_ver` equal; level = `feature_level`, or for 0 the level of `ostro_version` (`0.1.z` = 1, `0.0.z` / unparsable = refused); level ≥ `max(LEVEL_MIN, finalized − 1)` | `version mismatch: ...` / `version too old: worker feature level L (ostro_version V), the cluster is finalized at level F and needs M or newer` |
| `exact` | the Phase 7 checks: proto, codec, and with `require_same_version` also `ostro_version` and `wasmtime_version` | `version mismatch: ...` (unchanged) |

- The meta watch (it already closed revoked join tokens) closes, under `compatible`, every session left below the floor after finalization. The reason is `version too old: the cluster is finalized at level F (workers need level M or newer)`.
- While this node's `Member` record is `Draining` or `Removed`, every session gets `GoAway{reason: "node_draining"}` and new sessions are refused the same way.
- `Welcome.members` is this node's advertised endpoints plus every other `Active` member's `cluster_addr` (no ext 97 counts as Active).
- The worker sends `feature_level = ostro_types::compat::level_max()`.

## Upload gate (ENG-10, D-P13-17)

`upload_module` passes `Prepared.wit` (the selection `linker_set::select` computed during validation) to `upload_gate(view, &selection) -> Result<Vec<u8> /* Module ext */, ApiError>`:

- `min_level > finalized`: **409** `conflict`, message `module needs cluster level N; finalize the upgrade first (the cluster is finalized at level F)`, details `{reason: "feature_not_enabled", needed, finalized, wit_line}`.
- Baseline `{0.1, 1}`: accepted, no ext. Otherwise `compat::require(Feature::WitRequirementExt, level_max())` (the same 409 on refusal), then ext 101 `WitRequirement{line, min_level}` on the new `Module` record.
- Mixed lines, an unsupported line and an unknown `ostro:runtime` function are 400 from validation, as before (`invalid Wasm component: ...`). A re-upload of a recorded sha skips the gate (idempotent, unchanged).
- `upload_gate_for(view, "0.1", level)` is the same gate for callers without the engine types (the scenarios).

## Pre-warm (D-P13-19)

`p13::prewarm::start` runs one task per node:
- **When:** at start, then after every partition role change.
- **Which modules:** for each partition this node leads, is taking over, or follows, the `module_sha` of the non-terminal runs in the `running` keyspace and in the ready queue (at most 10,000 rows per partition per pass).
- **How:** reads each module's bytes from the blob store (length from the `Module` record) and calls `Runtime::prepare_cached`, at most 2 at a time, skipping modules already in L2. Each module is warmed once per process.
- **Metric:** `ostro_cluster_prewarm_modules_total{result=compiled|cached|failed}`.

Measured in `p13_prewarm`:
- After a restart, the queued run's module was in L2 by the time `/readyz` answered. The first poll after ready already found it (10 µs).
- AOT stats were `hits: 1, misses: 0, compiles: 0`: the persisted artefact was loaded and nothing was recompiled.
- Zero activations happened, and an unreferenced module was not prepared.
- With `prewarm::start` stubbed out, the same test fails at its 30 s bound.

First-activation latency under a rolling upgrade is 13-19's measurement.

## Version-gate scenarios (tier B)

`scenarios::version_gate` (6 bodies) runs on a cluster started at emulated level 1. `start_single_with(.., Some(1))` gives finalized 1, and `add_node(level_max: Some(1))` adds the other nodes. Each scenario has:
- a continuous workload that starts a run every 200 ms round robin on the running nodes; every acked run must complete;
- `no_gated_before_finalize_from(1, meta_log, every partition scan)` at the end.

The bodies own their cluster lifecycle, so `version_gate::all()` stays empty and `tests/version_gate.rs` drives them through `run(nodes, body)`.

Results:

| Scenario | 3 nodes (2 joined) | 1 node (`single_node_*`) |
|---|---|---|
| auto_finalize_after_last_upgrade | ok: finalized stayed 1 while nodes 1–2 ran level 2 (4 s sampled), then 2 **0.127 s** after node 3 restarted at level 2 (bound 5 s) | ok: **1.0 s** |
| hold_keeps_window_open | ok: stayed 1 for 4 s with the hold after the last upgrade; finalized **0.294 s** after `release` | ok: **0.99 s** after release |
| manual_finalize_refused_until_all_support | ok: `finalize` on the meta leader with a level-1 member left level 1; after the last upgrade it answered 200 at level 2 | ok |
| gated_upload_refused_before_finalize | ok: gate 409 `feature_not_enabled` for `{0.1, 2}` on all 3 nodes' meta views before (also with 2 of 3 upgraded), ext 101 after; the level-1 smoke module uploaded via every node before and after | ok |
| downgrade_before_finalize_ok | ok: with a hold, all three upgraded, node 3 restarted at level 1 served a run; level stayed 1 | ok |
| downgrade_after_finalize_refused | ok: node 3's start at level 1 failed with "data finalized by a newer Ostro (level 2, this binary supports up to 1)"; its 176 partition files unchanged (mtime + length); restart at level 2 served | ok (183 files) |

Each scenario also passes the workload check (every acked run completed) and `no_gated_before_finalize_from(1, ..)`. The whole file ran in 881 s locally (13 passed: 6 three-node, 6 single-node, the listing test).

History: on main before 13-09 the 3-node form failed at `POST /v1/cluster/tokens` (501). Once 13-09's joins landed, every node restarted after the joins failed to rejoin meta ("metadata raft did not elect a leader / catch up in time"). 13-09's `774e59d` ("restarted dynamic members and joined nodes") fixed that, and `d01b147` removed the `#[ignore]`s.

## Task commits

1. `8e5e689` feat(13-14): worker version compatibility
2. `77a0b04` feat(13-14): module upload gate + AOT pre-warm
3. `824a02d` test(13-14): tier-B version-gate scenarios
4. `f5e4663` test(13-14): version-gate clusters wait for 3 voters; ignore reason after 13-09
5. `65c6249` fix(13-14): version-gate workload acks runs before the first restart
6. `d01b147` test(13-14): run the 3-node version-gate scenarios

## Verification

- `cargo test -p ostro-server --locked --test worker_compat -- --test-threads=1`: 6 passed (`predicate_and_version_parsing`, `incompatible_worker_never_receives`, `embedded_takes_what_no_worker_can_run`, `old_level_worker_refused_at_admission`, `exact_policy_unchanged`, `goaway_on_node_drain`).
- `--test worker_session`: 18 passed. `--lib workers::`: 30 passed.
- Phase 7 worker suites (`--test-threads=1`): `placement_spill` 20 passed, `worker_remote` 9 passed, `worker_paging` 1 passed.
- `--test p13_upload_gate`: 2 passed. `--test p13_prewarm`: 1 passed.
- `cargo test -p ostro-repl-conformance --locked --test version_gate -- --test-threads=1` at main `029e2e0` + d01b147: 13 passed (881 s). Earlier single-node set: 5 consecutive green runs after the 65c6249 fix.
- `flow_e2e::throttle_vs_rate_limit` and `fx_redrive` (failpoints), red in the parallel run at 65c6249, pass alone locally (1 / 4 passed).
- `cargo test -p ostro-worker --locked -- --test-threads=1`: every binary green except `protocol::drain_flushes_unflushed_events_and_reports_evicted`. That test fails in a fresh worktree because `target/guests` has no `trivial_steps.wasm`: the worker test helper builds only three guests and reads a fourth. This predates this plan and is unrelated (deferred).
- `cargo clippy -p ostro-server -p ostro-worker -p ostro-repl-conformance --all-targets --locked -- -D warnings`: clean. `cargo fmt`: clean. `scripts/p12-grep-gates.sh`: rc 0.
- **Plan-level ci-local (remote, `ostro-k8s-ci --parallel`) at `d01b147`** (main with every 13-14 commit, 13-09 incl. 774e59d, 13-10, 13-11, 13-15): the run reports FAILED, but no failure is in 13-14 code. Of 10 groups:
  - **Never got a pod (4: poly, fp1, musl1, fp2).** Pod creation failed with `Error from server: ... database is locked` from the cluster API, the same as in two earlier runs. Their steps (polyglot, replication mutant matrix, campaign smoke, the partition / repl-conformance failpoint subsets, musl release + size gate) have no result.
  - **Passed (lint, obuild, test1, fp3):** fmt, clippy `-D warnings`, warning-free build, SDK/guests/order-saga, semantics sleepers, failpoint list sync, failpoint suites (fp3 subset), phase 7 failpoint legs, deny, vendored, the chronik/tokio-uring/prometheus check, one log path + P12 grep gates (clock gate included), workflow validation, ostro-build polyglot.
  - **test2:** `version_gate` 13 passed (all six 3-node scenarios plus the six single-node ones, 151 s). The step is red only on `ostro-server --test children_detached` (8 tests, `node open timed out` at 25.7 Gi group peak); it passes alone locally (9 passed).
  - **musl2:** red only on `ostro worker_failover::oscillation` ("an on-phase outlasted sustain", known load-sensitive).
  - Earlier runs: at `65c6249`, `flow_e2e::throttle_vs_rate_limit` (partition offline at start), `fx_redrive` (module PUT timeout) and `dlq_api` (server start timeout under musl load) failed. All pass alone locally (1 / 4 / 10 passed), and they passed at `54364b8`, which already held all 13-14 product code.
  - Two single-slot attempts (slot 0) never started: 30 min each with the pod Pending on CPU.

## Deviations from Plan

1. **[Rule 3 - Blocking] Requirement per workflow at reservation time.** `EntryView` / `ReadyEntryRow` carry no module sha, and the ready-row format is not this plan's to change. A reservation therefore checks every module registered for the workflow. This is safe but can be stricter than needed while an old version needs less. The re-grant path (`remote_executor.rs`, outside `files_modified`) checks the run's exact module.
2. **[Rule 1] The Phase 7 version test pins `exact`.** D-P13-18 makes `compatible` the default, under which a different `wasmtime_version` is admitted. `worker_session::session_version_mismatch_goaway` now starts its rig with `version_policy = Exact`, and its assertions are unchanged. `RigOpts` gains `version_policy` (default `Compatible`). Both files are outside `files_modified`.
3. **[Adaptation] The real-module 409 path is tested through the gate.** This build links no level-2 function. `FUNCS_0_1_ADDED` is empty without `wit-next-fixture`, and the server builds its runtime with the production linker in `server.rs` (13-02-only). So no real level-2 module passes validation on a server. `p13_upload_gate` drives `upload_gate` over a real meta group at finalized 1 and 2. The real upload path is covered with the 13-07 fixture guests (lite = level 1 accepted on nodes finalized at 1 and 2, mixed = 400, `p13-wit011` = 400 unknown function `note-hint`).
4. **[Adaptation] Manual finalize with a level-1 member.** 13-04's `finalize` returns 200 with the level unchanged (decision `Stay`), not 409. The scenario asserts that the level does not move. After the last upgrade the auto-finalizer can win the race (a hold would also block manual finalize), so the scenario only asserts that `finalize` answers 200 at level 2.
5. **[Scope] Task 3 depended on 13-09.** The 3-node tests were `#[ignore]`d in 824a02d / f5e4663 until 13-09's join (874f98c..84c6c8f) and its restart fix (774e59d) landed, then enabled in d01b147. `version_gate::all()` stays empty: the runner's static `ClusterHarness::start` cannot start at an emulated level, so `tests/version_gate.rs` drives the bodies.
6. **[Adaptation] `gated_upload_refused_before_finalize`** checks the gate (`upload_gate_for`) against each node's meta view, plus real level-1 uploads on every node, for the reason in deviation 3. `upload_gate_for` was added to `modules.rs` because `ostro-repl-conformance` has no `ostro-engine` dependency (Cargo is 13-01-only).
7. **[Adaptation] The downgrade file check** compares every file outside `meta/`, `cache/` and `tls/`. Every in-process start re-imports the test CA, which rewrites `tls/ca.crt` and `tls/ca.key` before the fence. Partition files (183) are byte-for-byte untouched in mtime and length.
8. **[Rule 1 - Bug, found by 13-09's CI] `single_node_downgrade_after_finalize_refused` flaked.** It failed with `workload: no run was acknowledged` in 13-09's parallel run at `54364b8` (musl test group) and locally. The cause was in this scenario, not in 13-09's boot changes or 13-11's controlled shutdown, which only make restarts longer:
   - The body restarts every node right after the workload starts.
   - It then waits only for finalization before stopping the only node.
   - So the workload, which starts one run every 200 ms, could miss every window in which a node was serving.

   Fix: `Workload::wait_acked(k, limit)`. `run()` waits for 3 acknowledged runs before the body, and the scenario waits for 3 more after finalization. The finish check (at least one ack, every acked run completes) is unchanged. Result: 5 of 5 consecutive single-node runs green.
9. **Commit shape:** tests and implementation landed together per task (no separate RED commits). Worktree: scratchpad + `target/slot-4` instead of `/tmp/ostro-wt-13-14` / `/tmp/ostro-target-13-14`.

No ci.yml change: the new test binaries run in the existing `cargo test` step.

## Handover

- **13-18:** `FeatureNotEnabled` maps to two different HTTP codes:
  - upload gate: 409 `conflict` (gRPC `ALREADY_EXISTS`);
  - 13-10 `cluster_api::placement_error`: `precondition_failed` (412 / `FAILED_PRECONDITION`, which 13-04's handover named).

  Pick one. Also: a refused in-process start leaves `tls/` rewritten (harmless, but noisy for disk oracles).
- **13-19:** pre-warm metric `ostro_cluster_prewarm_modules_total{result}` for the first-activation p99 measurement.
- **13-20 (docs):** `[workers] version_policy`, GoAway reasons `node_draining` / `version too old`, the upload 409 text, ext 101 only for non-baseline modules.

## Self-Check

PASSED: commits 8e5e689, 77a0b04, 824a02d, f5e4663, 65c6249 and d01b147 are on main. The created test files and `p13/prewarm.rs` exist at main.
