---
phase: 13-cluster-membership-routing-upgrades
plan: 15
subsystem: module retirement + reference-checked module GC (CLUS-07 first half)
tags: [module-gc, retire, ext-102, node-ops, module-refs, double-scan, coverage, mutant, tier-b]
requires:
  - phase: 13-02
    provides: "stubs control/module_retire.rs (retire, is_retired, check_not_retired; call sites in runs.rs start + modules.rs activation), p13/module_gc.rs (start, run_now), p13/module_refs.rs (LeaderModuleRefs::new(Weak<Node>, roles)); [modules] gc_interval / gc_grace / gc_protect_resumable"
  - phase: 13-03
    provides: "NodeOps.ModuleRefs proto + server delegation (node identities only); REST DELETE /v1/modules/{sha}, POST /v1/modules/gc"
  - phase: 13-04
    provides: "compat::require(view, Feature::RetiredExt, level_max)"
  - phase: 13-05
    provides: "MetaHandle::read_index_barrier"
  - phase: 13-08
    provides: "oracle p13_gc::no_module_gone_for_live, RetireEvent, RunResult"
provides:
  - "control::module_retire::{retire, is_retired, check_not_retired, retired_at, retired_error, active_versions, parse_sha, REASON_RETIRED, REASON_ACTIVE}"
  - "p13::module_refs::{LeaderModuleRefs (impl ModuleRefsSource), is_retryable, needs_module, SCAN_PAGE}"
  - "p13::module_gc::{ModuleGc::{new, for_ctx, round}, GcOptions, NodeRefs, NodeOpsRefs, start, run_now, run_with, MUTANT_SINGLE_SCAN, FP_BETWEEN_SCANS, FP_BEFORE_DELETE, METRIC_DELETED, METRIC_KEPT}"
  - "mutant gc::mutant::single_scan placed, biting (tests/p13_module_gc.rs failpoints::single_scan_mutant_bites)"
  - "tier-B group module_gc (5 scenarios) in scenarios::all()"
affects: [13-12, 13-13, 13-16, 13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "retire = read-modify-write per record with CAS, Module first; idempotent re-run completes missing marks"
    - "GC coverage = (partition, epoch) pairs checked against the led set before and after a paged runs scan; any uncovered pair postpones the round"
    - "scenario-level gate on a NodeRefs wrapper to hold scan 2 while the cluster changes"
key-files:
  created:
    - crates/ostro-server/tests/p13_module_retire.rs
    - crates/ostro-server/tests/p13_module_gc.rs
    - crates/ostro-repl-conformance/tests/module_gc.rs
  modified:
    - crates/ostro-server/src/control/module_retire.rs
    - crates/ostro-server/src/p13/module_refs.rs
    - crates/ostro-server/src/p13/module_gc.rs
    - crates/ostro-repl-conformance/src/scenarios/module_gc.rs
key-decisions:
  - "Retryable status set = statuses retry_run accepts: Failed, Cancelled, Terminated, TimedOut (every terminal status except Completed / ContinuedAsNew). Both retry modes need the module: Resumed runs in place, Restarted starts a new run pinned to the same module_sha. 'Within retention' = the record still exists (expiry deletes it)."
  - "run_now runs the round on the node that serves POST /v1/modules/gc (not forwarded to the meta leader); the periodic loop acts only on the meta leader. Safety never depends on leadership: every deletion re-reads behind a barrier and CASes, and each round needs its own two clean scans."
  - "The two scans run inside one round (scan 1, sleep gc_grace/2, scan 2), so POST /v1/modules/gc answers after at least gc_grace/2 (5 min at the defaults)."
  - "feature_not_enabled maps to 412 PreconditionFailed with details.reason (same as 13-02's cluster_api placement errors); module_active / module_retired are 409 Conflict."
  - "Retire is idempotent (second DELETE = 200 with the first retired_at_ms), as the plan says; 13-03's FakeModuleLifecycle answers 409 module_retired for a second retire (fake only, not touched)."
  - "GC keeps a retired module whose version became active (retire/activate race) and one whose Retired mark changed or became undecodable."
requirements-completed: []
duration: "about 2 h 45 min of work + remote ci-local"
completed: 2026-11-25
---

# Phase 13 Plan 15: Module retire + reference-checked module GC Summary

**`DELETE /v1/modules/{sha}` retires a module and its workflow versions (ext 102, gated by `compat::require(RetiredExt)`, refused while a version is active, idempotent), retired modules refuse starts and activations, each node answers `NodeOps.ModuleRefs` from the partitions it leads at exactly the asked epoch, and the GC deletes a module only after two scans `gc_grace / 2` apart through every partition leader found no live (or, by default, retryable) run. Any partition that is not covered postpones the whole round.**

## Retire and refusal rules (`control/module_retire.rs`)

`retire(ctx, sha)`:
1. `sha` must be 64 lowercase hex digits (400). Then `ctx.meta.read_index_barrier()` (503 on timeout).
2. The Module must exist (404 `module <sha> not found`).
3. If any workflow version of the module is some workflow's Active version → 409 `module_active`, `details.workflows = [{name, version}]`. A module's versions are the WorkflowVersion records with `module_sha256 == sha`, plus the Module record's `workflows` list.
4. Before the first proposal: `compat::require(view, Feature::RetiredExt, ctx.level_max)`. If it fails → 412 `feature_not_enabled` with `details {feature, needed, finalized}`.
5. Writes, one per pass, each re-planned from a fresh read, `Put` with `expected_version` = the record version:
   - the Module gets `Retired{retired_at_ms = ctx.clock.now_ms()}`;
   - then every WorkflowVersion still unmarked gets the same `retired_at_ms`.
   A lost CAS (`StaleVersion` / `Conflict` / `NotFound`) re-reads; 8 passes, then 503.
6. Already fully retired → 200 with the existing `retired_at_ms` and versions (idempotent; a crash between the writes is completed by the next call).

`is_retired(view, sha)` / `check_not_retired(view, sha)` read the Module's ext 102. An undecodable tag counts as retired (safe side). `check_not_retired` → 409 `module_retired`, `details.module`. It is called by 13-02's call sites: the start path (explicit version and active-version resolution) and activation. 13-17's repin targets call it too.

## Reference source (`p13/module_refs.rs`)

`LeaderModuleRefs::referenced(shas, partitions, protect_resumable)`, per asked `(partition, epoch)`:
- **Covered** only if this node's led set (`PartitionRoles::led`, published after `LeaderAnnounced` applied) holds exactly that pair before *and* after the scan. Otherwise the pair goes to `uncovered`. References found during a scan that lost coverage still count (err towards keeping).
- **Scan:** `query::runs_page` in pages of 512 on the blocking pool, so memory is bounded. It stops early once every asked sha has been found. The node is upgraded only to take the partition handle, so it is never held across an await.
- **A run references `RunRecord.module_sha`** when it is non-terminal (Scheduled / Ready / Running, waiting or paused included). With `protect_resumable`, a terminal run also references it when `is_retryable(status)` holds.

## GC (`p13/module_gc.rs`)

`ModuleGc::round(GcOptions{grace, protect_resumable})`:
1. **Candidates:** after a barrier, modules whose decodable Retired mark is at least `grace` old by the injected clock. Younger retirements count as `kept{reason="grace"}`.
2. **Scan 1:** the meta assignment view, grouped by leader. Each leader gets one `NodeOps.ModuleRefs` call with its `(partition, epoch)` pairs: `NodeOpsRefs`, a local call for this node, gRPC over the cluster transport for the others, 30 s bound per node.
   - These postpone the round (`postponed = true`, `reason`, nothing deleted): a partition with `leader == 0` or epoch 0, an unreachable node, or any uncovered pair.
3. **Failpoint `gc::between_scans`.** Unless the `gc::mutant::single_scan` mutant is on: sleep `grace / 2`, then **scan 2** over a fresh assignment view, with the same postponement rules.
4. **Failpoint `gc::before_delete`**, then a second barrier.
5. **Delete** each candidate absent from both scans, but only if it still exists with the same `retired_at_ms` and has no active version:
   - `Delete` every WorkflowVersion of the module (CAS on its version), then the Module record (CAS);
   - after 8 lost CAS attempts it is kept (`changed`) until the next round.
6. **Report:** `GcRunReport{candidates, deleted, referenced, postponed, reason}`. Metrics: `ostro_module_gc_deleted_total`, `ostro_module_gc_kept_total{reason=grace|referenced|postponed|changed|active}`.

`start`: one task. It sleeps `gc_interval`, runs a round only if this node is the meta leader, and stops on the P13Services token (a round in flight is cancelled). `run_now(ctx)` runs one round with `[modules]` options. `run_with(ctx, GcOptions)` takes explicit options; tier-B scenarios use it with a 2 s grace.

The blob is never touched here. Phase 8 blob GC removes it on its next pass, because its mark set includes every meta Module sha; the tier-B scenario shows the blob gone after one pass on every node.

### GC timings

| Setting | Default (D-P13-26) | Tests |
|---------|-------------------|-------|
| `gc_interval` | 10 min (first round one interval after start) | not used (rounds driven explicitly) |
| `gc_grace` | 10 min: candidate after 10 min retired, scans 5 min apart | 200 ms (server tests), 2 s (tier B: scans 1 s apart) |
| `gc_protect_resumable` | true (retired modules of retryable runs linger up to the run retention, 7 days by default) | both values |
| NodeOps call bound | 30 s per node | — |

## Rerun-path finding (for 13-17 / 13-18)

`check_not_retired` runs only in the start path (`control/runs.rs`) and in activation (`control/modules.rs`). These paths build `StartRun` directly and bypass it:
- **Phase 11 rerun:** `p11_rerun` / `rerun.rs`; `version = source` reuses the source's module.
- **D1 retry in Restarted mode:** `control/lifecycle.rs::restart` copies `rec.module_sha`.
- **DLQ restart and bulk restart:** `restart_with_key`.

They neither check retirement nor that the Module record still exists. Measured in the tier-B scenario `protects_retryable_terminal_runs`: after v1 was GC'd (protection off), `POST /v1/runs/{failed}/retry` answered 200 `mode: restarted`. The new run was pinned to the deleted module and executed, because the blob was still on disk. It failed at `s3`, as v1 always does. 13-18 (`rerun.rs` is in its file list) should refuse retired modules on these paths. 13-17 should map a missing module to `module_gone`.

The restart is also submitted to a random partition's local handle, so it can answer 503 `not_leader` when the serving node does not lead the new run's partition. At 7407aba it took 4 attempts. At 84c6c8f, with 13-12 routing on main, it still took 2: the restart path builds commands on partition handles and is not routed. The scenario retries up to 40 times. 13-12/13-18 should check this.

## Task Commits

1. `4147406` feat(13-15): module retire + reference source: module_retire.rs, module_refs.rs, tests/p13_module_retire.rs
2. `e342b6e` feat(13-15): module GC service: module_gc.rs, tests/p13_module_gc.rs
3. `7407aba` test(13-15): tier-B module GC scenarios: scenarios/module_gc.rs, tests/module_gc.rs

## Verification

- `cargo test -p ostro-server --locked --test p13_module_retire -- --test-threads=1`: 2 passed.
  - `retire_refusals_idempotence_and_start_refusal`: 409 `module_active` with workflows, 404, v2 refused while active, retire v1 → 200, second DELETE identical, ext 102 on Module + pipeline@1 with the same time, v2 untouched, explicit v1 start and v1 activation 409 `module_retired`, active v2 start completes on v2.
  - `module_refs_follow_run_status_and_coverage`: a Completed run is never a ref; a Failed run is a ref only with protection; a sleeping run is always a ref; a stale epoch is uncovered and not scanned; an empty ask answers nothing.
- `cargo test -p ostro-server --locked --test p13_module_gc -- --test-threads=1`: 6 passed (two clean scans delete WVs + Module, B and Active untouched, 6 calls with identical pairs; reference between scans keeps; reference in scan 1 keeps; unreachable / uncovered / leaderless postpone with reason, leaderless asks nobody; within grace not a candidate, 1 ms later deleted; active version keeps).
- Same with `--features failpoints`: 8 passed (+ `single_scan_mutant_bites`: 3 calls, A deleted under the mutant, kept without it; `failpoints_postpone_the_round` for both failpoints).
- `cargo test -p ostro-repl-conformance --locked --test module_gc -- --test-threads=1`: 1 passed (5 scenarios, about 39 s). Repeated 3 more times locally, all green. Re-run on main after 13-09/13-11/13-14 landed (7407aba): green.
  - Measured in the run: 8 v1 runs live at retire; v1 deleted in round 25 after all completed; gated round report `postponed: true, reason: "node 3 no longer leads partition 2 at epoch 1"`.
- `-p ostro-repl-conformance --test p13_skeleton`: 3 passed (names unique, topic list in `all()`).
- clippy `-D warnings` on ostro-server (lib + both tests, with and without failpoints) and ostro-repl-conformance (lib + module_gc + p13_skeleton, with and without failpoints): clean. `cargo fmt --check`: clean.
- **Plan-level ci-local**: `ostro-k8s-ci --parallel` at 84c6c8f (main with 13-09/13-11/13-12/13-14 and 13-04 Task 2), full run with musl and polyglot: **PARALLEL RUN FAILED, no failure from 13-15**.
  - PASS: fmt, clippy, build, sdk, guests, order-saga, polyglot block, clients, semantics sleepers, failpoint list sync, failpoint suites (incl. `ostro-repl-conformance` with failpoints: `module_gc` 1 passed), replication mutant matrix, deny, vendored, fixtures, forbidden, P12 grep gates (incl. clock gate), workflow validation, musl release.
  - FAIL `cargo test` (group test2): `ostro --test worker_failover::oscillation` (on the known-flaky list). cargo's fail-fast then skipped the rest of test2's targets, which include `p13_module_retire`, `p13_module_gc` and `module_gc`. Covered by a targeted cluster run at the same commit (slot 1): p13_module_gc 6 passed, p13_module_retire 2 passed, p13_module_gc `--features failpoints` 8 passed, module_gc 1 passed. `worker_failover::oscillation` failed again alone there.
  - FAIL `phase 7 failpoint legs + manifests`: `ostro --test worker_failover sigstop_zombie` ("ostro server did not start: no expected line within 90s"). Phase 7 worker test, not touched by 13-15, not on the flaky list. Reported.
  - FAIL `musl binary size < 60 MB` (known, a separate agent is fixing it) and `musl zigbuild test` (the same `worker_failover::oscillation`).
  - NORESULT `server campaign smoke`: group fp1 never started ("Error from server: ... database is locked" from the cluster API, an infrastructure error).
  - An earlier single-slot run at 7407aba ended early (connection lost during "semantics sleepers"). Its cargo test step failed only in 13-09's `p13_join::ids_after_tombstones_and_resume` (412 member_active); `module_gc` passed there.

**ci.yml / ci-local hunk for 13-20 (not applied; the files are 13-01/13-20's):** `ostro-repl-conformance` already runs with failpoints (the `module_gc` group runs there). `ostro-server`, however, is target-restricted in `FAILPOINT_CRATES`, so the mutant "bites" test `tests/p13_module_gc.rs::failpoints::*` runs in no CI step yet. Add the target to both lists:

```diff
-FAILPOINT_CRATES=(... ostro-server:dlq_api ostro-server:bulk_ops ostro:ops_kill)
+FAILPOINT_CRATES=(... ostro-server:dlq_api ostro-server:bulk_ops ostro:ops_kill ostro-server:p13_module_gc)
```
and in `.github/workflows/ci.yml` "Failpoint suites":
```diff
+          cargo test -p ostro-server --features failpoints --test p13_module_gc --locked -- --test-threads=1
```

## Deviations from Plan

1. **[Adaptation] Worktree / target / CI:** scratchpad worktree, `target/slot-5`, private index + CAS onto main (not `/tmp/ostro-wt-13-15` / `/tmp/ostro-target-13-15`). Plan-level ci-local ran on the cluster: `ostro-k8s-ci --parallel` (full run) at 84c6c8f, plus a targeted slot-1 run for the targets that fail-fast skipped.
2. **[Adaptation] Tier-B GC timings:** the in-process harness (`inproc.rs`, 13-08/13-18's file) cannot set `[modules]` keys per scenario. Scenarios therefore drive rounds through the node's own `P13Ctx` with `module_gc::run_with(ctx, GcOptions{grace: 2 s, ..})`, which is the same code path as `run_now` except for the options. The configured 10 min defaults keep the periodic loop idle.
3. **[Adaptation] `gc_survives_leader_changes`:** the scan reads a fresh assignment view per scan, so a leader change between scans normally lands on the new leader (covered). To exercise the `uncovered` path deterministically, the scenario wraps the real `NodeOpsRefs` in a gate that holds scan 2's first call. While it is held, the scenario isolates partition 2's leader until meta moves the partition, heals it, and waits until it no longer leads at the old epoch. The held call then reaches the old leader with the old epoch, the leader answers `uncovered`, and the round is postponed.
4. **[Rule 2] Oracle input:** `GET /v1/runs/{id}` carries the failure detail in `failure` (a JSON string); `RunResult::from_json` (13-08) reads only `error`. The scenarios fold `failure` into `RunResult.error` before calling `no_module_gone_for_live`, so the oracle sees real failure details. The oracle file is not changed; 13-18 should make `from_json` read `failure`, as noted in 13-08's handover.
5. **[Note] Meta barrier on the start / activation paths:** `check_not_retired` (fixed sync signature) reads the local view. `retire` and GC use `read_index_barrier`, but the start path (`runs.rs`, 13-12) and activation (`modules.rs`, 13-14) do not call a barrier before it. The barrier comes from 13-13's `route::consistency::before_meta_read` once 13-12's router calls it. Until then a node lagging behind a just-applied retirement could accept one more start; the GC grace (≫ replication lag) and 13-17's `module_gone` cover that residual window. The tier-B scenario barriers every node before asserting refusal from every node.
6. **[Note] `module_active` reason:** used as the plan specifies, but it is not in RESERVATIONS' P13 `details.reason` list (`feature_not_enabled, module_retired, module_gone, leader_changed`). 13-20 should add it (RESERVATIONS is 13-01/13-20's file).

## Open issues / handover

- **13-17:** `module_gone` for runs whose module record (and later blob) is gone, including restarts / reruns pinned to a GC'd module (see the rerun-path finding). Repin targets: call `module_retire::check_not_retired`.
- **13-18:** retired-module refusal on rerun / retry-restart / DLQ / bulk restart (`rerun.rs` etc.); `RunResult::from_json` should read `failure`; the start-during-GC race scenario; the mutant `gc::mutant::single_scan` site is `ostro-server/src/p13/module_gc.rs` and its "bites" test is `tests/p13_module_gc.rs::failpoints::single_scan_mutant_bites` (ostro-server failpoints build).
- **13-16:** `ostro modules gc` blocks for at least `gc_grace / 2` (5 min by default); give the client a long timeout or report progress. `ostro modules rm` maps 409 `module_active` (details.workflows) and 412 `feature_not_enabled`.
- **13-12:** `POST /v1/modules/gc` need not be forwarded (any node can run a round). `DELETE /v1/modules/{sha}` may run on any node (proposals are forwarded by meta).
- **13-20:** add `module_active` to RESERVATIONS; document `ostro_module_gc_*` metrics, the kept reasons and the retained-retryable linger in docs.

## Self-Check

PASSED: commits 4147406, e342b6e, 7407aba are on main; every created/modified file listed above exists on main.
