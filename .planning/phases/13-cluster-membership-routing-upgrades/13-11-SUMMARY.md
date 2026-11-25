---
phase: 13-cluster-membership-routing-upgrades
plan: 11
subsystem: graceful leader handoff + controlled shutdown
tags: [handoff, quiescing, write-fence, dispatcher, executor, hot-wait, controlled-shutdown, nodeops, tier-b]
requires:
  - phase: 13-01
    provides: "HandoffControl {prepare_handoff, in_handoff}, HandoffGuard (abort on drop), contracts::{HandoffExecutor, HandoffClient, HandoffOutcome, HandoffError}, LocalHandoff / NodeOpsHandoffClient / controlled_shutdown stubs"
  - phase: 13-02
    provides: "stub impl HandoffControl for Node; P13Ctx builds LocalHandoff + NodeOpsHandoffClient; p13::before_shutdown first in ServerHandle::shutdown"
  - phase: 13-03
    provides: "NodeOps.HandOff proto + NodeOpsService::routes on the cluster listener (node identities only)"
  - phase: 13-05
    provides: "MetaHandle::transfer_leader (waits up to 10 s), raft_detail progress, read_index_barrier"
  - phase: 13-08
    provides: "InProcCluster P13Harness (upgrade = graceful stop without isolation + restart)"
provides:
  - "ostro_partition::handoff: HandoffCell / HandoffMark / HandoffReport, impl HandoffControl for Node, quiesce(); HandoffGuard::{with_report, with_freeze, report, freeze_appends}"
  - "PartitionError::LeaderChanged (API 503 details.reason leader_changed); PartitionHandle::{handoff, handoff_cell, admission_open}"
  - "dispatcher admission flag + per-partition lease gauge; Reactivate while quiescing re-queues"
  - "executor input bridge closes on quiescing; engine evicts a parked activation at once on closed inputs"
  - "ostro_cluster::handoff: LocalHandoff (HandoffExecutor), NodeOpsHandoffClient (HandoffClient), HandoffMeta / HandoffLog views, with_parts, CATCHUP_STALL, ELECT_TIMEOUT"
  - "ostro_cluster::shutdown: controlled_shutdown / controlled_shutdown_with, ShutdownMeta, LedPartition, targets(), QUORUM_PROBE, HANDOFF_PARALLELISM"
  - "tier-B group handoff: 4 scenarios + run-outcome oracle"
affects: [13-10, 13-12, 13-13, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "epoch-keyed fence: the mark records the leader epoch it fences; every check compares with the handle's current epoch, so demotion lifts it and no role-driver change is needed"
    - "small trait views (HandoffMeta, HandoffLog, ShutdownMeta) over MetaHandle / PartitionLog so the sequences run on fakes; the 13-01 constructors are unchanged"
key-files:
  created:
    - crates/ostro-partition/tests/handoff.rs
    - crates/ostro-cluster/tests/handoff_executor.rs
    - crates/ostro-repl-conformance/tests/handoff.rs
  modified:
    - crates/ostro-partition/src/handoff.rs
    - crates/ostro-partition/src/handle.rs
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/src/node.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/tests/executor.rs
    - crates/ostro-engine/src/host/effects.rs
    - crates/ostro-engine/tests/hot_wait.rs
    - crates/ostro-cluster/src/handoff.rs
    - crates/ostro-cluster/src/shutdown.rs
    - crates/ostro-repl-conformance/src/scenarios/handoff.rs
key-decisions:
  - "The write fence lives in PartitionHandle::submit (and the dispatcher's pipelined grant, the one append that bypasses submit), not in leader/mod.rs, which has no submission path"
  - "Fence and admission are keyed by the leader epoch of the handoff: complete() leaves the mark, demotion (handle epoch 0) lifts it, a later term is never fenced; abort clears it. roles.rs untouched"
  - "Hot-wait eviction: the executor's input bridge drops the engine's input stream when the partition quiesces and the engine now evicts a parked activation at once on a closed stream (HotWaitElapsed, a flush/suspend point); BudgetPressure eviction is node-wide LRU and cannot target a partition, and Activate has no spare field"
  - "A Reactivate outcome while quiescing gives the lease back and re-queues the run (like Abandoned: n + 1, no attempt failure), so evicted runs replay on the new leader or here after an abort"
  - "Handoff ElectLeader carries ElectionInfo{reason: ELECTION_REASON_RESIGN, prev_leader: self, elected_at_ms: partition clock at prepare}; no handoff reason constant exists and ext.rs is 13-01-only"
  - "controlled_shutdown keeps its 13-02 signature (no log access): targets are ISR members in replica order (next on abort) instead of 'highest durable LEO'; the executor's catch-up wait covers lag"
  - "Metadata quorum probe (ReadIndex, 1 s) before any handoff: an isolated node (the tier-B harness kill isolates first) skips at once; plus a catch-up stall abort (2 s without progress) and a 5 s bound on the election proposal, so concurrent stops of a whole cluster do not sit out shutdown_handoff_timeout"
requirements-completed: [CLUS-06]
duration: "about 2 h 20 min (plan-level ci-local not waited for, see Verification)"
completed: 2026-11-25
---

# Phase 13 Plan 11: Graceful leader handoff + controlled shutdown Summary

**A leader can now quiesce a partition (admission off, hot waits evicted, in-flight activations given a grace), freeze its appends with a retryable `leader_changed`, wait until an ISR target has its whole log durable and elect it at `epoch + 1` through meta; any failure aborts and the old leader keeps serving. A stopping node hands every led partition off and transfers metadata leadership before the Phase 12 shutdown, bounded by `shutdown_handoff_timeout`. In tier B, 50 handoffs under load moved 50/50 with p99 unavailability about 270 ms, and graceful restarts of every node in turn failed no run.**

## Handoff sequence as landed

`NodeOps.HandOff` (or `NodeOpsHandoffClient::hand_off`, local short-circuit) → `LocalHandoff::execute(p, to, epoch)`:

1. **Refuse** unless meta names this node leader of `p` at `epoch` (`HandoffError::NotLeader`) and `to` ∈ published ISR (`TargetNotInIsr`); `to == self` → `Aborted`.
2. **`prepare_handoff(p, handoff_grace)`** (`Node`): role must be `Leader{epoch}` and the handle at that epoch; `HandoffCell::begin(epoch)` (admission off: the dispatcher leases nothing, re-queues `Reactivate`s; the executor input bridges close, so hot-parked activations evict at once); waits until the partition's lease count is 0 or the grace elapsed. The guard reports `{epoch, leased_left, evicted, started_at_ms}`. A second prepare of the same term is refused.
3. **`guard.freeze_appends()`** (W11): `submit` refuses with `PartitionError::LeaderChanged` (REST/gRPC 503, `details.reason = "leader_changed"`); the pipelined grant too; appends already handed to the log complete. `in_handoff(p)` is true from step 2 until abort or demotion.
4. **Catch-up** (10 ms polls, bounded by `handoff_catchup_timeout`): local `durable_end == log_end` and `to`'s durable LEO (leader follower progress) `>= log_end`. Each poll re-reads the assignment: leader/epoch changed → abort; `to` out of the ISR → abort; `to` behind and not progressing for `CATCHUP_STALL` (2 s) → abort.
5. Failpoint **`handoff::before_elect`** (`return` aborts), then **`ElectLeader{epoch + 1, leader: to, isr: published ISR unchanged, expected_isr_version, info: {resign, started_at_ms, prev_leader: self}}`**, bounded by `ELECT_TIMEOUT` (5 s; a late apply stays a valid in-ISR election). `StaleVersion` with meta already showing `to` leading above `epoch` (the target's own fresh term, 12-14) counts as moved.
6. Applied → failpoint **`handoff::after_elect`** (sleep/pause/panic only) → `guard.complete()` → `Moved{new_epoch}`; the Phase 12 role lifecycle demotes this node on the assignment change (epoch 0 lifts the fence). Anything else → `guard.abort()` (admission and appends back on) → `Aborted{reason}`.

Controlled shutdown (`p13::before_shutdown` → `controlled_shutdown(node_id, meta, handoff_client, shutdown_handoff_timeout)`): metadata members ≤ 1 → skip; quorum probe (ReadIndex within 1 s) fails → skip, all led partitions `kept`; every partition meta names this node leader of is handed to its targets (ISR in replica order, then the rest of the ISR; next target on abort/error), 4 at a time; then, if this node is the metadata leader, `transfer_leader` to the active voter with the highest matched index; the whole thing races one `shutdown_handoff_timeout` sleep (timeout → the rest fails over the Phase 12 way, meta transfer skipped). Per-partition results are logged; `ShutdownReport{moved, kept, meta_transferred}`.

### Failpoints (prefix `handoff::`, RESERVATIONS §11)

| Failpoint | Site | Effect of `return` |
|---|---|---|
| `handoff::before_elect` | after catch-up, before the ElectLeader proposal | abort (admission + appends back on), `Aborted{injected failure ...}` |
| `handoff::after_elect` | after the election applied, before `complete()` | ignored (use `sleep` / `pause` / `panic` to widen or kill the window) |

Both compile in only with `ostro-cluster/failpoints` (already in `FAILPOINT_CRATES` and the ci.yml Failpoint suites list: no CI change).

## Measured (tier B, in-process 3-node RF=3, WSL2 debug build under load)

- `handoff_under_load_no_failed_runs`: **moved 50/50**, aborted 0, refused 0; per-handoff unavailability (handoff call start → next acknowledged start on the partition) **p50 118-123 ms, p99 268-288 ms, max 268-288 ms** (two runs); 212 runs, all `completed`; scenario 10.3 s.
- `handoff_aborts_when_target_lags`: aborted after 1.95-2.36 s (`target left the ISR`: replica_lag_max 2 s wins the race with the 2 s stall bound), epoch unchanged, old leader serving.
- `controlled_shutdown_moves_everything`: busiest leader (2 of 4 partitions) handed both off before exiting; **stop + restart 302-319 ms**; 30 runs completed.
- `restart_cycle_no_drain`: stop + restart per node **312-439 ms**; 60 runs completed, none failed or cancelled.
- Unit level: an in-flight activation finishing within the grace returns at 0 leases (not at the grace); a hung target aborts at the 2 s stall bound instead of the 60 s catch-up bound; an unanswered election aborts at 5 s.

## Task Commits

1. `c055a55` feat(13-11): partition handoff quiescing
2. `998fbd1` feat(13-11): handoff executor + NodeOps client + controlled shutdown
3. `139356d` fix(13-11): bound the handoff election proposal
4. `e3f3bc7` test(13-11): tier-B handoff + controlled shutdown scenarios

## Verification

- Local (`target/slot-2`): `cargo test -p ostro-partition --test handoff` 6/6, `--lib handoff` 3/3; `-p ostro-cluster --test handoff_executor -- --test-threads=1` 16/16; `-p ostro-repl-conformance --test handoff` (4 scenarios pass), `--test failover` (3 pass), `--test placement` (2 pass), `--test p13_skeleton`, `--test skeleton`; `-p ostro-partition --lib --test fencing --test pause --test ack_after_apply` green.
- Cluster slot 1 (worktree snapshots of Task 1): `-p ostro-partition --test handoff --test dispatcher --test dispatcher_flow --test dispatcher_clock --test rebuild_digest --test role_lifecycle` all green; `-p ostro-engine --test hot_wait --test live_activations` green (incl. `hot_wait_closed_inputs_evict_at_once`); `-p ostro-server --locked --no-fail-fast`: 75 binaries green. One run of `bottomless_service::server_uploads_continuously_under_the_cut` failed ("manifest does not cover the runs yet") inside the full suite and passed alone (1.5 s): load-sensitive, unrelated to this plan (uploader path untouched).
- `cargo clippy -p ostro-partition -p ostro-engine -p ostro-server -p ostro-cluster -p ostro-repl-conformance --all-targets --locked -- -D warnings` clean (ostro-cluster also with `--features failpoints`); `cargo fmt --all -- --check` clean; `scripts/p12-grep-gates.sh` exit 0 (no wall clock in ostro-cluster: tokio time only); `cargo check --workspace --all-targets --locked` warning-free at `e3f3bc7` (on top of 13-04 Task 2, 13-09, 13-14, 13-15).
- Plan-level `ci-local --no-musl --no-polyglot`: **submitted, not completed when this SUMMARY was written.** `ostro-k8s-ci e3f3bc7 1 --no-musl --no-polyglot` is queued on cluster slot 1 behind 13-15's full ci-local (slot 1 is shared; cluster ci-local runs take 1.75-3 h each, so the result would arrive 4-7 h later). Its log will be `target/k8s-ci-logs/<ts>-s1-e3f3bc7*.log`. Covered meanwhile by the targeted runs above plus 17 more tier-B test binaries run locally at `e3f3bc7` (blobs, bootstrap, divergence, fetch_path, harness_smoke, hw, isr_honesty, min_insync, module_gc, object_storage, one_path, p13_harness_smoke, stale_epoch, standby, version_gate, zombie, failover_p99 (ignored by design)): all green in 471 s. Tier C (process_*) and the failpoint suites were not run separately.

## Deviations from Plan

### Auto-fixed / adapted

1. **[Rule 3] Files outside `files_modified`:** `ostro-partition/src/handle.rs` (the submit path the W11 fence belongs to, `PartitionError::LeaderChanged`, the per-handle cell), `ostro-partition/src/node.rs` (`leader_clock`), `ostro-partition/src/lib.rs` (re-exports), `ostro-server/src/control/runs.rs` (exhaustive `PartitionError` match → 503 `leader_changed`), `ostro-server/tests/executor.rs` (bridge test), `ostro-engine/src/host/effects.rs` + `tests/hot_wait.rs` (closed hot-wait inputs evict at once). `leader/mod.rs` needed no change (no submission path there). No shared 13-01/13-02-only file touched; no Cargo change.
2. **[Adaptation] Hot-wait eviction mechanism:** not the BudgetPressure path (node-wide LRU, no partition attribution); the executor closes the engine's input stream and the engine evicts a parked activation at once (`HotWaitElapsed`, after its pre-park flush). Previously a closed stream made the park wait out its window for nothing; behaviour otherwise unchanged.
3. **[Adaptation] Signatures kept:** `LocalHandoff::new` / `NodeOpsHandoffClient::new` / `controlled_shutdown` keep the 13-01/13-02 signatures (p13/mod.rs is 13-02-only). Consequences: no injectable `Clock` reaches LocalHandoff (timeouts use tokio time; the election timestamp comes from the partition clock through the guard), and the shutdown chooses targets by replica order instead of highest durable LEO.
4. **[Rule 2] Fast-fail bounds the plan did not list:** quorum probe before the shutdown handoffs, `CATCHUP_STALL` (2 s) and `ELECT_TIMEOUT` (5 s). Found in tier B: the harness `kill` isolates then shuts down gracefully, and `stop_all` stops every node at once; without them every such stop sat out up to 30 s (one teardown took 35 s before `ELECT_TIMEOUT`).
5. **[Adaptation] Election reason:** `ELECTION_REASON_RESIGN` with `prev_leader = self` marks a handoff; a dedicated reason value would need a 13-01 `ext.rs` change and a reservation (open item below).
6. **[Rule 1] Scenario signal retry:** a signal whose answer was lost across a leader change (503 outcome unknown) is retried; the retry's `409 run_terminal` means the run consumed it, so it counts as delivered (the outcome oracle checks `completed`).
7. **Worktree / target / CI:** scratchpad worktree, `CARGO_TARGET_DIR=target/slot-2`; heavy suites on cluster slot 1 (shared with 13-15, queued); the tier-B group ran locally while the cluster was saturated.

### Deferred / open

- **Teardown cost in tier B:** every graceful stop of a multi-node in-process node now runs the controlled shutdown: about 1 s extra per harness `kill` (quorum probe on an isolated node) and up to about 7 s worst case when a whole cluster stops at once (stall 2 s + election 5 s), typically about 100 ms. Phase 12 groups re-run green (`failover`, `placement`).
- **Remote (worker) activations** are not evicted by the input-bridge hook; they finish within the grace or are reported in `leased_left` and fenced after demotion.
- **Election reason:** consider a `ELECTION_REASON_HANDOFF` (5) in a 13-01-owned ext.rs follow-up if status/observability needs to tell handoffs from resignations.
- **13-10** consumes `NodeOpsHandoffClient` for leader moves; **13-13** reads `HandoffControl::in_handoff` for the read fence; **13-19** times the shutdown in tier C (SIGTERM path is the same `before_shutdown`).
- No ci.yml change needed (no new failpoint crate or target).

## Self-Check: PASSED

- Files on main: crates/ostro-partition/tests/handoff.rs, crates/ostro-cluster/tests/handoff_executor.rs, crates/ostro-repl-conformance/tests/handoff.rs, crates/ostro-cluster/src/{handoff,shutdown}.rs, crates/ostro-repl-conformance/src/scenarios/handoff.rs: FOUND.
- Commits on main: c055a55, 998fbd1, 139356d, e3f3bc7: FOUND.
- Plan grep checks: `impl HandoffExecutor for LocalHandoff` in handoff.rs, `transfer_leader` and `pub async fn controlled` in shutdown.rs, the four scenario names in scenarios/handoff.rs: present.
