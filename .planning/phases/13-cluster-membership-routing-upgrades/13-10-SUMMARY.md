---
phase: 13-cluster-membership-routing-upgrades
plan: 10
subsystem: partition reassignment (AlterReplicas apply, placement controller, leader balancer)
tags: [reassignment, kip-455, alter-replicas, placement, rf-raise, drain, evacuation, balancer, isr, mutant, tier-b]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 AlterReplicas shape + ext 100/99 + controller stubs, 13-02 request_reassign/request_rebalance/PlacementTrigger/PlacementError/level_max, 13-04 compat::require, 13-06 reassign::next_step_at/cancel_target/shrink_min_insync + plan::target_replicas + leaders::decide, 13-08 P13Harness, 13-09 MemberState driver (Draining→Removed once empty) + force remove, 13-11 NodeOpsHandoffClient"
provides:
  - "MetaState AlterReplicas apply (D-P13-12 rules, isr_version CAS, ext 100 verbatim)"
  - "placement::controller: PlacementController (meta-leader loop: drive in-flight moves, start planned moves ≤ max_concurrent_moves, leader balancer), observe / effective_policy / planned_moves / blocked_partitions / alter_command, request_reassign (validate, cancel before LeaderMove), request_rebalance, with_clock / with_live_source builders, TickReport"
  - "mutant reassign::mutant::raise_min_insync_early"
  - "ostro-log ReplicaTracker::set_replicas: the partition leader follows a same-epoch replica-set change"
  - "tier-B reassign group: 8 scenarios"
affects: [13-16, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "reassignment state only in meta (ext 100), controller = pure 13-06 machine + CAS proposals, so failover resumes"
    - "policy-gated automation: a static Phase 12 cluster without ReplicationPolicy only evacuates Draining/Removed members"
key-files:
  created:
    - crates/ostro-meta/tests/alter_replicas.rs
    - crates/ostro-cluster/tests/placement_controller.rs
    - crates/ostro-cluster/tests/reassign_mutants.rs
    - crates/ostro-cluster/tests/support_placement/mod.rs
    - crates/ostro-partition/tests/replica_removal.rs
    - crates/ostro-repl-conformance/tests/reassign.rs
  modified:
    - crates/ostro-meta/src/state.rs
    - crates/ostro-cluster/src/placement/controller.rs
    - crates/ostro-log/src/partition_log.rs
    - crates/ostro-log/src/replica/tracker.rs
    - crates/ostro-repl-conformance/src/scenarios/reassign.rs
    - crates/ostro-repl-conformance/src/scenarios/membership.rs (13-09 scenario, one check moved)
key-decisions:
  - "Absent ReplicationPolicy = the Phase 12 bootstrap seeds as placed (rf = largest replica count, min_insync = largest min_insync); without a policy the controller only evacuates Draining/Removed members and never rebalances or moves leaders on its own (a static cluster that never drains a node stays exactly as placed); explicit reassign/rebalance work regardless"
  - "Placement members = Active only (Joining, Draining, Removed excluded); epoch-0 partitions are never observed as placed"
  - "Removed replicas keep their data inert (Phase 12 role lifecycle: Offline, fetcher stopped, never served); roles.rs unchanged, no deferred deletion"
  - "The partition leader's ReplicaTracker follows replica-set changes published at the same epoch (needed for any Expand to finish)"
  - "request_rebalance on a non-leader starts the planned replica moves itself (proposals forward); the leader balancer runs only on the meta leader"
duration: "about 4 h"
completed: 2026-11-25
---

# Phase 13 Plan 10: Partition reassignment Summary

**`AlterReplicas` is validated at meta apply with the same rigour as Phase 12 elections; the meta leader's placement controller drives the 13-06 Expand → CatchUp → LeaderMove → Shrink machine with CAS proposals (state only in meta, so a new controller resumes), raises RF as members join under a `ReplicationPolicy`, evacuates Draining and Removed members, serves explicit reassign/cancel/rebalance, and balances preferred leaders one handoff at a time; `min_insync` never rises before Shrink and never above |ISR|.**

## AlterReplicas apply rules (as landed, `MetaState::alter_replicas`)

Checked in this order; any failure changes nothing (snapshot bytes identical):

| check | verdict |
|-------|---------|
| partition unknown | `NotFound` |
| partition at epoch 0, or command epoch 0 | `Invalid("… unplaced (epoch 0)")` |
| `epoch != current` | `StaleVersion { current: epoch }` |
| `expected_isr_version != isr_version` | `StaleVersion { current: isr_version }` |
| replicas / isr empty, contain 0, or list a node twice | `Invalid` |
| `isr ⊄ replicas` | `Invalid` |
| current leader ≠ 0 and ∉ replicas, or ∉ isr | `Invalid` |
| `min_insync == 0` or `> |isr|` | `Invalid` |

On success: replicas, isr and min_insync replaced, `isr_version + 1`, ext 100 = `put_reassignment(cmd.reassignment)` (postcard of the command's value, never decoded or validated) or removed for `None`; epoch, version, leader and every other ext entry unchanged. The doc comment records that `isr_version` is the CAS token of the whole replica-and-ISR state. `validate.rs` never references tag 100 (source test + a Put with undecodable tag-100 bytes is accepted). `elect_leader.rs` and the meta goldens are untouched and green.

## Controller loop (`placement::controller`)

Every `interval` (500 ms, `PlacementConfig` default) and immediately on a `PlacementTrigger`; acts only while `meta.is_leader()` (memory cleared on loss):

1. **observe**: members (MemberState, absent = Active), placed partitions (epoch ≥ 1 only), policy (ext 99), ext 100 per partition.
2. **drive** each in-flight reassignment with `next_step_at(obs, target, policy_min, now)`:
   - `Expand` → `AlterReplicas{replicas = current ∪ target, isr = current, min_insync = current, ext}` after `compat::require(AlterReplicas)` and `require(ReassignmentExt)`;
   - `CatchUp` → wait (the Phase 12 ISR expand adds the new replicas);
   - `LeaderMove{to}` → `HandoffClient::hand_off(leader, p, to, epoch)` in a background task, one per partition, retried ≥ 2 s apart;
   - `Shrink` → `AlterReplicas{replicas = isr = target, min_insync = shrink_min_insync(..), None}`.
   A target naming a member that is no longer Active is re-planned. `StaleVersion` → re-read next tick.
3. **start**: `plan::target_replicas(current (in-flight at their target), Active members, effective rf)`; with a policy or a forced pass every planned change may start, otherwise only partitions holding a replica on a Draining/Removed member; at most `max_concurrent_moves` in flight in total. A gate refusal stops the pass (`Action::Refused`, nothing proposed).
4. **balance**: when nothing is in flight and (`force` or policy present and `leader_rebalance_interval` elapsed since the last pass / since leadership was gained): `leaders::decide(published ISR, live ∩ Active, |Active|)` → at most one `hand_off`.

Blocked partitions (`min_insync > |ISR|`, nothing in flight) are returned in `TickReport::blocked`, by `blocked_partitions(obs)`, logged at debug and exported as `ostro_cluster_reassignments_blocked`. Metrics: `ostro_cluster_reassignments_in_flight` (gauge), `ostro_cluster_moves_total{result=started|step|completed|stale|rejected|refused|cancel|handoff_moved|handoff_failed}`.

`request_reassign(meta, partition, target, cancel, level_max, now_ms)` (any node; read barrier first, up to 8 CAS retries): gate → partition exists (`Invalid`) and placed (`Invalid`) → move: target non-empty, no duplicates, every member Active (`Invalid`, the message names the state) → `next_step_at` → Expand proposed; equal to current with nothing in flight = no-op; in flight to the same target = accepted; `min_insync > |ISR|` = `Conflict`. Cancel: nothing in flight = `Conflict`; `cancel_target` `None` (leader already moved) = `Conflict("… past LeaderMove …")`; else the ext-only re-target Expand, after which the controller shrinks back to the original replicas.

`request_rebalance(meta, trigger, level_max)`: gate, `trigger.notify()`; on a node that is not the meta leader it also starts up to the default `max_concurrent_moves` planned moves itself (forced plan), since the route table runs `rebalance` locally (MW).

## Timings per move (tier B, local WSL2, `--test-threads=1`)

| scenario | measured |
|----------|----------|
| rf_raised_on_join | RF 1 → 3 on all 4 partitions done by the time the second join returned; scenario 65–126 s (two joins + checks); 567–579 runs acknowledged, longest gap between acks 0.14–0.20 s |
| drain_evacuates_replicas | node 4 empty 0.5–1.4 s after `drain` returned (harness drain already waits for "no replica"); longest gap 0.11 s; 17–18 s |
| explicit_reassign_and_cancel | p0 `[1,2,3] → [4,2,3]` incl. a LeaderMove in 0.77–0.83 s; cancel during a held CatchUp restored `[old]`; longest gap 2.3 s (the handoff grace; not asserted here); 18–23 s |
| controller_failover_mid_move | move completed 5.6–6.4 s after killing the meta leader (new election + CatchUp released); longest gap 1.4 s; 39 s |
| move_blocked_before_finalize | 412 `feature_not_enabled` on every node for reassign and rebalance, no AlterReplicas in the meta log, upgrade oracle clean; 123 s (three level-1 joins) |
| removed_replica_stops_and_cleans | removed replica Offline, 10–13 records retained unchanged across 3 more acked writes and > session_timeout; 17–22 s |
| readded_replica_truncates | re-added replica back in the ISR after the leader moved twice; replica identity holds; 19 s |
| force_removed_dead_node_evacuated | 3 partitions of the dead node re-placed 0.7–1.1 s after the force remove returned; 21–25 s |

Controller unit timings (`placement_controller`, 20 ms rounds, fake partition layer): RF 1 → 3 over 4 partitions in ~30 rounds with ≤ 2 in flight; the whole suite runs in 2–11 s.

## Observed Phase 12 replica-removal behaviour (plan check W6)

The 12-13 role lifecycle already does the right thing for a removed replica, so `roles.rs` was **not** changed:

- the node leaving `replicas` → `roles::decide` = Offline: the fetcher is stopped, the router keeps a leader hint, the partition is not routable, no term is requested, a later leader change elsewhere does not wake it;
- the local log and store are **retained inert** (no deletion, no appends, never served), also well past `session_timeout`; disk reclamation of a removed replica is therefore an operational follow-up, not a safety issue;
- re-added → `become_follower` + a fetcher at the current epoch; the Phase 12 fetch path reconciles the stale log (divergence truncation, or a snapshot install below `log_start`) before the ISR expand.

`ostro-partition/tests/replica_removal.rs` asserts the above with a fake assignment source; tier B `removed_replica_stops_and_cleans` / `readded_replica_truncates` check it end to end (replica identity below the HW over the ISR including the re-added node).

## Task Commits

| # | commit | message |
|---|--------|---------|
| 1 | ed715d4 | feat(13-10): AlterReplicas apply |
| 2 | 97d1b02 | feat(13-10): placement controller + balancer |
| 3a | 353c370 | fix(13-10): partition leader tracks AlterReplicas replica-set changes |
| 3b | ef8f828 | test(13-10): tier-B reassignment scenarios |
| 3c | fe4510f | test(13-10): balancer test waits for each handoff to apply |

## Verification

Local runs (`target/slot-2`, nice 10, 6 jobs), each with a non-zero test count:

| run | result |
|-----|--------|
| `-p ostro-meta --test alter_replicas` | 19 passed |
| `-p ostro-meta` (whole crate, incl. elect_leader, goldens, newer_command) | all binaries ok |
| `-p ostro-cluster --test placement_controller -- --test-threads=1` (and default threads) | 9 passed |
| `-p ostro-cluster --features failpoints --test reassign_mutants --test placement_controller` | 1 + 9 passed (the mutant bites: baseline no stall, mutant leaves |ISR| < min_insync) |
| `-p ostro-cluster` (whole crate) | all binaries ok |
| `-p ostro-partition --test replica_removal --test role_lifecycle --test replica_services` | 2 + 9 + 5 passed |
| `-p ostro-log` (whole crate, incl. the new `set_replicas` unit test) | all ok |
| `-p ostro-repl-conformance --test reassign -- --test-threads=1` | 8 passed (331 s, then 386 s after enabling the audits on uncompacted logs) |
| `-p ostro-repl-conformance --test membership -- --test-threads=1` | 6 passed (145 s; force_remove_dead_node with the moved check) |
| clippy `-D warnings` (ostro-meta, ostro-cluster ± failpoints, ostro-partition, ostro-log, ostro-repl-conformance, all targets), `cargo fmt --all --check`, `scripts/p12-grep-gates.sh` | clean |

**Plan-level ci-local:** two `ostro-k8s-ci --parallel` runs.
- **ef8f828** (55 min): 4 groups (poly, obuild, test1, musl1) never started, because pod creation failed with "database is locked" (infra). Of the rest: lint, mutant matrix, campaign smoke, P12 grep gates and deny passed. Failures: `placement_controller::balancer_moves_one_preferred_leader_per_pass` (fp3, a fixed 100 ms pause under load; fixed in fe4510f), `worker_failover::{sustained_overload, oscillation}` (known flaky), and `flow_e2e::weights` (musl2, 503 not_leader right after a single-node start; passes 12/12 alone on slot 1).
- **fe4510f** (+ the untracked SUMMARY, 50 min): every group ran. PASS: fmt, clippy, build, sdk, guests, order-saga, all polyglot steps, ostro-build polyglot, sleepers, failpoint list sync, replication mutant matrix, campaign smoke (20 kills), phase 7 legs, deny, vendored, grep gates, workflow validation, musl release, and musl zigbuild test in musl1. test1 (cargo test half incl. ostro-cluster / ostro-meta / ostro-log / ostro-partition / repl-conformance) was green. FAIL, none from 13-10:
  - `worker_failover::oscillation` in test2 and musl2: known flaky.
  - `controller::a_new_controller_seeds_suspicion_and_does_not_elect_on_its_first_sample` in fp3: Phase 12 controller test, meta only, `AlterIsr` on a partition that real liveness had already taken offline under load. Passes 3/3 alone on slot 1 together with `placement_controller` and `reassign_mutants` (failpoints).
  - **musl binary size 60.4 MB** (> 60). Main was already at 60.2 MB, which another agent is fixing; this plan adds about 0.2 MB (the controller).

## Deviations from Plan

1. **[Rule 1/3 - Blocking bug] Partition leader ignored added replicas.** The leader's `ReplicaTracker` was built at `become_leader` only and `on_fetch` drops unknown replicas, so after an Expand the new replica never counted for the ISR expand and every move stayed in CatchUp (found by tier-B `explicit_reassign_and_cancel`). Fix in ostro-log (`set_published_isr` → `ReplicaTracker::set_replicas`), commit 353c370; an unchanged set is a no-op, so Phase 12 behaviour is unchanged (ostro-log and the role suites green).
2. **Replica removal keeps data inert** (W6): the plan allowed either deferred deletion or "retained inert exactly as Phase 12 defines; recorded". Phase 12 retains it inert; deleting partition data under a live Node would need the quiesce/reset machinery and is not a safety requirement, so `roles.rs` is untouched and `replica_removal.rs` asserts retention instead of deletion.
3. **Policy-gated automation.** Without `ReplicationPolicy` (every Phase 12 / tier-B static cluster; 13-16's `cluster init` writes it) the controller does not rebalance or run the balancer on its own; it only evacuates Draining/Removed members. Otherwise every static Phase 12 / 13 scenario would start moving replicas and leaders.
4. **Server wiring not changed (`p13/mod.rs` is 13-02-only).** The production controller therefore runs with its defaults: the clock is `SystemClock` (marked `clock-exempt`; `started_at_ms` is informational) and the balancer's live set is raft `recent_active` on the meta leader. Hunk for 13-02's owner below.
5. **Gate status 412, not 409.** The plan says 409 `feature_not_enabled`; 13-02's `cluster_api.rs` maps `PlacementError::FeatureNotEnabled` to `precondition_failed` (412) with `details.reason = "feature_not_enabled"` (13-15's retire uses the same). The scenario accepts 409 or 412 with the reason; the inconsistency with the module upload path (409) is for 13-20's docs / 13-02's owner.
6. **`p13_upgrade::no_reject_continue` not run.** It needs apply-outcome streams (`OutcomeRecord`) that no tier-B producer builds yet; the scenarios run `no_gated_before_finalize_from` (level-1 cluster) and the Phase 12 oracles instead. For 13-18.
7. **LA-rule / election audits** run only when the meta log still holds every election (the snapshot has all partitions at epoch 0); after a learner join the leader compacts past them (13-09 note), so `rf_raised_on_join` skips them after the joins (they run before). Replica identity and acked-on-disk always run.
8. **13-09 scenario touched.** `membership::force_remove_dead_node` asserted, after the removal, that the dead node "still holds replicas (evacuation is 13-10's)". With 13-10 the controller evacuates it within ~1 s, so the check now measures "holds replicas" right before the kill (same intent, no oracle changed).
9. **Extra test support file** `crates/ostro-cluster/tests/support_placement/mod.rs` (shared by placement_controller and reassign_mutants; includes 13-09's `support_membership`).
10. **Scenario shape.** `move_blocked_before_finalize` is in the registry with the single-node spec and starts its own level-1 cluster inside the body (`version_gate::start_level1_cluster(3)`), so 13-18's mutant matrix can reference it by name. All cluster scenarios first wait for finalized level 2 (the auto-finalizer needs a moment on a fresh cluster).
11. **Worktree / target:** scratchpad worktree, `target/slot-2`; plan paths `/tmp/ostro-wt-13-10` / `/tmp/ostro-target-13-10` not used (executor brief).

## p13/mod.rs hunk (not applied; 13-02-only file)

```rust
    // 13-10: placement controller and preferred-leader balancer.
    let live_cluster = Arc::clone(&ctx.cluster);
    tasks.push(
        PlacementController::new(/* unchanged */)
        .with_trigger(ctx.placement_trigger.clone())
        .with_clock(Arc::clone(&ctx.clock))
        .with_live_source(Arc::new(move |u: &std::collections::BTreeSet<u64>| {
            live_cluster.liveness().lock().live_set(u)
        }))
        .start(stop.clone()),
    );
```

(`live_set`'s exact signature is the 12-10 liveness accumulator's; adapt the closure if it takes a slice.) No ci.yml change is needed: `placement_controller` / `reassign_mutants` run in the normal and failpoint ostro-cluster steps, `reassign` in the repl-conformance step.

## Handover

- **13-16 (promotion):** `cluster init` writing `ReplicationPolicy{rf 3, min_insync 2}` is what switches on RF raise + balancing; nothing else to call. Joined nodes become placement members once Active.
- **13-09 / status owner:** `GET /v1/cluster/status` could show blocked partitions via `placement::controller::blocked_partitions(&observe(state))` (min_insync > |ISR| with nothing in flight); not added (ClusterStatus DTO is an API contract).
- **13-12 (routing):** `rebalance` is routed MW (local); the leader balancer pass only runs on the meta leader. Routing it `MetaLeader` would make a forced balancer pass work from any node.
- **13-18:** mutant `reassign::mutant::raise_min_insync_early` → `rf_raised_on_join` (under the mutant the Expand from RF 1 with min_insync 2 is refused at apply, so RF never rises and the scenario fails); emit_ungated → `move_blocked_before_finalize`. Make `la_rule` snapshot-aware (13-09 note) so `rf_raised_on_join` can audit after the joins; add an `OutcomeRecord` producer for `no_reject_continue`.
- **Follow-up (not 13-10):** reclaim the disk of a removed replica (retained inert today).

## Self-Check: PASSED

- FOUND on main: ed715d4, 97d1b02, 353c370, ef8f828, fe4510f
- FOUND: alter_replicas.rs (contains `min_insync_above_isr_rejected`), placement_controller.rs, reassign_mutants.rs, support_placement/mod.rs, replica_removal.rs, tests/reassign.rs
- controller.rs contains `reassign::step`, `compat::require` and `hand_off`; scenarios/reassign.rs names all eight scenarios
