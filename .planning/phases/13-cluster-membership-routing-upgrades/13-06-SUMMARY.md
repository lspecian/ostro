---
phase: 13-cluster-membership-routing-upgrades
plan: 06
subsystem: cluster membership + placement (pure algorithms)
tags: [membership, raft, conf-change, joint-consensus, placement, reassignment, kip-455, proptest, tdd]
requires:
  - phase: 13-01
    provides: "fixed signatures of membership::reconcile::{MemberObs, ReconcileInput, Step, next}, placement::plan::target_replicas, placement::reassign::{PartitionObs, ReassignStep, next_step}, placement::leaders::preferred_leader_moves; ext::Reassignment; MEMBER_* constants"
provides:
  - "membership::reconcile::next: learner-first reconciler (explicit leave-joint, odd halves, live-quorum checks, transfer-before-self-removal, tombstones)"
  - "placement::plan::target_replicas: greedy minimal-move balanced planner (replica and preferred-leader spread <= 1, draining emptied)"
  - "placement::reassign::{next_step, next_step_at, cancel_target, shrink_min_insync}: Expand -> CatchUp -> LeaderMove -> Shrink machine, W5 min_insync"
  - "placement::leaders::{decide, LeaderObs, LeaderMove, preferred_leader_moves}: one ISR-aware preferred-leader move per call"
  - "three model suites: tests/{reconcile,placement,reassign}_model.rs"
affects: [13-09, 13-10, 13-11, 13-16]
tech-stack:
  added: []
  patterns:
    - "pure decision modules (no tokio/Clock/IO) + action-sequence proptest models with a heal-and-settle convergence check"
key-files:
  created:
    - crates/ostro-cluster/tests/reconcile_model.rs
    - crates/ostro-cluster/tests/placement_model.rs
    - crates/ostro-cluster/tests/reassign_model.rs
  modified:
    - crates/ostro-cluster/src/membership/reconcile.rs
    - crates/ostro-cluster/src/placement/plan.rs
    - crates/ostro-cluster/src/placement/reassign.rs
    - crates/ostro-cluster/src/placement/leaders.rs
key-decisions:
  - "13-01's fixed signatures (final) win over the plan's interface sketch; ISR-aware leader balancing, now_ms stamping, cancel and the W5 formula are additive pub fns"
  - "A dead voter keeps counting towards the desired voter count: a transient failure never shrinks the meta group; a dead voter is only replaced by a caught-up live learner (Joint{add, demote})"
  - "The reconciler never demotes/removes the meta leader itself (raft-rs post_conf_change leaves that branch untested); it transfers first, and waits when no live caught-up Active voter can take over"
  - "A conf member without a Member record freezes the reconciler (Step::None) instead of guessing"
  - "Models use plain proptest action sequences: ostro-cluster has no proptest-state-machine dev-dependency and Cargo.* is 13-01-only"
duration: ~1h
completed: 2026-11-24
---

# Phase 13 Plan 06: Pure membership + placement algorithms Summary

**Learner-first meta reconciler with one-joint 1→3→5 / 5→3→1 changes, a greedy minimal-move placement planner, the KIP-455 reassignment machine with the W5 shrink rule, and an ISR-aware preferred-leader balancer. All four are pure, and three proptest models check them (every planted mutant is caught).**

## Final public signatures (13-09 / 13-10 read these)

```rust
// ostro_cluster::membership::reconcile  (13-01 signature, unchanged)
pub struct MemberObs { pub id: u64, pub state: u8 /* MEMBER_* */, pub live: bool, pub matched: u64, pub pending_snapshot: bool }
pub struct ReconcileInput { pub leader: u64, pub voters: Vec<u64>, pub voters_outgoing: Vec<u64>, pub learners: Vec<u64>,
    pub members: Vec<MemberObs>, pub commit: u64, pub pending_conf_change: bool, pub promote_lag: u64, pub max_voters: u8 }
pub enum Step { LeaveJoint, AddLearner(u64), Joint { add_voters: Vec<u64>, remove_voters: Vec<u64>, demote: Vec<u64> },
    RemoveLearner(u64), TransferLeader(u64), None }
pub fn next(input: &ReconcileInput) -> Step;

// ostro_cluster::placement::plan  (13-01 signature, unchanged)
pub fn target_replicas(current: &BTreeMap<u16, Vec<u64>>, members: &[u64], rf: u8) -> BTreeMap<u16, Vec<u64>>;

// ostro_cluster::placement::reassign  (next_step: 13-01; the rest additive, 13-06)
pub struct PartitionObs { pub partition: u16, pub epoch: u32, pub leader: u64, pub replicas: Vec<u64>, pub isr: Vec<u64>,
    pub isr_version: u64, pub min_insync: u8, pub reassignment: Option<ostro_meta::ext::Reassignment> }
pub enum ReassignStep { Expand { replicas: Vec<u64>, reassignment: Reassignment }, CatchUp, LeaderMove { to: u64 },
    Shrink { replicas: Vec<u64>, isr: Vec<u64>, min_insync: u8 }, None }
pub fn next_step(p: &PartitionObs, target: &[u64], policy_min_insync: u8) -> ReassignStep;        // started_at_ms = 0
pub fn next_step_at(p: &PartitionObs, target: &[u64], policy_min_insync: u8, now_ms: u64) -> ReassignStep;
pub fn cancel_target(p: &PartitionObs) -> Option<Vec<u64>>;
pub fn shrink_min_insync(current: u8, policy: u8, target_len: usize) -> u8;

// ostro_cluster::placement::leaders  (preferred_leader_moves: 13-01; the rest additive, 13-06)
pub struct LeaderObs { pub partition: u16, pub replicas: Vec<u64>, pub isr: Vec<u64>, pub leader: u64 }
pub struct LeaderMove { pub partition: u16, pub to: u64 }
pub fn decide(parts: &[LeaderObs], live: &BTreeSet<u64>, members: usize) -> Option<LeaderMove>;
pub fn preferred_leader_moves(partitions: &[(u16, Vec<u64>, u64)], live: &[u64], max_moves: usize) -> Vec<(u16, u64)>;
```

### Semantics the drivers rely on

**`reconcile::next`** checks these in order:
1. A conf change is pending → `None`.
2. `voters_outgoing` is non-empty → `LeaveJoint`.
3. A conf id has no Member record → `None`.
4. Voter step. N is built from the Active voters (leader first, then live, then most caught-up, then id), then the ready learners (Active, live, no pending snapshot, `matched ≥ commit − promote_lag`; most caught-up first), then the dead Active voters. N is truncated to `n` = the largest odd number ≤ min(Active members that are live or already voters, `max_voters`, number of candidates).
   - If N ≠ voters and the leader ∉ N → `TransferLeader` to the best live, caught-up, Active voter (one that stays in N is preferred). If there is no such voter, the reconciler moves on to the learner steps.
   - Otherwise → `Joint{add: N∖V, remove: V∖N that are Removed, demote: the rest of V∖N}`. This is emitted only when V, N and `V∖remove` each have a live quorum.
5. A Removed learner → `RemoveLearner` (smallest id).
6. A Joining or Active member outside the conf → `AddLearner` (smallest id).

Draining voters are demoted to learners. They are not removed: removal happens only once the record is Removed.

**`plan::target_replicas`** returns only the partitions whose list changes. Order matters: the first replica is the preferred leader. Every list has `min(max(rf,1), |members|)` distinct members. Replica counts and preferred-leader counts each differ by ≤ 1 across `members`. Non-members end up with 0 replicas, so the caller drains a node by leaving it out of `members`. An empty `current` list gets placed. Epoch-0 partitions should be filtered by the caller (D-P13-12); the planner does not know epochs. `members = []` → empty result.

**`reassign::next_step`**:

| observation | step |
|---|---|
| epoch 0, an empty target, or `replicas == target` with no ext | `None` |
| no ext, an ext for another target, or target ⊄ replicas | `Expand{replicas = current ∪ target (current order, then new ids), ext{target, adding = replicas_new ∖ original, removing = original ∖ target}}` |
| the same, but `min_insync > |isr|` | `None` (the AlterReplicas would be refused) |
| ext for this target and target ⊄ published ISR | `CatchUp` |
| target ⊆ ISR and the live leader ∉ target | `LeaderMove{to: target[0]}` |
| target ⊆ ISR and the leader ∈ target or offline | `Shrink{replicas: target, isr: target, min_insync: shrink_min_insync(..)}` |

For Expand, the controller sends the current `isr` and `min_insync` unchanged, plus `expected_isr_version = p.isr_version`. `original` = `replicas ∖ ext.adding`. A re-target therefore keeps the stale adds in `adding`, and Shrink later drops them.

**`cancel_target`** returns `Some(original)` only while the leader ∈ original (or the leader is offline). The controller then drives `next_step` towards it: an ext-only Expand re-targets, then Shrink restores the original replicas.

**`shrink_min_insync`** = `min(max(current, min(policy, |target|)), |target|)`, and at least 1.

**`leaders::decide`** returns at most one move. A partition is a candidate when its leader ≠ 0, its preferred replica ≠ leader, and the preferred replica is live and in the ISR. Partitions whose leader leads more than `ceil(P/members)+1` go first, then higher leader load, then partition id. `preferred_leader_moves` is the 13-01 ISR-less wrapper: it assumes ISR = replicas and applies `decide` up to `max_moves` times. 13-10 should call `decide` with the published ISR.

## Invariants checked (models)

- **reconcile_model** (plan unit tables + action-sequence model). The model starts from a random grown cluster: 1–7 members, an odd voter set ≤ max_voters, and learners. Actions are join, activate, drain, remove, liveness flaps, catch-up/lag, snapshot pending, commit advance, tick, apply, dropped proposal, raft auto-leave, and leader change without auto-leave. max_voters ∈ {1,3,4,5,7}. Checked on every step:
  - ≤ 1 pending change, and `None` while one is pending;
  - `LeaveJoint` whenever the config is joint and nothing is pending;
  - every Joint has both halves odd, the new half ≤ max_voters, live quorum of the new config and of `V∖remove`, promotes only ready learners, and never demotes or removes the leader;
  - TransferLeader goes only to a live voter;
  - tombstoned ids are never added or promoted.

  Checked on every applied config: incoming half odd, outgoing half odd, no voter that is also a learner. Then a heal-and-settle phase reaches a fixpoint in ≤ 64 steps with: the joint config left, no tombstone in the conf, |V| = the largest odd ≤ min(Active, max_voters), V ⊆ Active, and every Joining/Active member in the conf.
- **placement_model**: 1–7 nodes, P ∈ {4,16,64}, RF 1–3. Initial assignments are random (duplicates, non-members, empty lists) or a promoted RF=1 cluster. Checked: no duplicate replica, |replicas| = min(rf, members), replica-count spread ≤ 1, preferred-leader spread ≤ 1, non-members (draining) hold 0, no no-op entries, idempotent (planning the result again is empty, which is the minimal-move check), deterministic, and no node ends above max(its start, ceil(avg)). Leader balancer: at most one move, only to a live in-ISR preferred replica of a led (leader ≠ 0) partition, and `None` only when no candidate exists.
- **reassign_model**: the partition goes through controller steps, honest ISR expansion, ISR shrinks (including below min_insync), failovers, the leader going offline, failed handoffs, cancels and re-targets. Every emitted AlterReplicas passes the D-P13-12 apply rules (epoch ≠ 0, leader ∈ replicas, isr ⊆ replicas, leader ∈ isr, `1 ≤ min_insync ≤ |isr|`). Also checked:
  - only Shrink raises min_insync, and nothing lowers it below min(policy, |target|);
  - Shrink happens only when target ⊆ published ISR, uses exactly the W5 formula, keeps min_insync ≤ |target|, and never drops a live leader;
  - LeaderMove goes only to an in-ISR member of the target;
  - Expand ⊇ old ∪ target;
  - a cancel restores exactly the original replicas and is refused after the leader moved;
  - replicas ⊇ isr always.

  Settle: the run ends at `replicas == target`, ext cleared, leader ∈ target, and min_insync ≥ min(policy, |target|) whenever a reassignment ran. Named cases include `shrink_waits_for_target_in_isr` and `rf3_to_1_with_min_insync_2` (→ min_insync 1).
- **Mutation check** (manual, not committed). Planted mutants were all caught. Reconcile (6): the quorum checks, the `V∖remove` quorum, the snapshot guard, the transfer, the pending guard, and RemoveLearner. Reassign (5): the min_insync ≤ |isr| guard, the CatchUp gate, LeaderMove, the W5 formula, and the cancel-after-move guard. Plan (3): trim, replica balance, and leader balance.

## Release-mode case counts

`PROPTEST_CASES=1000 cargo test --release -p ostro-cluster --locked --test <t>` (LTO off, see deviation 4):

| suite | tests | release, 1000 cases | release, 20000 cases | debug, default 256 |
|---|---|---|---|---|
| reconcile_model | 21 (20 tables + `membership_model`) | ok, 0.10 s | ok, 1.81 s | ok |
| placement_model | 14 (9 cases + 3 properties + 2 leader properties/cases) | ok, 0.02 s | ok, 0.46 s | ok |
| reassign_model | 12 (11 cases + `reassign_model`) | ok, 0.02 s | ok, 0.43 s | ok |

The 1000→20000 timings scale linearly, so `PROPTEST_CASES` is honoured. The debug `membership_model` also passed at 3000 and 5000 cases. clippy `--all-targets -D warnings` and `cargo fmt --check` are clean for ostro-cluster. The purity grep (`tokio::|Clock|SystemTime|Instant::now`) over the four modules finds nothing.

## Plan-level ci-local (`nice -n 15 bash scripts/ci-local.sh --no-musl --no-polyglot` at a94dd9b, slot-1)

All steps passed except two, and neither is caused by 13-06:

- **cargo test: FAIL.** The only failure was `ostro-raft-driver driver::tests::commits_do_not_wait_for_ticks` (98 ms per sequential commit at load ~12). This is the vendored driver (13-05), and the failure is a timing flake under the Phase 12 campaign load: re-run alone it passed 3/3. The workspace run stopped at that point (cargo fail-fast) after 270 test binaries, which included all three 13-06 suites (green). I then ran the skipped packages separately with `--no-fail-fast`: ostro, ostro-raft-proto, ostro-repl-conformance, ostro-server, ostro-testkit, ostro-triggers, ostro-types, ostro-ui, ostro-witcheck, ostro-worker and ostro-worker-proto. Result: rc 0, 179 binaries, 1146 passed.
- **one log path + P12 grep gates: FAIL.** The clock gate flags `SystemClock` in `crates/ostro-cluster/src/compat.rs:40,391` and `crates/ostro-cluster/src/upgrade.rs:30,201`. Both files belong to 13-04 (88f4c78). This is out of scope for 13-06, and no gate was changed. 13-04 or the coordinator must either inject the Clock or extend the gate's allow-list.
- Passed: fmt, clippy, build, sdk, guests, order-saga, clients go, sleepers, failpoint suites, replication mutant matrix, server campaign smoke, phase 7 legs, deny, vendored, workflow validation. The 13-06 modules are not called from anywhere else yet.

## Task Commits

| # | commit | message |
|---|---|---|
| RED | 4a1ed54 | test(13-06): failing models for reconcile/placement/reassign (with additive stubs so RED compiles; every case expecting a step and all five model properties failed on the stubs; only the "expect None/empty" cases passed) |
| Task 1 | 97f2186 | feat(13-06): pure learner-first membership reconciler |
| Task 2 | a94dd9b | feat(13-06): pure placement planner, reassignment machine, leader balancer |

## Deviations from Plan

1. **[Rule 3 - Blocking] Models without proptest-state-machine.** The plan's artifact check expects `ReferenceStateMachine` in reconcile_model.rs. ostro-cluster has no `proptest-state-machine` dev-dependency (only the workspace pins it), and the Phase 13 shared-file rule makes `Cargo.*` 13-01-only. The models are therefore plain-proptest action sequences: a reference model, random transitions, invariants after each transition, and a convergence phase. The strength is the same (see the mutation check). The `contains: "ReferenceStateMachine"` artifact check does not match. If wanted, 13-01's owner can add `proptest-state-machine.workspace = true` to `[dev-dependencies]` in crates/ostro-cluster/Cargo.toml (this changes Cargo.lock) and the model can be ported.
2. **Fixed 13-01 signatures used instead of the plan's sketch.** The sketch's `next(m, cs, prog, live, me, ..)`, `plan(assign, nodes, rf) -> Vec<Move>`, `step(.., now_ms) -> Intent` and `decide(..)` were superseded by the 13-01 contracts, which are marked final. The missing capabilities are additive pub items: `next_step_at` (now_ms), `cancel_target`, `shrink_min_insync`, and `LeaderObs`/`LeaderMove`/`decide` (ISR-aware). The state machine's `ReassignState` enum is implicit: it is derived from `PartitionObs`, because the state lives only in meta.
3. **Interpretation of D-P13-8 "desired voters = largest odd ≤ min(live Active members, max_voters)".** A dead voter still counts towards the desired number, so a transient failure never auto-shrinks the group. It is only replaced when a caught-up learner exists. Read literally, the rule would shrink 3→1 when one voter dies. The plan's unit case "live {1,3} → None" is consistent with this interpretation.
4. **Release-mode runs use `--config profile.release.lto=false --config profile.release.codegen-units=16`.** The workspace release profile uses fat LTO. Under the concurrent Phase 12 campaign a fat-LTO build of the ostro-cluster dependency graph was too costly. The code is still built at opt-level 3, and the test semantics are unchanged.
5. **Plan paths:** the plan names `/tmp/ostro-wt-13-06` and `/tmp/ostro-target-13-06`. Per the executor brief, the work used a scratchpad worktree and `target/slot-1`.

## Handover notes

- **13-09 (membership driver):**
  - Refuse a voter Drain/Remove while a conf change is pending or the config is joint, and when no other live Active voter would remain. The model's `may_leave` gate encodes this.
  - Without that gate, there is a corner case: the leader is tombstoned while a pending shrink is about to make it the sole voter. The reconciler then waits forever, because it never demotes the leader itself.
  - Known limitations of never demoting the leader:
    - a 2-node cluster cannot drain its single voter until a third node joins;
    - `max_voters = 1` cannot move the meta leader.
  - Map a missing MemberState ext to the right `state` before calling `next`. Any conf id without a record freezes the reconciler.
  - `promote_lag` is in entries.
- **13-10 (placement controller):**
  - Filter epoch-0 partitions before `target_replicas` and `next_step`.
  - Pass `members` = Active, non-Draining, non-Removed ids.
  - Use `next_step_at(.., now_ms)` for the ext timestamp.
  - Expand carries the current isr/min_insync with `expected_isr_version = isr_version`.
  - Use `decide` with the published ISR for the 5-minute leader balancer.
  - `next_step` returns `None` (not `CatchUp`) while `min_insync > |isr|` and no reassignment for that target is in flight. A partition whose ISR has collapsed below min_insync therefore cannot start a reassignment until the ISR recovers. Surface that state in status.

## Self-Check: PASSED

- FOUND on main: 4a1ed54, 97f2186, a94dd9b
- FOUND: crates/ostro-cluster/tests/{reconcile,placement,reassign}_model.rs; the four modules implement `pub fn next`, `target_replicas`, `pub fn next_step`, `decide`
- Purity grep over the four modules: no matches
