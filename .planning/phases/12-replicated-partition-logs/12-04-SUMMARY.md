---
phase: 12-replicated-partition-logs
plan: 04
subsystem: replication-algorithms
tags: [replication, isr, high-watermark, kip-497, kip-595, kip-279, failover, proptest, mutants, tdd]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 ostro_log::replica module list, ostro_log::mutants names, FollowerStatus, EpochHistory::end_offset_for_epoch_at"
provides:
  - "ostro_log::replica::tracker::ReplicaTracker (B5 caught-up vs previous response end, durable_leo capped at fetch_offset)"
  - "ostro_log::replica::isr::{IsrState, PendingChange, should_expand, should_shrink, next_change, maximal}"
  - "ostro_log::replica::watermark::{next_hw, append_allowed} (KIP-497 maximal ISR, min_insync freeze, wraps compute_hw)"
  - "ostro_log::replica::divergence::{FetchCheck, check, follower_truncation_point} (KIP-595 / KIP-279 / KIP-101, W5)"
  - "ostro_log::replica::failover::{AssignmentIn, Plan, plan} (ISR-only, isr ∩ live, Offline keeps one member)"
  - "six repl::mutant sites placed: isr_from_assignment, hw_leader_leo, count_nondurable_leo, shrink_before_commit, unclean_election, ignore_min_insync (watermark half)"
affects: [12-07, 12-08, 12-10, 12-11, 12-13, 12-14]
tech-stack:
  added: []
  patterns:
    - "Pure, clock-injected replica code (now_ms parameter; no Instant/SystemTime/tokio in replica/)"
    - "Mutant failpoints evaluated through replica::mutant(name) = fail::eval under feature failpoints, const false otherwise"
    - "proptest-state-machine model with a trivial reference state and a simulated cluster as SUT"
key-files:
  created:
    - crates/ostro-log/tests/replica_unit.rs
    - crates/ostro-log/tests/replica_model.rs
    - crates/ostro-log/tests/replica_mutants.rs
  modified:
    - crates/ostro-log/src/replica/mod.rs
    - crates/ostro-log/src/replica/tracker.rs
    - crates/ostro-log/src/replica/isr.rs
    - crates/ostro-log/src/replica/watermark.rs
    - crates/ostro-log/src/replica/divergence.rs
    - crates/ostro-log/src/replica/failover.rs
key-decisions:
  - "All five replica modules are written from the Phase 12 spec (Kafka KIP semantics); no upstream code ported, so no port header and no VENDORED.md row (D-P12-26)"
  - "The shrink_before_commit site is in isr::maximal (the single decision site next_hw uses), not in watermark.rs as mutants.rs's doc line says; the bite test asserts both maximal and next_hw"
  - "Offline keep = the current leader when it is in the ISR (last member known to accept writes), otherwise the last ISR member"
  - "check(): no last_fetched_epoch above log_start returns SnapshotRequired (a follower holding records always knows their epoch)"
  - "ReplicaTracker::new marks every follower caught up at now_ms (Kafka new-leader reset) but should_expand also requires that the follower fetched at least once"
duration: ~1h
completed: 2026-11-22
---

# Phase 12 Plan 04: Pure replica algorithms Summary

**The pure, clock-injected replica algorithms are in `ostro_log::replica`: the B5 tracker, KIP-497 maximal-ISR HW with the min_insync freeze, KIP-595 divergence with KIP-279 iteration, and the ISR-only failover planner. A proptest-state-machine model checks election safety over 2,000 cases. All six mutants placed in this plan bite.**

## Commits

| Commit | Message |
| --- | --- |
| 4bb996c | test(12-04): failing replica algorithm tests (RED: did not compile against the stubs) |
| 1f94d0a | feat(12-04): replica tracker, ISR, watermark, divergence, failover planner (GREEN) |
| 31cc1f5 | test(12-04): replica state-machine model + mutant bites |

## Exported API (as landed)

```rust
// replica/tracker.rs
pub type FollowerStatusRow = crate::replicated::FollowerStatus;
pub struct ReplicaTracker { .. }                         // Clone, Debug
impl ReplicaTracker {
    pub fn new(self_id: u64, replicas: &[u64], leader_leo: Offset, now_ms: u64) -> Self;
    pub fn self_id(&self) -> u64;
    pub fn replicas(&self) -> Vec<u64>;
    pub fn on_local_durable(&mut self, durable_end: Offset);
    pub fn on_fetch(&mut self, replica: u64, fetch_offset: Offset, durable_leo: Offset,
                    leader_durable_end_now: Offset, now_ms: u64);
    pub fn on_truncate(&mut self, replica: u64);         // after a diverging response
    pub fn progress(&self) -> Vec<ReplicaProgress>;      // in_isr = false; next_hw sets it
    pub fn durable_leo(&self, replica: u64) -> Option<Offset>;
    pub fn fetch_offset(&self, replica: u64) -> Option<Offset>;
    pub fn last_fetch_ms(&self, replica: u64) -> Option<u64>;
    pub fn lag_ms(&self, replica: u64, now_ms: u64) -> Option<u64>;
    pub fn status(&self, now_ms: u64, lag_max_ms: u64) -> Vec<FollowerStatusRow>; // followers only
}
// replica/isr.rs
pub enum PendingChange { Add(u64), Remove(u64) }         // Copy, Eq
pub struct IsrState { pub published: Vec<u64>, pub version: u64, pub pending: Option<PendingChange> }
impl IsrState {
    pub fn new(published: Vec<u64>, version: u64) -> Self;
    pub fn proposed(&self, change: PendingChange) -> Vec<u64>;
    pub fn propose(&mut self, change: PendingChange) -> bool;   // false while one is pending
    pub fn commit(&mut self, version: u64);
    pub fn reject(&mut self);
    pub fn set_published(&mut self, published: Vec<u64>, version: u64);
}
pub fn maximal(s: &IsrState) -> Vec<u64>;
pub fn should_expand(t: &ReplicaTracker, replica: u64, hw: Offset, epoch_start: Offset, now_ms: u64, lag_max_ms: u64) -> bool;
pub fn should_shrink(t: &ReplicaTracker, replica: u64, now_ms: u64, lag_max_ms: u64) -> bool;
pub fn next_change(t: &ReplicaTracker, s: &IsrState, replicas: &[u64], hw: Offset, epoch_start: Offset, now_ms: u64, lag_max_ms: u64) -> Option<PendingChange>;
// replica/watermark.rs
pub fn next_hw(prev_hw: Offset, progress: &[ReplicaProgress], s: &IsrState, min_insync: usize) -> Offset;
pub fn append_allowed(s: &IsrState, min_insync: usize) -> bool;
// replica/divergence.rs
pub enum FetchCheck { Ok, Diverging { epoch: Epoch, end: Offset }, SnapshotRequired }
pub fn check(h: &EpochHistory, log_start: Offset, leo: Offset, fetch_offset: Offset, last_fetched_epoch: Option<Epoch>) -> FetchCheck;
pub fn follower_truncation_point(own: &EpochHistory, own_leo: Offset, diverging_epoch: Epoch, leader_end: Offset) -> Offset;
// replica/failover.rs
pub struct AssignmentIn { pub partition: u16, pub leader: u64, pub epoch: u32, pub replicas: Vec<u64>, pub isr: Vec<u64>, pub isr_version: u64 }
pub enum Plan { Elect { leader: u64, isr: Vec<u64> }, Shrink { isr: Vec<u64> }, Offline { keep: Vec<u64> }, Nothing }
pub fn plan(a: &AssignmentIn, live: &BTreeSet<u64>, resigned: Option<u64>) -> Plan;
```

Semantics, briefly:
- **Tracker (B5/W3):** `on_fetch` counts `min(durable_leo, fetch_offset)`, and that value never decreases (only `on_truncate` resets it). A fetch is caught up when `fetch_offset >=` the `leader_durable_end` passed with the **previous** fetch from that replica, or `>=` the current sample. The caller passes the durable end it sampled **before** the read. `status` lists followers only, with `in_sync = lag <= lag_max`.
- **ISR:** expand requires at least one fetch, lag within `lag_max`, and `durable_leo >= max(hw, epoch_start)`. Shrink fires when `lag > lag_max`, never for the leader. `next_change` returns nothing while a change is pending, tries shrinks first, and never proposes removing the last published member.
- **Watermark:** while `|published| < min_insync` it returns `prev`. Otherwise it is `max(prev, compute_hw(maximal ISR))`. A maximal-ISR member with no progress row counts as 0.
- **Divergence:** see the rules in the doc comment on `check`. `follower_truncation_point = min(leader_end, own end of that epoch, own_leo)`.
- **Planner:** an epoch-0 partition or an empty ISR gives `Nothing`. If the leader is up (and not `resigned`), dead ISR followers give `Shrink{isr ∩ live}`. Otherwise it elects the first replica, in `replicas` order, that is in the ISR, live and not resigned: `Elect{leader, isr ∩ live}`. If there is no such replica it returns `Offline{keep}`, or `Nothing` when the partition is already offline with a single member.

## Model (`tests/replica_model.rs`)

- Setup: 3 replicas and a meta store with an ISR-version CAS. The leader runs the real tracker, ISR, watermark and divergence code. The controller runs the real planner. A record is `(offset, epoch)`.
- **Transitions:** Tick, Append (gated by `append_allowed`), Fsync(r), FollowerFetch(r) (divergence check, then Ok / truncate / snapshot install), Crash(r) (loses the non-durable tail and drops leader state), Restart(r) (a restarted leader goes through ResignLeader apply), IsrPropose (`next_change`), IsrCommit (AlterIsr apply rules plus CAS, otherwise reject), IsrReject, Elect (`plan` over the live set, applied with the ElectLeader rules), MinInsync(1..=3).
- **Invariants:**
  - HW is monotone per leader.
  - HW never advances while `|published| < min_insync`.
  - HW ≤ every maximal-ISR member's durable LEO.
  - Every published ISR member durably holds the records below the highest HW ever published.
  - **At every election, the new leader's log contains every offset below every HW ever published.**
  - The elected leader is in the published ISR.
  - The ISR is never empty, and the leader is in it.
  - The leader's view of the published ISR equals meta's.
- Default run: 256 cases × 1..200 transitions. Verification run: `PROPTEST_CASES=2000 --release`, green.
- Coverage check (temporary stats, not committed): in 256 cases, 99 runs published HW > 0, 111 ran at least one election, and 19 installed a snapshot.
- Sanity check (not committed): breaking the code by hand makes the model fail for unclean election, the non-durable LEO, shrink-before-commit and the ignored min_insync freeze.
- `scripted_failover_keeps_committed_prefix` is a deterministic walk-through: replica 3 holds the HW, gets shrunk and the HW jumps, the leader crashes, 2 is elected with ISR [2], appends freeze under min_insync 2, they resume at min 1, and 3 catches up and is expanded back.

## Mutant → failing test (`tests/replica_mutants.rs`, `--features failpoints --test-threads=1`)

| Mutant | Site | Bite test (property violated with the mutant on) |
| --- | --- | --- |
| `repl::mutant::isr_from_assignment` | `isr::should_expand` | `isr_from_assignment_expands_an_empty_follower` |
| `repl::mutant::hw_leader_leo` | `watermark::next_hw` | `hw_leader_leo_ignores_followers` (HW = 50 instead of 40) |
| `repl::mutant::count_nondurable_leo` | `tracker::on_fetch` (leader half) | `count_nondurable_leo_counts_the_fetch_offset` (100 instead of 90) |
| `repl::mutant::shrink_before_commit` | `isr::maximal` | `shrink_before_commit_drops_a_pending_remove` (HW 40 instead of 10) |
| `repl::mutant::unclean_election` | `failover::plan` | `unclean_election_elects_outside_the_isr` (Elect{2} with ISR [1,3]) |
| `repl::mutant::ignore_min_insync` (watermark half) | `watermark::{next_hw, append_allowed}` | `ignore_min_insync_advances_and_appends_below_min` |

Each test checks that the property holds with the mutant off, fails with it on, and holds again after `fail::remove`.

## Provenance (D-P12-26)

| File | Provenance |
| --- | --- |
| replica/mod.rs, tracker.rs, isr.rs, watermark.rs, divergence.rs, failover.rs | Written from the Phase 12 spec (KIP-497/595/279/101 semantics, D-P12-5/6/7). No port header, no VENDORED.md row. |
| tests/replica_{unit,model,mutants}.rs | Written for this plan |

VENDORED.md is unchanged (it is listed in `files_modified`, but only genuine ports get a row there).

## Verification

- Plan `<automated>` block: green. `replica_unit` 42 passed, `replica_model` 2 passed, `replica_mutants` 6 passed (single-threaded), the release 2,000-case model passed, the grep for clocks and tokio is empty, and clippy `-D warnings` is clean with and without `failpoints`.
- `cargo test -p ostro-log` passed in full, both with and without `--features failpoints --test-threads=1`.
- `cargo check --workspace --all-targets --locked` passed after rebasing onto main (12-03, 12-05, 10-02 landed in between). `cargo fmt --all --check` and `scripts/check-vendored.sh` pass.
- The full `scripts/ci-local.sh --no-musl` was **not** run in this plan. Four executors were building in parallel and disk is limited, and the change is confined to `ostro-log` `replica/*` plus three new test files. The next gate run covers it.

## Deviations from Plan

1. **[Rule 3] `shrink_before_commit` placement.** The site is `isr::maximal`, which `next_hw` calls. `mutants.rs` names `replica/watermark.rs` as the place. A second wrapper would have split the decision, so I kept one site. The bite test checks both `maximal` and `next_hw`.
2. **Model: SnapshotRequired is handled, not asserted unreachable.** Some histories reach it without any trimming: a new leader whose first epoch is newer than a follower's uncommitted epoch-1 record. `EpochHistory` answers UNDEFINED there, and D-P12-6 says SnapshotRequired. The model asserts this happens only when the follower's oldest epoch predates the leader's, then installs the leader's state at its HW. Kafka's cache would answer `(requested, first higher start)` in this case. Ostro deliberately does not; see the note under Open items.
3. A few extra helper methods beyond the interface list: `IsrState::{new, proposed, propose, commit, reject, set_published}`, `ReplicaTracker::{self_id, replicas, fetch_offset, last_fetch_ms, on_truncate}`, and the `FollowerStatusRow` alias. None of the listed names changed.

## Open items for later plans

- 12-11 (follower/fetch): `check` returns SnapshotRequired when a follower only holds records of an epoch older than the leader's first epoch, even at `log_start == 0`. It is safe, but an empty-ish new leader can trigger a snapshot install. If that turns out to be too costly, a narrow refinement is possible: at `log_start == 0`, answer `Diverging{e, first_start}`. That needs a D-P12-6 decision, so I did not do it here.
- 12-08: feed `ReplicaTracker::on_fetch` the durable end sampled **before** `read_replica` (W3). Call `on_truncate` only after a diverging answer, and record progress only after `check` returns Ok.
- 12-10: `plan` takes `resigned` but leaves the resigned node inside `isr ∩ live`. ResignLeader apply already removes it when `|isr| > 1`.

## Self-Check: PASSED
