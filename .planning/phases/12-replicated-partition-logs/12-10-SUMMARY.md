---
phase: 12-replicated-partition-logs
plan: 10
subsystem: cluster controller + liveness
tags: [controller, liveness, election, isr, bootstrap, raft-driver, vendored]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-04 failover::plan (pure ISR-only planner); 12-05 meta apply of ElectLeader/AlterIsr/ResignLeader, MetaHandle::start_with, SuspectSink contract"
provides:
  - "vendored RaftHandle::sample_live over voters ∪ learners; Ostro wiped-voter remedy text (VENDORED.md P3)"
  - "ostro_cluster::liveness::{Liveness, LivenessConfig, LivenessHandle, NodeState, LiveSource, LiveFuture, SuspectSink}"
  - "ostro_cluster::controller::{Controller, ControllerConfig, ProposalRecord, FreshTerm, request_fresh_term, resign}"
  - "ostro_cluster::bootstrap::{placement, bootstrap_record, bootstrap_command}"
  - "ostro_cluster::members::upsert_self"
affects: [12-13, 12-14, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "controller acts only while meta leader; liveness re-seeded on every takeover"
    - "suspect confirmation = a sample taken confirm_window after the report with within = confirm_window (heard since the report?)"
    - "every controller proposal published with the assignment it was planned from (election-oracle audit in tests)"
key-files:
  created:
    - crates/ostro-cluster/tests/liveness.rs
    - crates/ostro-cluster/tests/controller.rs
    - crates/ostro-cluster/tests/support_controller/mod.rs
  modified:
    - vendor/ostro-raft-driver/src/driver.rs
    - VENDORED.md
    - crates/ostro-cluster/src/{liveness,controller,bootstrap,members}.rs
key-decisions:
  - "Regular liveness samples use within = session_timeout / 3, not session_timeout: a node present in a within-T sample can be dead for up to T, so dead-after-T-unseen would detect in up to 2T; with T/3 detection is <= T + T/3 + tick"
  - "A suspect is confirmed by a sample taken confirm_window (T/4) after the report with within = confirm_window: absent = not heard since the report = dead; present = cleared. A confirming sample taken right after the report would always still contain a kill -9'd node"
  - "Seeding: on takeover every node restarts its grace period; one absent from every sample of the first seed_window (T/3) becomes suspect and goes through the same confirmation. No node can be declared dead by the first sample of a new controller"
  - "A resigned leader is excluded from the planner for resign_hold (10 s) after its ResignLeader (ElectionInfo reason Resign, prev_leader); afterwards it may be re-elected if live and still in the ISR, so a sole-member resign is offline (leader 0) but not forever"
  - "Bootstrap keeps the epoch-0 record's isr_version (0 in practice) instead of writing a literal 0, so a CAS token never regresses"
  - "request_fresh_term returns AlreadyFresh when meta already names self leader at epoch >= min_epoch (idempotent under concurrent callers)"
requirements-completed: []
duration: ~1 h 30 min
completed: 2026-11-22
---

# Phase 12 Plan 10: Controller, liveness, bootstrap placement Summary

**The metadata leader now runs a partition controller. It places every epoch-0 partition once, for any voter count, through a `Put(Partition)` CAS from version 0. It turns the pure 12-04 planner's output into `ElectLeader`/`AlterIsr`, electing only from the published ISR and never proposing the legacy fence command. Liveness accumulates the leader's `sample_live` samples, which now include learners. A suspect report is confirmed by a targeted sample, and a new controller seeds suspicion instead of trusting its first sample. Each node registers its own `Member` addresses.**

## Performance

- Tasks: 3 of 3
- Commits: 21008ee (Task 1), 522ff54 (Task 2), 65db628 (Task 3), plus this docs commit
- Measured on the in-memory 3-voter group (`tests/controller.rs`, 50 ms meta tick, election 10):
  - liveness marks a node dead → `ElectLeader`/`AlterIsr` applied for every affected partition: **31–43 ms** (3 runs; the test asserts < 500 ms)
  - real liveness (`session_timeout` 1.5 s) after the meta leader, which also leads partitions, was stopped:
    - new controller elected 0.45–0.63 s after the stop
    - partitions re-led 0.90–1.05 s after takeover, which is seed 0.5 s + confirm 0.375 s + ticks
    - 1.35–1.60 s from stop to re-led

## Controller / liveness parameters

| Parameter | Default | Derived from |
|-----------|---------|--------------|
| controller tick | 100 ms | `ControllerConfig::tick` (tokio interval) |
| liveness tick | T/6 = 500 ms | `LivenessConfig::tick` (informational; the controller polls liveness every controller tick, and `wake()` fires early for due confirmations) |
| regular sample `within` | T/3 = 1 s | `sample_window` |
| dead after unseen | T = 3 s | `session_timeout` |
| confirm delay = confirm `within` | T/4 = 750 ms | `confirm_window` |
| seeding window | T/3 = 1 s | `seed_window` |
| resigned-leader hold | 10 s | `ControllerConfig::resign_hold` |
| proposals in flight | 32, one per partition | `max_in_flight` |

Worst-case detection with defaults:

- unseen: ≤ T + T/3 + tick ≈ 4.4 s
- confirmed suspect: report + 750 ms + tick
- new controller: seed 1 s + 750 ms

## `recent_active` / first-sample finding (research Pitfall 11, DECISIONS risk row)

The driver's `sample_live` does not use raft-rs `recent_active`. It uses its own `last_heard` map, which **survives role changes**. Recorded output of `sample_live_includes_learners`, from the first samples on a newly elected leader:

| run | wide (`within` 10 s) | narrow (`within` 300 ms) |
|-----|----------------------|--------------------------|
| 1 | `[1, 2, 3, 4]` | `[2, 3, 4]` |
| 2 | `[1, 2, 3]` | `[2, 3, 4]` |
| 3 | `[1, 2, 3]` | `[1, 2, 4]` |

What the samples show:

- **The dead old leader appears in a wide window.** The new leader heard from it as a follower (heartbeats), and that contact is still within `within`. The test asserts it is present in the wide sample and absent from the narrow one.
- **The surviving voter is always present** (it just voted).
- **A learner appears only once it has answered the new leader's heartbeats.** It is sometimes absent from the first wide sample but present a moment later.

So the first sample of a new leader both over-reports (stale follower-era contacts) and under-reports (peers that only talk to the leader). The controller handles both:

- it uses narrow regular windows;
- it never kills on a first sample (fresh grace period on takeover);
- it seeds suspicion only after `seed_window` and confirms with a targeted sample.

## Signatures for 12-13 / 12-14

```rust
// ostro_cluster::liveness
pub trait SuspectSink: Send + Sync { fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>); } // unchanged (12-02)
pub struct LivenessConfig { pub session_timeout, pub tick, pub sample_window, pub confirm_window, pub seed_window: Duration }
impl LivenessConfig { pub fn new(session_timeout: Duration) -> Self; }
pub enum NodeState { Live, Suspect, Dead }
pub struct Liveness;
impl Liveness {
    pub fn new(clock: Arc<dyn ostro_partition::Clock>, session_timeout: Duration) -> Self;
    pub fn with_config(clock: Arc<dyn Clock>, cfg: LivenessConfig) -> Self;
    pub fn track(&mut self, nodes: impl IntoIterator<Item = u64>);
    pub fn on_became_controller(&mut self);
    pub fn observe(&mut self, sample: &[u64]);          // regular sample (within = sample_window)
    pub fn report_suspect(&mut self, reporter: u64, suspect: u64);
    pub fn confirm_due(&self) -> Option<Duration>;      // Some(confirm_window) when due
    pub fn next_confirm_ms(&self) -> Option<u64>;
    pub fn confirm(&mut self, sample: &[u64]);          // within = confirm_window
    pub fn state(&self, node: u64) -> NodeState;
    pub fn live_set(&self, universe: &BTreeSet<u64>) -> BTreeSet<u64>;
}
#[derive(Clone)] pub struct LivenessHandle;            // Arc<Mutex<Liveness>> + Notify
impl LivenessHandle { pub fn new(l: Liveness) -> Self; pub fn lock(&self) -> MutexGuard<'_, Liveness>; pub fn report_suspect(&self, reporter: u64, suspect: u64); }
impl SuspectSink for LivenessHandle;                    // 12-14: pass Arc::new(handle.clone()) to MetaPeerService::new
pub trait LiveSource: Send + Sync + 'static {           // implemented by LivenessHandle
    fn on_became_controller(&self);
    fn poll<'a>(&'a self, raft: &'a RaftHandle, universe: &'a BTreeSet<u64>) -> LiveFuture<'a, BTreeSet<u64>>;
    fn wake(&self) -> LiveFuture<'_, ()>;
}

// ostro_cluster::controller
pub struct ControllerConfig { pub tick: Duration, pub rf: usize, pub min_insync: u8, pub max_in_flight: usize, pub resign_hold: Duration }
impl ControllerConfig { pub fn new(rf: usize, min_insync: u8) -> Self; }   // [replication] rf / min_insync seeds
pub struct ProposalRecord { pub proposer: u64, pub cmd: MetaCommand, pub before: PartitionAssignment, pub result: Result<MetaResult, String> }
pub struct Controller;
impl Controller {
    pub fn new(meta: Arc<MetaHandle>, live: Arc<dyn LiveSource>, cfg: ControllerConfig, clock: Arc<dyn Clock>) -> Self;
    pub fn subscribe(&self) -> broadcast::Receiver<ProposalRecord>;
    pub async fn run(self, shutdown: CancellationToken);   // spawn on every node; acts only as meta leader
}
pub enum FreshTerm { Elected { epoch: u32 }, AlreadyFresh { epoch: u32 }, Unplaced, NotLeader { leader: u64 }, NotInIsr, UnknownPartition }
pub async fn request_fresh_term(meta: &MetaHandle, self_id: u64, partition: u16, min_epoch: u32, clock: &dyn Clock) -> Result<FreshTerm, MetaError>;
pub async fn resign(meta: &MetaHandle, self_id: u64, partition: u16, epoch: u32, clock: &dyn Clock) -> Result<MetaResult, MetaError>;

// ostro_cluster::bootstrap
pub fn placement(voters_sorted: &[u64], partitions: u16, rf: usize) -> Vec<Vec<u64>>;
pub fn bootstrap_record(current: &PartitionAssignment, replicas: Vec<u64>, min_insync: u8, now_ms: u64) -> Option<PartitionAssignment>;
pub fn bootstrap_command(record: PartitionAssignment) -> MetaCommand;   // Put CAS Some(0)

// ostro_cluster::members
pub async fn upsert_self(meta: &MetaHandle, me: ostro_meta::Member) -> Result<bool, MetaError>;  // true = written
```

12-14 wiring, after 12-05's steps 1-6 and `InitCluster`:

1. `members::upsert_self(&meta, Member{node_id, addr, cluster_addr, repl_addr, ..})`.
2. `let live = LivenessHandle::new(Liveness::new(clock.clone(), session_timeout))`, passing `Arc::new(live.clone())` as the MetaPeer `SuspectSink`. The service is built before meta, so build the handle first.
3. `tokio::spawn(Controller::new(meta.clone(), Arc::new(live), ControllerConfig::new(rf, min_insync), clock).run(token))`.
4. The 12-13 `AssignmentSource` maps:
   - `request_term(p, min_epoch)` → `request_fresh_term(&meta, self, p.0, min_epoch.0, &*clock)`
   - `resign(p, e)` → `controller::resign(&meta, self, p.0, e.0, &*clock)`

Production needs `Arc<MetaHandle>`.

## Tests

| Suite | Result |
|-------|--------|
| `ostro-raft-driver` (incl. new `sample_live_includes_learners`, updated wiped-voter asserts) | 27 passed; learner test 3/3 runs |
| `ostro-cluster --test liveness` | 8 passed |
| `ostro-cluster --test controller -- --test-threads=1` | 8 passed, 3 runs (~15 s per run) |
| `ostro-cluster --features failpoints -- --test-threads=1` (lib 13, controller 9 incl. `elect::before_propose`, liveness 8, meta_peer 3) | green |
| `ostro-meta` full suite (driver change) | green |
| `cargo clippy -p ostro-cluster -p ostro-raft-driver --all-targets (± failpoints) -- -D warnings` | clean |
| `cargo fmt --all --check` | clean |
| `cargo check --workspace --all-targets --locked` after rebasing onto d4869f2 (12-03) | clean |
| `scripts/check-vendored.sh` | all 5 checks pass |
| `! grep -rn "CHRONIK_CLUSTER_JOIN\|remove-node" vendor/ostro-raft-driver/src`, `! grep SetPartitionLeader controller.rs` | hold |

What the controller tests cover:

- **Placement:** a fresh 3-node cluster places every partition once with RF 3, epoch 1 and ISR [leader]. Placement also completes across a meta-leader stop mid-bootstrap: every partition ends at stored version 1, and the records show at most one applied `Put` per partition.
- **Fresh terms:**
  - At epoch 0 they propose nothing: the applied index is unchanged.
  - A single node gets `[self]`, and takes exactly one `ElectLeader{epoch+1}` per partition per restart (`isr_version` +1). A repeated call answers `AlreadyFresh`.
  - A non-leader proposes nothing, and neither does a node outside the ISR.
- **No re-placement:** a pre-existing epoch-1 RF=1 data dir is not re-placed.
- **Failover:**
  - A dead leader is replaced by the first live ISR member in replicas order.
  - A live preferred replica outside the ISR is never chosen.
  - A dead follower only produces an `AlterIsr` shrink.
  - Exactly one applied `ElectLeader` per failed-over partition.
- **Offline:** when the whole ISR is dead the partition goes offline with leader 0, keeps its last member, and is proposed once. When that member returns it is re-elected.
- **Resign:** `ResignLeader` leads to an election of the next ISR member (reason Resign). A resign by the sole ISR member leaves the partition offline.
- **Seeding:** a new controller with real liveness elects at least `seed_window` after takeover, never on its first sample.
- **Oracle:** `audit()` checks every proposed command, applied or not:
  - only `ElectLeader`, `AlterIsr` and the epoch-0 `Put`;
  - every elected leader is in the published ISR;
  - an election only shrinks the ISR.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Regular sample window T/3 instead of `within = session_timeout`**
- **Found during:** Task 1 design.
- **Issue:** With `within = T`, a killed node stays in every sample for T. "Dead after T unseen" would then detect in up to 2T (6 s), which blows the 12-RESEARCH detection budget (3–3.5 s).
- **Fix:** `sample_window = T/3`, documented in the module and above.
- **Files:** `liveness.rs`. **Commit:** 21008ee

**2. [Rule 1 - Bug] Suspect confirmation needs a delayed, narrow sample**
- **Issue:** As literally specified, "an immediate sample; a suspect unseen in it is dead" never fires for a `kill -9`: the leader heard from the node moments earlier.
- **Fix:** The confirming sample is taken `confirm_window` after the report, with `within = confirm_window`.
- **Commit:** 21008ee

**3. [Rule 2 - Correctness] Resigned-leader exclusion is time-bounded (`resign_hold`)**
- **Issue:** The planner's `resigned` input has no natural end. A sole-member resign would otherwise stay offline forever, even after the node restarted healthy.
- **Fix:** The exclusion lasts `resign_hold`, 10 s by default.
- **Commit:** 522ff54

**4. Test adjustment: placement "exactly once" across a controller stop is asserted on the stored version**
- When a controller is stopped, its in-flight proposals are abandoned without a record, though one may still have applied.
- The test therefore asserts:
  - every partition's stored version is 1 (the `Put` CAS from 0 makes a second placement impossible);
  - the records show at most one applied `Put` per partition.
- **Commit:** 65db628

**5. Not in files_modified, by design**
- `lib.rs` keeps its 12-01 `dep_anchors` (`ostro_partition`/`ostro_meta` are now also used for real).
- RESERVATIONS §11 still lists `elect::before_propose` as "planned P12". It is now placed and tested; the phase gate (12-18) should flip the status.

## Open items

- I did not run the full `scripts/ci-local.sh --no-musl` (about 2 h 40 min), because of the shared build/disk budget. The targeted gate above is green. The phase gate (12-18) or the coordinator should include the full run.
- 12-14 must wire the following, in the order given under "12-14 wiring" above:
  - `LivenessHandle` into `MetaPeerService` as the `SuspectSink`
  - `Controller::run` on every node
  - `members::upsert_self`
- 12-13's `AssignmentSource` must call `request_fresh_term` and `resign`.
- `ControllerConfig::tick` uses a tokio interval. The wall-clock logic (windows, seeding, timestamps) uses the injected `Clock`.
- 12-17 tunes `session_timeout`. The liveness windows scale with it.

## Self-Check: PASSED

- Files exist at 65db628:
  - `crates/ostro-cluster/src/{liveness,controller,bootstrap,members}.rs`
  - `crates/ostro-cluster/tests/{liveness,controller}.rs`
  - `crates/ostro-cluster/tests/support_controller/mod.rs`
- Commits 21008ee, 522ff54 and 65db628 are on main.
