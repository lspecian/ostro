---
phase: 13-cluster-membership-routing-upgrades
plan: 05
subsystem: vendored raft driver + ostro-meta + ostro-cluster MetaPeer
tags: [raft, fatal-apply, fail-stop, read-index, lease, transfer-leader, joint-consensus, learner, meta-snapshot, vendored]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 contracts (MetaCommand 8 AlterReplicas, compat registry, MetaHandle seam notes)"
  - phase: 12-replicated-partition-logs
    provides: "12-05 MetaHandle multi-voter + MetaPeer; 12-10 sample_live over learners + Ostro remedy text"
provides:
  - "FatalApply: StateMachine::apply -> Result<Vec<u8>, FatalApply>; halt before applied advances; ProposeError::Fatal; RaftStatus.fatal"
  - "MetaSm fail-stop on undecodable / unknown-variant / newer-format entries and (level emulation) AlterReplicas above level_max; DecodeError/MetaError::NewerFormat"
  - "RaftHandle::{transfer_leader, read_index, lease_anchor, leader_lease_anchor, snapshot_now}; RaftStatus membership + per-peer progress"
  - "MetaHandle::{read_index_barrier(_until), transfer_leader, propose_conf_change, conf_state, raft_detail, lease_anchor, start_joiner, start_with_options}"
  - "MetaPeer: 256 MiB default limit, configuration-member authz, on-the-fly peers, ostro_cluster_meta_snapshot_bytes, RaftSlot::meta_lease_anchor"
affects: [13-02, 13-04, 13-09, 13-10, 13-11, 13-13, 13-16, 13-18, 13-19]
tech-stack:
  added: []
  patterns:
    - "lease anchor = send instant of the newest quorum-confirmed ReadIndex round (leader: own heartbeat round; follower: round trip through the leader)"
    - "per-instance level emulation through MetaOptions (ostro-meta has no ostro-types dependency)"
key-files:
  created:
    - vendor/ostro-raft-driver/src/membership_tests.rs
    - crates/ostro-meta/tests/newer_command.rs
    - crates/ostro-meta/tests/membership_handle.rs
    - crates/ostro-cluster/tests/meta_peer_large.rs
  modified:
    - vendor/ostro-raft-driver/src/{driver,log_store,lib}.rs
    - VENDORED.md
    - crates/ostro-meta/src/{sm,command,handle,lib}.rs
    - crates/ostro-meta/tests/{single_voter,meta_versions,meta_records,meta_golden,meta_policies}.rs
    - crates/ostro-cluster/src/meta_peer.rs
key-decisions:
  - "Follower lease anchor via a ReadIndex round trip through the leader instead of a per-RaftBatch RaftAck: MetaPeer.Raft is client-streaming with one RaftAck at stream end, and a proto change is 13-03-only (STOP rule). The ReadIndex anchor is strictly stronger (it also proves the leader had a quorum)."
  - "Level emulation is per instance (MetaOptions.level_max), not a cargo feature of ostro-meta (no Cargo changes allowed; ostro-meta does not depend on ostro-types). required_level() mirrors compat::feature_for_meta_variant; a meta_peer unit test pins them together."
  - "MetaHandle::raft_status() keeps its Copy MetaRaftStatus (08-16 gauges); the full status is raft_detail()."
  - "Log store refuses to hand a peer a snapshot whose ConfState lacks it and requests a fresh one (raft-rs never restores such a snapshot; a new learner could never catch up)."
requirements-completed: []
duration: "see Metrics"
completed: 2026-11-24
---

# Phase 13 Plan 05: vendored driver FatalApply / ReadIndex / transfer / progress, MetaHandle surface, MetaPeer large snapshots Summary

**The vendored driver now fails stop on a refused entry (`FatalApply`, halting before `applied` advances), serves linearizable `read_index`, hands leadership over with `transfer_leader`, publishes joint-consensus state and per-peer progress, and keeps a quorum-confirmed metadata lease anchor. MetaSm halts on newer or unknown metadata. `MetaHandle` exposes the barrier / transfer / conf-change / joiner surface. MetaPeer carries 256 MiB snapshots to learners it learns about at runtime.**

## Signatures as landed

```rust
// vendor/ostro-raft-driver (re-exported by ostro_meta)
pub struct FatalApply(pub String);                       // Display, Error
pub trait StateMachine { fn apply(&mut self, index: u64, term: u64, command: &[u8]) -> Result<Vec<u8>, FatalApply>; .. }
pub enum ProposeError { NotLeader { leader }, Dropped, OutcomeUnknown, Stopped, Fatal(String) /* new */ }
pub struct PeerProgress { pub id: u64, pub matched: u64, pub next_idx: u64, pub pending_snapshot: u64, pub recent_active: bool }
pub struct RaftStatus {
    id, term, leader_id, is_leader, commit, applied,
    voters /* sorted now */, learners /* sorted */, failed,
    // new (13-05):
    voters_outgoing: Vec<u64>, learners_next: Vec<u64>, auto_leave: bool,
    pending_conf: bool,              // pending_conf_index > applied
    progress: Vec<PeerProgress>,     // leader only, sorted by id; empty elsewhere
    fatal: Option<String>,           // FatalApply message (failed is set too)
}
impl RaftHandle {
    pub async fn transfer_leader(&self, to: u64) -> Result<(), ProposeError>; // NotLeader on a follower, Dropped for a non-voter
    pub async fn read_index(&self) -> Result<u64, ProposeError>;            // Safe ReadIndex; Dropped on leader/term change or after 2 election timeouts; NotLeader{None} without a leader
    pub fn lease_anchor(&self) -> Option<std::time::Instant>;               // any role (see "Lease anchor")
    pub fn leader_lease_anchor(&self) -> Option<std::time::Instant>;        // None unless leader
    pub fn snapshot_now(&self);                                             // snapshot + compact at the applied index
    // wait_applied now returns Err(ProposeError::Fatal(_)) after a FatalApply
}

// ostro_meta
pub enum DecodeError { Empty, UnknownFormat(u8), NewerFormat { found: u8, supported: u8 } /* new */, PreFreezeFormat { found }, Malformed(String) }
pub enum MetaError { Start, Propose, Decode, Timeout, Rejected, NewerFormat { found: u8, supported: u8 } /* new */, NotLeader { hint: Option<u64> } /* new */ }
pub mod sm { pub fn required_level(cmd: &MetaCommand) -> u32; pub const NEWER_ENTRY: &str; }
MetaSm::with_level_max(level_max: u32) -> (MetaSm, MetaView);   MetaView::newer_format() -> Option<u8>
pub struct handle::MetaOptions { pub sync: bool /* true */, pub level_max: Option<u32> /* None */ }
impl MetaHandle {
    pub async fn start_with_options(dir, MetaStart, MetaOptions) -> Result<MetaHandle, MetaError>;
    pub async fn start_joiner(dir, MetaStart, MetaOptions) -> Result<MetaHandle, MetaError>; // blank dir: no bootstrap conf; voters = known members
    pub async fn read_index_barrier(&self) -> Result<u64, MetaError>;                 // 10 s bound
    pub async fn read_index_barrier_until(&self, deadline: tokio::time::Instant) -> Result<u64, MetaError>;
    pub async fn transfer_leader(&self, to: u64) -> Result<(), MetaError>;             // waits up to 10 s for `to` to lead
    pub async fn propose_conf_change(&self, cc: ConfChangeV2) -> Result<Applied, MetaError>; // leader only; empty result -> Rejected; returns after conf_state shows it
    pub fn conf_state(&self) -> ConfState;
    pub fn raft_detail(&self) -> RaftStatus;            // raft_status() keeps its Copy MetaRaftStatus
    pub fn lease_anchor(&self) -> Option<std::time::Instant>;
}

// ostro_cluster::meta_peer
pub const DEFAULT_META_MAX_MESSAGE: usize = 256 * 1024 * 1024;   // was 64 MiB
pub const META_SNAPSHOT_BYTES_GAUGE: &str = "ostro_cluster_meta_snapshot_bytes";
impl RaftSlot { pub fn meta_lease_anchor(&self) -> Option<std::time::Instant>; }
```

## Fail-stop (FatalApply, D-P13-6 meta half)

- Driver: `apply` → `Err(FatalApply)` returns `Fatal` from `apply_entries` before `self.applied = entry.index`; the cycle's entries/HardState are not persisted (the commit index is relearned from the leader), pending proposals get `ProposeError::Fatal`, outstanding reads fail, `RaftStatus.{fatal, failed}` are published and the thread exits. Restart replays the entry and halts again (`fatal_apply_stops_before_applied`).
- MetaSm: `DecodeError::NewerFormat` (format byte > 2, message "…this cluster was finalized by a newer Ostro; upgrade this binary"), unknown variant, any other undecodable non-empty entry → `FatalApply("metadata entry written by a newer Ostro; upgrade this binary (entry N: …)")`. Empty bytes keep `Invalid` (the driver never passes them); the pre-freeze byte 1 keeps its D20 path (`Invalid` + `pre_freeze_format`). Level emulation: `MetaOptions.level_max = Some(1)` halts on `AlterReplicas` ("needs feature level 2"). `MetaHandle` start/propose map a newer-format halt to `MetaError::NewerFormat`.
- What a halt looks like in a group: the leader sends the commit before it applies, so followers normally halt at the same index. A voter that never learns the commit (e.g. the last survivor after two halts) cannot apply the entry either and stays without a quorum; nobody ever applies past it (`three_voters_unknown_variant_halts_a_quorum_and_nobody_applies_it`).

## Lease anchor (D-P13-15, plan checks W2/NB2) and the 12-18 stale-read follow-up

- Mechanism: the driver issues internal `ReadIndex` "lease rounds" (context byte 2, at most one in flight, one per heartbeat interval). The anchor is the **send instant** of the newest round that came back resolved (`fetch_max`, monotonic). It is reset to `None` on any term, leader or role change, so it is gone after a check-quorum step-down. It stops advancing as soon as rounds stop resolving. A one-voter leader returns `now` with no bookkeeping.
- On the leader a round is a heartbeat broadcast acknowledged by a quorum of voters (raft-rs read-only queue). Rounds run continuously on a multi-voter leader.
- On a follower or learner (**deviation from the plan text**) a round is `MsgReadIndex` → leader quorum round → `MsgReadIndexResp`. The meta leader stepped (heard) the follower's message after the anchor, and it still had a quorum at that point. Rounds run only while the anchor was asked for within the last 10 s. The first ask starts them and may return `None`, so 13-13 should poll at startup or keep asking.
- Measured (in-process, election 500-1000 ms): a cut-off 3-voter leader's anchor froze at the cut (never passed it) and went `None` when check-quorum stepped it down, about 1.0 s later. A read on an isolated leader failed with `Dropped` after about 0.99 s.
- **12-18 interaction:** a cut-off old meta leader serves **plain** local reads (`MetaView::read`) until check-quorum steps it down (about 2.6 s in 12-18). `read_index_barrier` and `read_index` cannot succeed there because they need the quorum acknowledgement, so they fail fast instead of returning stale data. Its `leader_lease_anchor` freezes at the last pre-cut round, so `now − anchor` grows from the cut, not from the step-down. 13-13 should gate consistency-sensitive meta reads on the barrier (or the lease anchor) rather than on `is_leader` / `leader_contact_age`. The driver's check-quorum timing is unchanged.

## Tests

| Binary | Tests |
|--------|-------|
| `ostro-raft-driver` lib (`membership_tests`, 14) | `learner_add_catch_up_promote`, `replace_via_joint_auto`, `leave_joint_after_leader_kill`, `read_index_on_follower`, `read_index_fails_fast_on_leader_loss`, `transfer_leader_moves_leadership`, `progress_reports_learner_matched`, `fatal_apply_stops_before_applied`, `fatal_apply_halts_every_voter_at_the_same_index`, `lease_one_voter_is_now`, `lease_anchor_is_the_send_instant`, `lease_none_after_leader_change`, `lease_cut_off_leader_stops_and_steps_down`, `snapshot_now_compacts_and_a_lagging_voter_installs_it`; `a_wiped_voter_stops_with_the_remedy_instead_of_panicking` asserts the new remedy (41 lib tests total) |
| `ostro-meta --test newer_command` (6) | `known_variants_decode_identically`, `one_voter_newer_format_halts_and_stays_halted`, `one_voter_unknown_variant_halts_not_invalid`, `one_voter_level_one_halts_on_alter_replicas`, `three_voters_level_one_voter_halts_others_continue`, `three_voters_unknown_variant_halts_a_quorum_and_nobody_applies_it` |
| `ostro-meta --test membership_handle` (5) | `barrier_on_another_voter_observes_the_write` (every ordered voter pair), `transfer_moves_leadership_and_proposals_continue`, `learner_added_by_conf_change_catches_up_and_shows_in_progress` (joiner via `start_joiner`), `lease_anchor_on_leader_and_follower`, `one_voter_barrier_and_lease` |
| `ostro-cluster --test meta_peer_large` (2, `--test-threads=1`) | `learner_catches_up_from_a_meta_snapshot_above_4_mib` (5,243,338-byte meta snapshot; caught up in ~0.4 s from one snapshot), `with_a_4_mib_limit_the_learner_never_catches_up` |
| `ostro-cluster` lib | `meta_peer::tests::meta_required_level_matches_the_compat_registry` |

## Task Commits

1. `4691f8e` feat(13-05): raft driver transfer/read_index/progress (driver, log store, VENDORED.md, MetaSm/command fail-stop, the whole MetaHandle surface, newer_command, implementor arms)
2. `489ab0d` feat(13-05): MetaHandle membership + read_index surface (membership_handle tests)
3. `20705f3` feat(13-05): MetaPeer large snapshot support (meta_peer.rs, meta_peer_large, driver `snapshot_now` without a Ready)

## Verification

- Driver: `cargo test -p ostro-raft-driver --locked` 41 passed (membership_tests 14), repeated 5x clean; `scripts/check-vendored.sh` all PASS; no `CHRONIK_CLUSTER_JOIN` in driver.rs.
- `cargo test -p ostro-meta --locked` all binaries ok (newer_command 6, membership_handle 5, rerun 3-6x clean after the restart-assertion fix).
- `cargo test -p ostro-cluster --locked --test meta_peer_large -- --test-threads=1` 2 passed (x3); `--test meta_peer` 3 passed; lib 28 passed.
- `cargo clippy -p ostro-cluster -p ostro-meta --all-targets -- -D warnings` clean; `cargo fmt --all -- --check` clean; `cargo build --workspace --all-targets --locked` warning-free.
- **Plan-level `nice -n 15 bash scripts/ci-local.sh --no-musl --no-polyglot` at `20705f3`** (slot-2): every step PASS (fmt, clippy, build, sdk, guests, order-saga, cargo test 5657 s, clients go, sleepers, failpoint sync + suites 6400 s, replication mutant matrix, server campaign smoke 20 kills, phase 7 legs, deny, check-vendored, fixtures, forbidden refs, workflow validation) **except `one log path + P12 grep gates`**: the clock gate flags `SystemClock` in `crates/ostro-cluster/src/compat.rs:40,391` and `crates/ostro-cluster/src/upgrade.rs:30,201`, both 13-04 files (landed in 88f4c78/08bd9be, still present at main a94dd9b). Not caused by 13-05 and out of scope (13-04 owns those files); reported to the orchestrator, no gate was changed. External PG/MySQL/S3 SKIP (no env), polyglot SKIP (flag), musl SKIP (flag).

## Deviations from Plan

### Auto-fixed / adapted

1. **[Rule 3 - Blocked by the proto rule] Follower lease anchor.** The plan asks for the send instant of the last `RaftBatch` whose `RaftAck` succeeded. `MetaPeer.Raft` is client-streaming and returns one `RaftAck` when the stream ends, so there is no per-batch ack. Adding one is a proto change, which is 13-03-only (STOP + re-plan). I implemented the anchor as the send instant of a `ReadIndex` round trip through the leader, which is a stronger guarantee: the leader stepped the follower's message after the anchor and had a quorum at that point. The API is the same (`RaftSlot::meta_lease_anchor`, `RaftHandle::lease_anchor`). The leader-side anchor uses the same rounds (a quorum-acked heartbeat broadcast) instead of hand-counting `MsgHeartbeatResponse`, so responses always map to the right round. **Flag for the D-P13-15 user review and 13-13.**
2. **[Rule 3] Level emulation without a cargo feature.** ostro-meta has no `level-emulation` feature and does not depend on ostro-types, and Cargo changes are 13-01-only. Emulation is set per instance through `MetaOptions.level_max` (`MetaHandle::start_with_options`). A three-voter test needs per-node levels anyway. 13-02/13-04 should pass `Some(ostro_types::compat::level_max())` under `level-emulation` (handover).
3. **[Rule 1 - Bug] New learners could never catch up.** raft-rs refuses to restore a snapshot whose `ConfState` lacks it, and the stored snapshot (the bootstrap one at index 1) predates the learner. `RaftLogStore::snapshot(_, to)` now answers `SnapshotTemporarilyUnavailable` and requests a fresh snapshot in that case (vendored `log_store.rs`, VENDORED.md P3). This file was not in `files_modified`.
4. **[Rule 2] Joiner start.** `MetaHandle::start_with` refuses a node outside `voters` and bootstraps a configuration, so I added `MetaHandle::start_joiner` (a blank dir gets no bootstrap configuration). The tests and 13-09 need it.
5. **[Rule 2] MetaPeer membership authz and dynamic peers.** The service only accepted static voters and the outbox only knew static peers, so a learner could not talk to the leader. The service now also accepts members of the current Raft configuration, and the outbox opens a queue on the fly for an unknown target (it uses the runtime captured at construction).
6. **Naming:** `MetaHandle::raft_status()` already exists (a `Copy` `MetaRaftStatus` for the 08-16 gauges) and the plan says to keep existing methods unchanged, so the full status is `raft_detail()`. `RaftStatus.failed` is kept and set together with the new `fatal`. `RaftStatus` "fatal watch" = `RaftHandle::subscribe()`.
7. **Plan said `MetaError::NewerFormat` in command.rs:** command.rs gets `DecodeError::NewerFormat {found, supported}` and handle.rs gets `MetaError::NewerFormat {found, supported}`. The meta_versions assertion for byte 3 changed from `UnknownFormat(3)` to `NewerFormat` (a planned behaviour change; byte 0 still `UnknownFormat`).
8. **Planned behaviour change in an existing test:** `meta_records::undecodable_command_is_invalid_and_bad_snapshot_is_rejected` became `…_halts_and_…`. `[2,0,0]` now halts (`FatalApply`) per D-P13-6; empty bytes and the pre-freeze byte still answer `Invalid`. This is not an oracle relaxation: the plan reverses that behaviour.
9. **Implementor/caller arms (allowed, NW5):** `single_voter.rs` CounterSm `Ok(..)`; the driver's test `Names` (also refuses `halt:`); `.unwrap()`/`.expect()` on direct `MetaSm::apply` calls in `meta_golden.rs`, `meta_policies.rs`, `meta_records.rs`. `elect_leader.rs` compiled unchanged (it compares `Result`s).
10. **Task split:** the MetaHandle methods landed in commit 1 (newer_command needed `MetaOptions`/`MetaError::NewerFormat` from the same file); commit 2 holds the membership_handle tests.
11. **`leave_joint_after_leader_kill` uses `ConfChangeTransition::Explicit`.** This makes the state "the leader died before its auto-leave" deterministic: raft-rs never auto-leaves on a later leader (raft.rs TODO), and killing the leader within the propose→apply window is racy. With Auto, `replace_via_joint_auto` shows the automatic leave.
12. **`snapshot_now`** (driver, new): test hook used by meta_peer_large to compact the leader's log (`snapshot_every` is not reachable through `MetaStart`). It also fires when raft has no Ready.
13. **Worktree/target:** scratchpad worktree, `CARGO_TARGET_DIR=target/slot-2`, nice 15, 4 jobs, instead of `/tmp/ostro-wt-13-05`.

### Not done here (owned elsewhere)

- The `[cluster] meta_max_message_bytes` config key: 13-02 added it (config.rs, >= 4 MiB) and wires it into `into_server` and `OutboxConfig.max_message` in cluster.rs (main 1f46a38); `DEFAULT_META_MAX_MESSAGE` is now 256 MiB.
- No ci.yml change is needed (`meta_peer_large` runs in the normal `cargo test`; each test binds its own loopback ports).

## Next Phase Readiness (handover)

- **13-02:** `meta_max_message_bytes` is wired (check `GrpcForwarder::with_limits` uses it too). Under `level-emulation`, start meta with `MetaHandle::start_with_options(dir, start, MetaOptions { level_max: Some(ostro_types::compat::level_max()), ..Default::default() })`.
- **13-04:** a newer meta format now surfaces as `MetaError::NewerFormat { found, supported }` from `MetaHandle::start*`/`propose` and as `MetaView::newer_format()`, which can feed `CompatError::NewerMetaFormat`. `RaftStatus.fatal` is the running-fence signal for meta.
- **13-09:** use `MetaHandle::start_joiner` for a joiner (blank meta dir). Add it with `propose_conf_change(AddLearnerNode)` on the leader. Promotion readiness comes from `raft_detail().progress` (`matched`, `recent_active`, `pending_snapshot`). `conf_state()` gives `voters_outgoing`/`auto_leave`; leave a stuck joint configuration with an empty `ConfChangeV2`. The leader must have the learner's address in the `AddressBook` (the outbox resolves through the transport). MetaPeer authorizes it once it is in the configuration.
- **13-11 / controlled shutdown:** `MetaHandle::transfer_leader(to)` waits until `to` leads (10 s, `Timeout` otherwise; raft aborts a transfer after an election timeout).
- **13-13:** use `RaftSlot::meta_lease_anchor()` / `MetaHandle::lease_anchor()` (monotonic `std::time::Instant`) for the partition-read lease and `read_index_barrier()` for meta reads. Call the anchor once at startup on followers to start the lease rounds. The 12-18 stale-read window is closed for barrier reads (see above).
- **13-18/19:** the driver tests use in-process timing (heartbeat 20 ms, election 500-1000 ms). The WSL2 numbers above are advisory.

## Self-Check

PASSED: created files exist at 20705f3 (membership_tests.rs, newer_command.rs, membership_handle.rs, meta_peer_large.rs); commits 4691f8e, 489ab0d, 20705f3 are on main.
