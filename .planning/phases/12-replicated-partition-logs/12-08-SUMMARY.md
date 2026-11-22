---
phase: 12-replicated-partition-logs
plan: 08
subsystem: partition-log
tags: [replication, isr, high-watermark, one-path, rf1, recovery, d-p12-4, f4, mutants, tdd]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 ReplicatedLog replica API + LogError::{NotEnoughReplicas, NotLeader} + mutant names; 12-04 ReplicaTracker, IsrState, next_hw, append_allowed, divergence check/follower_truncation_point"
provides:
  - "ostro_log::PartitionLog: the one ReplicatedLog implementation (leader, follower, install primitives); Rf1Log gone from crates/"
  - "Tracker-driven HW for every RF: WAL commit callback -> ReplicaCore::on_durable -> ReplicaTracker / follower min -> next_hw -> HwVector::publish_hw; hw_publishes counts every advance"
  - "F4 HW at open = applied (clamped into [log_start, LEO]); logs open unassigned; become_leader(e) needs e > latest_epoch() (B2)"
  - "read_replica (durable-capped), append_replica (verbatim, verified), set_follower_hw, record_progress, set_published_isr, reset_to, replica_status"
  - "ReplicaSet::solo(node); FollowerProgress.leader_durable_end (W3 pre-read sample); LogNodeConfig.{clock: Arc<dyn LogClock>, replica_lag_max_ms}; ostro_log::{LogClock, SystemLogClock, DEFAULT_REPLICA_LAG_MAX_MS}"
  - "D-P12-4 parsed-base rule in recover_partition"
  - "ci-local step 'one partition log path (12-08)' (check_one_log_path)"
affects: [12-10, 12-11, 12-12, 12-13, 12-14, 12-18]
tech-stack:
  added: []
  patterns:
    - "Replica core split from the log (no WAL reference) so the WAL-owned commit callback reaches it through a OnceLock slot per partition"
    - "RF=1 bootstrap = become_leader(next_term_epoch, ReplicaSet::solo(node)) before catch-up"
key-files:
  created:
    - crates/ostro-log/tests/partition_log_leader.rs
    - crates/ostro-log/tests/partition_log_follower.rs
    - crates/ostro-log/tests/partition_log_mutants.rs
  modified:
    - crates/ostro-log/src/partition_log.rs (renamed from rf1.rs)
    - crates/ostro-log/src/{node,lib,replicated,recovery,cut,hw,import}.rs
    - crates/ostro-partition/src/{node,roles}.rs
    - scripts/ci-local.sh
key-decisions:
  - "LogNode::open keeps its signature: floors[p] already is the state's applied offset, so it doubles as the F4 HW-at-open input (clamped into [log_start, LEO]; the unreadable sentinel clamps to LEO). No second parameter."
  - "ostro-partition picks the term epoch and calls become_leader before catch-up (both Leader and Inspect modes), so replay still covers [applied, LEO) exactly as before; Inspect-mode logs now report role Leader (nothing is appended)"
  - "FollowerProgress gains leader_durable_end: the W3 durable end sampled before read_replica cannot be taken inside record_progress"
  - "append accepts epoch >= leader epoch (StaleEpoch below it), as the RF=1 log did; a meta-assigned epoch is enforced at become_leader"
  - "The WAL V2 frame header carries no offset; D-P12-4's parsed base is read from the envelope header (first 47 bytes of the record for topic 'p')"
duration: ~6h
completed: 2026-11-22
---

# Phase 12 Plan 08: One log implementation Summary

**`Rf1Log` is now `PartitionLog`, the only `ReplicatedLog` implementation. RF=1 is the replica set `[self]` with `min_insync = 1`. Every HW advance, RF=1 included, goes from the WAL commit callback through the replica tracker into the single `publish_hw` point. The follower and install primitives the replication plans need are implemented, and the D-P12-4 torn-first-record rule is in recovery. The Phase 2/5/8 log, partition and crash suites are green, and the frozen format is unchanged (format_corpus, rebuild_digest).**

## Commits

| Commit | Message |
| --- | --- |
| b1f83e2 | feat(12-08): PartitionLog with tracker-driven HW through publish_hw (one path) |
| 97173f9 | test(12-08): follower append, durable-capped replica reads, reset_to |
| e08c688 | fix(12-08): parsed-base rule for a torn first record of an anchored log (D-P12-4) |
| 2078807 | refactor(12-08): rename Rf1Log to PartitionLog everywhere + one-path grep gate |

## API as landed

```rust
// node.rs
pub trait LogClock: Send + Sync + Debug { fn now_ms(&self) -> u64; }
pub struct SystemLogClock;                       // default
pub const DEFAULT_REPLICA_LAG_MAX_MS: u64 = 10_000;
pub struct LogNodeConfig { /* .. */ pub clock: Arc<dyn LogClock>, pub replica_lag_max_ms: u64 }
impl LogNode {
    pub async fn open(cfg: LogNodeConfig, floors: &[Offset]) -> Result<(LogNode, PartitionLogs), LogError>;
    // unchanged signature; floors[p] = applied offset -> HW at open (F4)
}
pub type PartitionLogs = Vec<Result<Arc<PartitionLog>, LogError>>;
// partition_log.rs
pub struct PartitionLog;   // pub(crate) fn new(pid, &LogNodeConfig, wal, hw_vector, RecoveryReport)
impl PartitionLog {
    pub fn recovery(&self) -> &RecoveryReport;
    pub fn hw_publishes(&self) -> u64;
    pub async fn seed_epochs(&self, history: &[(Epoch, Offset)]) -> Result<(), LogError>;
}   // set_role is gone: roles change only through become_leader / become_follower
// replicated.rs
impl ReplicaSet { pub fn solo(node: u64) -> Self }   // replicas = isr = [node], min_insync 1
pub struct FollowerProgress { pub replica, pub durable_leo, pub fetch_offset, pub now_ms, pub leader_durable_end: Offset }
```

Semantics:
- **Open.** HW = `clamp(floor, log_start, LEO)`. The durable end = the recovered LEO. Role is `Offline` (unassigned). `append` → `NotLeader{leader: None}`.
- **become_leader(e, set).** `StaleEpoch{latest}` unless `e > latest_epoch()` (seeded history included). Also refused below the current role epoch, and when `set.isr` does not contain the node (`Failed`). A fresh `ReplicaTracker` over `replicas ∪ isr` starts at the durable end. The HW is republished at once, so with ISR `{self}` it equals the durable LEO.
- **become_follower(e, leader).** Refused when `e` is older than the log's latest epoch or the current role epoch, and when `leader` is self or 0. The tracker is dropped.
- **HW.** Leader: `next_hw(prev, tracker.progress(), isr, min_insync).min(durable_end)`. Follower: `min(newest leader HW, durable_end)`. Either way the HW is published only when it moves up, through `HwVector::publish_hw`, and `hw_publishes += 1`. Truncation lowers the HW (`retract_hw`) and the durable end, and restarts the tracker at the new end.
- **append.** `NotLeader` off the leader role. `NotEnoughReplicas{isr, min}` while `|published| < min_insync`, refused before anything is touched. `StaleEpoch` below `max(leader epoch, current)`.
- **record_progress.** Leader only. Calls `tracker.on_fetch(replica, fetch_offset, durable_leo, leader_durable_end, now_ms)`, then republishes.
- **set_published_isr(epoch, set).** Leader of exactly `epoch` (otherwise `StaleEpoch` / `NotLeader`). The ISR must contain the leader. An older `isr_version` is ignored. Updates the ISR and `min_insync`, then republishes.
- **read_replica.** Same verified loop as `read` (tail cache → sparse index), capped at the durable end instead of HW. Returns at least one whole batch.
- **append_replica.** Follower only. For each envelope: size ≤ max, `peek_header` (CRC32C), then a local V2 frame around the **same bytes**, `verify_record` (partition, base == LEO, header/trailer), epoch == batch epoch, ≥ latest, ≤ followed epoch. Then enqueue (`acks = 1`), extend the epoch cache and return the new LEO. Bad input is `LogError::Wal(..)`: nothing is written and the partition is not failed.
- **truncate_to.** A follower is refused below HW (`Failed("truncate below HW: ..")`). Leader/unassigned behave as before (the RF=1 repair path).
- **reset_to(start, epochs).** Refused on a leader. Quiesces, `wal.truncate_to(0)` (drops records, tail cache, durable mark), deletes every remaining non-active segment file, fsyncs the partition dir. Then `log_start = LEO = durable = HW = start`, epochs = the given history.
- **replica_status.** Role, epoch (role epoch, else latest), HW, durable end, published ISR (leader only), version, min_insync, tracker follower rows (in_sync vs `replica_lag_max_ms`), hw_publishes.

## How callers pass applied / clock

- `ostro-partition` `Node::open`: the floors are unchanged (`meta.applied`, or `u64::MAX` when the state is unreadable). It sets `lcfg.clock = Arc::new(LogClockAdapter(cfg.clock))`, the node's injected `Clock`. `PartitionStart::run` seeds the anchored epochs, then computes `next_term_epoch(epoch_floor, latest)` and calls `become_leader(epoch, ReplicaSet::solo(node_id))` **before** spawning the apply machine. Catch-up then replays `[applied, LEO)` as before, and step 5 appends `LeaderAnnounced` at that epoch. This is the temporary RF=1 bootstrap until 12-13/12-14 make it meta-first.
- `roles::open_partition_log_for_tests` returns an unassigned log (documented), so fixtures call `become_leader` / `become_follower`.
- Test helpers (`ostro-partition/tests/common`, the ostro-log suites, the conformance `oracle_selftest`) lead with `latest + 1` before appending.

## Crash and format numbers

Release crash loops at `OSTRO_CRASH_ITERS=10` (as 05-21), at 2078807 on a loaded machine (load 11-14):
- `crash_loop_kill9`: 10 iterations, 979 acks verified, 745 runs verified, max reopen 65 ms
- `crash_loop_torn_tail`: 10 iterations, 868 acks verified
- `crash_loop_corruption_detected`: 10 iterations, 9 corruptions detected
- `crash_loop_snapshots`: 10 iterations, 2662 acks verified (339 below log_start), max log_start 647, max snapshots on disk 9, 12 interrupted snapshot writes
- `crash_loop_io_errors` (failpoints): 10 iterations, 1049 acks verified, 6 partition failures injected

Frozen format v0.1 unchanged: `format_corpus` green, and `PROPTEST_CASES=256 rebuild_digest` 8 passed (2,862 s under load). No byte written by the log changed. `append_replica` stores the leader's envelope bytes inside the same V2 frame `append` builds.

## D-P12-4: WAL frame-header finding

The vendored WAL V2 frame header (`RECORD_HEADER_LEN = 12`: magic u16, version u8, flags u8, length u32, crc u32) carries **no offset**. The frame's base/last/count sit in the trailer, after the payload, which is exactly what a torn write loses. The base the rule needs is read from the **envelope header**, which follows `topic_len u16, topic "p", partition i32, payload_len u32`. Envelope bytes 16..24 are the base. The base-carrying prefix is therefore 47 bytes (`BASE_PREFIX_LEN`). `parsed_base` checks that prefix structurally: WAL magic/version/length, topic `"p"`, partition, envelope magic and partition. No CRC can be checked on a torn record.

The rule: with `floor > 0` (not the unreadable sentinel) and no verified record before it, the first bad record is torn iff it is in the last segment, has the torn shape, and either its prefix names `base == floor` or fewer than 47 bytes remain. A complete prefix naming another base, or one that does not parse, stays `Corrupt`. The segment-base fallback the plan mentions was not needed. Segment files are named by segment id, not offset, so it would not have worked anyway.

## Rename ripple (mechanical, `Rf1Log` → `PartitionLog`)

crates/ostro-crashtest/src/bin/crash-target.rs, crates/ostro-crashtest/tests/apply_throughput.rs, crates/ostro-log/src/lib.rs, crates/ostro-log/tests/{append_group_commit,fatal_io,hw_cut,import,read_envelopes,recovery_empty_anchor,replicated_log_conformance,trim_prefix}.rs, crates/ostro-partition/examples/gen_format_corpus.rs, crates/ostro-partition/src/leader/{cron,dispatcher,mod}.rs, crates/ostro-partition/src/{node,roles,router}.rs, crates/ostro-partition/tests/{apply_rearm,children_chain,dispatcher,dispatcher_failpoints,dispatcher_flow,timers}.rs, crates/ostro-partition/tests/common/mod.rs, crates/ostro-server/src/control/{lifecycle,mod,p11_dlq,p11_rerun,p7_pause,replay,runs,schedules}.rs, crates/ostro-server/src/{effects,executor,p7,sink}.rs, crates/ostro-server/src/flow/rate_limit.rs, crates/ostro-server/src/services/{dlq_hook,run_spans}.rs, crates/ostro-server/src/workers/registry.rs, crates/ostro-server/tests/{children_derive,derive_phase5,sink}.rs, crates/ostro-server/tests/children_fx/mod.rs, crates/ostro-server/tests/common/{mod,p7_workers}.rs.

None of these files was dirty or listed by an in-flight plan (10-02, 11-18, 12-03, 12-05 `files_modified` checked). `control/mod.rs` changed on main in between (a landed plan), and the rebase was clean. `control/replay.rs` landed meanwhile (10-05) and was renamed as well.

## Deviations from Plan

1. **[Rule 3] No separate `applied` open parameter.** `LogNode::open`'s `floors` already are the state's applied offsets by contract, so they feed F4 directly. A second, always-equal parameter would only invite a mismatch.
2. **[Rule 1] The term is picked before catch-up, in both start modes.** If `become_leader` stayed where `set_role` was (after catch-up), F4's HW = applied would leave catch-up replaying nothing. `StartMode::Inspect` (crash loops, restore, archive) would then never apply the log tail. Leading first keeps the observable RF=1 behaviour identical. In Inspect mode the log's role is now `Leader` (no LA appended). Nothing checks it.
3. **[Rule 2] `FollowerProgress.leader_durable_end`** was added to the 12-02 contract. W3 (12-04's hand-off) needs the durable end sampled *before* `read_replica`, and `record_progress` cannot sample it afterwards. No production caller existed yet.
4. **[Rule 3] Test adaptations outside `files_modified`.** `hw_cut.rs`'s source-grep now asserts that the commit callback reports to the replica core and that `publish_hw` is called only from `ReplicaCore::publish` (plus the `rf1_bypass` mutant site). `import.rs` and `read_envelopes.rs` lead before reading/appending. `ostro-repl-conformance/tests/oracle_selftest.rs` (12-03, landed meanwhile) leads its fixture log. Epoch expectations in `trim_prefix` / `replicated_log_conformance` / `recovery_empty_anchor` follow the new term per open: B2 means a reopened log's term is above its logged epoch.
5. **Commit granularity.** The follower/install primitives live in the same file as the Task 1 changes, so they landed in the Task 1 commit. The Task 2 commit holds their test suite. RED commits were not made separately: tests were written alongside the code. Commits b1f83e2..e08c688 carry a temporary `pub type Rf1Log = PartitionLog` alias so each commit builds. 2078807 removes it.
6. **Segment-read count for `read_replica` not asserted.** Neither the vendored WAL nor ostro-log has a metric or hook counting segment opens. `read_replica` uses the same `fetch` path as `read` (durable-gated tail cache, then the sparse `PartitionIndex`), so there is no full-segment re-read path. Adding a counter would mean editing the vendored WAL.
7. **`append` epoch rule.** An append with `epoch > leader epoch` is still accepted, raising the current epoch, as the RF=1 log did. Only `< max(leader epoch, current)` is refused. Production callers always append at the leader epoch. A stricter rule can come with 12-13's meta-first lifecycle.

## D-P12-6 note

12-04's SnapshotRequired-on-untrimmed-older-epoch behaviour is untouched (recorded for user review by 12-04). The follower tests use only cases the rule answers with Ok/Diverging.

## Verification

Per-task `<automated>` blocks:
- Task 1: `cargo test -p ostro-log` green (all binaries), `--features failpoints --test-threads=1` green (293 tests across both runs), `partition_log_mutants` 2 passed (each mutant bites, then holds again after `fail::remove`). `partition_log_leader` 6 passed.
- Task 2: `partition_log_follower` 5 passed (`--test-threads=2`).
- Task 3: `recovery_empty_anchor` 12 passed. No `Rf1Log` in crates/. No `rf == 1` in ostro-log/src. `cargo build --workspace --all-targets --locked` warning-free (before the rebases; `cargo check --workspace --all-targets` clean after the final rebase onto 5188995). `cargo test -p ostro-partition` 462 passed before the roles fixture fix, then the fixed roles test passed. `ostro-crashtest --features failpoints --test-threads=1` green.

Also run:
- the ostro-partition failpoint/crash targets (`dispatcher_failpoints`, `fatal_io_state`, `trim_safety`, `snapshot`, `--features failpoints --test-threads=1`): 31 passed
- `ostro-repl-conformance --test oracle_selftest`: green
- clippy `-D warnings` on ostro-log (with and without failpoints) and ostro-partition: clean
- `cargo fmt --all --check`: clean
- the new `check_one_log_path` function: it reports the remaining hits before the ripple and passes after it

Not run: the full `scripts/ci-local.sh --no-musl`. Four other executors were building, and the full ostro-partition/ostro-server single-threaded failpoint suites take hours. The plan-level gate is left to the next gate run, as with 12-04. The ostro-server suite ran only for the ripple-heavy targets: `sink` 12, `executor` 16, `derive_phase5` 8 and `children_derive` 19 passed. clippy `-D warnings` on ostro-repl-conformance and ostro-crashtest (all targets) is clean.

## Open items

- 12-11: build `FollowerProgress` with `leader_durable_end` = `durable_end()` sampled before `read_replica`. Call `record_progress` only after `check` returns Ok. On a diverging answer the leader's tracker still holds the follower's old durable progress (there is no `on_truncate` on the trait). Truncation never goes below the HW, so this does not affect the HW, but 12-11 may want a trait hook.
- 12-13/12-14: replace the RF=1 bootstrap `become_leader(next_term_epoch, solo)` in `PartitionStart::run` with the meta-assigned epoch. Decide whether Inspect mode should lead or follow-at-own-durable-end. Consider tightening `append` to `epoch == leader epoch`.
- 12-18: storage-format §10 text for F4 (HW at open = applied, clamped) and D-P12-4 (47-byte envelope-header prefix, see above).

## Self-Check: PASSED

- Files: partition_log.rs and the three new test files are present.
- Commits b1f83e2, 97173f9, e08c688 and 2078807 are on main.
