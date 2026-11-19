---
phase: 05-workflow-semantics-core-hardening
plan: 21
subsystem: storage-recovery
tags: [ostro-log, recovery, empty-log-anchoring, epoch-cache, partition-startup, phase-12-f3]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 gate + contract (baseline); Phase 12 pre-freeze finding F3 adopted in 05-DECISIONS.md"
  - phase: 02-durable-partition-log-state-machine
    provides: "02-07 verified recovery scan + torn-tail rule, 02-08 Rf1Log sequencer/HW, 02-12 Node startup order + validate(), 02-16 tail-only recovery fsync"
provides:
  - "Empty-log anchoring: recover_partition returns log_start = LEO = floor when no record survives; Rf1Log sequencer and HW start there, so the first append lands at the floor"
  - "Rf1Log::seed_epochs(history): seeds the epoch history of a record-less log and raises the sequencer's current epoch (stale appends refused); refused once the log has records"
  - "Node::open seeds an anchored log's epochs from CursorMeta.epoch_history, so the new term is cursor epoch + 1; 'state ahead of log' is raised only for non-empty logs"
  - "Rule text for the 05-20 FROZEN storage-format §10 (below)"
affects: [05-20, 08-01, 08-09, 08-18, 12-04, phase-12]

tech-stack:
  added: []
  patterns:
    - "Recovery anchors, never invents: an empty scan only decides where the next append lands; no bytes are written"
    - "Floor above i64::MAX is the 'state unreadable' sentinel and anchors at 0"

key-files:
  created:
    - crates/ostro-log/tests/recovery_empty_anchor.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-21-SUMMARY.md
  modified:
    - crates/ostro-log/src/recovery.rs
    - crates/ostro-log/src/rf1.rs
    - crates/ostro-log/src/node.rs
    - crates/ostro-partition/src/node.rs
    - crates/ostro-partition/tests/startup_order.rs
    - .planning/ROADMAP.md

key-decisions:
  - "Empty-log anchoring lives in recover_partition (log_start = LEO = floor); a floor above i64::MAX (Node::open's unreadable-state sentinel u64::MAX) anchors at 0"
  - "Epoch seeding is a post-open Rf1Log::seed_epochs(history), called by Node in PartitionStart::run before the apply machine spawns; pairs kept verbatim (offsets at or below log_start), sequencer current_epoch raised to the seeded latest"
  - "Torn-tail rule deliberately unchanged: with no verified record before it, a bad first record is 'above the floor' only when floor == 0, so a power-loss tear of an anchored log's first record (floor > 0) fails Corrupt; pinned by a test, Phase 12 decides"

patterns-established:
  - "Empty-log anchoring (Phase 12 F3)"

requirements-completed: []

duration: 37min
completed: 2026-11-19
---

# Phase 5 Plan 21: Empty-log anchoring (Phase 12 F3) Summary

**A partition log with no records now recovers anchored at the state's `applied_offset` (`log_start = LEO = HW = applied`), its first append lands at that offset, and `Node::open` seeds its epoch history from `CursorMeta.epoch_history`, so the new term starts at cursor epoch + 1. "State ahead of log" is raised only for non-empty logs. No bytes and no on-disk format changed.**

## Performance

- **Duration:** about 37 min
- **Started:** 2026-11-19T06:20:29Z
- **Completed:** 2026-11-19T06:57:26Z
- **Tasks:** 2
- **Files modified:** 6 code/test files, plus ROADMAP and this SUMMARY

## Accomplishments

- `recover_partition(wal_dir, pid, floor)`: when no record survives the scan (no segments, only empty segments, or a torn cut leaving nothing), `log_start = leo = floor` and `epochs` is empty. A floor above `i64::MAX` (Node's unreadable-state sentinel `Offset(u64::MAX)`) anchors at 0, as before. Non-empty logs are unchanged.
- `Rf1Log`: the sequencer and HW already started at `recovery.leo`, so no plumbing was needed. The anchored first append is assigned `floor`. Reads, the reopen through the segment index, and `truncate_to(anchor)` are all tested.
- `Rf1Log::seed_epochs(&[(Epoch, Offset)])`: an async post-open seed for a record-less log. It keeps the pairs verbatim and raises the sequencer's `current_epoch` to the seeded latest, so older-epoch appends get `StaleEpoch`. It is refused (`LogError::Failed`) once the log has records or when the history is not ascending. An empty history is a no-op.
- `ostro-partition` `Node`: `PartitionStart::run` seeds an empty log from the cursor before the apply machine spawns. Without the seed, the term picks epoch 1, its `LeaderAnnounced` is rejected as stale and the partition fails. This was verified by temporarily disabling the call: the new test fails. `validate()` is unchanged in code and documented: an anchored empty log always passes.

## Task Commits

1. **Task 1: anchor an empty log at the floor + seed_epochs**: `6eca13d` (feat)
2. **Task 2: partition startup on an anchored empty log, epochs seeded from the cursor**: `3a405b8` (feat)

**Plan metadata:** the docs commit with this SUMMARY and the ROADMAP checkbox (hash in the final report).

TDD note: the tests and the implementation were written together and committed per task. There is no separate RED commit. Seeding was shown to be load-bearing by disabling it, which made the new startup test fail.

## Tests added

`crates/ostro-log/tests/recovery_empty_anchor.rs` (10):
- the empty dir and empty segment files anchor at 500 / 77;
- floor 0 is unchanged;
- the `u64::MAX` and `i64::MAX + 1` sentinels anchor at 0;
- for a non-empty log (from 0, and from 500), start and end ignore the floor;
- the anchored append at 500/502 reads back, the HW is 505, and a reopen keeps start 500;
- truncate back to the anchor;
- seeded epochs answer `end_offset_for_epoch` and fence epoch 2 below seeded 3, then append epoch 4 at 500;
- seeding is refused on a non-empty log and for bad histories;
- pinned conservative torn-first-record rule (see Deviations / hand-off).

`crates/ostro-partition/tests/startup_order.rs` (+3):
- `empty_wal_anchors_at_applied_and_rebuild_matches`: N = 6 and `wal/p/0` deleted after a clean stop. The node opens Healthy. LogRecovered leo N, TermStarted{epoch 2, offset N}, epochs `[(1,0),(2,N)]`, and the next command applies at N+1. The state copy at N is then restored and the anchored log replayed in Inspect mode: `(applied, state_hash, digest)` are identical.
- `anchored_log_keeps_the_fatal_cases`: on an anchored, non-empty log `[N, N+2)`, a state at applied 2 gives "snapshot required", and applied 1000 gives "state ahead of log".
- `empty_wal_and_fresh_state_start_at_zero`: floor 0 is unchanged (term epoch 1 at offset 0).

## Verification

- `cargo test -p ostro-log --locked` and `cargo test -p ostro-log --features failpoints --locked -- --test-threads=1`: green.
- `cargo test -p ostro-partition --test startup_order --test rebuild_basic --locked`: green. Clippy for ostro-log and ostro-partition with `-D warnings` is clean.
- **`bash scripts/ci-local.sh --no-musl`**, run in a detached worktree at `3a405b8` (free of other executors' uncommitted edits): **all steps passed**, including fmt, clippy, the warning-free build, `cargo test` (606 s), the failpoint suites for ostro-wal, ostro-blob, ostro-log, ostro-partition and ostro-crashtest (905 s), deny and vendored checks. No flaky reruns were needed.
- **02-15 crash loops at `OSTRO_CRASH_ITERS=10`** (release, same worktree):
  - `crash_loop_kill9`: 10 iterations, 1094 acks verified, 822 runs verified, max reopen 194 ms;
  - `crash_loop_torn_tail`: 10 iterations, 954 acks verified;
  - `crash_loop_corruption_detected`: 10 iterations, 9 corruptions detected (one leg skipped by design: "no sealed segment yet");
  - `crash_loop_io_errors` (failpoints): 10 iterations, 915 acks verified, 11 partition failures injected. All ok.
- Phase 2 invariants are intact. The torn-tail rule, the tail-only recovery fsync, `Corrupt` on everything else, and floor protection for non-empty logs are all unchanged; the existing `torn_tail`/`corruption` suites pass unmodified. `FAILPOINT_CRATES` is untouched and in sync with ci.yml.

## Decisions Made

See the frontmatter `key-decisions`. In short: anchoring lives in `recover_partition`; the floor sentinel above `i64::MAX` anchors at 0; seeding is a post-open `Rf1Log::seed_epochs` that Node calls; the torn-tail rule is deliberately unchanged.

## Deviations from Plan

- **Files:** `epoch_cache.rs` and `hw.rs` were not needed. `rf1.rs` received `seed_epochs` and a doc line; no plumbing change was needed, because the sequencer and HW already start at `recovery.leo`.
- **Torn first record of an anchored log (documented, not a code change):** with `floor > 0` and no verified record before it, a torn first record is not provably above the floor, so recovery keeps returning `Corrupt`. This is the existing Phase 2 rule. It is pinned by `torn_first_record_of_anchored_log` and documented on `recover_partition`. `kill -9` cannot produce it; only power loss before the anchored log's first fsync can. It is handed to Phase 12 below.
- **ROADMAP:** only the 05-21 checkbox was flipped, in a hunk built from HEAD. The Phase 5 progress row was left alone because 05-03 has an uncommitted edit to it in the shared working tree.
- **STATE.md:** not touched (owned by 05-07). Notes are here.

## Hand-off: rule text for 05-20 (FROZEN storage-format §10)

Insert in §10 "Recovery rules" as a step-3 addendum, and reword step 4:

> **Empty-log anchoring (Phase 12 F3).** If a partition's log has no records (no segment files, only empty ones, or nothing left after a torn-tail cut) and its state store is readable, the log is anchored at the state's applied offset: `log_start = LEO = HW = applied_offset`, and the next append is assigned offset `applied_offset`. The log epoch cache, which is otherwise derived from envelope epochs, is seeded from `cursor.epoch_history` (`Rf1Log::seed_epochs`, called by `Node::open` before catch-up), so the new term's epoch is above every applied `LeaderAnnounced` epoch. No bytes are written by anchoring. An unreadable state (floor sentinel `u64::MAX`) anchors at 0. A **non-empty** log with `applied > LEO` stays fatal ("state ahead of log"), and `applied < log_start` stays "snapshot required". Anchoring does not change the torn-tail rule: with no verified record before it, a bad first record counts as above the floor only when `applied_offset == 0`.

Step 4 should read: "Validate: for a non-empty log, `applied > LEO` is 'state ahead of log' (fatal for the partition); `applied < log_start` is 'snapshot required'. An empty log is anchored (above) and always validates."

05-02 owns `docs/storage-format.md` in wave 2. This text is for 05-20's FROZEN pass (or for 05-02 if it is still editing §10).

## Hand-off: Phase 8 (08-01 / 08-09 / 08-18)

- 08-18 `ImportMode::Create` with zero batches (an idle partition whose snapshot equals the cut) now opens: install the state at `applied = S`, leave `wal/p/<pid>` empty or absent, and `Node::open` anchors at S, seeds epochs from the cursor in the snapshot (OSNP carries the cursor row verbatim) and starts term `max(cursor epoch) + 1` at offset S. No Phase 8 code is needed in ostro-log. The F3 prerequisite "must be implemented before 08-18 ships" is met.
- A restore must not leave stale non-empty segments that end below S. Recovery would see a non-empty log with `applied > LEO` and fail "state ahead of log", by design. Delete the partition's WAL dir before installing the state (see the Phase 12 ordering note).
- Bottomless (08-11/08-13) uploading from an anchored log starts at `log_start = S`. The remote chain for that partition begins at S, after the snapshot.

## Hand-off: Phase 12 (12-04 install, F4/F5)

- **Install order (Pattern 6) is supported:** reset the log first (empty dir), then install the state. A crash between the two leaves an empty log plus the old state at `applied = A`, which now anchors at A and fetches from A. Do not reorder.
- **Seeding:** `Rf1Log::seed_epochs` needs a record-less log (`log_end == log_start`, empty cache). After a follower install, call it, or reopen through `Node::open`, before the first fetched batch is appended. Pairs are kept verbatim, including offsets below `log_start`, so `end_offset_for_epoch` answers for history below the start (F5). If 12-04 wants the cache clamped to `log_start` instead, use `EpochCache::truncate_from_start` semantics.
- **Open decision, torn first record:** an anchored log whose first record is torn by power loss before its first fsync fails open as `Corrupt` (the offsets of an unparseable first record are unknown). The follower-install window will hit this more often than RF=1. Possible fixes: treat a first record whose parsed frame base equals the floor as above the floor, or have install fsync an empty anchor marker. Both need a design decision. The pinned test is `torn_first_record_of_anchored_log`.
- **F4 (HW at open = applied_offset)** is not implemented here: HW at open is still the recovered LEO. For an anchored log the two are equal.

## Issues Encountered

None. The build in the shared tree (with other wave-2 executors' uncommitted edits) and the clean worktree both compiled warning-free.

## Next Phase Readiness

- F3 is done. 05-20 needs only the rule text above in the FROZEN storage-format doc.
- `target/wt-05-21` and the scratch worktree were removed at the end.

## Self-Check: PASSED

- FOUND: crates/ostro-log/tests/recovery_empty_anchor.rs
- FOUND: commits 6eca13d, 3a405b8
