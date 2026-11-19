---
phase: 02-durable-partition-log-state-machine
verified: 2026-11-19T00:00:00Z
status: passed
score: 5/5 must-haves verified
---

# Phase 2: Durable Partition Log & State Machine Verification Report

**Phase Goal:** Each partition is a crash-safe, CRC-verified journal behind an RF=1 `ReplicatedLog`, with a deterministic state machine (fjall, replacing redb per the S3 decision) that can always be rebuilt from the log. The on-disk format and command envelope are fixed here.
**Verified:** 2026-11-19
**Status:** passed
**Re-verification:** No, initial verification

Verification ran in a detached worktree at HEAD (8ffdc01) with a separate target dir, because plan 04-09 may be editing ostro-types, ostro-partition and ostro-server concurrently. Both were removed afterwards.

Runs done for this verification:

- `cargo test -p ostro-types -p ostro-partition -p ostro-log -p ostro-blob -p ostro-wal` (debug): 39 test binaries, 336 passed, 0 failed. This includes `rebuild_digest` at the default 24 cases and `shuffler_exactly_once` (P=16, 177 s).
- `cargo test -p ostro-log -p ostro-wal --features failpoints -- --test-threads=1`: 163 passed, 0 failed.
- `OSTRO_CRASH_ITERS=10 cargo test --release -p ostro-crashtest --test crash_loop -- --ignored`: all 3 passed.
  - kill9: 956 acks and 716 runs verified, max reopen 49 ms.
  - torn tail: 10/10 cut, 834 acks verified.
  - corruption: 9/9 detected.
- `OSTRO_CRASH_ITERS=10 ... --features failpoints ... crash_loop_io_errors`: passed. 851 acks verified, 11 partition failures injected.

Not re-run. These are taken from the 02-16 SUMMARY:

- `crash_loop_kill9` at 500 iterations.
- `rebuild_digest` at `PROPTEST_CASES=1000`.
- The 50-iteration crash legs.
- Full `scripts/ci-local.sh` (14 steps, with musl).
- The reopen and throughput measurements.

## Observable Truths

| # | Truth (ROADMAP success criterion) | Status | Evidence |
|---|-------|--------|----------|
| 1 | Torn tails are truncated on restart and the log end is found by scanning the segments. A flipped bit outside the torn tail makes partition open fail loudly, never skip silently. Injected EIO/ENOSPC on write or fsync is fatal for that partition and is never reported as success. | VERIFIED | `ostro-log/src/recovery.rs::recover_partition` verifies every record in every segment. The tail is torn only if all three hold: last non-empty segment, above the floor, and incomplete or zero-sector. Anything else returns `LogError::Corrupt`, and the cut uses set_len + fsync file + fsync dir. Tests pass: tests/torn_tail.rs (12) and tests/corruption.rs. The failpoint tests also pass: vendored poison.rs (P1), ostro-log fatal_io.rs and ostro-partition fatal_io_state.rs. The crash loops (torn, corruption, io_errors) passed here at 10 iterations. |
| 2 | Deleting a partition's redb state and restarting rebuilds byte-identical state: the rolling state hash matches. Apply never passes HW, and `AppendAck` is returned only after apply-time validation. | VERIFIED | fjall replaces redb. Rebuild tests pass: rebuild_basic.rs, store_digest.rs and rebuild_digest.rs (24 cases here, 1000 recorded). In `machine.rs::apply_once`, reads are bounded by `hw` and a batch with `end() > hw` is a hard error. In the same file, `b.commit(&mu)` (l.513) runs before `acks.resolve` (l.522). apply_hw.rs and ack_after_apply.rs pass. |
| 3 | Every leadership term begins with `LeaderAnnounced{epoch}`. A command carrying a stale `(epoch, attempt)` token is rejected at apply. | VERIFIED | In `node.rs::catch_up_and_announce`, `next_term_epoch(floor, latest+1)` is followed by submitting `LeaderAnnounced`. The handle is returned only after it is Accepted. At apply, a command whose envelope epoch differs from the cursor epoch is rejected (StaleEpoch), unless it is the announcing `LeaderAnnounced` itself. Tokens are checked in `apply/run.rs`. fencing.rs and apply_core.rs pass. |
| 4 | Cross-partition messages (child start/complete, signals, cancels, cron starts) are delivered exactly once across P=16 partitions through the outbox and shuffler, even when replayed after a crash. Duplicate `TimerFired` entries have no effect. | VERIFIED | `apply/inbound.rs` dedupes on `(src, seq)` against `inbound_seen` and rejects a gap. Cron starts dedupe on `__cron/<id>` plus fire_time_ms (BE). `leader/shuffler.rs` keeps one batch in flight per target. shuffler_exactly_once.rs passes with P=16, restarts and lost/duplicated acks. A timer firing whose timer row is already gone is a no-op Duplicate. apply_cross.rs and timers.rs pass. |
| 5 | A payload over 256 KiB is stored in the blob store and referenced from the journal. WAL segments are never deleted above the last durable checkpoint or before the startup readiness barrier. | VERIFIED | `handle.rs:189`: a payload of `bytes.len() > INLINE_PAYLOAD_LIMIT` goes through `blobs.put` (ostro-blob: tmp + fsync + rename + dir fsync, sha256 verified on get) and becomes `PayloadRef::Blob`. Inline payloads over the limit are rejected at apply. `leader/retention.rs::trim_floor` = min(ckpt, snapshot_floor, retention_floor). Trimming needs a `TrimPermit`, which only an opened `ReadyBarrier` can mint. snapshot_floor = 0 in Phase 2. blob_offload.rs, trim_prefix.rs, trim_safety.rs and startup_order.rs pass. |

**Score:** 5/5

## Specific Checks

### 1. Recovery fsyncs only the last non-empty segment (02-16, commit 22b39be)

`recover_partition` now fsyncs only `retained.last()`. That is the last non-empty segment, or the cut segment after a torn cut, which is the same segment. The claim is that every older retained segment is already durable. I checked this against the vendored WAL:

- **Every rotation fsyncs first.** `rotate_to_new_segment` (group_commit.rs l.1521) has three callers, and each calls `file.sync_all()` while holding the writer lock before it creates the new file:
  - `force_seal_segment` (l.1598), used by idle seal (l.2323) and shutdown.
  - `check_and_seal_segment` (l.1701), used for size and age rotation.
  - The truncation path (l.1875). Its straddler cut also runs set_len + `sync_all` + dir fsync (l.2065).
  - If the fsync fails, the partition is poisoned (P1) and no rotation happens.
  - With `rotate=false` (shutdown), or when the file is empty, the file is fsynced but no new segment is created.
- **A crashed tail is fsynced before a newer segment can exist.** In `LogNode::open` (ostro-log/src/node.rs), step (1) awaits recovery for every partition. Only then does step (2) construct `GroupCommitWal`.
  - The `GroupCommitWal` constructor only runs `discover_sealed_segments`, which creates no files.
  - Segment files are created lazily by `get_or_create_queue` on the first append, at id max+1.
  - A partition whose recovery failed gets no `Rf1Log`, so it never appends and never gets a new segment.
  - `GroupCommitWal` is used only in ostro-log (rf1.rs, node.rs). No other crate opens the WAL.
- **Edge cases:**
  - *Empty last segment* (process died after rotating or opening a queue, before writing): emptiness is judged by file length, so the fsync goes to the newest non-empty segment, which is correct. The segment before it was fsynced by the rotation or by the previous recovery.
  - *Crash between creating the new file and the dir fsync*: the old segment was already fsynced, and the new one is empty or not yet acknowledged.
  - *Torn cut at byte 0*: the cut segment becomes empty and is fsynced by `cut`. The segment before it is sealed, so it is durable.
  - *Crash during recovery*: the files are laid out as before, so the next recovery does the same work.
  - *Failed recovery*: returns before the fsync, and no newer segment can be created.
- **Residual (info, not a gap):**
  - No test asserts the durability argument directly. Power loss cannot be simulated in-tree, and LazyFS is listed as optional and manual in 02-VALIDATION.md.
  - The argument also depends on the vendored WAL keeping fsync-before-rotate. VENDORED.md patch tracking and `check-vendored.sh` guard the vendored code. If a new rotation path is ever added upstream, this needs re-checking.

Verdict: sound. I found no path where a non-last retained segment holds un-fsynced bytes.

### 2. RMW apply throughput (80.8k cmds/s at the 16 MiB default cache vs the ≥ 90k S3 rule)

This is not a Phase 2 gap.

- **No Phase 2 criterion or requirement sets a rate.**
  - None of the five ROADMAP success criteria above mentions throughput.
  - STOR-05 reads: "Partition state machine applies committed commands deterministically into redb (`Durability::None` + periodic `Immediate` checkpoint of `applied_offset`); apply never passes HW". Its superseding note (fjall, one `WriteBatch` per batch, `persist(SyncAll)` every 100 ms/256 batches) also contains no rate.
- **Where the 90k rule comes from.** It is the Phase 1 decision rule in PROJECT.md Key Decisions: "Locked rule (≥ 90k cmds/s ≈ 30k steps/s in the planned durability mode)". It was used to choose fjall over redb, and fjall passed it in S3 with 106.9k cmds/s.
- **What 02-16 required.** Its must-have says "Apply throughput ... measured and recorded". Its task text explicitly expects a shortfall to be handled as an open concern: "If the store-level RMW rate is below 90k cmds/s, do NOT silently accept it: record it prominently in the SUMMARY under 'Deviations / open concerns'." That was done in 02-16-SUMMARY, docs/storage-format.md §11, and STATE.md (open concern routed to Phase 4 ServerConfig and the 05-15 perf plan).
- **Product NFRs are far below this rate.** QA-04 asks for ≥ 10,000 steps/s single node (Phase 5 ROADMAP: ≥ 5,000). The measured 80.8k cmds/s per partition is about 27k steps/s.
- **Likely cause.** The shortfall depends on cache size: 153k cmds/s at 128 MiB, with exactly 1.00 keyspace read per command. It is a per-partition memory budget question, not a defect in the apply path.

Classification: a tuning item, properly deferred and tracked.

### 3. docs/storage-format.md against the code (checked at HEAD)

- **Command oneof** tags 1..7 (LeaderAnnounced, StartRun, AttemptGranted, EventsAppended, TimerFired, InboundBatch, OutboxTruncated) match `command.rs`.
- **Message fields** match field by field: StartRun 1..13, AttemptGranted, EventsAppended 1..5, TimerFired 1..4, InboundBatch, OutboxTruncated, ParentRef, AttemptTokenMsg, JournalEvent, ScheduleTimer/CancelTimer, CompleteRun, ChildCompleted 1..5, Signal, CancelRun, CronStart, InboundMsg.
- **Oneofs:** Derived 1..5 and OutboxMsg 1..5 match. PayloadRef (1 inline, 2 blob) and BlobRef (1 sha256, 2 len) match `payload.rs`.
- **Enums and limits:** ConflictPolicy and TerminalStatus values match. `INLINE_PAYLOAD_LIMIT = 256 * 1024` matches.
- **Keyspaces:** the 13 keyspaces and their order match `store/mod.rs::TABLES`, including the names.
- **Key layouts** match `store/keys.rs`:
  - Strings as u16 BE len ‖ utf8.
  - Ready priority as `!((p as u32) ^ 0x8000_0000)`.
  - Timers as due ‖ run ‖ op, with `START_DELAY_OP = u64::MAX`.
  - Outbox as target u16 ‖ seq.
  - inbound_seen value as seq u64 BE (`batch.rs:124`).
  - Payload kind 0 = input, 1 = result (`apply/run.rs`).
  - Cron dedupe as `"__cron/" + schedule_id` with fire_time_ms BE (`apply/inbound.rs`).
- **Stored values:** RunRecord field order matches `values.rs`, and `VALUE_FORMAT = 1` matches. CursorMeta fields match.
- **Meta keys** (7 names, `FORMAT_VERSION = 1`) match `store/meta.rs`.
- **Envelope:** MAGIC, `HEADER_LEN = 32`, the CRC covering `[..28] ‖ body`, and `DEFAULT_MAX_ENVELOPE_BYTES = 4 MiB` match `envelope.rs`.
- **Outcome codes:** Accepted 0, Duplicate 1, and `Rejected = 16 + index` (StaleEpoch 0 … Invalid 10, giving 16..26) match `apply/outcome.rs`.

I found no discrepancies.

## Required Artifacts

| Artifact | Status | Details |
|---|---|---|
| `crates/ostro-types` (ids, routing, envelope, command, payload) | VERIFIED | Golden vectors in tests/golden. Tests command.rs, envelope.rs and ids.rs pass. |
| `vendor/ostro-wal` (P1 poison, P2 dir fsync, P3 failpoints) | VERIFIED | poison.rs, dir_fsync.rs, suffix_truncation.rs and idle_seal_rotation.rs pass, with and without failpoints. |
| `crates/ostro-log` (recovery, verify, Rf1Log, LogNode, barrier, trim) | VERIFIED | Wired into `ostro-partition/src/node.rs`. 8 test files pass. |
| `crates/ostro-partition` (store, apply, machine, handle, node, shuffler, timers, retention) | VERIFIED | 17 test files pass. |
| `crates/ostro-blob` | VERIFIED | Used by `handle.rs`. blob_store.rs and blob_failpoints.rs pass. |
| `crates/ostro-crashtest` (crash-target, crash_loop, reopen_time, apply_throughput) | VERIFIED | Crash loops pass here. The measurement tests are ignored by design. |
| `docs/storage-format.md` | VERIFIED | Checked against the code (Specific Check 3). docs/03-architecture.md links to it twice. |

## Key Link Verification

| From | To | Via | Status |
|---|---|---|---|
| LogNode::open | recover_partition | all partitions recovered before `GroupCommitWal::new` | WIRED |
| WAL commit callback | HW watch | `compute_hw` in node.rs | WIRED |
| apply machine | ReplicatedLog::read | bounded by HW, hard error past HW | WIRED |
| apply machine | acks | resolve after `ApplyBatch::commit` | WIRED |
| PartitionHandle::submit | BlobStore::put | payload > 256 KiB offloaded | WIRED |
| Node startup | LeaderAnnounced | `catch_up_and_announce` before the handle is returned | WIRED |
| retention | trim_prefix | `TrimPermit` from an opened `ReadyBarrier` | WIRED |

## Requirements Coverage

| Requirement | Status | Evidence |
|---|---|---|
| STOR-01 | SATISFIED | Envelope (envelope.rs) plus group commit (append_group_commit.rs). |
| STOR-02 | SATISFIED | Verified full scan, corruption.rs, crash loop corruption leg. |
| STOR-03 | SATISFIED | torn_tail.rs, crash loop torn leg. |
| STOR-04 | SATISFIED | `ReplicatedLog` trait, replicated_log_conformance.rs. |
| STOR-05 | SATISFIED | fjall apply (superseding redb per S3), checkpoint cadence, apply ≤ HW. The throughput concern is deferred (Specific Check 2). |
| STOR-06 | SATISFIED | rebuild_basic.rs, rebuild_digest.rs, kill9 digest checks. |
| STOR-07 | SATISFIED | Failpoint suites, crash_loop_io_errors. |
| STOR-08 | SATISFIED | trim_prefix.rs, trim_safety.rs, startup_order.rs. |
| STOR-09 | SATISFIED | ids.rs golden vectors, P mismatch fatal (node.toml and store meta). |
| STOR-10 | SATISFIED | fencing.rs, apply_core.rs. |
| STOR-11 | SATISFIED | ack_after_apply.rs. |
| STOR-12 | SATISFIED | apply_cross.rs, shuffler.rs, shuffler_exactly_once.rs. |
| STOR-13 | SATISFIED | timers.rs (window bound, 100k timers), idempotent TimerFired. |
| STOR-14 | SATISFIED | blob_offload.rs, ostro-blob tests. |

All 14 IDs map to Phase 2 in REQUIREMENTS.md, and every plan's `requirements` field is covered. None are orphaned.

## Anti-Patterns

None blocking. I did not scan every source file for TODO or stub markers. The verdict rests on the test, failpoint and crash-loop evidence above. Two known deferrals are recorded in STATE.md and are not Phase 2 criteria:

- Reopen still grows by about 0.5 ms per restart, from the many small per-restart WAL segments.
- The block cache size needs to be set from a node memory budget.

## Accepted Deviations

- fjall 3.1.13 replaces redb as the state store (Phase 1 S3 decision). The success criterion 2 and STOR-05 wording that says "redb" is read as "the state store".
- 02-07's must-have "every retained segment has been fsynced once" now holds as "every retained segment is durable after recovery" (Specific Check 1).
- The `dlq` and `effects` keyspaces are reserved, with no rows written in Phase 2.

## Human Verification

None required for sign-off. LazyFS power-loss simulation and the remote CI legs (aarch64 musl, macOS) are optional manual items in 02-VALIDATION.md and do not block the phase.

## Gaps Summary

No gaps. The phase goal is achieved. Store-level RMW throughput below the S3 90k rule at the default cache is an open tuning concern, owned by Phase 4/5. It is not a Phase 2 success criterion.

_Verified: 2026-11-19_
_Verifier: Claude (gsd-verifier)_
