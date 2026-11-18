---
phase: 02-durable-partition-log-state-machine
plan: 12
subsystem: storage
tags: [node, startup, readiness-barrier, leader-epoch, fencing, rebuild, partition-count, router, leader-services]
requires:
  - phase: 02-08
    provides: "LogNode::open(cfg, floors), Rf1Log (recovery(), set_role, epochs), ReadyBarrier/TrimPermit"
  - phase: 02-10
    provides: "InboundBatch/OutboxTruncated/TimerFired apply (exercised by the rebuild workload)"
  - phase: 02-11
    provides: "MachineHandle (set_emit_actions, wait_applied, health, shutdown), PendingAcks, PartitionHandle"
provides:
  - "ostro_partition::node::{Node::{open, handle, slots, router, barrier, startup_events, shutdown}, NodeConfig::new, StartMode {Leader, Inspect}, PartitionSlot {Ready, Failed}, NodeError}"
  - "ostro_partition::startup::StartupEvent (+ StartupEvent::partition())"
  - "ostro_partition::leadership::{LeadershipSource, LocalLeadership, next_term_epoch}"
  - "ostro_partition::clock::{Clock, SystemClock, ManualClock}"
  - "ostro_partition::router::{PartitionRouter, RouteFuture, InProcessRouter::{new, register, clear}}"
  - "ostro_partition::leader::{LeaderCtx, LeaderConfig, LeaderTasks::shutdown, spawn_all}; leader::{shuffler,timers,retention}::spawn stubs + ShufflerConfig/TimerConfig/RetentionConfig defaults"
  - "tests/common: open_node(dir, P, mode), copy_dir(src, dst), run_on(p)"
affects: [02-13, 02-14, 02-15, 02-16, 04-runtime]
tech-stack:
  added: []
  patterns:
    - "Startup = state stores first (floors) -> LogNode recovery -> validate -> per-partition task (machine, catch-up with actions off, LeaderAnnounced) -> barriers + router registration -> leader services"
    - "Per-partition failure isolation: every step's error becomes PartitionSlot::Failed + PartitionFailed event; node-level errors only for node.toml, LogNode itself and the blob store"
    - "Unreadable state store -> log floor u64::MAX, so recovery never cuts a tail on behalf of an unknown state"
    - "spawn_all gives each partition's services its own stop channel, forwarded from the node-wide shutdown watch"
key-files:
  created:
    - crates/ostro-partition/src/node.rs
    - crates/ostro-partition/src/startup.rs
    - crates/ostro-partition/src/leadership.rs
    - crates/ostro-partition/src/clock.rs
    - crates/ostro-partition/src/router.rs
    - crates/ostro-partition/src/leader/mod.rs
    - crates/ostro-partition/src/leader/shuffler.rs
    - crates/ostro-partition/src/leader/timers.rs
    - crates/ostro-partition/src/leader/retention.rs
    - crates/ostro-partition/tests/startup_order.rs
    - crates/ostro-partition/tests/fencing.rs
    - crates/ostro-partition/tests/rebuild_basic.rs
  modified:
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-partition/src/store/values.rs
decisions:
  - "A partition whose state store fails to open gets log floor Offset(u64::MAX): no tail is torn-eligible, so its WAL files are never modified while the state is unknown"
  - "TermStarted.offset is read from the log's epoch history (latest entry == the new epoch) rather than log_end before the append"
  - "Node uses one shared BlobStore at <data>/blobs for all partitions (content-addressed); a blob-store open failure is NodeError::Io"
  - "Inspect mode leaves actions off, the role Offline, the router empty and every barrier closed"
  - "Node::shutdown logs per-partition machine shutdown errors instead of returning them (everything acknowledged is in the WAL; reopen replays from applied_offset)"
  - "InProcessRouter::deliver returns NotLeader for unregistered or failed (unhealthy) partitions; Node::shutdown clears the router so no handle outlives the node"
  - "node.toml parser ignores unknown keys and # comments (forward compatible); format_version != 1 is NodeError::NodeFile"
metrics:
  duration: 35min
  completed: 2026-11-18
---

# Phase 2 Plan 12: Node, Ordered Startup, Term Fencing, Rebuild Summary

`Node::open` now brings up P partitions from one data directory in a fixed, recorded order: `node.toml` P check → state stores (durable floors) → shared log recovery with those floors (torn tails cut before any append) → validation → catch-up with actions discarded → a new term opened by `LeaderAnnounced{epoch}` → readiness barrier and router registration → leader services (only once every partition is routable). A corrupt or inconsistent partition becomes `PartitionSlot::Failed` with a reason naming the segment, while the others start. Deleting `state/pNNNN` rebuilds the partition from the log with the same digest, rolling hash and applied offset.

## What was built

- **node.rs:** `NodeConfig` (defaults: node id 1, `LocalLeadership`, `SystemClock`, Leader mode), `StartMode`, `PartitionSlot`, `NodeError`, and `Node` with `open/handle/slots/router/barrier/startup_events/shutdown`.
  - `node.toml` is written durably (tmp, fsync, rename, dir fsync) and checked with a small parser.
  - Validation fails a slot with "state ahead of log: rebuild the state store" (applied > LEO) or "snapshot required (Phase 8)" (applied < log start).
  - Each partition's catch-up and term start runs in its own task. A failure there shuts that partition's machine down.
  - Shutdown order: stop leader services, clear the router, graceful machine shutdowns (final checkpoint), then WAL shutdown.
- **startup.rs:** the `StartupEvent` enum, plus a crate-private ordered event log that also traces each step at info.
- **leadership.rs:** `next_term_epoch(floor, latest) = max(floor, latest + 1, 1)`.
- **clock.rs:** `SystemClock`, which is the only wall-clock read and carries the clippy allow, and `ManualClock` (`set`/`advance`/`changed`).
- **router.rs:** `PartitionRouter` with boxed futures, and `InProcessRouter` over registered `PartitionHandle<Rf1Log>`s.
- **leader/:** `LeaderCtx`, `LeaderConfig`, `LeaderTasks` and `spawn_all`. The shuffler, timer and retention stubs wait for shutdown; their signatures and config defaults are fixed as specified, ready for 02-13 and 02-14.
- **Orchestrator fix:** the `CursorMeta.outbox_truncated` doc comment now states the inclusive meaning: rows up to and including the seq are deleted.

## Verification

- `cargo test -p ostro-partition`: every suite passes. The new suites are startup_order 6, fencing 3 and rebuild_basic 2, plus unit tests for the node file, `next_term_epoch` and `ManualClock`.
  - startup_order: the per-partition event order is exactly StateOpened < LogRecovered < Validated < CaughtUp < TermStarted < BarrierOpened < LeaderServicesStarted, and every LeaderServicesStarted comes after the last BarrierOpened. Also covered:
    - In Inspect mode, `mint` returns `NotReady` and there are no term or service events.
    - `node.toml` mismatch: P=16 against stored 4 gives `PartitionCountMismatch{16, 4}`. With `node.toml` deleted, every store reports its own count mismatch.
    - A bit flip in partition 2's sealed segment fails only that slot, with the file name in the reason, and the router answers `NotLeader` for it.
    - A torn tail gives `LogRecovered{torn: true}`, the file is cut back to its prior length, and the term and submits continue at 6, 7.
    - A hand-raised applied offset fails the slot with "state ahead of log".
  - fencing: three opens give epochs 1, 2, 3. The first record of every epoch is `LeaderAnnounced{epoch}`, and the state's `epoch_history` equals the log's. Also covered:
    - A zombie `{1,1}` is rejected after a restart, `{2,1}` is accepted, and a stale-epoch handle is refused by the log.
    - A floor of 10 over log epoch 2 gives epoch 10, and the next local term is 11.
  - rebuild_basic: a P=4 workload ran 200 starts (keyed, unkeyed and delayed), grants, events with timers, cross-partition signals and completions, router deliveries and outbox truncation. After deleting p0000 and p0002, the digest, rolling hash and applied offset are equal on every partition. Deleting every state dir and reopening in Leader mode replays fully, starts epoch 2, and accepts new appends.
- `cargo clippy -p ostro-partition --all-targets [--features failpoints] -- -D warnings`: clean.
- `cargo fmt --all --check`: clean.
- `bash scripts/ci-local.sh --no-musl`: all steps passed (fmt, clippy, warning-free build, cargo test, failpoint suites, deny, vendored checks). The run included Phase 3's concurrent in-progress engine changes in the working tree.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Correctness] Floor for a partition with an unreadable state store**
- **Issue:** `LogNode::open` needs a floor for every partition. A floor of 0 for a failed state would make recovery eligible to cut that partition's tail even though its true durable floor is unknown.
- **Fix:** the floor is `Offset(u64::MAX)`, so nothing counts as torn, the log files are left untouched, and the slot is already Failed.
- **Files modified:** crates/ostro-partition/src/node.rs
- **Commit:** 7629f74

**2. Additive API**
- `StartupEvent::partition()`, `InProcessRouter::{new, register, clear}` and `tests/common::run_on` were added. The router rejects partitions that are registered but unhealthy with `NotLeader`.

### TDD note
- Task 1: the implementation and tests were written in the same step and committed together (7629f74), so there is no separate RED commit. All six behaviours were verified by running the suite before committing.
- Task 2: the tests passed against the Task 1 implementation without changes (25ecd2b, test-only commit).

## Requirements

- **STOR-09** complete: fixed P is persisted in `node.toml` and in every store's meta, and a mismatch at open is fatal. Run ids have embedded their partition since 02-03.
- **STOR-10** complete: every term begins with `LeaderAnnounced{epoch}`, a restart is a new term, and tokens are validated at apply (02-09) and rejected across restarts.
- **STOR-03** not marked: the torn tail is cut at node open before appends, but 02-15 (crash loops) still lists it.
- **STOR-06** not marked: the basic rebuild is done, but the rebuild-digest proptest is in 02-15.
- **STOR-08** not marked: the startup barrier is wired, but retention (02-14) still lists it.

## Notes for later plans

- **02-13/02-14:** fill `leader::{shuffler,timers,retention}::spawn` without changing their signatures.
  - `ctx.shutdown` turns true on `LeaderTasks::shutdown` or `Node::shutdown`.
  - `ctx.barrier` is already open.
  - Actions are on (`set_emit_actions(true)` happens before the barrier opens).
  - `ctx.router` delivers through `PartitionHandle::submit_one(Command::inbound_batch(..))`.
- **Tests:** `tests/common::open_node(dir, P, StartMode)` with `copy_dir` is the template for multi-partition tests. Inspect mode gives digest comparisons without appending.

## Commits

- aee0f92 docs(02-12): correct CursorMeta.outbox_truncated doc (inclusive watermark)
- 7629f74 feat(02-12): Node with ordered startup, leadership/clock/router, leader skeleton (STOR-08, STOR-09, STOR-03)
- 25ecd2b test(02-12): term fencing across restarts and rebuild-from-log digests (STOR-10, STOR-06)

## Self-Check: PASSED

All 12 created files exist; commits aee0f92, 7629f74, 25ecd2b present.
