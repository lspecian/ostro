---
phase: 02-durable-partition-log-state-machine
plan: 13
subsystem: storage
tags: [shuffler, outbox, exactly-once, cross-partition, leader-services, proptest, fault-injection]
requires:
  - phase: 02-10
    provides: "InboundBatch (src, seq) dedupe + SeqGap rejection, OutboxTruncated, cancel cascade, cron dedupe"
  - phase: 02-12
    provides: "Node, InProcessRouter, LeaderCtx + leader::shuffler::spawn stub signature, tests/common::open_node/run_on"
provides:
  - "leader::shuffler: per-source shuffler task, one in-flight InboundBatch per target, truncation after target ack, 50 ms..1 s backoff"
  - "ostro_partition::node::RouterWrapper + NodeConfig.router_wrapper (router test doubles for leader services)"
  - "tests/common: open_node_cfg, fast_shuffler, start_and_grant, grant, send_cmd, signal_kind, cancel_kind, submit_ok, inbox, signal_names, cancel_count, outbox_rows, wait_until, wait_drained"
  - "tests/common/flaky_router.rs: FlakyControl (seeded, restart-surviving knobs) + FlakyRouter (lost request / lost ack)"
affects: [02-15, 02-16, 04-runtime, 13-cluster-routing]
tech-stack:
  added: []
  patterns:
    - "Shuffler round per target = read lowest rows -> deliver -> OutboxTruncated(last seq) inside one spawned task; the target stays in flight until the truncation is applied"
    - "Wake-ups: OutboxAppended action, round completion, backoff deadline, idle_poll tick; Lagged action stream = full rescan"
    - "Leader-service test doubles plug in through NodeConfig.router_wrapper; their control state lives outside the Node so it survives restarts"
key-files:
  created:
    - crates/ostro-partition/tests/shuffler.rs
    - crates/ostro-partition/tests/shuffler_exactly_once.rs
    - crates/ostro-partition/tests/common/flaky_router.rs
  modified:
    - crates/ostro-partition/src/leader/shuffler.rs
    - crates/ostro-partition/src/node.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-partition/tests/rebuild_basic.rs
decisions:
  - "OutboxTruncated is submitted inside the delivery round task, so a target stays in flight until its truncation has been applied: no concurrent re-read of rows that are about to be deleted"
  - "Batches are capped by max_batch_msgs and by 1 MiB of encoded message bytes (always at least one message), well under the 4 MiB envelope limit"
  - "A failed truncation stops the shuffler only if the partition machine is unhealthy; otherwise it is retried with backoff (e.g. WAL backpressure)"
  - "Rejected batches (SeqGap or any other rejection) are logged at error and retried with backoff from a fresh read; a malformed/undecodable lowest row backs off without sending (a gap would follow)"
  - "On shutdown in-flight rounds are aborted; the next shuffler resends from the table and the target dedupes"
  - "Exactly-once property uses a proptest-generated op schedule driven by a plain async loop, not proptest-state-machine (Node is async and consumed by shutdown on Restart)"
metrics:
  duration: 40min
  completed: 2026-11-18
---

# Phase 2 Plan 13: Outbox Shuffler and Exactly-Once Delivery Summary

Each partition leader now runs a shuffler that moves its outbox rows to their target partitions. For each target it sends one ascending `InboundBatch` at a time, starting at the lowest untruncated seq. Once the target answers `Accepted` or `Duplicate`, the shuffler appends `OutboxTruncated{target, upto}` locally. Automated tests at P=16 show exactly-once delivery for all five message kinds, including across graceful restarts, lost requests and lost acks (duplicate deliveries). The outboxes always drain.

## What was built

- **`leader/shuffler.rs`**: the shuffler task, which keeps the `spawn(ctx, cfg)` signature fixed by 02-12.
  - **State:** an in-flight set of targets, a `JoinSet` of delivery rounds, and a per-target backoff (50 ms, doubling up to 1 s).
  - **A round:** `reader.prefix(Outbox, outbox_prefix(t), max_batch_msgs)` → decode → `router.deliver` → `submit_one(OutboxTruncated)`.
    - `Err`: back off, then resend from a fresh read.
    - `Rejected`: log at error, then back off.
  - **Wake-ups:** `Action::OutboxAppended`, a finished round, a backoff deadline, or the `idle_poll` tick. A lagged action stream triggers a full rescan.
  - **Metrics:** `ostro_shuffler_batches_total{src,target}`, `ostro_shuffler_retries_total`, and the gauge `ostro_outbox_rows{partition}`.
- **`node.rs` (small additive change):** adds `pub type RouterWrapper` and `NodeConfig.router_wrapper: Option<RouterWrapper>` (default `None`). If it is set, leader services get `wrap(InProcessRouter)`.
- **Test helpers:** `tests/common/mod.rs` gains the multi-partition helpers listed under `provides`. `tests/common/flaky_router.rs` adds the fault-injecting router.

## Verification

- **`cargo test --release -p ostro-partition --test shuffler`:** 6 passed in about 3 s. All 6 failed against the 02-12 stub (RED check).
  - 1,000 signals between random runs at P=16 are delivered exactly once and in send order per sender. A probe router confirmed no concurrent `deliver` for any (src, target) pair.
  - The same holds with 5 ms deliveries, and deliveries to different pairs do overlap.
  - Child start in p1 → p7 sets the parent ref. Completion gives exactly one `ChildCompleted`, and the parent's `children` count drops to 0.
  - Cancel cascade: parent in p0 → children in p3 and p12 → grandchild in p5. All four runs are `cancel_requested`, each with exactly one `CancelRequested`.
  - Two `CronStart` messages for the same (schedule, fire_time) from p2 and p11 start exactly one run on p6.
  - Restart: 300 signals were sent with a 50 ms router, and the node was shut down with rows still pending. After reopening and draining, every signal arrived exactly once.
- **`cargo test --release -p ostro-partition --test shuffler_exactly_once`:** 3 passed in about 45 s.
  - Proptest, 16 cases: up to 40 ops of Send/Cancel/Restart/SetFlakiness, with flakiness from 0 to 0.5 and at most 3 restarts. The final signal multiset and cancel flags/counts match the model, and per-sender order is preserved.
  - A fixed heavy schedule (lost ack 0.4, lost request 0.3, 180 signals, repeated cancels, 3 restarts) asserts that both fault kinds actually occurred.
  - A fault-injection sanity test shows both faults occur and order is still exact.
- **`PROPTEST_CASES=64`:** the proptest passed in 133.75 s, about 2.1 s per case.
- **Full crate:** `cargo test -p ostro-partition` passes in both debug and release, on every suite.
- **`cargo clippy -p ostro-partition --all-targets -- -D warnings`:** clean.
- **`cargo fmt --all --check`:** clean.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `rebuild_basic.rs` raced the live shuffler**
- **Found during:** full-suite run, also reported by the orchestrator (02-14).
- **Issue:** the 02-12 workload hand-delivered every outbox through the router and truncated it. With a live shuffler, `deliver` returned `Duplicate` where the test expected `Accepted`. Also, the "before" snapshot could be taken while the services were still appending.
- **Fix:** the workload now calls `settle()`, which waits until every outbox is empty and no log has grown for 300 ms. It then asserts that every partition received cross-partition signals. The file was outside `files_modified`, but the break was caused by this plan.
- **Files modified:** crates/ostro-partition/tests/rebuild_basic.rs
- **Commit:** e4a744c

**2. Additive API:** `NodeConfig.router_wrapper` / `RouterWrapper`, as the plan allowed. The shared helpers went into `tests/common/mod.rs` so both suites can use them.

**3. Test design choice:** the exactly-once suite uses a proptest op schedule driven by a plain loop, not `proptest-state-machine`. This option was allowed by the plan and is recorded in the decisions above. I added two fixed tests so that lost acks and lost requests are guaranteed to be exercised, because random schedules may draw low flakiness.

### Orchestrator note on clippy `let_unit_value`

The reported `shuffler_exactly_once.rs:223` lint came from an intermediate version of `exactly_once_heavy_faults`. I rewrote that test before committing, and clippy `--all-targets` is clean at HEAD.

## Requirements

- **STOR-12 complete.** The apply half (inbound dedupe, gap rejection, truncation) was done in 02-10. This plan adds the shuffler and proves exactly-once delivery across P=16 with restarts and flaky acks.

## Commits

- 98ac7b2 feat(02-13): outbox shuffler with single in-flight batch per target (STOR-12)
- 6bcabec test(02-13): randomised exactly-once property with flaky acks and restarts (STOR-12)
- e4a744c fix(02-13): rebuild_basic relies on the live shuffler instead of a hand-run shuffle

## Self-Check: PASSED

Created files exist (shuffler.rs, shuffler_exactly_once.rs, flaky_router.rs); commits 98ac7b2, 6bcabec, e4a744c present.
