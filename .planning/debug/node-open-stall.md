---
status: awaiting_human_verify
trigger: "Intermittent 'node open timed out: Elapsed(())' in rebuild_digest::live_state_equals_rebuilt_state (and shuffler_exactly_once, startup_order) under load avg 15-30; OPEN_T=30s"
created: 2026-11-19T00:00:00Z
updated: 2026-11-19T12:12:00Z
---

## Current Focus

hypothesis: open time is dominated by fresh fjall state-store creation (~294 serialized fsyncs per store); under I/O load fsync latency x 294 exceeds OPEN_T. No liveness bug.
test: 3 suites in parallel under dd(fdatasync)x4 + 10 CPU spinners, OPEN_T=300s, stage tracing
expecting: slow fresh opens (state-store stage), all complete; reopens stay fast
next_action: user confirms ci-local under load no longer times out; then archive to resolved/

## Symptoms

expected: 16-partition Node::open completes in ~1.3 s fresh, ~100-170 ms reopen
actual: occasionally exceeds 30 s OPEN_T -> "node open timed out: Elapsed(())"
errors: node open timed out: Elapsed(())
reproduction: run partition tests during ci-local with other cargo builds loading the machine (load 15-30); reruns usually pass
started: observed since at least 05-02 (f079dd7 raised OPEN_T 10s -> 30s)

## Eliminated

- hypothesis: lost wakeup on a watch (HW / applied / health / barrier) during catch-up or LeaderAnnounced
  evidence: all waits check the current value first (code read); catch-up + LA stages <= 1.0 s in every one of ~460 traced opens, including under load 58
  timestamp: 2026-11-19T12:02Z

- hypothesis: tokio worker starvation from blocking I/O on async threads
  evidence: store/blob open and recovery are spawn_blocking; the slow stage is entirely inside the spawn_blocking PartitionStore::open threads (strace: fsync), with log/catch-up/LA fast
  timestamp: 2026-11-19T12:02Z

- hypothesis: recovery blocked on another partition's lock / ordered startup barrier
  evidence: LogNode::open stage <= 0.03 s in all traced opens; barriers are opened after all partitions started and are not awaited by open
  timestamp: 2026-11-19T12:02Z

## Evidence

- timestamp: 2026-11-19T11:40Z
  checked: code read of Node::open, PartitionStart::catch_up_and_announce, MachineHandle (wait_applied, health, wait_failed, apply loop), PendingAcks, Rf1Log append/wait_committed, LogNode commit callback, WAL committer loop, ReadyBarrier
  found: every watch wait checks the current value before awaiting (wait_for / borrow_and_update then changed); PendingAcks keeps outcomes resolved before registration; the WAL committer has an interval tick in addition to Notify, so a lost notify is bounded by max_wait_time_ms (2 ms); state store open, blob open and log recovery run in spawn_blocking; the apply machine is a dedicated OS thread using Handle::block_on
  implication: no lost-wakeup pattern or blocking-on-async-worker stall found on the open path

- timestamp: 2026-11-19T11:49Z
  checked: rebuild_digest live_state_equals_rebuilt_state (24 cases, 137 opens) with per-stage OTRACE timing, background load ~7-13 (other executors)
  found: reopens of an existing store: state stage 8-58 ms. Fresh store creation (first open of 'live', every 'rebuild-a/b' after state deletion): 1.08-2.6 s, one 10.28 s (total open 10.31 s). Log recovery, catch-up and LeaderAnnounced stages <= 0.1 s in every open
  implication: the slow part is PartitionStore::open on a fresh directory

- timestamp: 2026-11-19T11:50Z
  checked: strace -f -y of startup_order fresh_node_startup_order (4 partitions)
  found: 1205 fsync/fdatasync in the test; 294 for state/p0000 alone, 291 of them on ONE thread (serialized): fjall meta keyspace (keyspaces/0, its current/ and tables/ dirs) rewritten per created keyspace, plus 3 per user keyspace. Fresh state stage took 3.1 s there
  implication: fresh open cost ~= 294 x fsync latency; 10.3 s => ~35 ms/fsync; 30 s needs ~100 ms/fsync, plausible at load 15-30 with GB writers on WSL2 ext4

- timestamp: 2026-11-19T12:00Z
  checked: rebuild_digest + shuffler_exactly_once + startup_order in parallel under 4x dd(1.5 GB, fdatasync) loops + 3x buffered dd(4 GB) loops + 10 CPU spinners + other executors (load 28-58); OPEN_T=300 s; fsync probe (4 KiB file + dir fsync)
  found: ~270 opens, all completed; worst 8.1 s; every slow open in the fresh state-store stage; probe fsync p50 37 ms, p95 96 ms, max 1.58 s
  implication: slow, never stuck

- timestamp: 2026-11-19T12:02Z
  checked: strace -ff -T of fresh_node_startup_order at load ~58 (state stage 8.4 s)
  found: each of the 4 store-creating threads: 291 fsyncs summing 6.58-6.80 s (~23 ms each), futex ~1.0-1.8 s (fjall-internal), everything else < 0.2 s
  implication: fresh open time ~= 291 x fsync latency; no wait outside fjall's own I/O

## Resolution

root_cause: Not a liveness bug. Node::open is fsync-bound on fresh state stores. fjall 3.1.13 Database::keyspace() on a new DB calls meta_keyspace.create_keyspace (an lsm ingestion: new table + version, fsynced with their dirs) for every keyspace, serialized under the keyspaces write lock, so PartitionStore::open on an empty dir issues ~295 fsyncs on one thread. A fresh open costs ~295 x fsync latency (partitions in parallel). Under heavy I/O on WSL2 ext4 (data=ordered, GB writers) fsync averages tens to hundreds of ms, pushing fresh opens past the 30 s OPEN_T. rebuild_digest is most exposed: every case does 3 fresh-store opens (live + 2 rebuilds after state deletion).
fix: common::OPEN_T 30 s -> 120 s, doc records the measurements; timers/fencing/trim_safety use OPEN_T for Node::open (they used T = 10 s). dispatcher.rs (05-11 owned) needs the same 2-line change, reported as a patch.
verification: 678 traced opens under a 300 s limit with load 28-58 (dd fdatasync x4, buffered dd x3, CPU spinners, parallel binaries): all completed, max 10.9 s, all slow ones in the fresh state-store stage; strace: 6.7 s of an 8.4 s stage in 291 fsyncs. After the fix: startup_order, timers, fencing, trim_safety, rebuild_digest, shuffler_exactly_once pass; clippy --tests clean; rustfmt clean. Commit 9bc882a.
files_changed: [crates/ostro-partition/tests/common/mod.rs, crates/ostro-partition/tests/fencing.rs, crates/ostro-partition/tests/timers.rs, crates/ostro-partition/tests/trim_safety.rs]
