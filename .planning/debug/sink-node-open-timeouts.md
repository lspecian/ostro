---
status: resolved
trigger: "cargo test -p ostro-server --test sink --locked fails 6-7/19 (varying) with 'node open timed out: Elapsed(())' at sink.rs:42 (10 s bound); common/mod.rs:187 (20 s) tripped in resume_rearm; load avg 15-34. Last 19/19 at 453bd42 (06-14)."
created: 2026-11-20T14:35:00Z
updated: 2026-11-20T14:58:00Z
---

## Current Focus

hypothesis: CONFIRMED H0 - no regression; load-bound fresh opens vs a 10 s outlier bound
test: done
expecting: n/a
next_action: none (resolved; harness aligned on common::OPEN_T)

## Symptoms

expected: sink 19/19 (as at 453bd42)
actual: 6-7/19 fail per run, different set each run, all at sink.rs:42 `node open timed out: Elapsed(())` (T = 10 s); common/mod.rs:187 (T = 20 s) tripped once in resume_rearm (passes 4/4 alone)
errors: node open timed out: Elapsed(())
reproduction: cargo test -p ostro-server --test sink --locked under load avg 15-34
started: after 453bd42; commits since: 08-13 (29938fb, 6450244, c50e23b), 08-19 (62f4519, b65095d), 08-14 (57e658c, e50ba14), 06-15..06-17, 6983bb2; 08-09 earlier

## Eliminated

- hypothesis: H1 08-14 cron driver made Node::open slower
  evidence: 57e658c is an ancestor of 453bd42 (the passing build); ostro-partition/src identical between 453bd42 and main
  timestamp: 2026-11-20T14:40Z

- hypothesis: H2 08-09 snapshot scan / recover_install on open
  evidence: 13265ac is an ancestor of 453bd42; open path identical
  timestamp: 2026-11-20T14:40Z

- hypothesis: H3 08-19 blob GC (gc lock, mtime touch) adds work to Node::open
  evidence: BlobStore::open unchanged, GcLock::default is an in-memory RwLock; touch fsync only on duplicate put. Measured 1-partition Node::open: fresh median 1731 ms (453bd42) vs 1791 ms (main), n=50 each, interleaved, load 6-15; reopen ~50 ms both
  timestamp: 2026-11-20T14:46Z

- hypothesis: any regression at all between 453bd42 and main (sink pass-rate difference)
  evidence: same machine, interleaved runs of both binaries fail identically under load: 4x dd fdatasync -> 0/19 both (x2 each, load 13-20); 1x dd + ambient agents (load 22-43) -> 453bd42 0/19, 7/19, 19/19 vs main 0/19, 19/19, 12/19; all failures sink.rs:42 'node open timed out: Elapsed(())'. At load ~7 both 19/19 (serial 44.6 s vs 38.1 s; parallel 5.1 s vs 7.0 s)
  timestamp: 2026-11-20T14:53Z

## Evidence

- timestamp: 2026-11-20T14:35Z
  checked: .planning/debug/node-open-stall.md
  found: fresh fjall PartitionStore::open ~295 serialized fsyncs; under load fsync p50 37 ms / p95 96 ms -> fresh opens up to ~11 s; partition tests' OPEN_T raised to 120 s (9bc882a)
  implication: a 10 s bound on a fresh open is already known to be exceedable under load without any regression

- timestamp: 2026-11-20T14:40Z
  checked: git merge-base --is-ancestor for each "landed since" commit vs 453bd42
  found: 57e658c (08-14 cron driver, the Node::open change), 13265ac (08-09 snapshot open-path install) and 29938fb (08-13) are ALREADY ancestors of 453bd42 (parallel executors; 57e658c committed 13:10, 453bd42 13:19). New since 453bd42: 6450244, c50e23b, e50ba14, 62f4519, b65095d, 06-15/16/17/18/19, 6983bb2
  implication: H1 (cron) and H2 (snapshot scan) cannot explain a 453bd42 -> main difference; they were in the passing build

- timestamp: 2026-11-20T14:42Z
  checked: git diff 453bd42 main -- crates/ostro-partition/src crates/ostro-log/src; crates/ostro-blob/src; sink.rs; tests/common/mod.rs
  found: ostro-partition/src and ostro-log/src identical. ostro-blob/src/lib.rs: BlobStore gains gc_lock: GcLock (derive(Default) over std RwLock<()>, no I/O); put() takes the lock shared and, for an already-present blob, touch()es it (set_modified + sync_all = 1 extra fsync per duplicate put). BlobStore::open body unchanged. sink.rs unchanged; common/mod.rs only adds `pub mod fx;`
  implication: by code, Node::open does exactly the same work at both commits. H3 can only affect put latency for duplicate blobs (not open). Measurement still required to rule out an environmental/build difference

- timestamp: 2026-11-20T14:46Z
  checked: scratch probe dbg_sink_open_probe (10x fresh open + reopen of 1-partition Node, 5 rounds per commit, alternating order)
  found: fresh ms 453bd42 median 1731 / mean 1827 / p90 1950 / max 4501; main median 1791 / mean 1945 / p90 2410 / max 4267; reopen 36-152 ms both (one 1928 ms outlier at 453bd42). Loads 6.0-14.9
  implication: no material difference (~3% median, inside run-to-run noise at varying load)

- timestamp: 2026-11-20T14:50Z
  checked: sink binary at both commits: --test-threads=1 and default, at load ~7; then under 4x dd(800 MB, fdatasync) loops; then 1x dd loop with other agents building (load 22-43)
  found: load ~7: both 19/19. Heavy fsync load: both 0/19 every run. Mid load: varying failing sets at both commits (453bd42 7/19 pass in one run, main 12/19 in another), always sink.rs:42 10 s bound
  implication: the reported symptom reproduces at 453bd42; it is load, not code. sink runs 19 tests in parallel, each creating a fresh store => ~19x295 concurrent fsyncs; 10 s is the tightest open bound in the workspace (common 20 s, derive_phase5 30 s, ostro-partition OPEN_T 120 s)

- timestamp: 2026-11-20T14:54Z
  checked: Node::open bounds in crates/ostro-server/tests
  found: sink.rs:40 T=10 s (own const), derive_phase5.rs:704 T=30 s (own const), common/mod.rs:189 TestNode::open_with T=20 s (used by resume_rearm, executor*, leases, failpoints_phase5)
  implication: three different bounds for the same fsync-bound operation; align on one OPEN_T

- timestamp: 2026-11-20T14:56Z
  checked: fixed harness (OPEN_T = 120 s for Node::open in sink, derive_phase5, TestNode::open_with)
  found: sink 19/19 x3 (19.9 s / 11.6 s / 6.7 s, load 21-24); sink 19/19 under the 4x dd fdatasync load that gave 0/19 at both commits (10.0 s, load 16-21); resume_rearm 4/4 (load 18); derive_phase5 16/16; clippy -p ostro-server --tests -D warnings clean; rustfmt clean
  implication: the timeouts were the bound, not the code

## Resolution

root_cause: Not a regression. Node::open's code path is identical at 453bd42 and main (the cron/snapshot open changes were already in 453bd42; 08-19 only touches BlobStore::put). A fresh 1-partition open costs ~295 serialized fjall fsyncs (node-open-stall.md); sink runs 19 fresh opens in parallel, and under machine-wide I/O load (load 15-43 from parallel agents) fsync latency pushes them past sink.rs's 10 s bound - the tightest Node::open bound in the workspace. 453bd42 fails identically under the same load; its 19/19 was a quieter machine.
fix: Decision - align, do not raise per-file. crates/ostro-server/tests/common/mod.rs gains `pub const OPEN_T: Duration = 120 s` (same value/reasoning as ostro-partition tests' OPEN_T, documented with these measurements). Used for Node::open in TestNode::open_with (was T = 20 s), sink.rs open() (was T = 10 s) and derive_phase5.rs open() (was T = 30 s); both files now `mod common;`. Other bounds (appends, shutdown, MetaHandle::start, which is raft-based, not fjall) unchanged. No product change: Node::open has not regressed.
verification: sink 19/19 x3 at load 21-24, plus 19/19 under 4x dd fdatasync (0/19 before at both commits); resume_rearm 4/4; derive_phase5 16/16; clippy -D warnings clean for ostro-server tests; fmt clean.
files_changed: [crates/ostro-server/tests/common/mod.rs, crates/ostro-server/tests/sink.rs, crates/ostro-server/tests/derive_phase5.rs]
