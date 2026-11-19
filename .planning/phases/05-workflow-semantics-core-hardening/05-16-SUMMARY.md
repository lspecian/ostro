---
phase: 05-workflow-semantics-core-hardening
plan: 16
subsystem: performance
tags: [perf, nfr-2, d15, d16, ops-06, dispatcher, pipelined-grant, start-path, run-started, benchmark, wsl2]

requires:
  - phase: 05-10
    provides: "trivial-steps guest (profiles A/B)"
  - phase: 05-11
    provides: "run_started::first_events/build_run_started (D23 strip, continued_from), executor first activation"
  - phase: 05-13
    provides: "ControlPlane start/await_result/run_history, retention on start"
  - phase: 05-15
    provides: "semantics_e2e/lifecycle_api pre-start pins, WF-04 sleepers baseline, in_flight_jobs probe"
provides:
  - "crates/ostro-server/tests/perf.rs: #[ignore] benchmark (profiles A/B/expiry, inproc|http, verify_on_disk after restart)"
  - "docs/benchmarks.md: D16 definitions, commands, native NVMe human_needed procedure, WSL2 baseline / before-after / Phase 5 results"
  - "Pipelined AttemptGranted in the dispatcher (job starts once the grant's offset is fixed) + one record read per dispatch"
  - "DispatcherStats.leases gauge (real lease probe), grants_rejected, grant_fault test hook"
  - "Apply support for StartRun.run_started (tag 23): RunStarted verbatim at seq 0, next_seq 1, B1 hook"
  - "run_started::encode_for_start / record_of_start (API-side pre-encoding, byte-identical to the executor's)"
affects: [05-17, 05-18, 05-19, 05-20]

tech-stack:
  added: []
  patterns:
    - "Pipelined log append: append through ReplicatedLog::append (offset fixed under the sequencer lock), start dependent work, confirm the outcome later with machine.wait_applied + a record read"
    - "Benchmarks count acknowledged-and-on-disk work only: restart on the same dir, count through the history API"
    - "Device flush counter (/proc/diskstats field 19) as the fsync/s proxy when no WAL metrics accessor is reachable"

key-files:
  created:
    - crates/ostro-server/tests/perf.rs
    - crates/ostro-partition/tests/apply_run_started.rs
    - docs/benchmarks.md
  modified:
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/tests/dispatcher.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-server/src/run_started.rs
    - crates/ostro-server/src/executor.rs

key-decisions:
  - "Pipelined grant kept: the apply token check stays the authority (ack task compares current_attempt after wait_applied); a new grant of a run waits for an unacknowledged earlier one and goes above a rejected grant's token, so no two concurrent grants and zombies stay fenced"
  - "StartRun.run_started is supported by apply but NOT set by the API: it breaks D3's empty pre-start journal pinned by 05-13/05-15 suites; measured benefit (idle p50 9.7 -> 3.4 ms, ~30k steps/s) recorded; decision carried to 05-20"
  - "Latency gate measured on an idle node (OSTRO_BENCH_CONCURRENCY=1, NFR-2.4 'idle'); throughput gate at 64 clients"
  - "fsyncs/s reported as device flushes/s (no WAL metrics accessor or metrics recorder is reachable from ostro-server tests without a new dependency)"

patterns-established:
  - "Grant pipelining: unacked map (wait before regrant), rejected-token floor, EnqueueReady classification by record read while the grant is unacknowledged"

requirements-completed: [OPS-06]

duration: ~3h
completed: 2026-11-19
---

# Phase 5 Plan 16: Perf harness + start-path optimisations Summary

**D16 benchmark harness (acknowledged and verified on disk after a restart) with WSL2 numbers recorded, plus a pipelined AttemptGranted that lifts profile A from about 16k to 20k steps/s. `StartRun.run_started` cuts idle start → first step from 9.7 to 3.4 ms when the API sets it. Apply supports the field, but the API does not set it yet because D3 pins an empty journal before start.**

## Performance

- **Duration:** about 3 h (most of it release builds with fat LTO and benchmark runs)
- **Started:** 2026-11-19T12:55Z
- **Completed:** 2026-11-19
- **Tasks:** 3
- **Files modified:** 8 code/doc files + planning docs

## Accomplishments

- `perf.rs`: profiles A (10-step runs), B (1-step runs) and expiry (50,000-event run, retention 1 s). Transport is `inproc` or `http`, and every option is set by env. Steps count only after `verify_on_disk`: the harness restarts on the same data dir, counts `step_completed` through the history API, and fails on any mismatch. It reports fs type (refuses tmpfs), kernel, CPUs, steps/s, runs/s, p50/p99 start → first step, CPU, device flushes/s, raw fdatasync cost and bytes on disk.
- Pipelined grant in the dispatcher, with fencing intact. A test runs 1,000 runs with about a third of first grants rejected by apply: every run completes exactly once, per-run tokens strictly increase, and every token's first append is at a higher offset than its grant.
- Dispatcher record reuse: one `read_run` per dispatch instead of two (the grant path reads it once and the job gets it, patched with the token).
- Apply support for `StartRun.run_started`, with tests. The API-side encoder is byte-identical to the executor's synthesis.
- Real `DispatcherStats.leases` gauge, which answers the 05-15 "no dispatcher lease gauge" gap. TimerStats are still not exposed; see Hand-off.
- OPS-06 expiry cost measured in release: apply blocked for **44.7-44.8 ms** for a 50,000-event run, far below the 1 s threshold, so no bounded-delete risk is carried.

## Task Commits

1. **Task 1: Benchmark harness + baseline WSL2 measurement:** `fa469db` (test)
2. **Task 2a: Pipelined AttemptGranted + one record read per dispatch:** `0ec6391` (feat)
3. **Task 2b: StartRun.run_started apply support (API switch reverted):** `db27f4b` (feat)
4. **Task 3: Final WSL2 measurement + human_needed gate record:** `37d24b9` (docs)

## WSL2 numbers (advisory, D15)

Release build, P=16, WSL2 6.6.87.2, 12 logical CPUs, ext4 on the virtual disk. Idle = 1 client, 300 runs. The full tables are in `docs/benchmarks.md`.

**Same-session before/after** (fdatasync 2.4-3.1 ms):

| change | A inproc steps/s (64 clients) | A http steps/s | idle start→first step p50 / p99 | idle http p50 | B inproc runs/s |
|---|---|---|---|---|---|
| baseline (21d9e58) | 15,598 / 16,513 / 16,184 | 15,839 | 9.83 / 13.4 ms | 11.1 ms | 1,667 |
| + pipelined grant, one read per dispatch (kept) | 20,061 / 20,854 | 18,966 | 9.72 / 15.1 ms | 10.7 ms | 1,622 / 1,428 |
| + `StartRun.run_started` from the API (measured, **not kept**) | 30,001 / 30,363 | 29,759 | 3.38 / 5.5 ms | 3.56 ms | 3,213 |

**Final (Phase 5 results):**
- profile A, 64 clients: **19,589-20,110 steps/s** at 2.6-3.1 ms fsync (12,068-15,740 when the virtual disk's fsync rose to 4-7 ms); http 19,385;
- idle start → first step p50: **7.4-9.7 ms** at 2.2-2.6 ms fsync (18.6-19.5 ms at 4.6 ms);
- profile B: 1,018-1,271 runs/s at 4.2-4.9 ms fsync;
- device flushes 390-850/s; CPU 1.2-3.3 cores; 8.97 MB on disk for 2,000 × 10-step runs, 25.3 MB for 10,000 × 1-step runs.

Verdict: the profile A target (≥ 5,000) is cleared on WSL2. The idle p50 target (< 5 ms) is not met on WSL2: the path is **fsync-bound**, with three WAL commits in series before the first step, each costing 2.2-4.7 ms here, and p50 tracks the fsync cost. The `run_started` switch (carried to 05-20) removes one commit and measured 3.4 ms.

**Expiry (OPS-06):** 50,000 events block apply for 33.6-44.8 ms; the record is gone 37-48 ms after its due time.

**WF-04 sleepers after the changes** (debug, serial, Task 2 run; the ci-local serial step also passed): 1,000 REST starts in 528-558 ms (baseline about 380 ms), 1,000 2 s sleepers drained in 2.03 s (baseline about 2.2 s), RSS 16.5 KiB per sleeper (baseline 11.4 KiB, bound 64 KiB). All of these are within noise for process-wide RSS and a busy WSL2 disk. The start time includes the same fsync-bound StartRun commit.


## Deviations from Plan

### Optimisation 2 (`StartRun.run_started`) reverted at the API, kept in apply (plan rule)

- **Found during:** Task 2, while running the 05-13/05-15 suites with the API setting the field.
- **What happened:** `control/runs.rs` pre-encoded RunStarted through `run_started::encode_for_start` into tag 23. Apply journaled it at seq 0. Three suites then failed by construction:
  - `semantics_e2e::schedule_to_start_without_capacity` (empty journal and `next_seq == 0` for an s2s timeout and for terminate-before-start, D3);
  - `semantics_e2e::signal_immediately_after_start` (asserts the B1 Pending path: "nothing journaled before RunStarted");
  - `lifecycle_api::cancel_before_start_is_flag_only_and_idempotent` (`next_seq == 0` before start).
- **Why it is not local:** the fix means amending D3 (a never-started run would carry RunStarted, and terminate-before-start would journal RunTerminated) and editing other plans' test files.
- **Measured benefit with the API setting the field (release, WSL2):**
  - profile A: 30,001 and 30,363 steps/s (opt 1 alone: 20,061 and 20,854);
  - idle start → first step p50 3.38/3.36 ms, p99 5.5 ms (opt 1 alone: 9.72 ms);
  - http idle p50 3.56 ms;
  - profile B: 3,213 runs/s.
- **What landed:**
  - apply support with its tests (`apply_run_started.rs`);
  - `encode_for_start`/`record_of_start` with a byte-identity unit test;
  - executor docs.
  The API leaves `run_started: None`, so behaviour is unchanged. Enabling it later means setting the field in `runs::start_run` and in `lifecycle::restart`'s hand-built StartRun (05-13 hand-off), plus the D3 test updates.

### Auto-fixed issues

**1. [Rule 1 - Bug] A rejected pipelined grant whose job reported `Fenced` first left the run un-dispatched**
- **Found during:** Task 2 (the new 1,000-run rejection test: 992/1000 completed).
- **Issue:** the job's fenced append came back before the grant's ack. `Fenced` dropped the lease, and the later rejected ack saw a non-current lease and did nothing.
- **Fix:** on a rejected or unknown ack, if neither a lease nor a queued entry exists for the run and it is still live, re-queue it from the record. The next grant goes above the rejected token (`rejected` floor), so the zombie stays fenced.
- **Files modified:** `crates/ostro-partition/src/leader/dispatcher.rs`. **Commit:** `0ec6391`.

**2. [Rule 1 - Bug] Two existing dispatcher tests read `current_attempt` at job start**
- With a pipelined grant the job may start before apply has applied the grant. `rebuild_after_restart_...` and `stalled_executor_...` now poll (`wait_attempt`). The property they check is unchanged.

**3. [Rule 3 - Blocking] The bench held the control plane across its restart**
- The `Arc<ServerControlPlane>` kept the old server's meta and partition handles alive, so the restarted server never became ready. It is now dropped before `verify_on_disk`. Bench-only, fixed before the first commit.

**4. Lints:** `clippy::collapsible_if` in `apply/run.rs`, and `type_complexity` in the new dispatcher tests (type aliases). Both fixed in the Task 2b commit.

### Measurement notes (not deviations of code)

- **fsyncs/s** is the block device's completed flush count (`/proc/diskstats` field 19). The WAL's `ostro_wal_batches_total` and the `GroupCommitWal::get_metrics` accessor cannot be reached from an ostro-server test without a new dependency (a metrics recorder) or a production accessor outside this plan's files.
- **Start → first step** is measured to the first `StepCompleted` journal ts (the engine's wall clock at record time), per the plan. The latency gate uses one client (NFR-2.4 "idle"). Under 64 clients the figure is mostly queueing.
- **No TimerStats probe.** 05-15 asked for real TimerStats in `ServerHandle` ("if you can"). That needs `server.rs` (`TimerConfig.stats` is `None` there), which is outside `files_modified`, so it was not done. The dispatcher lease gauge did land, through `DispatcherStats`, which ServerHandle already exposes.


## Gate

- Task 2 verify, after both optimisations (debug, worktree `wt-05-16`, `CARGO_TARGET_DIR=target/wt-05-16`):
  - `cargo test -p ostro-partition --locked` (29 binaries) and `cargo test -p ostro-server --locked` (19 binaries): all green;
  - `semantics_sleepers -- --ignored --test-threads=1`: green;
  - `ostro-engine --test replay_corpus`: green;
  - clippy `-p ostro-partition -p ostro-server --all-targets -D warnings`, with and without `--features failpoints`: clean.
- The new dispatcher suite (17 tests, including the 1,000-run rejection test) passed in 5 consecutive runs.
- With the API experiment applied, the three D3-pinned tests failed, as described in the deviations, and passed again after the revert.
- **`bash scripts/ci-local.sh --no-musl`** on the committed code: **all steps passed on the first run.** Steps: fmt, clippy, the warning-free build, SDK, guests, order-saga, cargo test (610 s), sleepers (serial), failpoint suites (602 s), deny, vendored, deny fixtures, the chronik grep and workflow validation; musl was skipped. No flake, so no reruns were needed.
- Task 1/3 verify: `OSTRO_BENCH_PROFILE=a OSTRO_BENCH_RUNS=200 ...` and the default A/B runs pass (release), and `docs/benchmarks.md` has the "native" and "human_needed" sections and "Phase 5 results".

## Self-Check: PASSED

- FOUND: crates/ostro-server/tests/perf.rs (contains `verify_on_disk`, `restart`)
- FOUND: crates/ostro-partition/tests/apply_run_started.rs
- FOUND: docs/benchmarks.md (contains `Phase 5 results`, `human_needed`, `native`)
- FOUND commits on main: fa469db, 0ec6391, db27f4b, 37d24b9


## Hand-off notes

### 05-17 (order-saga, quickstart, docs)
- The start path you exercise is the final one: pipelined grant, executor RunStarted append; the API does not set `StartRun.run_started`. Nothing observable changed for clients: GET, history and journals are identical, and a never-started run still has an empty journal.
- Docs can state the benchmark definition (D16) and point to `docs/benchmarks.md`. Gate numbers are human_needed on native NVMe.

### 05-18 (failpoints)
- Candidate new failpoint: between the dispatcher's grant append and its ack (the `pipelined_grant` → `spawn_ack` window), so a crash lands with a granted-but-unapplied attempt while the job has already started. Recovery is unchanged: the rebuild re-grants every Running run in the new epoch.

### 05-19 (kill campaign)
- Kills can now land between a grant's append and its apply, with the executor's first append already submitted behind it. Invariant to check: the journal never holds events whose token was not granted before them in log order. The rejection test's log scan (`tests/dispatcher.rs::pipelined_grants_never_overlap_and_rejections_are_fenced`) is a reusable checker shape: per-run AttemptGranted tokens strictly increase, and every token's first EventsAppended has a higher offset.
- `DispatcherStats.leases` is a real gauge (shared across partitions, moved by deltas), usable as a "no lease leaks" probe after a campaign.

### 05-20 (gate)
- **Decision needed (D3 vs start latency).** With `StartRun.run_started` set by the API, idle start → first step p50 is 3.4 ms on WSL2, under NFR-2.4's 5 ms. Without it the figure is about 9.7 ms on WSL2; the native NVMe figure is still to be measured. Enabling it means:
  - amend D3: a never-started run's journal holds RunStarted at seq 0, and terminate or s2s timeout before start journal `RunTerminated` at seq 1;
  - update the three pinned tests;
  - set the field in `runs::start_run` and `lifecycle::restart`.
  Apply already supports it. If it is enabled, the 05-12 format corpus must include a StartRun carrying tag 23 (05-12 hand-off: the corpus is generated at 05-20 after this plan).
- Re-measure on native NVMe with the documented commands (human_needed) and record them in 05-VALIDATION.md.
- **Expiry cost:** 44.7-44.8 ms of apply blocking for 50,000 events (release, WSL2). No bounded-delete continuation needed; no 05-20 risk.
- **Next optimisation candidates** (if the native numbers miss):
  1. the `run_started` switch above;
  2. a pipelined executor RunStarted append (synthesise seq 0 in memory and let the first flush follow it in log order; keeps D3, but needs sink changes);
  3. `PartitionSink::new` re-reads the run record (one more read per job; could take `Job.record.next_seq`);
  4. HTTP overhead (about 10% at 64 clients, 1 ms at idle);
  5. journal reads in the executor (`load_journal` decodes the whole journal per job).
- RESERVATIONS: StartRun field 23 can be flipped to `exists` (apply reads it now).


## STATE notes (for the orchestrator; STATE.md not edited, no gsd-tools mutators run)

- Position: 05-16 complete (wave 7). Phase 5: 20/24 SUMMARYs.
- Decisions to record:
  - the pipelined grant (apply token check stays the authority; unacked wait and rejected-token floor);
  - `StartRun.run_started` supported by apply but not set by the API until D3 is revisited at 05-20;
  - the latency gate is measured at 1 client.
- Gate status: **human_needed**. Run the documented commands on native Linux NVMe with 8 vCPU and record the results in 05-VALIDATION.md.
- No blockers.
