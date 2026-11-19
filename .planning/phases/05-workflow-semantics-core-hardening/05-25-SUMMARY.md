---
phase: 05-workflow-semantics-core-hardening
plan: 25
subsystem: api
tags: [d24, d3, run-started, start-path, latency, nfr-2.4, perf]

requires:
  - phase: 05-16
    provides: "StartRun.run_started (tag 23) apply support; run_started::encode_for_start / record_of_start (byte-identical to the executor synthesis); WSL2 measurement of the switch"
  - phase: 05-13
    provides: "ServerControlPlane start path and lifecycle restart (hand-built StartRun{retry_of})"
  - phase: 05-15
    provides: "semantics_e2e suites that pinned D3 (schedule_to_start_without_capacity, signal_immediately_after_start)"
provides:
  - "control::runs::attach_run_started: pre-encodes RunStarted into StartRun.run_started via prepare_payload (inline <= 256 KiB, else a durable blob)"
  - "API start (REST/gRPC) and retry-restart journal RunStarted at seq 0 inside the StartRun apply (D24)"
  - "D24 in 05-DECISIONS.md (amends D3); StartRun 23 semantics in docs/storage-format.md"
  - "Same-session WSL2 before/after in docs/benchmarks.md"
affects: [05-17, 05-19, 05-20]

tech-stack:
  added: []
  patterns:
    - "Submitter pre-encodes a journal event (D21 form) and apply journals it verbatim; the executor skips its own append when next_seq > 0"
    - "Interleaved A/B perf binaries (cargo --no-run, copy, patch, rebuild) to compare variants on a noisy virtual disk"

key-files:
  created:
    - .planning/phases/05-workflow-semantics-core-hardening/05-25-SUMMARY.md
  modified:
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/src/control/lifecycle.rs
    - crates/ostro-server/src/run_started.rs
    - crates/ostro-server/tests/semantics_e2e.rs
    - crates/ostro-server/tests/lifecycle_api.rs
    - crates/ostro-server/tests/start_run_api.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-DECISIONS.md
    - docs/storage-format.md
    - docs/benchmarks.md
    - .planning/ROADMAP.md

key-decisions:
  - "D24 (orchestrator, amends D3): the API sets StartRun.run_started; a never-activated API run has RunStarted at seq 0 (next_seq == 1)"
  - "Pre-start signals and cancels on API runs are journaled directly at seq >= 1; B1 Pending slots stay for inbound, child and continue-as-new starts"
  - "D3's 'RunTerminated only when next_seq > 0' rule is unchanged; it now always holds for API runs, so terminate or s2s timeout before start journals RunTerminated at seq 1"
  - "The encoded RunStarted goes through prepare_payload: a >256 KiB input makes both the input and the RunStarted event blobs (the BlobRef invariant holds)"
  - "restart reads the predecessor's input bytes (fetch_payload; a blob is read) to build the successor's RunStarted"

requirements-completed: []

duration: ~1h45m
completed: 2026-11-19
---

# Phase 5 Plan 25: D24 RunStarted at start Summary

**The API start and retry-restart paths now set `StartRun.run_started`, so `RunStarted` is journaled at seq 0 inside the StartRun apply. One WAL commit leaves the start path. Same-session WSL2 A/B: idle start→first step p50 ~24 → ~8.5 ms at ~8 ms fsync (5.7 ms at 3.3 ms fsync), and profile A at 64 clients ~12.2k → ~18.1k steps/s.**

There is no PLAN file. The orchestrator's brief is the plan, and it records decision D24, which amends D3.

## Performance

- **Duration:** about 1 h 45 min, of which about 1 h was release builds and benchmarks
- **Completed:** 2026-11-19
- **Tasks:** 4
- **Files modified:** 10

## Accomplishments

- `control::runs::attach_run_started(handle, StartRun, input)` does three things:
  - encodes the run's `RunStarted` with `run_started::encode_for_start`. The encoding is byte-identical to the executor's synthesis: D23 stripped workflow name, tenant, deadline, `continued_from`.
  - passes the bytes through `prepare_payload`;
  - sets tag 23 to `JournalEvent{seq: 0, kind: RunStarted, payload}`.
- `start_run` uses it after `prepare_payload(input)`. The input `Bytes` are cloned, which is cheap. `dry_run` returns before any of this, so a dry run encodes nothing, writes no blob and submits nothing.
- `lifecycle::restart` uses it for the hand-built `StartRun{retry_of}`. The successor's RunStarted carries the predecessor's input bytes: a blob ref is fetched, and a missing input is `null`. It has no `continued_from`. The stored qualified workflow name is reused, and `encode_for_start` strips it for the journal.
- Continue-as-new successors, inbound outbox starts and child starts still leave the field empty. Those runs keep the executor's RunStarted append, the B1 Pending slots, and `first_events`' `CancelRequested` at seq 1.
- Tests encode D24 (below). `docs/storage-format.md` documents StartRun 23 as always set by the API. D24 is in 05-DECISIONS.md. `docs/benchmarks.md` has the same-session before/after.

## Task Commits

1. **Task 1: API start path sets StartRun.run_started:** `4630500` (feat)
2. **Task 2: D24 test updates:** `f340835` (test)
3. **Task 3: D24 decision + storage-format:** `8863a1e` (docs)
4. **Task 4: perf before/after:** `1190a31` (docs)

**Plan metadata:** the final docs commit (this SUMMARY + ROADMAP).

## Updated tests

| test | before (D3) | now (D24) |
|---|---|---|
| `semantics_e2e::schedule_to_start_without_capacity` | empty journal, `next_seq == 0` for the s2s timeout and for terminate-before-start | new helper `assert_never_started_then` checks:<br>- the journal is exactly `[RunStarted@0, RunTerminated@1]`;<br>- reason `schedule_to_start` / the terminate reason;<br>- `current_attempt.is_none()`, so no step ran;<br>- `next_seq == 2` |
| `semantics_e2e::signal_immediately_after_start` | `next_seq == 0` after three signals (B1 Pending) | before activation:<br>- `next_seq == 4`;<br>- journal `[RunStarted, SignalPayload×3]`;<br>- payload seqs `[1, 2, 3]`.<br>After the restart with capacity: delivered `["a","b","c"]`, and each `SignalMatched` follows its payload in arrival order |
| `lifecycle_api::cancel_before_start_is_flag_only_and_idempotent` → **`cancel_before_start_journals_after_run_started_once_and_is_idempotent`** | flag only, `next_seq == 0` | - before: RunStarted only, `next_seq == 1`;<br>- the dry run changes nothing;<br>- the first cancel journals `(0, RunStarted), (1, CancelRequested{"first"})`;<br>- the repeat cancel and the repeat dry run leave the snapshot identical;<br>- `next_seq == 2`;<br>- REST and gRPC |
| `lifecycle_api::terminate_rest_and_grpc` (comment-level D3 reference) | history empty or ending in RunTerminated | the history starts with RunStarted and ends with RunTerminated |
| `semantics_e2e::retry_restart_links_a_new_run` (added assertion) | — | the restarted run's seq 0 is RunStarted with the predecessor's input and no `continued_from`, and there is exactly one RunStarted |
| `start_run_api::api_start_journals_run_started_at_seq_zero` (new) | — | with `slots = 0`:<br>- `next_seq == 1` and a single RunStarted@0;<br>- seq-0 fields match the command: name, tenant, module hash, JSON input, seed, accepted time;<br>- for a 300 KiB input, both the input and the RunStarted are blobs |

Also reviewed. These still pass and needed no change:
- the rest of semantics_e2e, lifecycle_api, start_run_api and tenancy_api;
- sdk_e2e, runs_api, executor, executor_phase5, derive_phase5, leases, resume_rearm;
- semantics_sleepers (`--ignored`);
- crates/ostro: cli_verbs (all insta snapshots unchanged, so nothing was re-blessed), e2e_dev, dev_startup;
- the replay corpus;
- partition dispatcher and apply_run_started.

The `next_seq == 0` assertions in `executor_phase5.rs:300` and `derive_phase5.rs:912` are deliberately unchanged. They use a raw `StartRun` and a continue-as-new successor, which are the paths that still start with an empty journal.

## Before / after (WSL2, advisory, D15)

Interleaved A/B of two release perf binaries built from the same tree: **before** is the API leaving the field empty, **after** is 05-25. The disk was slow this session (raw fsync p50 7.8-8.6 ms).

| profile A | before | after |
|---|---|---|
| idle (1 client, 300 runs), start→first step p50 | 24.10 / 24.33 / 23.89 ms | **8.53 / 8.41 / 8.51 ms** |
| idle p99 | 28.2 / 58.6 / 33.3 ms | 13.3 / 14.7 / 14.6 ms |
| idle steps/s | 273-315 | 386-420 |
| 64 clients, steps/s | 12,592 / 11,907 | **18,373 / 17,766** |
| 64 clients, start→first step p50 | 33.6-34.6 ms | 17.3-17.9 ms |
| bytes on disk, 2,000 × 10 steps | 8.97-8.99 MB | 8.18-8.19 MB |

One after-only idle run at 3.3 ms fsync: p50 **5.71 ms** / p99 23.9 ms. For reference, 05-16 measured 3.38 ms with the same switch at 2.4-3.1 ms fsync. The NFR-2.4 gate is still **human_needed** on native NVMe.

## Decisions Made

See `key-decisions`. One implementation choice beyond the brief: the encoded RunStarted goes through `prepare_payload`. For an input under 256 KiB the StartRun command carries the input twice, once raw and once inside RunStarted. Above 256 KiB a second blob is written: the RunStarted bytes, which differ from the raw input blob. This is the D21 rule, and it keeps apply from ever decoding or fetching the payload.

## Deviations from Plan

### Auto-fixed issues

None. The three pinned tests failed by construction as 05-16 predicted, and they were the only failures before the test updates.

### Scope notes

- **`crates/ostro-server/src/executor.rs` module doc is stale.** The file belongs to 05-18 and was not edited. Lines ~19-22 say "The server's own start paths leave the field empty (D3: a run that never started keeps an empty journal), so this append is the normal path." Suggested replacement:
  ```
  //!    the append is skipped. Since 05-25 (D24) the API start and retry-restart paths set
  //!    the field, so this append is the path for inbound, child and continue-as-new starts.
  ```
- RESERVATIONS was not edited. StartRun 23 should become `exists`, but the brief leaves the RESERVATIONS flip to 05-20 (below).
- Benchmarks were run by interleaving two copies of the perf test binary rather than through `cargo test`. Two `cargo test` "before" runs at 11-12 ms fsync (p50 25-34 ms) are in the log only: the disk was too noisy for a clean comparison.

## Issues Encountered

- The WSL2 virtual disk's fsync rose from 3.3 ms to 8-12 ms during the session, while other executors were building. The interleaved A/B removes most of that noise. Absolute numbers are not comparable with 05-16's tables.

## Hand-off

### 05-17 (docs, order-saga)
- Done in parallel (bf3c199). Its `docs/workflow-semantics.md` already describes D24: a one-event journal before the first activation, pre-start signals and cancels journaled after RunStarted, and RunStarted@0 + RunTerminated@1 for a never-started run. Nothing to change there.
- order_saga.rs ran green under ci-local with this change (see Gate).

### 05-19 (kill campaign)
- A crash between the StartRun commit and the first grant now leaves `next_seq == 1` with RunStarted journaled. Recovery must not append a second RunStarted. The executor skips its append when `next_seq > 0`.
- Invariant for the checker: **exactly one RunStarted per run, at seq 0**, for both API and executor-built starts.
- Pre-start signals on API runs no longer use Pending slots. Child, inbound and continue-as-new starts still do, so the campaign should cover both shapes.

### 05-20 (gate / freeze)
- **Format corpus:** include a `StartRun` carrying tag 23 (`run_started`, a `JournalEvent{seq 0, kind RunStarted, payload inline}`). It is now produced on every API start. Ideally also include one blob-ref variant (input > 256 KiB).
- **RESERVATIONS §2:** StartRun field 23 `run_started`: `planned P5 (05-01)` → `exists`. Apply reads it since 05-16, and the API sets it since 05-25.
- The NFR-2.4 native NVMe measurement should be taken on this start path (05-25). The WSL2 figures are in docs/benchmarks.md.

## Gate

- Debug, worktree `wt-05-25`, `CARGO_TARGET_DIR=target/wt-05-25`:
  - `cargo test -p ostro-server --locked`: all binaries green after the test updates. Before the updates exactly the three pinned tests failed.
  - `cargo test -p ostro --locked`: cli_verbs (no snapshot diffs), e2e_dev and dev_startup green.
  - `semantics_sleepers -- --ignored --test-threads=1`, `ostro-engine --test replay_corpus`, and `ostro-partition --test dispatcher --test apply_run_started`: green.
- `bash scripts/ci-local.sh --no-musl` from the worktree at main `1190a31`, which includes 05-17's order_saga.rs and 05-18's failpoint chain: **EXIT 0**. fmt, clippy `-D warnings`, the warning-free build, the sdk workspace, guests, the order-saga example, cargo test (219 `test result: ok`, no failures), serial sleepers, failpoint list sync and suites, cargo deny (duplicate-crate warnings only, as before), vendored/deny fixtures, the no-chronik check and workflow validation all passed. No flaky reruns were needed.

## Self-Check: PASSED

- Commits found on main: 4630500, f340835, 8863a1e, 1190a31.
- Files found on main: control/runs.rs (attach_run_started), tests/start_run_api.rs (api_start_journals_run_started_at_seq_zero), docs/benchmarks.md (05-25 section), 05-DECISIONS.md (D24 row).
