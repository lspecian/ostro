---
phase: 05-workflow-semantics-core-hardening
plan: 15
subsystem: e2e-tests
tags: [e2e, wf-03, wf-04, wf-05, wf-08, wf-09, api-05, api-06, api-07, ops-06, d1, d21, b1, b2, restart, replay, sleepers, retention, continue-as-new]

requires:
  - phase: 05-03
    provides: "Runtime::live_activations, patch-v1/v2/v3 guests, first-completion op index"
  - phase: 05-08
    provides: "apply waits/signals/tokens/cancel, B1 pending signals, D21 blob-backed events"
  - phase: 05-09
    provides: "apply terminate/retry-resume/timeouts/continue-as-new/retention expiry"
  - phase: 05-10
    provides: "race-fuzz, signals, timeouts, tokens, can-chain, crash-saga guests"
  - phase: 05-11
    provides: "derive waits/CAN, resume re-arm, executor.slots = 0 no-capacity hook, module_override, policy resolution"
  - phase: 05-13
    provides: "REST lifecycle ops, start retention/schedule_to_start, policies, long-poll follow_continuations"
provides:
  - "crates/ostro-server/tests/semantics_e2e.rs: 16 phase-level scenarios through the real in-process server (REST)"
  - "crates/ostro-server/tests/semantics_sleepers.rs: WF-04 1,000-sleeper proof (#[ignore], serial CI step)"
  - "tests/common: start_raw, in_flight_jobs, running_rows, timer_rows, rss_bytes, deploy_guest, post_start/start_rest, get_json/post_json, list_all"
  - "scripts/ci-local.sh check_sleepers step + identical ci.yml step"
affects: [05-16, 05-17, 05-19, 05-20]

tech-stack:
  added: []
  patterns:
    - "Fresh-Runtime replay with a recording FakeSink: outcome bytes == stored result, 0 appends, recorded journal (incl. WaitResolved) == stored journal"
    - "'Unpause' a no-capacity server (executor.slots = 0) by restarting it with slots: deterministic pre-start states without a production pause hook"
    - "Mid-chain stall: ExecutorHooks::pause_before_append = Semaphore(0) (a gate, permits are taken and returned), restart, lift the gate"
    - "Raw StartRun (TestServer::start_raw) only for fields the API derives from workflow metadata (run_timeout_ms); everything after the start goes through the server"

key-files:
  created:
    - crates/ostro-server/tests/semantics_e2e.rs
    - crates/ostro-server/tests/semantics_sleepers.rs
  modified:
    - crates/ostro-server/tests/common/mod.rs
    - scripts/ci-local.sh
    - .github/workflows/ci.yml

key-decisions:
  - "A signal sent after a timeout ended the run is 409 run_terminal (the run is terminal; 05-13 apply checks terminal first), not buffered. A signal sent before the wait exists on a live run is buffered and matched FIFO"
  - "Run-timeout scenarios submit StartRun to the partition with run_timeout_ms: the API takes run_timeout only from the workflow's declared metadata and no test guest declares a short one. Execution, the timeout, retry, signals and results all go through the server"
  - "WF-04 lease/window probes are proxies (the server exposes neither TimerStats nor a lease gauge): in-flight jobs = dispatched + reactivated - finished job records; durable running rows; durable timer rows due within the 60 s window span (0 => the window holds none of them)"
  - "RSS bound kept at the plan's 64 KiB per sleeper; measured 11.4 KiB (WSL2, debug, P=4)"
  - "The patched-in-flight proof adds a negative control: a second v1 run re-pinned to patch-v3 diverges (Failed, resumable), proving the override really swaps the module"

requirements-completed: [WF-03, WF-04, WF-08, WF-09, OPS-06]

duration: ~1h15m
completed: 2026-11-19
---

# Phase 5 Plan 15: Workflow Semantics E2E Summary

**Every Phase 5 workflow semantic now has an end-to-end proof through the real in-process server, with restarts: gather/race replay identity under concurrent signals, 1,000 sleepers holding no activation, signal timeouts and buffering, patched in-flight code, run and schedule-to-start timeouts, cancel/terminate/retry including resume after a timeout, B1 signals before the first activation, tokens, 1 MiB payloads, continue-as-new chains across a restart, and retention expiry with idempotency-key reuse.**

## Performance

- **Duration:** about 1 h 15 min (gate excluded).
- **Tasks:** 3 of 3.
- **Files:** 2 created, 3 modified.
- **Suite time:** `semantics_e2e` 16 tests in about 9.5 s (debug); `semantics_sleepers` 2 tests in about 4 s.

## Task Commits

1. **Task 1 (harness, sleepers, CI steps):** `00fe457` (test). `semantics_sleepers.rs`, `tests/common/mod.rs`, `scripts/ci-local.sh`, `.github/workflows/ci.yml`.
2. **Task 1 gather_race + Tasks 2 and 3:** `642a269` (test). `semantics_e2e.rs`. All three tasks write this one new file, so it went in as one commit; splitting it would have meant committing intermediate files that were never run as a whole.

Both were committed through a private index (`GIT_INDEX_FILE` seeded with `read-tree HEAD`, blobs hashed from the scratch worktree), followed by `git reset -q -- <paths>` and a `git show --stat HEAD` check. The tests were written against existing behaviour, so there is no RED commit.

## What landed

### semantics_e2e.rs (REST, real server)

| Test | Proves |
|---|---|
| `gather_race` | race-fuzz, 5 seeds, K=4 signal waits (`a,b,a,c`), N=6 sleeps `[50,50,a,a,b,400]` ms. 4 concurrent senders with 0..150 ms jitter, then a forced `restart()` (5/5 seeds were mid-run). Each run completes with all 10 ops reported. Replay on a fresh Runtime (another token key) with a recording `FakeSink`: identical output bytes, 0 appends, identical `WaitResolved` lists. |
| `signal_timeout` | `timeout` mode gives `"timeout"` with one `WaitTimedOut`. A later signal is 409 `run_terminal` and journals nothing. In `before` mode, two signals sent during the sleep are both journaled before `SignalWaitStarted`; the single wait matches the oldest (`SignalMatched{op, payload_seq = first}`). |
| `patched_in_flight` | patch-v1 run sleeping, then a restart with `module_override` mapping it to the patch-v2 sha (v2 deployed under tenant `acme`, since bindings are write-once). It completes `["A","B"]` with 0 `PatchMarker`. Control: a second v1 run mapped to patch-v3 ends Failed and resumable (nondeterminism). A fresh v2 run gives `["A","C","B"]` with exactly 1 `PatchMarker`. |
| `run_timeouts` | `sleep_forever` with run_timeout 500 ms: TimedOut, reason `run_timeout`, resumable, history ends in `RunTerminated("run_timeout")`, GET `timed_out`. `busy_steps` 50M with 1 s: TimedOut, and `live_activations()` returns to 0. |
| `schedule_to_start_without_capacity` | `executor.slots = 0`, REST start with `schedule_to_start_timeout: "300ms"`. GET shows `schedule_to_start_ms: 300`; the run ends TimedOut, reason `schedule_to_start`, with an empty journal. Terminate before start: Terminated, `next_seq == 0`, empty journal. |
| `schedule_to_start_with_capacity_never_fires` | With a 1 s s2s, a 1.5 s sleep completes. A delayed start (1.5 s) with a 1 s s2s completes, so s2s is measured from ready_at. |
| `cancel_cleanup` | crash-saga sleeping after `step0..2`. Cancel: Cancelled, with steps `step0..2, undo2, undo1, undo0` (shielded compensation). |
| `terminate_runs_no_guest_code` | Same saga terminated: Terminated with `RunTerminated` last. Over the next 500 ms the step list stays frozen (no undo) and no further activation starts. |
| `resume_after_signal_wait_timeout` (B2a) | signals `long_wait` with run_timeout 300 ms: TimedOut. Retry: `resumed`, same id. Re-armed (`open_waits == 1`, Scheduled, `resumed_count == 1`). Signal 7: Completed with 7. `SignalMatched{op = original wait op, payload_seq = SignalPayload}`, no `TimerFired`/`WaitTimedOut` for that op, exactly one `RunTerminated`. |
| `resume_after_sleep_timeout` (B2b) | timeouts `sleep_ms 800` with run_timeout 200 ms: TimedOut. Wait until 1.2 s; retry gives `resumed` and the run completes `"slept"` within 5 s. Exactly one `RunTerminated("run_timeout")`, so no second run timeout fired. |
| `retry_restart_links_a_new_run` | Guest-failed run: `restarted`, new id with `retry_of`. |
| `signal_immediately_after_start` (B1) | `executor.slots = 0`, start `fifo`, 3 signals right away (`next_seq == 0`). Restart with slots: Completed `["a","b","c"]`. `RunStarted` comes first, the 3 `SignalPayload` events follow, and the `SignalMatched` events reference them in arrival order, each after its payload. |
| `tokens` | Complete: the payload. Second completion: 409. A 200 ms token: `"timeout"`, then 409. |
| `large_payloads` (D21) | A 1 MiB signal to `long_wait` and a 1 MiB token to `tokens` produce byte-identical results. Restart, then replay both from their blob-backed journals on a fresh Runtime: 0 appends, identical bytes and journal. |
| `continue_as_new` | `{n:1,max:4}`: 4 runs linked by `continued_from`, earlier ones ContinuedAsNew with `result.next_run_id`, the last Completed 4. `result?wait=10s&follow_continuations=true` answers 4 from the last run. Then a 30-run chain stalled at 3-4 runs (append gate), restarted, gate lifted: it completes with 30 runs, and the run list holds exactly the two chains (no duplicate successor). |
| `retention` (OPS-06) | `PUT /v1/workflows/signals/policy {"retention":"1s"}`. Start with key K: `retention_ms == 1000`, dedupe returns the same id before expiry. Expiry about 1.0 s after completion was observed; then GET 404, history 404, absent from the list, and K creates a new id. A Terminated run resumed before its expiry still exists 2.5 s later, then completes and expires. Restart: the expired runs stay gone and a 4 s run expires after the restart. |

### semantics_sleepers.rs (WF-04, serial)

- **`sleepers_hold_no_memory`:** 1,000 REST starts of `timeouts` `sleep_ms` 1 h, 32 in flight (started in about 380 ms; all `Scheduled` in the status list about 300 ms later). Then:
  - `live_activations() == 0`, `parked_count() == 0`, `budget.used() == 0`;
  - in-flight jobs 0 and durable running rows 0;
  - 1,003 durable timer rows (1,000 sleepers plus 3 warm-up expiry timers), 0 due within the 60 s window span.
  - **RSS: 99.1 MB → 110.8 MB, delta 11,722,752 B = 11,722 B (11.4 KiB) per sleeper**, against the 64 KiB bound.
- **`wake_all_short_sleepers`:** 1,000 runs of 2 s, all Completed `"slept"` about 2.2 s after the last start. libtest orders tests by name, so this test runs after the RSS one and its freed heap cannot hide the RSS growth.
- Both tests are `#[ignore = "serial: dedicated ci step"]`. ci-local `check_sleepers` ("semantics sleepers (serial)", right after `cargo test`) and the ci.yml step (same condition as `cargo test`, `!matrix.zig`) both run `cargo test -p ostro-server --test semantics_sleepers --locked -- --ignored --test-threads=1`. `FAILPOINT_CRATES` is untouched.

## Verification

- `cargo test -p ostro-server --test semantics_e2e --locked`: 16 passed, run 5 times in a row (about 9.5 s each).
- `cargo test -p ostro-server --test semantics_sleepers --locked -- --ignored --test-threads=1`: 2 passed.
- `continue_as_new` alone: 10 runs green after the chain-walk fix (see Deviations).
- Neighbouring suites that share `tests/common` (`lifecycle_api`, `resume_rearm`, `executor_phase5`) still pass. All ostro-server test binaries build.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings` and `cargo fmt --all --check` are clean.

## Gate

`CARGO_TARGET_DIR=target/wt-05-15 bash scripts/ci-local.sh --no-musl` on committed HEAD `642a269` (which includes 05-14's `5a3f506`, `1a924b9`, `45ab54c`), in a detached scratch worktree outside the repo. **Every step passed on the first run, exit 0**, so no reruns were needed:

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  1s
cargo clippy -D warnings             PASS  37s
cargo build (warning-free)           PASS  56s
sdk workspace fmt + clippy           PASS  20s
sdk workspace test + wasm build      PASS  22s
guests build (wasm32-wasip2)         PASS  0s
order-saga example                   PASS  20s
cargo test                           PASS  647s   (148 result blocks, semantics_e2e included)
semantics sleepers (serial)          PASS  7s     (new step)
failpoint suites                     PASS  837s
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  0s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  1s
zig/musl legs                        SKIP  (--no-musl)
```

## Deviations from Plan

### Auto-fixed issues (test-side only)

**1. [Rule 1 - Bug] Chain walk read a successor before it existed**
- **Found during:** Task 3 (`continue_as_new`, about 1 run in 2).
- **Issue:** `continued_to` is committed before the predecessor's outbox `StartRun` creates the successor, so `run_record(next)` panicked "run exists".
- **Fix:** `chain()` stops at a link whose run does not exist yet (05-13 hit the same window in `await_result`). **Commit:** `642a269`.

**2. [Rule 3 - Blocking] `pause_before_append` is a gate, not a counter**
- A `Semaphore(k)` never runs out, because every append takes a permit and returns it. The mid-chain stall now runs a 30-run chain until it has 3 or more runs, then closes the gate with `Semaphore(0)`. **Commit:** `642a269`.

### Plan items resolved differently

- **Run timeout through the API.** The API has no per-run run_timeout; it comes from the workflow's declared metadata, and no test guest declares a short one. The run-timeout and resume-after-timeout scenarios therefore start with `TestServer::start_raw`, a `StartRun` submitted to the owning partition with `run_timeout_ms`. Everything after the start goes through the server and REST. No production change; see Hand-off.
- **"Unpause the dispatcher" (B1, s2s).** The server has no pause hook (05-11 decision: `executor.slots = 0`). B1 "unpauses" by restarting the server with `slots = 64`, which also exercises the pending signals across a restart.
- **Timer window and dispatcher leases (WF-04).** The server exposes neither `TimerStats` nor a lease gauge, and adding them is a production change outside this plan. The test uses the proxies in key-decisions. The window proxy is exact by the timer service invariant (the window holds only rows due within `now + window_span_ms`).
- **"Stop the server and read the journal" (WF-03).** Journals are read with `read_journal` from the running server after completion. `large_payloads` reads after a restart. Reading from a stopped dir would need a second node open on the same data dir.
- **`retry_resume` with busy_steps.** Covered by the two B2 tests (TimedOut, then resumed with the same id, then completes). The finite busy_steps variant was not added because its duration in debug is machine-dependent.
- **Signal after timeout.** Asserted as 409 `run_terminal`, as defined by 05-13. Buffering applies only to live runs, which `signal_timeout` `before` and the B1 test prove.

## Known limitations / gaps (no production change made)

- The server exposes no `TimerStats` (window length) and no dispatcher lease gauge. `ServerHandle` could expose both (`TimerConfig.stats` is `None` in `server.rs`). Owner: 05-16 or 05-20.
- The API cannot set a per-run run timeout. If one is wanted, it is an additive `StartRunBody.run_timeout` (owner 05-13 follow-up / Phase 7). Otherwise, a guest declaring `run_timeout = "500ms"` would let a future e2e use the pure API path.
- The main checkout's working tree is stale for paths committed through private indexes (this plan's five files, and 05-14's). The orchestrator should restore them once no executor is running (`git checkout -- <paths>` / `git restore`); this plan did not copy files into the main checkout.

## Hand-off notes

### 05-16 (perf)
- **WF-04 baseline:** 11.4 KiB RSS per sleeping run (debug, WSL2, P=4), 1,000 REST starts in about 380 ms, 1,000 2 s sleepers drained in about 2.2 s. Re-measure in release if the start path changes; the sleepers test stays as the regression guard (bound 64 KiB).
- If the start path writes `RunStarted` at `StartRun`, these must keep passing: `signal_immediately_after_start` (pending signals journaled after `RunStarted`, in order), `schedule_to_start_without_capacity` (empty journal on an s2s timeout and on terminate-before-start), and `cancel_before_start` in lifecycle_api.
- `in_flight_jobs()` relies on `DispatcherStats.dispatched + reactivated` matching one `JobRecord` per job. Keep that 1:1, or add a real lease gauge.

### 05-17 (docs)
- Documented behaviour, proven here:
  - a signal after the run ended is 409 `run_terminal`;
  - signals before the wait (or before the first activation) are buffered FIFO;
  - resume re-arms waits and past-due sleeps fire at once, with no second run timeout;
  - schedule-to-start counts from ready_at (delayed starts are fine) and only for never-granted runs;
  - retention 1 s expires about 1 s after completion; afterwards GET, history and list are 404/absent, and the idempotency key is free again;
  - a resumed run is not deleted by its old expiry timer.
- Run timeout is per workflow (declared metadata), not per start request.

### 05-19 (crash campaign)
- `semantics_e2e.rs` shows the restart-mid-flight patterns: a forced `restart()` with signals in flight, a mid-chain stall via `pause_before_append = Semaphore(0)`, and a restart with `executor.slots` flipped.
- `replay_fresh` / `assert_replays_identically` (fresh Runtime plus recording `FakeSink`, comparing outcome bytes, 0 appends and the recorded journal) are the checker shape for campaign survivors. Copy them; they are private to the test file.

### 05-20 (gate)
- WF-03, WF-04, WF-08, WF-09 and OPS-06 are marked complete in REQUIREMENTS.md by this plan.

## STATE notes (for the orchestrator; STATE.md not edited, no gsd-tools mutators run)

- 05-15 complete (3/3 tasks), commits `00fe457`, `642a269`.
- Decisions:
  - a signal after the run ended is 409 `run_terminal`, not buffered;
  - run-timeout e2e uses a raw StartRun, since the API derives run_timeout from workflow metadata only;
  - WF-04 lease/window checks are proxies until the server exposes `TimerStats` and a lease gauge;
  - the RSS bound stays at 64 KiB per sleeper (measured 11.4 KiB).
- Requirements: WF-03, WF-04, WF-08, WF-09, OPS-06 complete.
- Housekeeping:
  - restore the main checkout's stale working-tree paths (05-14 and 05-15) when no executor is running;
  - the scratch worktree and `target/wt-05-15` have been removed.

## Self-Check

PASSED

- FOUND: crates/ostro-server/tests/semantics_e2e.rs (1,105 lines, min 300) and semantics_sleepers.rs (in HEAD)
- FOUND: commits 00fe457, 642a269
- FOUND key-link patterns: `restart` + `Runtime`/`activate` replay in semantics_e2e.rs; `live_activations` in semantics_sleepers.rs; `semantics_sleepers` in scripts/ci-local.sh and .github/workflows/ci.yml
