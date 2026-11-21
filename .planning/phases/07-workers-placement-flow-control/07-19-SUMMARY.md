---
phase: 07-workers-placement-flow-control
plan: 19
subsystem: e2e tests (spawned ostro server + ostro worker)
tags: [e2e, flow-control, concurrency, weights, priority, fairness, throttle, rate-limit, pause, auto-pause, sc4]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-08 NodeFlow (concurrency, weights, GCRA throttle, rate limit, auto-pause); 07-14 control plane (queue flow, queue/workflow/run pause+resume, /retry alias); 07-16 P7Cluster spawned harness; 07-04 pause apply (timeout clears the mark)"
provides:
  - "crates/ostro/tests/flow_e2e.rs: SC4 through the binary (11 tests)"
  - "Grant-log activation timing for spawned-binary tests (JSON logs + dispatcher `dispatching` lines)"
affects: [07-22]
tech-stack:
  added: []
  patterns:
    - "Activation intervals = [dispatcher grant log time, run completed_at_ms]: an upper bound of execution overlap"
    - "Paused state over REST read through the /retry dry run (mode resumed only for a live paused run)"
key-files:
  created:
    - crates/ostro/tests/flow_e2e.rs
  modified: []
key-decisions:
  - "Activation starts come from the dispatcher's debug `dispatching` log line, not cpu-burn's started_ms (that is the run's creation time, 07-08 deviation 2)"
  - "weights and priority run a 1-partition server: DRR weights and priority bands are per partition dispatcher"
  - "Rate limit uses 3 per 60 s burst 3 (07-08's spec) instead of 3 per 1 s, so a loaded machine cannot refill a token mid-test"
  - "auto_pause uses wit-surface `panic`: retry-probe's `trap` mode needs raw UTF-8 input, which the JSON start APIs cannot send"
  - "timeout_while_paused uses smoke's declared 60 s run timeout: no test guest declares a shorter one and the start API has no run-timeout field"
requirements-completed: [DISP-03, DISP-04, DISP-05, OPS-01]
metrics:
  duration: "~95 min"
  completed: 2026-11-21
---

# Phase 7 Plan 19: SC4 e2e (flow control and pause) Summary

**`crates/ostro/tests/flow_e2e.rs` proves ROADMAP Phase 7 success criterion 4 through the real binaries, on the 07-16 P7Cluster harness (TLS server, plus a real `ostro worker` for one leg). Queue concurrency 3 held, with at most 3 overlapping. Weights 3:1 gave 30:10 of the first 40 grants. Priority-10 runs all started before priority-0 runs. With keyed concurrency, no key ever ran twice at once, and Jain's index was 0.98. The throttle spaced first activations 200 ms apart, accepted every start and did not delay a re-activation. The rate limit returned 3 × 429 `rate_limited` with Retry-After 20, created nothing for them, and an idempotent re-submit was not limited. Pause, resume, auto-pause, the /retry alias, timeout-while-paused and pause on a worker all behave as specified.**

## Commits

1. `951cf74` test(07-19): SC4 limits, weights, priority, fairness, throttle vs rate limit
2. `aa87fe1` test(07-19): SC4 pause, resume and auto-pause e2e

## How time is measured

- `cpu-burn`'s `started_ms` is the run's first journal time, which is its creation (07-08 deviation 2). In `queue_limits`, the guest intervals overlap 12 runs at once, which is meaningless. So they are recorded only.
- **Activation start:** the server runs with `[log] format = "json"` and `RUST_LOG=info,ostro_partition::leader::dispatcher=debug`. The test parses every `dispatching` line (`fields.run`, `fields.n`, `timestamp` in µs).
- **End:** the run's `completed_at_ms`.
- **Why this is a valid bound:** the grant comes before the activation, and the lease (and its flow permit) is released only after the terminal record is applied. So every execution lies inside `[grant, completed_at]`, and the overlap counted over these intervals is an upper bound of the true concurrency. Start order (for priority and weights) is the order of the first grants.
- **Paused state:** the REST run view has no pause field. `is_paused` uses `POST /v1/runs/{id}/retry {"dry_run": true}`, which answers `mode = resumed` only for a live paused run (07-14 alias; it submits nothing) and checks that the status is not terminal.

## Measurements

All 11 tests ran in one `--test-threads=1` run: 132.6 s, load average about 11–18, debug build.

| Test | Result |
|---|---|
| `queue_limits` | 15 × cpu-burn 300 ms, queue concurrency 3, 8 embedded slots: **max overlap 3**, grant-to-completion span 472 ms (earlier run 468 ms), all completed. 3.5 s |
| `weights` | 1 partition, 2 slots, `wa` weight 3 / `wb` weight 1, 60 + 30 runs held behind a workflow pause and released at once: first 40 grants **A 30 : B 10 = 3.00** (two runs: 3.00 and 3.00). 4.8 s |
| `priority` | 1 partition, 1 slot, 20 runs with priorities 0/10 interleaved and held behind a queue pause: start order `HHHHHHHHHHLLLLLLLLLL`. 3.4 s |
| `keyed_fairness` | 4 partitions, 4 slots, `key_concurrency 1`, k0 has 100 runs and k1..k9 have 10 each, submitted round-robin behind a queue pause. **Per-key max concurrency 1 for all 10 keys**; node overlap 4. Completions per key in the contended window: [8, 7, 10, 8, 7, 8, 9, 8, 6, 8], **Jain 0.9828** (earlier run 0.9837). 7.9 s |
| `throttle_vs_rate_limit` | Throttle `{5, 1000 ms, burst 1}`: 15 starts all 201 in 125 ms. First-activation gaps 200–201 ms, **median 201 ms** (earlier runs 200 and 200), span 2,813 ms, all completed. Re-activation: `timeouts sleep_ms 2500` (`hot_wait` 200 ms, so it evicts) re-granted (tokens increasing) and completed 2.53 s after its start under a 1-per-10-min throttle, while 1 of 2 first-activation probes stayed held. Rate limit `{3, 60 s, burst 3}`: starts #0–#2 got 201; **#3–#5 got 429, `Retry-After: 20`, `error.code = rate_limited`**; `GET /v1/runs?queue=rq` lists 3 runs; an idempotent re-submit of `rk-0` returned 200 with the same run and `created: false`, and the list still has 3 runs. 8.7 s |
| `pause_run` | `signals fifo`: after signal 1, the run is paused mid-run (`changed` true, then false on a repeat; `is_paused`). Signal 2 sent while paused journals `signal_payload` + `signal_matched` (apply-side match) and nothing else; no new `signal_wait_started`; grants 2 → 2 over 2 s. Signal 3 and resume: completed with `[1, 2, 3]`. 6.7 s |
| `pause_queue` | An in-flight cpu-burn 1500 ms completes after the pause. 5 runs held 2.37 s with no grant, status `ready`; resume: all 5 completed in 213 ms. 6.2 s |
| `pause_workflow` | Same with a `cpu-burn` workflow pause: held 2.58 s, completed 110 ms after resume. A `smoke` run on the same queue completed while cpu-burn was paused (fix-0705 holds). 6.1 s |
| `auto_pause` | `[executor] max_attempt_failures = 2`, `retry_backoff = 50ms`, queue `on_retries_exhausted = pause`, wit-surface `panic`: the run is paused at attempt 2, not failed. No grant in 1 s. `POST /retry` returns 200 `{mode: resumed}` and the run is re-granted (grants 2 → 3). The guest panics again, so the run is auto-paused again (non-terminal). Without the policy the same workflow ends `failed` after 2 grants. 5.3 s |
| `timeout_while_paused` | smoke (60 s run timeout) is paused before its first grant (queue paused → start → run pause → queue resume). No grant for 2 s. **TimedOut 60,002 ms after creation while paused.** `POST /resume` returns 409. `POST /retry` returns 200 `{mode: resumed}`, and the run completes 56 ms later with its input as the result (no stale pause mark, B7). 63.6 s |
| `pause_on_worker` | Workers queue (`pool=pz` selector, real `ostro worker`, 2 slots), `timeouts busy_steps 20000`, paused while `running_on_workers > 0`. The lease was released 1.2 ms after the pause response. The run stayed non-terminal and paused, its journal frozen at 1 event for 2 s, 1 grant. Resume: completed `busy:20000` 10.8 s later, grant tokens [1, 2], 2 `remote activate` lines, `b0` completed once, 0 worker ERROR lines. 16.2 s |

## Verification

- Task 1 verify (`-- --test-threads=1 queue_limits weights priority keyed_fairness throttle_vs_rate_limit`) on the Task 1 file: `test result: ok. 5 passed`.
- Task 2 verify (`-- --test-threads=1 pause timeout_while_paused`): `test result: ok. 6 passed`, 107 s.
- Plan verification (`cargo test -p ostro --test flow_e2e --locked -- --test-threads=1`): `test result: ok. 11 passed`, 132.6 s.
- Every test also passed individually while it was being developed.
- `cargo clippy -p ostro --test flow_e2e --locked -- -D warnings`: clean on both commits. `cargo fmt -p ostro -- --check`: clean. Build warning-free.
- Not run: `bash scripts/ci-local.sh` (shared-load policy; 07-17/18/20/21 and the Phase 8 campaign were running). 07-22 runs the gate.

## Deviations from Plan

1. **[Plan defect] cpu-burn `{started_ms, ended_ms}` cannot date activations.** `started_ms` is the creation time (07-08 deviation 2). Intervals come from the dispatcher grant log plus `completed_at_ms` (see "How time is measured"). The guest values are recorded only.
2. **[Plan defect] retry-probe `trap` cannot be started through the binary.** Its modes are raw UTF-8 input, but REST and gRPC start take JSON (the input bytes are `"trap"` with quotes, which falls into the default mode). `auto_pause` uses the wit-surface workflow `panic` instead (a retryable `AttemptFailed(Trap)` on every attempt).
   - Consequence: no test guest has an activation failure that can be fixed at runtime, so "resume with a mode that succeeds → the run completes" cannot be shown for an auto-paused run.
   - What is shown instead: /retry acts as resume (200 `resumed`, the run is granted again), and the still-broken run auto-pauses again instead of failing.
   - The "/retry then completes" leg is covered by `timeout_while_paused`.
   - A guest whose activation trap depends on a signal or state (for example, panic until signal `fix`) would close this. That needs a guests/ change outside this plan.
3. **[Plan adaptation] Run timeout 3 s → smoke's declared 60 s.** The start API has no run-timeout field, and run timeouts come only from the guest's declared metadata. smoke (60 s) is the shortest among the test guests, so the test takes about 64 s.
4. **[Plan adaptation] `ostro run pause|resume` CLI → REST.** The CLI client still rejects `https://` (07-15/07-16 known limitation), and P7Cluster is TLS-only. The CLI verbs are covered in `cli_p7.rs` (its `verbs_*` tests).
5. **[Plan adaptation] Rate limit `{3, 60 s, burst 3}`** instead of `{3, 1 s, burst 3}`, the same choice 07-08 made: a token refills every 333 ms with a 1 s period, which a loaded machine can exceed across 6 TLS requests. The 1 s arithmetic is unit-tested in 07-08.
6. **[Plan adaptation] `weights` and `priority` run with 1 partition.** Priority bands and DRR weights are per partition dispatcher, so with 4 partitions a lower-priority run on another partition can legitimately start first.
7. **[Harness gap, worked around in-file] `common::is_terminal` lacks `timed_out` / `terminated` / `continued_as_new`.** `P7Api::wait_terminal` uses it, so it never returns for a timed-out run (the first `timeout_while_paused` attempt hung until its 150 s limit). The file has its own `terminal()` / `wait_end()`. `crates/ostro/tests/common/mod.rs` is outside this plan; follow-up for its owner.
8. **[Plan adaptation] Pause on a worker is read from the lease gauge, not from journal growth.** The worker appends a busy activation's events when the activation ends: `busy_steps 2000` showed 1 event until completion and then 2,002, and `busy_steps 60000` showed no append for 90 s. So "executing" and "lease released" use `GET /v1/queues/wq/demand` `running_on_workers`, and "frozen" is the journal length over 2 s after the pause.

## Observations / product notes (not worked around outside the file)

- **The REST run view has no pause state** (`RunDto` has no `paused` or pause reason). The auto-pause reason (`retries_exhausted`) is not observable over REST at all; this test infers paused-ness from the `/retry` dry run. A `pause: {reason, detail, at_ms}` field on `GET /v1/runs/{id}` would make OPS-01 observable to operators. Follow-up for 07-22 or Phase 11.
- A signal sent to a paused run is matched against its open wait at apply time (`signal_matched` journaled while paused). The run is still not activated until resume. This is consistent with "journaled but does not wake it".
- Worker-side buffering (deviation 8): a long single activation on a worker sends no Append until it ends. On a lease revoke the whole activation's work is discarded and re-executed (pause_on_worker: the resumed activation took 10.8 s). That is correct, but long activations on workers lose more work per revoke than embedded ones. Recorded for 07-21/07-22.
- `auto_pause`: after a resume, the run's `attempt_failures` is still 1, so a single further failure exhausts it again (grants 2 → 3 → paused). A resume does not reset the failure budget, unlike the D1 retry of a terminal run (F9 resets it). Possibly intended; flag for review.

## Self-Check: PASSED

- FOUND: crates/ostro/tests/flow_e2e.rs (1,175 lines, ≥ 250)
- FOUND: commits 951cf74, aa87fe1 on main
