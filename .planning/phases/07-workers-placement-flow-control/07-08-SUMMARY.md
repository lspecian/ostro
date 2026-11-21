---
phase: 07-workers-placement-flow-control
plan: 08
subsystem: flow-control (ostro-server flow/*, executor exhaust hook, StartRun admission)
tags: [flow-control, gcra, throttle, rate-limit, concurrency, pause, auto-pause, weights]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-02 FlowControl/FlowPermit/QueueGauges contract, NodeFlow stub, p7 seam, [flow] config, ErrorCode::RateLimited; 07-04 PauseRun apply (token fencing); 07-05 dispatcher admission/reservation, FlowPermit commit after Granted, changed() arm"
provides:
  - "flow::NodeFlow: FlowControl over QueuePolicy/WorkflowPolicy (pause, queue/key concurrency, first-activation GCRA throttles, weights, changed())"
  - "flow::limits: Counters (node-wide counters + throttle TATs, tentative charges) and RateCells (StartRun GCRA), bounded key tables"
  - "flow::policy_view::PolicyView: ArcSwap policy cache refreshed on every meta apply"
  - "flow::rate_limit::{admit_start, admit}: 429 rate_limited before the StartRun proposal"
  - "executor::{ExhaustAction, ExhaustPolicy}, EngineExecutor::with_exhaust_policy; auto-pause branch submitting PauseRun{retries_exhausted}"
  - "flow::install fills cfg.flow / cfg.gauges, applies [flow] + the injected clock, starts the meta watcher"
affects: [07-12, 07-14, 07-16, 07-17, 07-22]
tech-stack:
  added: []
  patterns:
    - "GCRA in u64 nanoseconds from the injected clock's ms; tentative charge + conservative rollback"
    - "Policy cache keyed by names seen, re-read on every meta apply (meta has point getters only)"
key-files:
  created:
    - crates/ostro-server/tests/flow_control.rs
    - crates/ostro-server/tests/auto_pause.rs
  modified:
    - crates/ostro-server/src/flow/mod.rs
    - crates/ostro-server/src/flow/limits.rs
    - crates/ostro-server/src/flow/policy_view.rs
    - crates/ostro-server/src/flow/rate_limit.rs
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/p7.rs
key-decisions:
  - "Counters count every admitted entry (not only limited queues), so a limit added while runs are leased sees them"
  - "NodeFlow gets [flow] and the clock in flow::install (p7.rs stays at its one 07-08 edit); NodeFlow::new keeps the 07-02 signature"
  - "Rate-limit dedupe lookup: one point read of the Dedupe table on the key's partition (same read as the dry run), done only for rate-limited queues"
  - "Auto-pause outcome mapping: Accepted -> Released; Fenced/StaleEpoch/RunTerminal/RunNotFound -> Fenced; other rejects -> fall back to CompleteRun{Failed}; partition errors -> Reactivate"
requirements-completed: [DISP-03, DISP-04, DISP-05]
metrics:
  duration: "~1h40m"
  completed: 2026-11-21
---

# Phase 7 Plan 08: Node flow control and auto-pause Summary

**`NodeFlow` is now the dispatcher's `FlowControl`. It enforces node-wide queue and key concurrency, applies a hand-rolled nanosecond GCRA throttle to first activations only, honours queue and workflow pauses, and serves DRR weights. Policy edits wake the dispatchers through `changed()`. StartRun is rate limited (429 `rate_limited` + Retry-After, idempotent re-submits free). An exhausted retryable run auto-pauses under `on_retries_exhausted = pause`.**

## Commits

1. `cf6fc39` feat(07-08): node flow control (concurrency, throttle, pause, weights)
2. `46a5056` feat(07-08): StartRun rate limit with 429 rate_limited
3. `a01adc0` feat(07-08): auto-pause on exhausted retries

## GCRA (flow/limits.rs)

- Time is `now_ms × 10⁶` ns from the injected clock (the dispatcher passes `now_ms`; StartRun uses `cp.now_ms()`, the same clock).
- `limit` and `burst` are normalised to `max(1, v)`. `T = period_ns / limit` (at least 1 ns; 5000/s gives 200 µs exactly), `τ = T × (burst − 1)`. `period_ms == 0` is invalid, and that spec is ignored.
- A request conforms iff `now ≥ TAT − τ`; otherwise `until = TAT − τ`, and `until_ms` is that value rounded up. A conforming request sets `TAT = max(TAT, now) + T`.
- Dispatcher throttle: the queue TAT and the key TAT are both checked first, and neither moves unless both conform.
  - `try_admit` charges tentatively. It increments the `running` counters and advances the TATs, remembering `(prev, written)` for each TAT.
  - `FlowPermit::commit` keeps the charge. Dropping a committed permit releases the counters only.
  - Dropping an uncommitted permit rolls back. Counters decrement. Each TAT goes back to `prev` if it still equals `written`; otherwise it becomes `max(cur − T, max(prev, now))`, never above `cur`.
- StartRun (`RateCells::admit`): one mutex covers check-both then commit-both. A non-conforming start returns `retry_after_ms = ceil_ms(until) − now`, at least 1.

## Memory bounds

- Queue cells: one per queue name seen.
- Key cells: bounded by `[flow] max_tracked_keys`, per table (counters/throttle and rate limit).
  - A cell with no running activation and a TAT in the past carries no state. It is evicted after `[flow] idle_key_evict` (the sweep runs at most every `idle/2`, and no more often than once per second) or immediately when the table is over the cap.
  - If the table is still over the cap, the least recently used idle cells are evicted. This is lossy: such a key may start early.
  - Cells with running activations are never evicted; their number is bounded by the node's leases.
- `PolicyView` caches one entry per queue or workflow name the node has looked up. Names are not evicted.

## Policy view and wake-ups

- The meta state has point getters only, so `PolicyView` keeps an `ArcSwap<Snapshot{applied, queues, workflows}>` keyed by the names it has seen.
- `NodeFlow` calls `refresh()` before each decision. When meta has not moved, that is one compare.
- The watcher started by `flow::install` (stopped by `P7Ctx::shutdown`) refreshes after every meta apply. A changed policy bumps `changed()`.
- A committed permit of a concurrency-limited queue or key bumps `changed()` when released. A rollback always bumps it.

## Idempotency lookup source

The start path had no dedupe lookup outside the dry run, so `rate_limit::admit` makes **one point read** of `Table::Dedupe` (`dedupe_key(workflow, key)`) on the partition the key routes to. That is the same read the dry run does. It runs only when the queue has a `rate_limit` or `key_rate_limit`, so unlimited queues pay nothing. Read-budget note: a rate-limited start with an idempotency key costs one extra point read. There is a benign race: two concurrent first starts of one new key may both be charged.

## executor.rs diff (+103 / −2, one hunk group)

- New: `ExhaustAction {Fail, Pause}`, `trait ExhaustPolicy`, the field `exhaust: Option<Arc<dyn ExhaustPolicy>>`, the builder `with_exhaust_policy`, `auto_pause`, `pause_detail`, plus one outcome-table row in the module docs.
- Changed: `map_outcome` and `attempt_failed` take `record: &RunRecord` (passed from `drive`). After the existing `Retry` early return, the branch `f.retryable() && policy == Pause` submits `PauseRun{run_id, reason: 1, detail: failure JSON (the same JSON CompleteRun{Failed} carries, message shortened to fit 4096 bytes), token: Some(current token), requested_at_ms}`.
  - Accepted → `Released`.
  - Fenced / StaleEpoch / RunTerminal / RunNotFound → `Fenced`.
  - Any other reject → warning, then the old `fail_run`.
  - Partition error → `Reactivate`.
- Non-retryable failures and executor-decided failures (module or record) never reach the branch.
- p7.rs (its one 07-08 edit): `p7::executor` wraps `engine.with_exhaust_policy(ctx.shared.flow)` before the `Arc` and the `OnceLock` store.

## Verification

- `cargo test -p ostro-server --lib -- flow::`: 15 passed. GCRA burst/spacing, normalisation, the 5000/s case (5000 admits in 1 s at 200 µs), concurrency, unkeyed entries, first-activation-only throttle, both-TAT check, rollback/commit, conservative rollback, eviction and cap, rate cells both-or-neither, permits through `NodeFlow`, the watcher bump on a policy edit, exhaust precedence, rate-limit admission.
- `cargo test -p ostro-server --test flow_control`: 10 passed. Queue concurrency 2 (flow counters max = 2, dispatcher leases ≤ 2). Key concurrency 1 (per-key max 1, keys in parallel). Throttle 5/s: 6 first activations span ≥ 900 ms, every start 201. Re-activation after a durable sleep is not throttled under a 1-per-10-min throttle, while a second first activation is held. Queue pause and workflow pause hold entries and resume through `changed()`. Weights 3:1 give 26..=34 of the first 40 grants. rate_limit: 429 + Retry-After + no run created; idempotent re-submits free; per-key isolation; gRPC RESOURCE_EXHAUSTED → `RateLimited`.
- `cargo test -p ostro-server --test auto_pause --test executor --test leases`: 2 / 8 / 4 passed.
- `cargo test -p ostro-server --test start_run_api --test runs_api`: 10 / 2 passed.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. Build warning-free. `cargo fmt -p ostro-server`: applied.

## Deviations from Plan

1. **[Plan adaptation] Rate-limit test spec.** The tests use `limit 3 per 60 s burst 3` instead of "per 1 s", so a slow machine cannot refill a token between starts (T = 20 s; the 4th start is told to retry about 20 s later). The 1 s arithmetic (Retry-After 334 ms) is covered by the unit test `rate_limit_charges_and_skips_existing_idempotent_starts`.
2. **[Plan adaptation] Concurrency evidence.** `cpu-burn`'s `started_ms` is the run's first journal time (its creation), not the activation start, so it cannot measure execution intervals. The concurrency tests sample `NodeFlow::running` / `key_running` and `DispatcherStats.leases` every 2 ms. The throttle test uses `ended_ms`: the journaled time just after a 1 ms step.
3. **[Rule 1 - Bug, test helper] Meta Put semantics.** `MetaCommand::Put{expected_version: None}` is create-only, so the test helpers CAS on the current version and assert `MetaResult::Ok`.
4. **Integration tests use the system clock.** `TestServer` has no clock injection. Direct `NodeFlow` tests drive time explicitly through `now_ms`. Nothing in the product reads time outside the injected clock.

## Issues / follow-ups

- **Workflow pause blocks other workflows on the same queue (dispatcher).** In 07-05, `Admit::Paused` → `fair.skip_queue(queue)`. When a paused workflow's entry is the queue's next candidate, the whole queue is skipped for that round on that partition, and other workflows sharing the queue starve until the workflow resumes. Reproduced: a `smoke` run on queue `default` timed out while `cpu-burn` was paused on `default`. A queue-level pause is fine. A workflow pause needs a per-entry or per-workflow skip in `leader/fair.rs` / `dispatcher.rs`, which belong to 07-05 and are outside this plan's files. The test runs the other workflow on its own queue and documents this.
- OPS-01 is not marked complete: the control-plane pause/resume (07-14) is still open.

## Hand-off

- **07-12 (remote runner):** call the same outcome mapper with the exhaust policy. Either route remote `AttemptFailed` through `EngineExecutor::attempt_failed` (it needs the job's `RunRecord`), or build the remote executor with `.with_exhaust_policy(ctx.shared.flow.clone())` and reuse `auto_pause`/`pause_detail`; they are private today, so make them `pub(crate)` when wiring.
- **07-14 (control plane):** queue and workflow pause/resume are meta CASes on `QueuePolicy.paused` / `WorkflowPolicy.paused`. The flow watcher refreshes on the apply and bumps `changed()`, so nothing else is needed for dispatchers to wake. Rate-limit and throttle edits work the same way. `/retry` on a paused run → resume is 07-14's.
- **07-16 (demand):** `ready_unblocked` already excludes queues `NodeFlow` returns `Paused`/`QueueBlocked` for and keys it returns `KeyBlocked` for. `NodeFlow::running(queue)` is available as a diagnostic.
- **07-22:** clock grep: `flow/*` reads time only through `now_ms` arguments, `cp.now_ms()` (runtime clock = server clock) and the installed `P7Ctx.clock`.

## Full suite

`cargo test -p ostro-server --locked` on a01adc0: 44 targets, 0 failures (lib 100; flow_control 10, auto_pause 2, executor 8, executor_phase5 12, leases 4, start_run_api 11, runs_api 2, semantics_e2e 18, sdk_e2e 9, lifecycle_api 13, sink 19, etc.). During `tests/outbound_traceparent.rs` (which passed) the log shows `ERROR ostro_partition::machine: apply thread panicked partition=3`. This is the apply-thread panic a separate debugger is investigating, and it is not caused by this plan. `bash scripts/ci-local.sh --no-musl` was not run, following the shared-load policy used by 07-03, 07-04 and 07-05. The gate here is the plan's verify blocks, the full ostro-server suite and ostro-server clippy.

## Self-Check: PASSED
- FOUND: crates/ostro-server/src/flow/{mod,limits,policy_view,rate_limit}.rs, tests/flow_control.rs, tests/auto_pause.rs
- FOUND: commits cf6fc39, 46a5056, a01adc0 on main

## Post-plan fixes

- **fix-0705 closes the "workflow pause blocks other workflows on the same queue" follow-up above.**
  - `NodeFlow::admit` now returns `Admit::Paused { scope: PauseScope::Queue }` for a paused queue, and `Admit::Paused { scope: PauseScope::Workflow }` for a paused workflow. The queue check now comes first, so when both are paused the whole queue is skipped.
  - The dispatcher holds back only the workflow-paused entry, until the next `changed()` bump. The flow watcher already bumps on a policy apply, so the resume wakes it with no change here. See 07-05-SUMMARY "Post-plan fixes".
  - `tests/flow_control.rs::paused_workflow_holds_entries_and_resume_wakes_dispatchers` is back on the shared-queue scenario: `smoke` runs on queue `default` while `cpu-burn` is paused on `default`. It starts two `smoke` runs, and both must complete before the resume.
