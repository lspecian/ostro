---
phase: 05-workflow-semantics-core-hardening
plan: 11
subsystem: server-seam
tags: [derive, executor, dispatcher, resume-rearm, continue-as-new, retention, policies, d1, d9, d10, d21, d23, f9, f11, pf-f2, b2, wf-08]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 contract (RegisterWait/CancelWait, CompleteRun.resumable, outbox StartRun); 05-02 RunRecord fields; 05-03 engine (completion rule, live_activations, SignalPayload/RunTerminated as inputs); 05-07 WorkflowPolicy/QueuePolicy meta records; 05-08 idempotent RegisterWait, no CancelRequested before seq 0; 05-09 hard stops, RetryRun, CAN batch shape; 05-10 guests; 05-22 durable attempt_failures; 05-24 split_qualified"
provides:
  - "derive::derive(events, DeriveCtx) with Phase 5 mappings; derive::rearm_unresolved; derive::successor_start + ModuleResolver/PolicyResolver traits (MetaModules, NoModules, InheritPolicy)"
  - "PartitionSink::with_resolvers; continue-as-new successor built in the sink (run id + seed once per flush)"
  - "EngineExecutor: durable retry limit, resumable failures, resume re-arm, first-activation CancelRequested, ExecutorHooks::module_override, with_resolvers"
  - "dispatcher: after_failure re-grant at once (F9), revoke on any RunCompleted (tested), zero-slot no-capacity hook (tested)"
  - "run_started: D23 unqualified RunStarted.workflow + tenant, continued_from, first_events"
  - "retention::resolve / resolve_run_policy / MetaPolicies; ServerConfig [runs] retention/max_retention"
affects: [05-13, 05-15, 05-16, 05-17, 05-20]

tech-stack:
  added: []
  patterns:
    - "Derive stays pure: I/O (record read, payload prepare, run id + seed) happens in the sink before derive; resolvers are read-only trait objects with fakes in tests"
    - "Durable counters are written at the moment a failure is known (grant with after_failure immediately), never after an in-memory delay"
    - "Zero-event EventsAppended as the re-arm vehicle (CAS on the journal end like Suspend/EnqueueReady)"

key-files:
  created:
    - crates/ostro-server/src/retention.rs
    - crates/ostro-server/tests/derive_phase5.rs
    - crates/ostro-server/tests/executor_phase5.rs
    - crates/ostro-server/tests/resume_rearm.rs
    - crates/ostro-partition/tests/apply_rearm.rs
  modified:
    - crates/ostro-server/src/derive.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/run_started.rs
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/tests/config.rs
    - crates/ostro-server/tests/sink.rs (outside files_modified, see Deviations)
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/tests/dispatcher.rs

key-decisions:
  - "F9: on ExecOutcome::Retry the dispatcher grants AttemptGranted{after_failure: true} immediately and starts the job after the delay (lease held in phase Granting, slot held during the backoff). A delayed grant would lose the count on a restart inside the backoff and allow a 4th failure"
  - "No-capacity hook = executor.slots = 0 (Semaphore with 0 permits; add_permits releases runs). No dispatcher code change and no DispatcherConfig field (tests/common/mod.rs is not 05-11's file)"
  - "Every AttemptFailed completion is resumable (trap, fuel, memory, nondeterminism, protocol violation, exhaustion: D1 executor-level); module_not_found / no_module / module_invalid / bad_run_record stay non-resumable"
  - "Terminal batches carry no RegisterWait/CancelWait (apply rejects them in CAN batches; on_terminal removes waits anyway); timers in terminal batches are kept as before"
  - "Continue-as-new successor: run id = uuid v7 on the predecessor's partition, OS-random seed, requested/ready = ceil_ms(CAN event ts); version = continuation's, else predecessor's for the same workflow, else active; an unresolvable target completes the run Failed with reason continue_as_new_failed (instead of looping on Unavailable)"
  - "Without meta (harnesses) the successor reuses the predecessor's pinned module/run timeout for its own workflow+version and keeps its retention; policy retention 0 = keep predecessor's"
  - "Retention resolution: zeros fall through at every level (retention always > 0); schedule-to-start: the first level that sets it decides (0 = none); [runs] max_retention caps; zero runs.retention rejected at load"
  - "RunStarted.tenant = record tenant, else the qualified name's prefix; RunStarted.continued_from = predecessor id"
  - "Input bridge forwards SignalPayload (96) and RunTerminated (71) too: the engine requires contiguous input seqs, so skipping them would make a hot activation fail with BadJournal"

requirements-completed: []

duration: ~1h30m
completed: 2026-11-19
---

# Phase 5 Plan 11: Server Seam Summary

**The server now drives every Phase 5 semantic end to end: derive registers and cancels signal/token waits, completes guest cancels as Cancelled, and turns continue-as-new into a successor StartRun (D23 name, F11 tenant/tags/traceparent, PF-F2 parent, meta-resolved module, policy-resolved retention); the executor re-arms unresolved ops after a resume, journals a pre-start cancel after RunStarted, records executor failures as resumable, enforces the retry limit from the durable attempt_failures across restarts, and can swap a run's module for the WF-08 proof; retention/schedule-to-start resolve request > WorkflowPolicy > QueuePolicy > `[runs]` (168h server, 24h dev).**

## Performance

- **Duration:** about 1 h 30 min.
- **Tasks:** 3 of 3.
- **Files:** 5 created, 11 modified (one outside files_modified, see Deviations).
- **Commits:** 4 code commits plus this docs commit.

## Task Commits

1. **Task 1, derive + sink:** `502ddff` (feat). derive.rs, sink.rs, lib.rs, derive_phase5.rs, tests/sink.rs.
2. **Task 2, executor + dispatcher lifecycle:** `f9b9237` (feat). executor.rs, run_started.rs, dispatcher.rs, tests/dispatcher.rs, apply_rearm.rs, executor_phase5.rs, resume_rearm.rs, plus a clippy fix in derive.rs.
3. **Task 3, policy resolution + `[runs]`:** `e6e8a24` (feat). config.rs, retention.rs, server.rs, lib.rs, tests/config.rs, one full-server test in executor_phase5.rs.
4. **Orchestrator request, concurrent SetPartitionLeader:** `b5d257b` (perf). server.rs only.

Tests were written with the code in each task (no separate RED commit). The re-arm tests were checked by mutation: with the re-arm disabled all four `resume_rearm` cases fail.

## What landed

**derive (Task 1)**
- `SignalWaitStarted{name, timeout}` at seq s, ts t → `RegisterWait{s, Signal, name, ceil((t + timeout·1e6)/1e6) or 0, ceil(t/1e6)}`; `AwaitTokenCreated` → `RegisterWait{Token, ""}`. Same formula in `rearm_unresolved`, so re-armed rows equal the originals (tested).
- `OpCancelled` on a signal/token op (same batch or from history via the existing `prior` lookup) → `CancelWait`; timers keep `CancelTimer`; effects/children nothing.
- `RunFailed{Cancelled}` → `Cancelled`; other `RunFailed` → `Failed`; `RunCompleted` → `Completed`; all `resumable: false`.
- `RunContinuedAsNew` → `SendMessage(OutboxMsg::StartRun(successor))` then `CompleteRun{ContinuedAsNew, {"next_run_id"}}`. Apply's CAN path (05-09) accepts the batch; tested through a real partition, including the successor's record and input.
- `rearm_unresolved(journal)`: open = no `completes_op()==Some(op)` input and no guest `OpCancelled{op}` (05-03 rule); effects, children, `CANCELLATION_OP` and terminal journals produce nothing.
- The Phase 4 token failure is gone (`TOKENS_UNSUPPORTED` removed); effects and children keep the unsupported completion (regression test).
- D21: `journal_io::read_journal` already fetches blob-backed rows; a 1 MiB `SignalPayload` journaled by apply is read back intact (no code change).

**Executor and dispatcher (Task 2)**
- Revoke on terminal: the dispatcher already dropped the lease on every `Action::RunCompleted`; tests now assert it for Terminated and TimedOut (dispatcher level, with a holding fake executor) and at server level (busy activation stopped within 1 s, `live_activations() == 0`, `RunTerminated` last, nothing appended afterwards).
- F9: see key-decisions. `Job.failures` = `record.attempt_failures`; the in-memory failures map is removed. The restart test (backoff 1 s/2 s, restart inside the second backoff) fails the run after exactly 3 trap attempts with `attempt_failures == 2` surviving the restart.
- Resumable: exhausted traps and nondeterminism (patch-v1 journal replayed on patch-v3 via the override) complete `Failed` with `resumable == true`.
- First activation: `run_started::first_events` appends `CancelRequested{cancel_reason}` at seq 1 in the RunStarted flush. timeouts `sleep_forever` ends Cancelled; sdk-cancel `shielded_sleep` runs its shielded cleanup and completes `"cleaned"`.
- Resume re-arm (B2): in-memory `run -> resumed_count` marker; on the first job with a larger `resumed_count` the executor reads the journal, submits `rearm_unresolved` as a zero-event `EventsAppended` at the journal end, and only then loads the journal for the activation. Conflict → Reactivate. A restart re-runs it harmlessly (idempotent RegisterWait, timer key).
- `ExecutorHooks::module_override: ModuleOverrideSlot` (`set(Some(Arc<dyn Fn(&RunId) -> Option<[u8;32]>>))`), `#[doc(hidden)]`. patch-v1 → patch-v2 after a restart completes `["A","B"]` on v2.
- D23: `RunStarted.workflow` is the module-local name (`split_qualified`), tenant from the record or prefix; a run of `acme/info` executes wit-surface `info` with `tenant=Some("acme")`.
- Continue-as-new chains: tenant/tags/traceparent/retention copied to every successor (F11), `RunStarted.continued_from` set; a chain under a live parent keeps `parent` on every hop and the parent's inbox receives exactly one ChildCompleted (PF-F2).

**Policies (Task 3)**: `retention::resolve` (pure) / `resolve_run_policy(meta_view, &cfg.runs, workflow, queue, RunPolicy)` / `MetaPolicies`; `[runs] retention` + `max_retention`, `"runs"` in `SECTIONS` (`OSTRO_RUNS__RETENTION=48h` tested), zero retention rejected, `7d` rejected by the parser (W11). `server.rs` wires `MetaModules` + `MetaPolicies` into the executor; a full-server test proves a successor gets the meta module and the `WorkflowPolicy` retention.

## Verification

- `cargo test -p ostro-server --test derive_phase5 --test sink` 16 + 15 pass.
- `cargo test -p ostro-partition --test dispatcher --test apply_rearm` 15 + 1 pass; the `--lib` dispatcher unit tests pass.
- `cargo test -p ostro-server --test executor_phase5 --test resume_rearm --test executor --test leases` 12 + 4 + 8 + 4 pass.
- `cargo test -p ostro-server --test config` 20 pass; `--lib` (incl. retention, run_started, derive units) 36 pass.
- `cargo clippy -p ostro-server -p ostro-partition --all-targets -- -D warnings` clean; `cargo fmt --check` clean.
- Gate: see "Gate" below.

## Decisions Made

See `key-decisions`. STATE.md is not edited by this plan (orchestrator rule).

### STATE notes (for the orchestrator to fold in)

- 05-11 complete: server seam (derive, executor/dispatcher lifecycle, policies), commits `502ddff`, `f9b9237`, `e6e8a24`, `b5d257b` (concurrent SetPartitionLeader).
- Decision: F9 counts the failure with an immediate `AttemptGranted{after_failure}`; the job waits out the backoff holding its lease/slot.
- Decision: the no-capacity test hook is `executor.slots = 0` (no pause flag).
- Decision: attempt-failure completions are resumable; module/record failures are not.
- Decision: unresolvable continue-as-new → `Failed` / `continue_as_new_failed`.
- Decision: `[runs] retention` 168h server / 24h dev, `max_retention` optional, zero rejected.
- Open item: a plain API start still submits `retention_ms = 0` (keep forever) until 05-13 calls `resolve_run_policy`.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1: Bug] Input bridge dropped SignalPayload / RunTerminated**
- **Found during:** Task 2 design review of the hot-input path.
- **Issue:** `input_bridge` forwarded only events with `completes_op()`. Apply journals `SignalPayload` (96) on a live run and `RunTerminated` (71) on a hard stop; the engine's `deliver_inputs` requires contiguous seqs and accepts both kinds, so the next forwarded input after a skipped 96 would fail the activation with `BadJournal`.
- **Fix:** `is_input()` mirrors the engine rule (completing kinds + 96 + 71). **Files:** executor.rs. **Commit:** `f9b9237`.

**2. [Rule 3: Blocking] `tests/sink.rs::await_token_fails_run_unsupported` asserted the removed Phase 4 behaviour**
- The plan removes the token failure and lists `--test sink` in its verify command, but `tests/sink.rs` is not in files_modified. Replaced the test with `await_token_registers_a_wait` (Running, `open_waits == 1`, the run goes on). No other hunk in that file. No later plan lists the file. **Commit:** `502ddff`.

**3. [Rule 2: Correctness] F9 grant timing** (see key-decisions): the plan text says "the dispatcher re-grants with after_failure", which read literally (grant after the delay) loses the count on a restart during the backoff; the immediate grant makes the plan's restart test hold exactly.

**4. Unresolvable continue-as-new completes `Failed`** rather than erroring the append (an `Unavailable` would re-activate forever).

**5. Orchestrator request (mid-run): concurrent `SetPartitionLeader` at startup.** `server.rs` is in files_modified, so `record_partition_leaders` now builds every partition's proposal and awaits them together (a small dependency-free `join_all`: no `futures` crate, Cargo.lock untouched), letting the meta driver group-commit them. Results are still checked per partition in partition order, and a rejection is still a fatal `Invariant`. No new MetaCommand. Commit `b5d257b`. Not separately timed here; the warm-restart and startup suites pass.

### Plan items resolved without code

- `apply/run.rs` already accepts zero-event `EventsAppended` with intents; no apply change. `tests/apply_rearm.rs` pins it (accepted, `next_seq` unchanged, idempotent repeat, SeqMismatch on a stale end).
- Dispatcher revoke on `RunCompleted` needed no code change (test-only, as W10 expected).
- `journal_io` already fetched blob-backed rows (D21); test only.

## Gate

- **`bash scripts/ci-local.sh --no-musl`** (worktree at `e6e8a24`): every step PASS (toolchain, fmt, clippy, warning-free build, SDK fmt/clippy/test/wasm, guests, order-saga, failpoint suites 968 s, deny, vendored, deny fixtures, forbidden crates, workflow validation) except `cargo test`, which stopped at `ostro` `server_config::default_data_dir_is_dot_ostro_and_persists`, a listed load flake (ci-local's `cargo test` has no `--no-fail-fast`, so later crates did not run in that step).
- **After rebasing onto `ab623e2`** (load hardening) plus the server.rs change: `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo test --workspace --locked --no-fail-fast` ran all 137 test binaries: 136 green (incl. `default_data_dir`, `sdk_e2e`, `bootstrap`, `e2e_dev`, `start_run_api`, `tenancy_api`, every 05-11 suite) and one failure, `ostro-raft-driver` `driver::tests::commits_do_not_wait_for_ticks`, a listed load flake that passes when rerun alone.
- No failure was caused by 05-11; nothing is left red on main.

## Hand-off notes

### 05-13 (server ControlPlane)
- **Start policies:** in `runs::start_run`, call `ostro_server::retention::resolve_run_policy(cp.meta().view(), &cp.config().runs, &workflow, &queue, RunPolicy { retention_ms: req.retention, schedule_to_start_ms: req.schedule_to_start })` with the **qualified** workflow/queue locals and put the result in `StartRun.retention_ms` / `schedule_to_start_ms`. Today a plain start submits `retention_ms: 0` (keep forever); the resolver never returns 0 retention.
- **Retry mode:** `RunRecord.resumable` is now set by the executor for every attempt failure (trap, fuel, nondeterminism, protocol violation, exhaustion) and by apply for Terminated/TimedOut; guest terminals (`Failed` from `RunFailed`, `Cancelled`) are `resumable == false` → restart as a new linked run. Module/record failures (`no_module`, `module_not_found`, `module_invalid`, `bad_run_record`) are not resumable either.
- **GET run attempt count:** `RunRecord.attempt_failures` is the durable retry count (incremented at the failure, before the backoff).
- **Continue-as-new follow:** the predecessor's result payload is `{"next_run_id": "<id>"}` and `RunRecord.continued_to` is set atomically. An unresolvable target ends `Failed` with `reason_code = "continue_as_new_failed"`.
- **Cancel before start** needs nothing from the API: the executor journals `CancelRequested` at seq 1.
- **start_run_api.rs** (05-24 hand-off): the journal name is stripped now, so tenant runs can be driven to `completed` (upload with `?tenant=` and assert qualified workflow/queue).

### 05-15 (e2e)
- **No-capacity hook:** `executor.slots = 0` in `ServerConfig` (or `Opts.slots = 0` / a `Semaphore::new(0)` in `DispatcherConfig`): runs stay `Ready`, never granted, so schedule-to-start fires. For a TestNode-style harness keep the `Arc<Semaphore>` and `add_permits` to release.
- **Module override:** `hooks.module_override.set(Some(Arc::new(move |r: &RunId| (*r == run).then_some(sha))))`; works with `start_with_hooks`. Use a short `engine.hot_wait` so patch-demo's 2 s sleep evicts before the restart.
- **Resume after timeout:** `RetryRun` → first activation re-arms (signal wait `open_waits == 1`, past-due sleep fires within a timer tick, past wait timeout → `WaitTimedOut`, never `TimerFired`). `resume_rearm.rs` has the four cases at node level; the e2e can mirror them over the API once 05-13 lands.
- **busy_steps** journals nothing until the end (steps flush at waits), so wait for `live_activations() >= 1`, not for `next_seq`.
- **Signals to a hot activation** now work: the bridge forwards `SignalPayload`.

### 05-16 (perf / start path)
- `executor.rs` touch points: `drive()` step 2 (`first_events` RunStarted + optional CancelRequested) and step 2b (re-arm, only when `resumed_count > 0`; a single journal read). If 05-16 writes `RunStarted` at `StartRun` (`StartRun.run_started`), it must also carry the pre-start `CancelRequested` rule (or keep the executor path for runs with `cancel_requested`), and must use `run_started::first_events`/`build_run_started` for the D23-stripped name and `continued_from`.
- Dispatcher: retries hold their lease and slot during the backoff (`regrant_after_failure`); `Job.failures` is read from the record.
- The successor of a continue-as-new is started on the predecessor's partition (no partition-count lookup in the sink).

### Others
- **05-12 / 05-20 corpus:** successors' `RunStarted.continued_from` is now set, and tenant runs journal the unqualified workflow with `tenant`. Corpus entries recorded before 05-11 with `continued_from: None` still replay (the engine does not compare it).
- **05-17 docs:** continue-as-new result `{"next_run_id"}`; policy changes affect future runs only; `[runs] retention` defaults and the `7d` → hours rule.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/retention.rs
- FOUND: crates/ostro-server/tests/derive_phase5.rs
- FOUND: crates/ostro-server/tests/executor_phase5.rs
- FOUND: crates/ostro-server/tests/resume_rearm.rs
- FOUND: crates/ostro-partition/tests/apply_rearm.rs
- FOUND: commit 502ddff
- FOUND: commit f9b9237
- FOUND: commit e6e8a24
- FOUND: commit b5d257b
