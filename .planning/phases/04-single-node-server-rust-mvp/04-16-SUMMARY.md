---
phase: 04-single-node-server-rust-mvp
plan: 16
subsystem: server-executor
tags: [executor, dispatcher, leases, self-fence, zombie-fencing, hot-inputs, retries]
requires:
  - phase: 04-14
    provides: Executor trait, Job/ExecOutcome/LeaseRenewer, soft leases with expiry -> revoke + grant n+1
  - phase: 04-15
    provides: PartitionSink, build_run_started, journal_io, derive helpers
  - phase: 04-13
    provides: TimerFired journaled as an input + InputAppended for Running runs, Suspend CAS
  - phase: 04-07
    provides: Runtime::prepared, EvictReason::StepBackoff
  - phase: 04-06
    provides: sdk-basic guest
provides:
  - ostro_server::executor::{EngineExecutor, ExecutorConfig, MAX_RETRY_BACKOFF, REASON_*}
  - ostro_server::executor::{ExecutorHooks, JobRecord, input_bridge} (doc(hidden), tests)
  - PartitionSink::submit_intents_at(from_seq, derived)
  - crates/ostro-server/tests/common (TestNode in-process harness)
  - retry-probe modes trap and sleep:MS
affects: [04-17, 04-18, 04-20, 06-effects-bridge]
tech-stack:
  added: [tokio-util (ostro-server, CancellationToken)]
  patterns:
    - "Ordered input bridge: dispatcher seqs and sink completions are wake-ups only; the bridge reads the journal strictly by seq from the loaded end and forwards each input once"
    - "CAS from the engine's view of the journal end (EngineSink.engine_end), never the sink's next_seq"
    - "Self-fenced job stops all I/O, then waits for the dispatcher's revoke before returning (a Fenced outcome would drop the run without a re-grant)"
key-files:
  created:
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/tests/common/mod.rs
    - crates/ostro-server/tests/executor.rs
    - crates/ostro-server/tests/leases.rs
  modified:
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/Cargo.toml
    - guests/retry-probe/src/lib.rs
    - Cargo.lock
key-decisions:
  - "Suspend / EnqueueReady CAS uses the engine's journal end (initial loaded end, then each accepted engine append's next), not PartitionSink::next_seq: a sink-journaled unsupported completion the engine never saw must fail the CAS"
  - "Input bridge reads the journal strictly by seq on every wake-up (dispatcher seq or sink event); closes the 04-15 out-of-order BadJournal race and skips inputs already in the loaded journal"
  - "Self-fence deadline = min(lease.expires_at(), last renewal + cfg.lease_ttl) - self_fence_margin; once cancelled (revoke or self-fence) engine appends are refused locally and nothing more is submitted"
  - "A self-fenced job waits for job.revoked before returning, because ExecOutcome::Fenced makes the dispatcher drop the lease without re-granting; outcomes of revoked leases are ignored"
  - "max_attempt_failures counts failed attempts including the last (3 = attempts 1 and 2 retried, the 3rd fails the run); delay retry_backoff * 2^failures capped at 60 s"
  - "Executor-decided run failures: no_module, module_not_found, module_invalid, bad_run_record (JSON {reason_code, kind, message}); attempt failures use the AttemptFailure reason code"
  - "Evicted{External} without revoke (flush Unavailable) and Unavailable submissions re-activate after a 100 ms pause; StepInterrupted re-activates"
requirements-completed: [DISP-07]
duration: 70min
completed: 2026-11-19
---

# Phase 4 Plan 16: EngineExecutor + DISP-07 lease suite Summary

**Runs now execute end to end inside one process: the dispatcher hands a leased job to `EngineExecutor`, which loads the pinned module, writes `RunStarted` on the first activation, replays the journal on the Wasm engine and maps every activation outcome to Suspend / EnqueueReady / re-activation / retry / failure. A stalled executor self-fences or is revoked, the run is re-dispatched as attempt n+1, and the zombie's late append is rejected `Fenced`.**

## Performance

- **Duration:** about 70 min
- **Tasks:** 2 (tests written against the harness; both suites green on the first run)
- **Files:** 4 created, 5 modified

## Accomplishments

- **`EngineExecutor`** (`crates/ostro-server/src/executor.rs`), one activation per job:
  1. Module: `runtime.prepared(sha)`, otherwise blob (`PartitionSink::fetch_blob`) + `prepare_cached`. No pin, a missing blob or an invalid component completes the run `Failed`.
  2. `PartitionSink` with the job token; every accepted append renews the lease.
  3. `RunStarted` (seq 0) from the record + stored input when `next_seq == 0`.
  4. `read_journal` + `repair_unsupported` (re-read if it appended).
  5. Ordered input bridge (see decisions) into `Activate.inputs`.
  6. Lease watcher: `revoked` -> cancel; heartbeat every `heartbeat`; local self-fence deadline; `renew() == false` -> cancel.
  7. Outcome state machine, documented as a table in the module docs.
- **`ExecutorConfig`** (`heartbeat`, `lease_ttl`, `self_fence_margin`, `max_attempt_failures`, `retry_backoff`; `for_lease_ttl(ttl)`, `Default` = 30 s lease) and **`ExecutorHooks`** (activations, self_fences, `pause_before_append` gate, `suppress_heartbeat`, `only_attempt`, per-job `JobRecord`s).
- **`PartitionSink::submit_intents_at`**: explicit-`from_seq` intents-only append for the engine-view CAS.
- **retry-probe**: `trap` (panics outside a step) and `sleep:MS` (step `before`, durable sleep + wait, step `after`, completes `slept:MS`). Existing modes unchanged.
- **Harness** (`tests/common/mod.rs`): `TestNode::open/open_with/stop` (P=4, dispatcher + engine executor, shared `Runtime` and hooks across restarts), `install_module`, `put_blob`, `start` (StartRun on `partition_for_new_run`), `wait_terminal/wait_until/wait_event/wait_job`, `journal`, `result`, `jobs_of`; local `guest_wasm`.
- **tests/executor.rs** (8): sdk-basic -> Completed with typed JSON, journal[0] RunStarted with module hash; `sleep:1000` hot_wait 100 ms -> Suspend -> Scheduled -> TimerFired -> attempt 2 completes; `requeue` -> two StepBackoff evictions + EnqueueReady, 2 StepAttemptFailed, run completes `err:retryable:transient`; `trap` (blob-only module) -> attempts 1,2,3 -> Failed `{"reason_code":"trap",...}`, no RunFailed event; missing module -> Failed `module_not_found`, no activation; hot input `sleep:300` hot_wait 2 s resolves in the same activation (1 grant, 1 activation); restart mid-sleep replays `before` once; bridge ordering with out-of-order wake-ups.
- **tests/leases.rs** (4, DISP-07): heartbeat keeps a 1.6 s hot wait leased (> 3 x 400 ms, no expiry, no self-fence); stall (attempt 1: heartbeat suppressed, appends gated, slots=1) -> Evicted{External}, lease expired, attempt 2 completes; zombie append with token n=1 while attempt 2 is live -> `AppendError::Fenced` / apply `Fenced`, and again after completion; journal has only `before`/`after`, one RunCompleted; self-fence at the executor's 350 ms deadline while the dispatcher's 3 s lease is still held, then re-dispatch; pre-restart token -> `StaleEpoch` (sink: `Fenced`) after reopen.

## Task Commits

1. **Task 1: EngineExecutor + harness + executor suite + retry-probe modes**: `6c36302` (feat)
2. **Task 2: DISP-07 lease suite**: `380da97` (test)

## Verification

- `cargo test -p ostro-server --test executor --locked`: 8 passed (~11.7 s); `--test leases`: 4 passed (~4.4 s). Five back-to-back repeats of both suites: all green.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings` and `cargo fmt --all --check`: clean.
- `bash scripts/ci-local.sh --no-musl` with a private `CARGO_TARGET_DIR=target/wt-04-16` at 380da97: toolchain, fmt, clippy (29 s), warning-free build, `cargo test` (420 s, includes ostro-partition and ostro-server), failpoint suites (410 s; no `Node::open timed out` flake this run), deny, check-vendored, deny-fixtures, forbidden deps and workflow validation all **PASS**; musl legs SKIP.

## Decisions Made

See frontmatter `key-decisions`. The two that matter most for later plans:

- **CAS at the engine's view.** `PartitionSink::next_seq` follows the sink's own unsupported-completion appends, which the engine may not have consumed. Suspending at that seq would lose the wake-up (04-13 rule). The executor tracks `engine_end` in its sink wrapper and submits `Suspend`/`EnqueueReady` there; `SeqMismatch` -> `Reactivate`, never a re-submit.
- **Self-fence waits for revoke.** The dispatcher treats `ExecOutcome::Fenced` as "a newer attempt owns the run" and drops the lease without re-granting. A job that fenced itself before the dispatcher's expiry therefore must not return `Fenced`; it stops all I/O and waits on `job.revoked` (guaranteed within the dispatcher TTL because it no longer renews), then returns `Released` (ignored for a revoked lease).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] 04-15 input ordering race fixed instead of documented**
- **Issue:** A dispatcher-forwarded `TimerFired` and a sink unsupported completion could reach the engine out of seq order -> `BadJournal`.
- **Fix:** The sink's `inputs` sender goes to the executor's bridge, not the engine. Both sources are wake-ups; the bridge reads the journal from its watermark and forwards inputs strictly by seq. Test: `input_bridge_forwards_inputs_in_journal_order`.
- **Commit:** 6c36302

**2. [Rule 2 - Correctness] `PartitionSink::submit_intents_at`** (sink.rs was not in `files_modified`)
- **Issue:** `submit_intents` CASes at `next_seq`, which can include a completion the engine never saw (lost wake-up).
- **Fix:** Additive method with an explicit `from_seq`; the executor uses the engine's view.
- **Commit:** 6c36302

**3. [Rule 1 - Bug avoidance] Self-fenced jobs wait for revoke** (see Decisions). The plan's "Evicted{External} after revoke/self-fence -> Released" holds, but only after the revoke is observed.

**4. Hook shape.** `pause_before_append` is `Mutex<Option<Arc<Semaphore>>>` (settable at runtime), plus `only_attempt`, `self_fences` and `jobs: Vec<JobRecord>` so the suites can scope the stall to attempt 1 and assert outcomes. The gate is released by cancellation (a cancelled job's append is refused), so attempt 1's activation reports `Evicted{External}`.

**5. `sleep:MS` has steps around the sleep** (`before`/`after`), so the same mode serves the restart replay and zombie-absence assertions. The `sleep` behaviour is otherwise as planned.

**6. AttemptGranted counts** are asserted via `RunRecord.current_attempt.n` (grants are log commands, not journal events) plus per-job token records.

## Known gaps / notes

- **Inbox inputs** (signals, child results, cancel) still do not bump `next_seq` (04-12/04-13 gap, Phase 5).
- **RunStarted append is not gated** by `pause_before_append` (only engine appends are); the stall scenario stalls the first engine append.
- **Hot-wait inputs before lease registration** are dropped by the dispatcher; covered by the CAS (next append conflicts -> re-activate), as 04-13 described.
- **Retry counter is in memory** (`Job::failures`): a restart resets attempt-failure accounting (journal-level step retries are durable; run-level attempt failures are not). Acceptable for Phase 4; Phase 7 (DLQ) may persist it.

## Hand-off: 04-17 (server bootstrap)

- **Wiring:** `let rt = Arc::new(Runtime::new(engine_cfg, RuntimeOptions{ clock, token_key: <meta-persisted key>, cache_dir: Some(<data>/aot), .. }))`; `let exec = Arc::new(EngineExecutor::new(rt.clone(), ExecutorConfig::for_lease_ttl(lease_ttl)))`; `NodeConfig.leader.dispatcher = Some(DispatcherConfig { executor: exec, lease_ttl, slots: Arc::new(Semaphore::new(n)), stats })`. Keep `ExecutorConfig.lease_ttl == DispatcherConfig.lease_ttl` in production; `for_lease_ttl` gives heartbeat ttl/3, margin ttl/10, 3 attempts, 1 s backoff.
- **Modules:** the executor needs the component in the node's blob store (`handle.blobs().put(wasm)`, node-wide) under its sha; call `rt.prepare_cached(&wasm)` at deploy so the first dispatch hits L2. Missing blob -> run `Failed {"reason_code":"module_not_found"}`.
- **StartRun:** set `module_sha`, `requested_at_ms` = now (RunStarted.accepted and logical time derive from it), `rng_seed` 32 random bytes, `run_timeout_ms` from describe(). Input bytes are wrapped as `application/json`.
- **Run result:** `query::run_payload(r, run, PAYLOAD_RESULT)`; failures are JSON `{reason_code, kind, message}`.
- **Engine `activation_timeout` (60 s) vs lease:** the heartbeat keeps long activations leased, so `lease_ttl` (30 s default) only catches stuck executors.
- **Shutdown:** `Node::shutdown` revokes leases; executors stop within one cancellation (no appends after revoke).
- `ExecutorHooks`/`JobRecord`/`input_bridge` are `doc(hidden)` test surface; 04-20 can reuse `tests/common` patterns for the full-server stall/zombie repeat.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/executor.rs (`impl Executor for EngineExecutor`)
- FOUND: crates/ostro-server/tests/common/mod.rs, tests/executor.rs, tests/leases.rs
- FOUND: commits 6c36302, 380da97
