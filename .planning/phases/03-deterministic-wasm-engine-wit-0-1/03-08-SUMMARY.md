---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 08
subsystem: engine
tags: [wasmtime, activation, evict-replay, fake-sink, testkit, classification]
requires:
  - "03-07: HostState/HostInit, build_linker, run_return_from_wit, record_terminal, flush, Evict{WaitUnsatisfied} trap"
  - "03-03: OstroEngine, configure_store, OstroLimiter.breach, NodeMemoryBudget"
  - "03-04: validate_component"
provides:
  - "ostro_engine::runtime::{Runtime, RuntimeOptions, Prepared, WorkflowMeta}; Runtime::{new, engine, linker, clock, prepare, describe, activate}"
  - "ostro_engine::activation::{classify, Classified, outcome_code}"
  - "ostro_engine::testkit::{FakeClock, RunSpec, Rule, Script, FakeSink, FakeState, Drive, drive_to_completion, replay_journal, DEFAULT_ACCEPTED_AT}"
affects: [03-09, 03-10, 03-11, 03-12, phase-04]
tech-stack:
  added: []
  patterns:
    - "Activation = fresh Store over the committed journal; call_run raced (biased) against the cancel token; flush after the trap unwinds; sink.done exactly once after the Store is dropped"
    - "classify(err, breach, stderr_tail) is a pure function (unit-tested with synthetic wasmtime::Error values)"
key-files:
  created:
    - crates/ostro-engine/tests/activation.rs
  modified:
    - crates/ostro-engine/src/runtime.rs
    - crates/ostro-engine/src/activation.rs
    - crates/ostro-engine/src/testkit.rs
key-decisions:
  - "classify takes (err, Option<Breach>, stderr_tail) instead of &HostState: the limiter is private to HostState and only reachable via HasLimits::limiter(&mut); a pure signature is also directly unit-testable"
  - "Hot-wait input delivery (Activate.inputs) is ignored in 03-08: every unsatisfied wait evicts; 03-11 adds hot wait"
  - "Cancellation wins select ties (biased): an already-cancelled activation runs no guest code and appends nothing"
  - "Diverged attempts (NonDeterminism, from a trap or from record_terminal) never flush; every other failure flushes best effort so completed live steps survive the retry"
  - "drive_to_completion re-activates without inputs after AppendConflict/External evictions; stops as stuck only on a WaitUnsatisfied eviction with nothing to deliver"
requirements-completed: []
duration: 25min
completed: 2026-11-18
---

# Phase 3 Plan 08: Activation Driver + Runtime Facade + FakeSink Testkit Summary

Workflows now run end to end on the fake `ActivationSink`. The new `Runtime` facade prepares a component (validation plus `WorkflowPre`), runs activations, evicts on unsatisfied waits and replays on re-dispatch. Every attempt-ending error is classified to a stable reason code. `FakeSink` (behind feature `testkit`) stands in for the Phase 2 partition leader.

## What was built

- **`runtime.rs`**
  - `RuntimeOptions { clock, token_key }`. `Default` uses `SystemClock` and a process-local key, sha256 over the time and pid. Phase 4 passes the cluster key.
  - `Runtime::new(cfg, opts)` builds `OstroEngine` and `build_linker`. Accessors: `engine()`, `linker()`, `clock()`.
  - `prepare(wasm)` runs `validate_component`, then `instantiate_pre`, then `WorkflowPre::new` and returns `Prepared { sha, module_hash_hex, pre }`. This is the uncached path; 03-11 adds the cache.
  - `describe(&Prepared)` uses a throwaway store with a synthetic `RunStarted` (zero seed) and returns `Vec<WorkflowMeta>`, including `run_timeout_ms` (WIT-06).
  - `activate(&Prepared, Activate, sink)` calls `activation::run_activation`.
- **`activation.rs`**
  - `run_activation` steps:
    1. `HostState::new`. A bad journal becomes `ProtocolViolation("bad journal: ..")`.
    2. Create the store and call `configure_store`.
    3. `instantiate_async`. An error becomes `Trap(explain_instantiate_error)`.
    4. `call_run` races the cancel token. If the token fires, the run is `Evicted{External}`.
  - Mapping of what `call_run` returns:
    - **Ok:** `record_terminal`, then flush, then `Completed`, `ContinuedAsNew` or `Failed`. A `NonDeterminism` from `record_terminal` becomes `AttemptFailed` and nothing is flushed.
    - **Err:** goes through `classify`. `Evict` flushes and returns `Evicted{reason}`. `NonDeterminism` returns `AttemptFailed` without a flush. Any other failure is flushed best effort, then `AttemptFailed(class)`.
    - **Flush errors:** `Fenced` gives `Fenced`, `Conflict` gives `Evicted{AppendConflict}`, `Unavailable` gives `Evicted{External}`.
  - `sink.done` is called exactly once, after the Store is dropped (which releases the limiter budget).
  - Metrics: `ostro_activation_outcomes_total{outcome}` and `ostro_activation_duration_seconds`. Each activation gets a tracing span `activation{partition,run,attempt}`.
  - `classify` checks in this order: `NonDeterminism`, `Evict`, `ProtocolViolation`, `GuestExit`, limiter breach or `MemoryExceeded` (both give `MemoryExceeded`), `Trap::OutOfFuel` / `Interrupt` / `StackOverflow` (giving `FuelExhausted`, `Timeout`, `StackOverflow`). Anything else is `Trap("{e:#}; stderr: <tail>")`.
- **`testkit.rs`**
  - `FakeClock::new(start)` advances 1 ms per call; `with_step(start, step)` sets the step, and 0 freezes the clock.
  - `RunSpec::new(run, workflow, input)` and `run_started_event(module_hash_hex)`.
  - `Rule`: `CompleteEffects`, `Signal`, `CompleteTokens`, `CompleteChildren`, `CancelAfterActivations`, `FenceOnAppend`, `ConflictOnAppend`.
  - `Script` builder (`rule`, `max_activations` (default 100), `fire_timers` (default true)). When no rule matches, effects and children fail with `NonRetryable{kind:"unsupported"}`.
  - `FakeSink::append`: an attempt mismatch or a scripted fence gives `Fenced`. A scripted conflict or `from_seq != len` gives `Conflict{expected}`. Non-contiguous seqs give `Unavailable("bad seqs")`.
    - Other `FakeSink` methods: `seed`, `set_attempt`, `commit_inputs`, `journal`, `batches`, `outcomes`, `appended_events`, `append_count`.
  - `drive_to_completion` returns `Drive { outcome, journal, activations, appended_batches, latest_status, outcomes }`.
  - `replay_journal` returns `(outcome, appended events)`.

## Verification

- `cargo test -p ostro-engine --lib activation`: 6 classify / flush-mapping / outcome-code unit tests pass.
- `cargo test -p ostro-engine --test activation`: 11 pass.
  - det-basic completes in 2 activations (`Evicted{WaitUnsatisfied}`, then `Completed`), and the journal ends with `RunCompleted`. `latest_status` is `{"phase":"done"}`.
  - Replay of that journal returns the same output and appends 0 events.
  - The node budget returns to its earlier value after each activation, and ends at 0.
  - `FenceOnAppend(1)` gives `Fenced`, recorded by `done`.
  - `ConflictOnAppend(1)` gives `Evicted{AppendConflict}`, then a completed run over 3 activations with the same output as the clean run.
  - Tampering with the s1 hash gives `NonDeterminism{seq: s1}`. The DLQ reason starts with `nondeterminism: seq=` and contains both summaries; 0 events are appended.
  - The guest workflows behave as expected:
    - `non-retryable` gives `Failed{NonRetryable{kind:"bad-input"}}`, and the journal ends with `RunFailed`.
    - `panic` gives `Trap(..boom..)`, with the `before-panic` step flushed.
    - `exit` gives `GuestExit(7)`.
  - `describe` reports `surface` with `run_timeout_ms == Some(30000)`.
  - An already-cancelled token gives `Evicted{External}` with 0 appends.
- `cargo test -p ostro-engine` passes for every test binary. `cargo clippy -p ostro-engine --all-targets --all-features -- -D warnings` and `cargo fmt -p ostro-engine --check` are clean.
- `bash scripts/ci-local.sh --no-musl`: build, test (whole workspace), failpoints, deny and vendored checks PASS. fmt and clippy FAIL only in `crates/ostro-log` (src/node.rs, rf1.rs, epoch_cache.rs, tests/replicated_log_conformance.rs). Those are uncommitted in-progress files from the concurrent 02-08 executor and outside this plan's scope.

## Deviations from Plan

### Auto-fixed Issues / design choices

**1. [Rule 3 - Blocking] `classify` signature**
- **Found during:** Task 1
- **Issue:** The plan's signature is `classify(e, &HostState)`, but the limiter's `breach` field is private to `HostState`, and `state.rs` is outside this plan's files.
- **Fix:** The signature is now `classify(e, Option<Breach>, stderr_tail)`. The driver reads `breach` through `HasLimits::limiter()`. A side benefit is that the function is pure and directly unit-testable.
- **Commit:** ab1709f

**2. [Rule 2 - Correctness] Conflict test uses a frozen clock**
- **Found during:** Task 2
- **Issue:** The conflicted drive runs one extra activation, so it makes more clock calls. Live timestamps would then differ (the TimerStarted fire_at, and s2's uuid-v7 / now), and the output would differ from the clean run.
- **Fix:** `FakeClock::with_step(start, 0)` makes live timestamps depend only on the journal. The clean run and the conflicted run share that runtime.
- **Commit:** ea9270a

**3. [Rule 2 - Correctness] Biased cancel + external-cancel test**
- **Found during:** Task 2
- **Fix:** Cancellation now wins `select` ties, so a cancelled activation never runs guest code. Added `activation_external_cancel_evicts`.
- **Commit:** ea9270a

**4. [Rule 2] Drive loop re-activates after conflict/external evictions**
- **Found during:** Task 2
- **Issue:** As the plan describes the loop, the run is treated as stuck when there are no new inputs. A conflict eviction never has new inputs, so the run would stop.
- **Fix:** Only a `WaitUnsatisfied` eviction with nothing to deliver counts as stuck.
- **Commit:** ea9270a

## Known Stubs

- `Activate.inputs` (hot-wait input channel) is not consumed. Every unsatisfied wait evicts; 03-11 adds hot wait and LRU budget eviction.

## Requirements

None of the three requirements is closed by this plan alone, so all stay Pending.
- **ENG-03:** the DLQ-shaped nondeterminism outcome is delivered. The divergence-at-exact-seq suite in 03-09 still claims it.
- **ENG-05:** breach classification ends only the attempt, but end-to-end breach tests are in 03-10.
- **ENG-09:** evict-and-replay is delivered, but hot wait and LRU node-budget eviction are in 03-11.

## Commits

- ab1709f feat(03-08): Runtime facade and activation driver with failure classification
- ea9270a feat(03-08): FakeSink testkit and activation integration tests

## Self-Check: PASSED

All four files exist; commits ab1709f and ea9270a are in git log.
