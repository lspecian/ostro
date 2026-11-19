---
phase: 04-single-node-server-rust-mvp
plan: 15
subsystem: server-seam
tags: [activation-sink, derive, fencing, blob-offload, run-started, journal-read, unsupported-effects]
requires:
  - phase: 04-12
    provides: RunRecord Phase 4 fields, Suspend CAS + InvalidCode::NotRunning, ostro_partition::query helpers
  - phase: 04-04
    provides: StepAttemptFailed (kind 29), additive journal codec rule
  - phase: 04-02
    provides: ostro-server crate skeleton and workspace pins
provides:
  - ostro_server::derive (pure journal-event -> Derived mapping, two-phase API, failure JSON helpers)
  - ostro_server::sink::PartitionSink (ActivationSink over PartitionHandle; next_seq, is_terminal, submit_intents, repair_unsupported, stored outcome)
  - ostro_server::run_started::{build_run_started, input_payload, JSON_CONTENT_TYPE}
  - ostro_server::journal_io::{read_journal, read_journal_from, read_event, JournalReadError}
affects: [04-16, 04-18, 04-20, 06-effects-bridge]
tech-stack:
  added: []
  patterns:
    - "Pure sync mapping + caller-side I/O: the sink prepares the terminal result payload and resolves OpCancelled targets before calling derive::intents"
    - "Phase 4 unsupported completions are journal inputs written with the activation's own token right after the scheduling append"
key-files:
  created:
    - crates/ostro-server/src/derive.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/run_started.rs
    - crates/ostro-server/src/journal_io.rs
    - crates/ostro-server/tests/sink.rs
  modified:
    - crates/ostro-server/Cargo.toml
    - crates/ostro-server/src/lib.rs
    - Cargo.lock
key-decisions:
  - "derive API is two-phase: terminal_result_bytes(events) -> caller prepare_payload -> intents(events, prior: &dyn Fn(u64) -> Option<EventBody>, result: Option<PayloadRef>). derive stays sync, pure and clock-free"
  - "Run result bytes: RunCompleted stores the output's raw data (SDK JSON; content type stays in the journal event). Failures store JSON {reason_code, kind, message}; RunFailed uses reason_code workflow_failed (timeout / cancelled for those variants); unsupported adds detail"
  - "AwaitTokenCreated fails the run with CompleteRun{Failed, unsupported} in the SAME append (atomic), not a second one; effects/children get their completion inputs in a second append"
  - "RunTerminal, RunNotFound and Invalid(NotRunning) map to AppendError::Fenced (activation must stop), besides Fenced/StaleEpoch"
  - "PartitionSink is generic over L: ReplicatedLog with default Rf1Log; next_seq starts at the RunRecord's journal end and follows accepts and SeqMismatch.expected"
  - "fetch_blob(sha) stats the blob file to learn its length, then BlobStore::get verifies length + sha256 (no ostro-blob API change)"
requirements-completed: []
duration: 35min
completed: 2026-11-19
---

# Phase 4 Plan 15: Storage <-> engine seam (derive, PartitionSink, RunStarted, journal_io) Summary

**`ostro-server` now has the real `ActivationSink`. Each engine flush becomes exactly one fenced `EventsAppended`: intents come from a pure, unit-tested mapping, payloads over 256 KiB go to the blob store first, and apply outcomes map to `Ok(next)`, `Fenced`, `Conflict{expected}` or `Unavailable`. Effects, children and await tokens get the locked Phase 4 "unsupported" non-retryable completion.**

## Performance

- **Duration:** about 35 min
- **Tasks:** 2 (both TDD-style; tests written alongside and green on the first run)
- **Files:** 5 created, 3 modified

## Accomplishments

- **derive.rs** (`pub fn intents`)
  - TimerStarted at seq s -> `ScheduleTimer{op: s, due_at_ms: ceil(fire_at/1e6)}`.
  - `OpCancelled{op}` -> `CancelTimer{op, due_at_ms}` when op is a timer. The target is looked up in the batch first, then through `prior`. An unknown op is `DeriveError::UnknownOp`.
  - `RunCompleted` -> `CompleteRun{Completed}`, `RunFailed` -> `CompleteRun{Failed, failure JSON}`, `RunContinuedAsNew` -> `CompleteRun{Failed, {"reason_code":"unsupported","detail":"continue-as-new arrives in Phase 5"}}`.
  - `completed_at_ms = ceil(ts/1e6)`. `CompleteRun` is always last; a terminal event that is not last, or a second one, is `TerminalNotLast`.
  - Everything else maps to nothing, including `StepAttemptFailed` (04-16 requeues itself).
  - Helpers: `ceil_ms`, `failure_json`, `run_failed_json`, `unsupported_json`, `REASON_*`.
  - 11 unit tests.
- **run_started.rs**
  - `build_run_started(rec, input: Payload, module_hash_hex) -> Result<Event, RunStartedError>`. It is field-for-field equal to the engine testkit's `RunSpec::run_started_event`; a unit test asserts this.
  - Fields: accepted = `created_at_ms * 1e6`; deadline = accepted + `run_timeout_ms`; tenant; `parent_run_id` (string form).
  - A seed that is not 32 bytes gives `BadSeed`. `input_payload(bytes)` wraps the input with `application/json`.
  - 4 unit tests.
- **journal_io.rs**
  - `read_journal`, `read_journal_from` and `read_event` read pages of 1024 rows from `query::journal_rows` in `spawn_blocking`, fetch blob payloads (length and sha verified) and run `decode_event`.
  - Rows must be contiguous (`Gap` otherwise). Every error names the run and the seq.
- **sink.rs** (`PartitionSink<L = Rf1Log>`)
  - `new(handle, run, token, inputs, on_renew)`, `next_seq()`, `is_terminal()`, `token()`, `run()`, `handle()`.
  - `submit_intents(derived)`: an events-free append at `next_seq`, for the executor's Suspend, EnqueueReady or failure CompleteRun CAS.
  - `repair_unsupported(&journal)`; `outcome()` / `take_outcome()` (filled by `done`).
  - `fetch_blob`.
  - The append path is the research sketch, plus a check that the attempt matches `token.n` and that the run matches.
- **tests/sink.rs**: 15 tests against a real single-partition `Node`:
  - accept, round-trip and renew; timer scheduled and cancelled from the journal;
  - newer grant -> Fenced; wrong attempt number -> Fenced with nothing submitted; stale from_seq -> Conflict; node shut down -> Unavailable;
  - a 300 KiB step stored as a blob and decoded intact; RunCompleted -> run Completed, result bytes and completed_at, then Fenced;
  - effect and child completions journaled and forwarded on `inputs`; repair; await token -> run Failed "unsupported", then Fenced;
  - Suspend via `submit_intents`, then NotRunning -> Fenced; `done` / `fetch_blob`; a decode failure that names the run and seq.

## Task Commits

1. **Task 1: derive + run_started + journal_io (+ deps, Cargo.lock)**: `ee3621b` (feat)
2. **Task 2: PartitionSink + tests/sink.rs**: `26436bd` (feat)

## Verification

- `cargo test -p ostro-server --lib --locked`: 17 passed (derive 11, run_started 4, plus the existing config tests).
- `cargo test -p ostro-server --test sink --locked`: 15 passed (1.8 s).
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean.
- `bash scripts/ci-local.sh --no-musl` ran in a scratch worktree at 199ca4d with only the 04-15 commits cherry-picked, using the shared target dir. Toolchain, fmt, clippy, warning-free build, `cargo test` (370 s), deny, check-vendored, deny-fixtures, forbidden-deps and workflow validation all **PASS**. The musl legs were SKIP. The failpoint suites step **FAILED** in `ostro-partition` `rebuild_basic` with `SeqMismatch{expected: 2}`, but this was cross-worktree contamination, not a code failure:
  - Every worktree compiles to the same workspace-relative artifact hashes in the shared `target/`. The failpoints build therefore reused 04-13's newer `ostro-partition`, which journals `TimerFired`; 04-13 fixed the test in 177b787.
  - Re-running the whole step with an isolated target dir (`cargo test -p ostro-partition --features failpoints -- --test-threads=1`, plus `ostro-crashtest` clippy and tests) on the same tree passed: 19 suites in ostro-partition, 0 failures.
- Re-verified on main at f4dab4a (after 04-13 and 04-14 landed) with an isolated target: `cargo test -p ostro-server --locked` passed (lib 17, config 16, sink 15); `cargo clippy --workspace --all-targets --locked -- -D warnings` and `cargo fmt --all --check` were clean.

## Deviations from Plan

### Design choices within the plan's latitude

**1. Await token failure goes in the same append.** The plan says "AwaitTokenCreated -> CompleteRun{Failed, reason unsupported} ... and subsequent appends return Fenced". I put the `CompleteRun` in the scheduling append itself, so there is no window where the token exists but the run is not failed. It is skipped when the batch already ends the run.

**2. [Rule 2 - Correctness] Unsupported completions skipped when the batch ends the run.** Otherwise the second append would hit `RunTerminal`.

**3. [Rule 2 - Correctness] `repair_unsupported`.** The scheduling append can commit and the completion append can then fail with Unavailable. In that case `append` returns `Unavailable` and the effect would have no completion. `repair_unsupported(&journal)` journals any missing completions. 04-16 must call it on the journal it loads, before activating.

**4. [Rule 2 - Correctness] Completion appends retry on SeqMismatch.** There are up to 4 attempts, renumbered from `expected`, because a `TimerFired` input (04-13) can land between the two appends.

**5. Extra rejection mapping.** `RunTerminal`, `RunNotFound` and `Invalid(NotRunning)` map to `Fenced`, per the 04-12 hand-off. The plan named only Fenced and StaleEpoch.

**6. Dependencies.** `tokio-util` was not added because nothing uses it yet; 04-16 adds it for `CancellationToken`. `ostro-blob` was added for `BlobRef`/`hex`. `tempfile` was already a normal dependency.

**7. `build_run_started` returns `Result`.** Apply does not validate the length of `rng_seed`.

## Known gaps

- **Race between dispatcher and sink inputs.** The engine's `deliver_inputs` requires contiguous seqs. If a dispatcher-forwarded `TimerFired` and a sink-sent unsupported completion are sent out of seq order on the same channel, the engine reports `BadJournal`. That is rare, and it only happens when a timer and an effect are both pending.
- **Result content type.** The result payload stores only the bytes. The content type is in the `RunCompleted` journal event if 04-18 needs it.

## Notes for 04-16 (executor)

- **Construct the sink:** `PartitionSink::new(job.handle.clone(), job.run, job.token, Some(engine_inputs_tx), Some(Arc::new(move || lease.renew())))`, then `Arc` it and pass it to `Runtime::activate`. `RenewFn = Arc<dyn Fn() + Send + Sync>`.
- **First activation:** when `next_seq() == 0`, build `build_run_started(&rec, input_payload(fetch_payload(run_payload(PAYLOAD_INPUT))), &ostro_blob::hex(&sha))?` and `sink.append(&run_ref, token.n, 0, vec![ev])`.
- **Load:** `read_journal(&handle, &run)`, then `sink.repair_unsupported(&journal)`. If that appended anything, re-read the journal.
- **Input bridge:** `journal_io::read_event(&handle, &run, seq)` gives one row.
- **CAS submissions:** `sink.submit_intents(vec![Derived::suspend()])` or `vec![Derived::new(Intent::EnqueueReady(..))]`. `Err(Conflict)` means re-activate; `Err(Fenced)` covers Fenced, StaleEpoch, RunTerminal and NotRunning.
- **Failure completion:** `submit_intents(vec![CompleteRun{Failed, result: prepare_payload(derive::failure_json(af.reason_code(), af.reason_code(), &af.dlq_reason()))}])`.
- **Terminal check:** after the activation, `sink.is_terminal()` means a terminal `CompleteRun` was accepted, including the await-token failure. Release the lease even if the engine outcome is `Fenced`.
- **Outcome:** `sink.take_outcome()` returns what `done()` stored. The engine also returns it from `activate`.
- **RunRef:** `RunRef{partition: run.partition().0, run_id: run.to_string()}`. The sink rejects any other value with Unavailable.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/derive.rs (`pub fn intents`)
- FOUND: crates/ostro-server/src/sink.rs (`impl<L: ReplicatedLog> ActivationSink for PartitionSink<L>`)
- FOUND: crates/ostro-server/src/run_started.rs (`pub fn build_run_started`)
- FOUND: crates/ostro-server/src/journal_io.rs (`pub async fn read_journal`)
- FOUND: crates/ostro-server/tests/sink.rs
- FOUND: commits ee3621b, 26436bd
