---
phase: 11-run-operations-embedded-ui
plan: 06
subsystem: engine / worker protocol / server executor / docs
tags: [limits, ops-07, engine, replay, worker-wire, docs]
requires:
  - 11-02 (RunLimits in EngineConfig.run_limits)
  - 11-04 ([limits] config, ServerConfig::to_engine_config, tests/common/p11.rs)
provides:
  - ostro_engine::run_limits::{check_live_append, LimitBreach, LIMIT_HISTORY, LIMIT_CHILDREN, LIMIT_PAYLOAD}
  - AttemptFailure::LimitExceeded{limit, max, actual} (non-retryable, reason_code limit_exceeded)
  - ReplayError::LimitExceeded, ReplayState::set_run_limits
  - WireAttemptFailure::LimitExceeded (postcard variant 8, appended last)
  - EngineLimits fields 10/11/12 (max_history_events, max_children_per_run, max_payload_bytes)
  - guests/p11-limits
  - docs/limits.md (machine-parseable table for the 11-11 doc-sync test)
affects: [11-07 (executor.rs), 11-11 (GET /v1/limits + doc-sync), 11-10 (engine files)]
tech-stack:
  added: []
  patterns:
    - "limit check on the single live append path (ReplayState::append_live), never on replay"
key-files:
  created:
    - crates/ostro-engine/tests/run_limits.rs
    - crates/ostro-server/tests/limits_run.rs
    - guests/p11-limits/Cargo.toml
    - guests/p11-limits/src/lib.rs
    - docs/limits.md
  modified:
    - crates/ostro-engine/src/{run_limits,error,activation}.rs
    - crates/ostro-engine/src/replay/{cursor,wait}.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-worker-proto/proto/ostro/worker/v1/execute.proto
    - crates/ostro-worker-proto/src/outcome.rs
    - crates/ostro-worker-proto/tests/wire.rs
    - crates/ostro-worker/src/runner.rs
    - crates/ostro-server/src/workers/service.rs
    - crates/ostro-server/src/executor.rs (doc row only)
decisions:
  - "The check runs inside ReplayState::append_live, which now returns Result: every live append (command, patched, wait resolution, complete_step, record_attempt_failed) funnels through it, and replay never does"
  - "complete_step appends before closing the step guard, so a breach leaves no half-closed state"
  - "LimitBreach is also the trap payload; classify() downcasts it right after NonDeterminism, before evictions"
  - "Payload size = Payload.data length of the step Ok output, run result, child input and continue-as-new input; a step Err outcome counts its encoded length. The step outcome is decoded only when its encoded length already exceeds the max"
  - "WireAttemptFailure::LimitExceeded carries the limit name as a String; into_engine maps it back to the three &'static str constants, and an unknown name becomes ProtocolViolation"
  - "executor.rs needed no code change: the existing non-retryable path already completes CompleteRun{Failed, resumable: true} with dlq_reason as the message"
  - "workers/service.rs engine_limits now uses ServerConfig::to_engine_config() (was cfg.engine.to_engine_config()) so [limits] reaches workers"
metrics:
  duration: "~1h15m"
  completed: 2026-11-22
---

# Phase 11 Plan 06: Live-only engine limits Summary

The engine now enforces the per-run limits from `EngineConfig.run_limits` (history length, children per run, payload size), and only when the guest appends a new live event. A breach appends nothing. The attempt ends with the non-retryable `AttemptFailure::LimitExceeded`, and the executor turns that into a resumable `Failed` run with the message `limit_exceeded: <limit> <actual> > <max>`. Workers receive the limits in `EngineLimits` and report a breach through the `WireOutcome` mirror. Journals that already exceed a lowered limit still replay green with 0 appended events. `docs/limits.md` publishes every limit.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 6d2a668 | `feat(11-06)`: live-only run limits in the engine. The WireOutcome variant lands in this same commit (D-P11-13) so that every commit compiles. |
| 2 | c5d57d9 | `feat(11-06)`: limit_exceeded on the worker wire and in the executor; publish docs/limits.md |

## Where the check sits in the cursor

`ReplayState::append_live(seq, body) -> Result<(u64, u64), ReplayError>` calls `run_limits::check_live_append(&self.limits, seq, self.children, &body)` before it builds the event. This is the only live append path:

- `command` (all journaled guest commands, the terminal included)
- `patched` (a live `PatchMarker`)
- `wait` (a live `WaitResolved`)
- `complete_step`
- `record_attempt_failed`

Replayed events go through `match_next` / `consume` and are never checked. `self.children` is counted from the journal in `ReplayState::new` (every `ChildStarted`) and incremented on each live `ChildStarted`. `ReplayState::set_run_limits` is called once per activation in `activation.rs` with `cfg.run_limits`; the default is `RunLimits::default()`, so `ReplayState::new` keeps its signature.

The rules:

- **History.** A non-exempt append at `next_seq` breaches when `next_seq + 1 > max`, with `actual = next_seq + 1`. `RunCompleted`, `RunFailed`, `RunContinuedAsNew` and `StepAttemptFailed` are exempt (W8).
- **Children.** A `ChildStarted` breaches when `children + 1 > max`.
- **Payload.** The append breaches when the payload bytes exceed `max`.

How a breach becomes an attempt failure:

- Inside a host call, `trap()` wraps the `LimitBreach` and `classify()` maps it.
- On the terminal / interrupted-step paths, `replay_failure()` maps it.
- In both cases the live events appended before the breach are flushed (best effort, like other non-nondeterminism failures).

## EngineLimits field numbers (worker protocol, appended)

| Field | Number | Meaning |
|---|---|---|
| `max_history_events` | 10 | `[limits] max_history_events`; 0 = worker engine default |
| `max_children_per_run` | 11 | `[limits] max_children_per_run`; 0 = default |
| `max_payload_bytes` | 12 | `[limits] max_payload_bytes`; 0 = default |

`WireAttemptFailure::LimitExceeded {limit: String, max, actual}` is postcard variant index 8, appended after `Trap` (7). The existing encodings are unchanged and pinned by a test: `AttemptFailed(Trap("x"))` encodes as `[4, 7, 1, b'x']`.

## Server/worker files actually touched

- Server: `crates/ostro-server/src/workers/service.rs` (`engine_limits`).
- Worker: `crates/ostro-worker/src/runner.rs` (`engine_config`).
- `crates/ostro-worker/src/session.rs` is listed in `files_modified` but did not need a change, because the runtime is built through `runner::engine_config`.

## docs/limits.md table

| Name | Default | Unit | Where enforced |
|---|---|---|---|
| `max_history_events` | 50000 | events per run | engine live |
| `max_children_per_run` | 10000 | ChildStarted per run | engine live |
| `max_payload_bytes` | 4194304 | bytes | engine live + API (11-09/11-11) |
| `max_request_bytes` | 4194304 | bytes | API |
| `max_module_bytes` | 67108864 | bytes | API |
| `MAX_CONTEXT_BYTES` | 65536 | bytes | engine (set-status JSON, random-bytes) |
| `MAX_RUN_TAGS` | 64 | tags | API + apply |
| `MAX_RUN_TAG_LEN` | 128 | bytes | API + apply |
| `MAX_SIGNAL_NAME_LEN` | 256 | bytes | API + apply |
| `MAX_REASON_LEN` | 1024 | bytes | API + apply |
| `MAX_REQUEST_ID_LEN` | 64 | bytes | API + apply |
| `MAX_PAGE_LIMIT` | 1000 | items | API (capped) |
| `MAX_RESULT_WAIT_MS` | 60000 | ms | API (capped) |

The full file adds the configurable, error and replay-behaviour columns, plus two sections: "How a run past a limit behaves" and "Never enforced during replay". Status and context share one constant, `MAX_CONTEXT_BYTES`, so they are a single row. D-P11-13 lists them separately.

## Deviations from Plan

1. **[Rule 3] Files outside `files_modified`:**
   - `crates/ostro-engine/src/host/state.rs`: one arm in `trap()`, because the `ReplayError` match is exhaustive.
   - `crates/ostro-engine/src/replay/wait.rs`: `?` on its `append_live` call.
2. **WireOutcome variant moved into Task 1's commit.** The mirror's conversions are exhaustive, so Task 1 alone would not compile. D-P11-13 asks for "the same commit" anyway.
3. **Guest workflow names** are prefixed: `limits-history`, `limits-children`, `limits-big-output`, `limits-big-result`, plus `limits-leaf` as the child. `limits-history` takes `end: ok|fail|flaky` to exercise the W8 exemptions.
4. **executor.rs** got a doc-table row only. No code change was needed (see decisions).
5. **Workflow per the executor brief:**
   - Scratch worktree detached at main, slot `target/slot-2`, main moved by CAS.
   - Rebased once, onto 11-09 (416a600), and `cargo check --workspace --all-targets` re-run green afterwards.

## Tests

- `cargo test -p ostro-engine --test run_limits`: 6 passed. Covers:
  - the history breach at 21 > 20, with the journal at exactly 20 events
  - W8: completed, failed and StepAttemptFailed records exempt; the closing StepCompleted breaches at 22
  - children 4 > 3
  - payload 2097152 > 1048576 for step output and run result; exactly 1 MiB passes
  - replay of the history/children/payload journals under lowered limits with 0 appended
  - a replayed journal past the limit fails at its first live append, and completes again under the default limits
- `cargo test -p ostro-engine` (all targets): green. lib: 96 passed. The new unit tests in `run_limits`, `error` and `activation` classify are included.
- `cargo test -p ostro-server --test limits_run`: 2 passed.
  - Embedded: Failed + resumable, message starting `limit_exceeded: history_events 21 > 20`, journal of 20 events. Then raise the limit, restart, retry: `{"mode":"resumed","run_id":<same>}` and the run completes with 32 events and `resumed_count == 1`.
  - Worker-executed queue: same failure, and the worker emitted `ActivationStarted` for the run.
- `cargo test -p ostro-worker-proto --test wire`: 8 passed (3 new).
- `cargo test -p ostro-worker`: green.
- `cargo test -p ostro-server --test worker_remote --test worker_session --test executor --test p11_contract`: green.
- `cargo clippy --workspace --all-targets -- -D warnings`: clean.
- `cargo clippy -p {ostro-engine, ostro-server, ostro-worker} --features failpoints --all-targets -- -D warnings`: clean.
- `cargo fmt --check`: clean.
- No new `SystemTime::now` / `Instant::now` / `SystemClock` reads.
- Not run: the full `scripts/ci-local.sh`, because several agents share the machine. The affected crates' suites and the workspace clippy cover this change.

## Self-Check: PASSED

- Created files exist: `crates/ostro-engine/tests/run_limits.rs`, `crates/ostro-server/tests/limits_run.rs`, `guests/p11-limits/{Cargo.toml,src/lib.rs}`, `docs/limits.md`.
- Commits 6d2a668 and c5d57d9 are present.
