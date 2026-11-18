# Phase 5 decisions (adopted defaults; user may override)

**Status:** adopted by the planner on 2026-11-18. The project runs in YOLO + auto_advance mode and the user was not available, so no blocking decision checkpoint was used. For every decision D1–D18 in `05-RESEARCH.md` § "Open Questions / Decisions to Confirm Before the Freeze", the research's recommended option was adopted. Plan 05-01 Task 1 copies each one into `.planning/STATE.md` § Decisions by hand. It also adds user-review items to § Pending Todos.

**User-review items** (flagged in STATE.md Pending Todos by 05-01):
- D7, step-local RNG semantics. These freeze with WIT 0.1.
- D8, the relaxed cold-start criterion and P=16 as the frozen default.
- D14, the WIT pre-freeze review outcome: doc comments only, no record changes.
- The WIT freeze flip itself, done in 05-17. After it, `runtime.wit` can only change additively, through a new `0.1.N` package.

An override that lands before 05-17 can be folded in. An override after 05-17 is bound by the freeze rules.

| ID | Topic | Adopted option | Consequence in the plans |
|----|-------|----------------|--------------------------|
| D1 | Retry semantics | **Resume-in-place** for `resumable` terminal runs, meaning executor-level failure: trap, fuel, memory, nondeterminism, attempt failures exhausted, Terminated, TimedOut. **Restart as a new linked run** (`StartRun.retry_of`) when the journal ends in a guest terminal event (`RunFailed`, or a `Cancelled` status derived from `RunFailed{cancelled}`). **409 `run_completed`** for Completed. | 05-07 implements `RetryRun` (resume). 05-11 decides the mode at the API from `RunRecord.resumable` and submits `RetryRun` or a new `StartRun{retry_of}`. |
| D2 | Timeout semantics | Run timeout is a **hard stop** giving `TimedOut`, with no guest cleanup and reason `run_timeout`. Schedule-to-start applies to the **initial start only** (never-granted runs), reason `schedule_to_start`. | Reserved timer ops `RUN_TIMEOUT_OP` and `SCHEDULE_TO_START_OP` (05-01 constants, 05-07 apply). |
| D3 | Terminate | Distinct `Terminated` status. It cascades a **graceful** `CancelRun` to children and journals a **`RunTerminated{reason}` (kind 71)** history marker. The marker is journaled only when `next_seq > 0`: a run that never started gets no seq-0 marker, so "exactly one RunStarted at seq 0" holds. The same marker, with reason `run_timeout`/`schedule_to_start`, is used for TimedOut. | 05-01 kind 71. 05-07 apply. |
| D4 | Wait timeouts | New input kind **`WaitTimedOut{op}` (code 70)**. `TimerFired` is not reused. | 05-01 kind. 05-03 engine maps it to `Err(Failure::Timeout)`. 05-06 apply journals it. |
| D5 | cancel-op | `cancel-op` **completes the op with `Err(Cancelled)`**: an engine change that matches the WIT doc. The first completion in journal order wins. | 05-03. |
| D6 | patched | Memoised per change-id, and unqueried `PatchMarker`s are skipped. The server-level proof uses a test-only `#[doc(hidden)] ExecutorHooks::module_override`. There is no re-pin API before Phase 13. | 05-03 (engine), 05-09 (hook), 05-13 (e2e). |
| D7 | Step-local RNG (03-11) | **Frozen as is.** Randomness inside a live step comes from `sha256("ostro/step-rng/v1" ‖ seed ‖ seq)`, so each retry attempt gets a fresh stream. Documented in the WIT doc comment (05-05) and in the SDK docs (05-08). Phase 9 SDKs must reseed at step boundaries the same way. **USER REVIEW.** | 05-05 doc comment; 05-08 SDK docs. |
| D8 | Startup criterion, partition count | Keep the Phase 4 relaxed criterion: warm restart and `--ephemeral` < 1 s, cold first run measured and reported. **P=16 is the frozen single-node default** (dev = server format). **USER REVIEW.** | 05-01 records it. 05-17 re-measures and reports. The STATE.md blocker "Fixed partition count must be confirmed" is resolved as adopted-pending-review. |
| D9 | Retention defaults | `[runs] retention` defaults: **7 days server, 24 h dev profile**. Order: request override, else `WorkflowPolicy`, else `QueuePolicy`, else config. Resolved at start and stored in `StartRun.retention_ms`. Policy changes affect future runs only. WAL bytes are not reclaimed until Phase 8 (`snapshot_floor = 0`). | 05-09 (meta policies + resolver + config), 05-07 (EXPIRE_OP apply), 05-16 docs. |
| D10 | Continue-as-new | **In scope for Phase 5.** It adds `TerminalStatus::ContinuedAsNew` and outbox `Kind::StartRun` (tag 6), both of which freeze now. | 05-01 contract, 05-07 apply, 05-09 derive/executor resolver, 05-11 long-poll `follow_continuations`. |
| D11 | SDK cancellation surfacing | **(b)**: `Op::wait()` observes cancellation by default (`wait([op, CANCELLATION], Any)` gives `Err(Cancelled)`), and `ctx.shielded(\|\| …)` runs cleanup code without that. SDK-only, no WIT change. | 05-08. Existing SDK tests whose journals change shape must be updated deliberately, with the reason in the SUMMARY. |
| D12 | Minimal deploy | Add **`ostro deploy <file.wasm> [--activate]`**, upload only. CLI-05 build-if-needed stays in Phase 10. | 05-12. |
| D13 | Signal dedupe | Optional **`request_id`**, stored in a run-scoped **`SignalSeen`** keyspace and deleted with the run. | 05-02 keyspace, 05-06 apply. |
| D14 | WIT pre-freeze review | **No record/variant/enum shape changes in Phase 5.** Every candidate takes the "new function / new interface in `0.1.N` later" path. The candidates are object presign expiry and content-type, the http redirect/size policy, child-options tags and schedule-to-start, run-info queue and tags, and wait-signal metadata. Reasons: these effect shapes belong to Phases 6 and 9 and are still unknown, so freezing guessed fields would be worse; some request records are journaled inside existing event payloads, and changing them would force an `EVENT_CODEC_VER` bump; and it avoids regenerating SDK bindings mid-phase. **Phase 5 WIT edits are doc comments only.** **USER REVIEW.** | 05-05 writes the review table to `docs/wit-0.1-freeze-review.md` and updates the doc comments. |
| D15 | Perf gate environment | Native Linux NVMe, 8 vCPU, run by a human. WSL2 numbers are **advisory**. They are measured, reported and never a failure reason by themselves. | 05-15 and 05-17: `human_needed` acceptance path. |
| D16 | Benchmark step definition | A step counts when its `StepCompleted` is acknowledged **and found on disk after a restart**. **Profile A** (10-step runs) is the gate. **Profile B** (1-step runs) is reported. | 05-15. |
| D17 | Wasmtime 51 (~2026-12-20) | **Do not bump during Phase 5** once 05-05 has started. Bump after 05-17 (the post-freeze PR), with `wit-component`/`wit-parser` bumped together and the replay corpus green. | STATE.md Pending Todo (05-01). |
| D18 | On-disk freeze scope | Frozen sources of truth: the log (envelope, `Command` prost, journal event codec, command hash), `node.toml`, the blob layout, and the meta Raft log/snapshot encoding. The **partition state-store value encoding is derived**: a future `STATE_FORMAT` bump rebuilds state from the log (after Phase 8, from snapshot + tail). | 05-02 writes the policy into `docs/storage-format.md`. 05-17 marks it "FROZEN v0.1". |

## Additional planner decisions (Claude's discretion, from research recommendations)

- **Contract tags.** The provisional numbers below are re-checked against the Phase 4 SUMMARYs in 05-01 Task 1. If Phase 4 used any of them, shift to the next free number and record the shift in the 05-01 SUMMARY.
  - `Cmd::SignalRun` = 8, `RequestCancel` = 9, `TerminateRun` = 10, `RetryRun` = 11, `CompleteToken` = 12.
  - `StartRun` 19 `schedule_to_start_ms`, 20 `retention_ms`, 21 `retry_of`, 22 `continued_from`, 23 `run_started` (optional `JournalEvent`).
  - `derived::Intent::RegisterWait` = 7, `CancelWait` = 8; `CompleteRun.resumable` = 4.
  - `TerminalStatus::{Terminated=3, TimedOut=4, ContinuedAsNew=5}`; `outbox_msg::Kind::StartRun` = 6.
  - Journal `WaitTimedOut` = 70, `RunTerminated` = 71.
- **Reserved timer ops.** `START_DELAY_OP = u64::MAX` (exists), `RUN_TIMEOUT_OP = u64::MAX-1`, `SCHEDULE_TO_START_OP = u64::MAX-2`, `EXPIRE_OP = u64::MAX-3`. The range `u64::MAX-15..=u64::MAX` is reserved, and guest `ScheduleTimer`/`CancelTimer` intents in it are rejected with `BadIntent`.
- **Waits keyspace.** One keyspace, `Waits`, with two key shapes told apart by a 1-byte prefix: `0x01 ‖ run ‖ op BE` for the wait row and `0x02 ‖ run ‖ name_len u16 ‖ name ‖ op BE` for the FIFO-by-name index. A run-scoped prefix delete needs two prefix ranges, which is documented.
- **Apply split.** `apply/waits.rs` (05-06), `apply/lifecycle.rs` and `apply/retention.rs` (05-07), and `apply/inputs.rs` and `apply/terminal.rs` (05-02, shared). 05-02 lands the stubs and hooks, so wave 3 plans never touch `apply/mod.rs`, `run.rs` or `timer.rs`.
- **rebuild_digest generators.** These live in `tests/common/gen_waits.rs` (05-06) and `tests/common/gen_lifecycle.rs` (05-07). 05-02 creates them as empty stubs wired into `tests/rebuild_digest.rs`.
- **Bench harness.** `crates/ostro-server/tests/perf.rs` (`#[ignore]`), with no new crate and no new deps. The gate command is `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf -- --ignored --nocapture`.
- **WIT tooling.** A new dev-tool crate, `crates/ostro-witcheck` (`publish = false`), with `wit-component =0.261.0` (feature `semver-check`) and `wit-parser =0.261.0`, the versions already in Cargo.lock through wasmtime 50.
- **CLI transport.** REST via `reqwest =0.13.5`, promoted to a normal dependency of `ostro-cli` (`default-features = false, features = ["json", "http2"]`, no TLS until SEC-01).
- **Crash campaign sharding.** S independent data dirs, default `OSTRO_CAMPAIGN_SHARDS=20` × 500 faults. Up to `OSTRO_CAMPAIGN_PARALLEL` (default 6) shards run concurrently, and progress persists per shard so runs are resumable. Faults are counted per shard and summed. The ≥ 10,000 gate is the sum over a local run of all shards, executed in shard batches that each fit the tool timeout or run in the background.

## Standing rules (every Phase 5 plan)

- The build stays warning-free. clippy runs with `-D warnings`. `bash scripts/ci-local.sh` is the gate. `FAILPOINT_CRATES` in `scripts/ci-local.sh` stays in sync with the "Failpoint suites" step of `.github/workflows/ci.yml`.
- `Cmd` tags 1–7 are frozen and journal kinds are append-only. There is no `EVENT_CODEC_VER` bump: the research identifies no existing-kind encoding change.
- No `chrono`, no `bincode`, no wasmtime `cache` feature.
- Never reference a Chronik clone path. No git remotes and no pushes; tags stay local.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Executors must not run gsd-tools `state advance-plan`, `roadmap update-plan-progress`, `phase complete` or `requirements mark-complete`. Tracking edits are made by hand.
