# Phase 5 decisions (adopted defaults; user may override)

**Status:** adopted by the planner on 2026-11-18 and revised on 2026-11-19 after the plan check. The project runs in YOLO + auto_advance mode and the user was not available, so no blocking decision checkpoint was used. For every decision D1–D18 in `05-RESEARCH.md` § "Open Questions / Decisions to Confirm Before the Freeze", the research's recommended option was adopted. Plan 05-01 Task 1 copies each one into `.planning/STATE.md` § Decisions by hand and adds user-review items to § Pending Todos.

**User-review items** (05-01 flags them in STATE.md Pending Todos):
- D7: step-local RNG semantics. These freeze with WIT 0.1.
- D8: the relaxed cold-start criterion, and P=16 as the frozen default.
- D14: the WIT pre-freeze review outcome (doc comments only, no record changes).
- The WIT freeze flip itself, done in 05-19. After it, `runtime.wit` changes only additively, through a new `0.1.N` package.

An override that lands before 05-19 can be folded in. An override after 05-19 is bound by the freeze rules.

| ID | Topic | Adopted option | Consequence in the plans |
|----|-------|----------------|--------------------------|
| D1 | Retry semantics | **Resume-in-place** for `resumable` terminal runs: executor-level failure (trap, fuel, memory, nondeterminism, attempt failures exhausted), Terminated and TimedOut. **Restart as a new linked run** (`StartRun.retry_of`) when the journal ends in a guest terminal event (`RunFailed`, or Cancelled derived from `RunFailed{cancelled}`). **409 `run_completed`** for Completed. On resume: `RunRecord.resumed_count` is incremented; the first activation of the resumed run **re-arms every unresolved op** from the replayed journal (ScheduleTimer for `TimerStarted`, RegisterWait for `SignalWaitStarted`/`AwaitTokenCreated` with the original due time; past-due fires immediately); a resumed run gets **no new run timeout**. | 05-08 `RetryRun`. 05-10 re-arm. 05-07 idempotent RegisterWait. 05-12 picks the mode from `RunRecord.resumable`. 05-14 adds e2e tests for resuming after a timeout. |
| D2 | Timeout semantics | Run timeout is a **hard stop** → `TimedOut`, no guest cleanup, reason `run_timeout`. Schedule-to-start applies to the **initial start only** (never-granted runs), reason `schedule_to_start`. The S2S timer is deleted by `RunRecord.s2s_due_ms`. | Reserved ops `RUN_TIMEOUT_OP`, `SCHEDULE_TO_START_OP` (05-01 constants; 05-02 record field; 05-08 apply). |
| D3 | Terminate | Distinct `Terminated` status. Children get a **graceful** `CancelRun` cascade. A **`RunTerminated{reason}` (kind 71)** history marker is journaled only when `next_seq > 0`, so "exactly one RunStarted at seq 0" holds. TimedOut uses the same marker with reason `run_timeout`/`schedule_to_start`. Every hard stop emits `Action::RunCompleted`. | 05-01 kind 71. 05-08 apply. |
| D4 | Wait timeouts | New input kind **`WaitTimedOut{op}` (code 70)**. `TimerFired` is not reused. Terminal transitions delete pending wait-timeout timer rows, so a stale timer can never surface as `Ok(Timer)` after a resume. | 05-01 kind. 05-03 engine maps it to `Err(Failure::Timeout)`. 05-02 cleanup in on_terminal. 05-07 apply. |
| D5 | cancel-op | `cancel-op` **completes the op with `Err(Cancelled)`**. The first completion in journal order wins. | 05-03. |
| D6 | patched | Memoised per change-id; unqueried `PatchMarker`s are skipped. The server-level proof uses a test-only `#[doc(hidden)] ExecutorHooks::module_override`. No re-pin API before Phase 13. | 05-03 (engine), 05-10 (hook), 05-14 (e2e). |
| D7 | Step-local RNG (03-11) | **Frozen as is**: randomness inside a live step comes from `sha256("ostro/step-rng/v1" ‖ seed ‖ seq)`, so each retry attempt gets a fresh stream. Documented in the WIT doc comment (05-05) and the SDK docs (05-06). Phase 9 SDKs must reseed at step boundaries the same way. **USER REVIEW.** | 05-05, 05-06. |
| D8 | Startup criterion, partition count | Keep the Phase 4 relaxed criterion: warm restart and `--ephemeral` < 1 s, cold first run measured and reported. **P=16 is the frozen single-node default**, so the dev and server formats are the same. **USER REVIEW.** | 05-01 records it. 05-19 re-measures. The STATE.md partition-count blocker is resolved pending review. |
| D9 | Retention defaults | `[runs] retention`: **`168h` (7 days) for server, `24h` for the dev profile**. Write durations as hours, because jiff `parse_unsigned_duration` rejects `7d`. Resolution order: request override > `WorkflowPolicy` > `QueuePolicy` > config. Resolved at start into `StartRun.retention_ms`. Policy changes affect future runs only. WAL bytes are not reclaimed until Phase 8 (`snapshot_floor = 0`). Blobs referenced by expired runs are not garbage-collected in Phase 5. | 05-10 (meta policies, resolver, config), 05-08 (EXPIRE_OP), 05-12 (API), 05-16 (docs). |
| D10 | Continue-as-new | **In scope for Phase 5.** It adds `TerminalStatus::ContinuedAsNew` and outbox `Kind::StartRun` (tag 6). | 05-01 contract, 05-08 apply, 05-10 derive/resolver, 05-12 long-poll follow. |
| D11 | SDK cancellation surfacing | **(b)**: `Op::wait()` observes cancellation by default (`wait([op, CANCELLATION], Any)` → `Err(Cancelled)`); code in `ctx.shielded(\|\| …)` does not. SDK-only, no WIT change. | 05-06. Existing SDK tests whose journals change shape are updated deliberately, with the reason in the SUMMARY. |
| D12 | Minimal deploy | **`ostro deploy <file.wasm> [--activate]`**, upload only. CLI-05 build-if-needed stays in Phase 10. | 05-13. |
| D13 | Signal dedupe | Optional **`request_id`** in a run-scoped **`SignalSeen`** keyspace, deleted with the run. | 05-02 keyspace, 05-07 apply. |
| D14 | WIT pre-freeze review | **No record/variant/enum shape changes in Phase 5.** Every candidate takes the "new function / new interface in `0.1.N` later" path. Candidates: object presign expiry and content-type, http redirect/size policy, child-options tags and S2S, run-info queue and tags, wait-signal metadata. Phase 5 WIT edits are **doc comments only**. **USER REVIEW.** | 05-05 writes `docs/wit-0.1-freeze-review.md`. |
| D15 | Perf gate environment | Native Linux NVMe, 8 vCPU, run by a human. WSL2 numbers are **advisory**: measured and reported, never a failure reason on their own. | 05-15 (harness, measurements) and 05-19 (re-measure, human_needed record). |
| D16 | Benchmark step definition | A step counts when its `StepCompleted` is acknowledged **and found on disk after a restart**. **Profile A** (10-step runs) is the gate; **profile B** (1-step runs) is reported. | 05-15. |
| D17 | Wasmtime 51 (~2026-12-20) | **No bump during Phase 5** once 05-05 starts. Bump after 05-19, in a post-freeze PR, together with `wit-component`/`wit-parser`, with the replay corpus green. | STATE.md Pending Todo (05-01). |
| D18 | On-disk freeze scope | Frozen sources of truth: the log (envelope, `Command` prost, journal event codec, command hash), `node.toml`, the blob layout, and the meta Raft log/snapshot encoding. **Partition state-store values are derived**: a future `STATE_FORMAT` bump rebuilds from the log (after Phase 8, from snapshot + tail). | 05-02 writes the policy to `docs/storage-format.md`. 05-19 marks it "FROZEN v0.1". |

## Additional planner decisions (Claude's discretion, from research recommendations)

- **Contract tags.** The numbers below are provisional. 05-01 Task 1 re-checks them against the Phase 4 SUMMARYs; any shift is recorded there.
  - Commands: `Cmd::SignalRun`=8, `RequestCancel`=9, `TerminateRun`=10, `RetryRun`=11, `CompleteToken`=12.
  - `StartRun` fields: 19 `schedule_to_start_ms`, 20 `retention_ms`, 21 `retry_of`, 22 `continued_from`, 23 `run_started`.
  - Intents: `Intent::RegisterWait`=7, `CancelWait`=8. Field: `CompleteRun.resumable`=4.
  - Statuses: `TerminalStatus::{Terminated=3, TimedOut=4, ContinuedAsNew=5}`. Outbox: `outbox_msg::Kind::StartRun`=6.
  - Journal kinds: `WaitTimedOut`=70, `RunTerminated`=71.
- **Reserved timer ops.** `START_DELAY_OP = u64::MAX`, `RUN_TIMEOUT_OP = u64::MAX-1`, `SCHEDULE_TO_START_OP = u64::MAX-2`, `EXPIRE_OP = u64::MAX-3`. The range `u64::MAX-15..=u64::MAX` is reserved; guest timer intents in it are rejected with `BadIntent`.
- **Waits keyspace.** One keyspace with two key shapes: `0x01 ‖ run ‖ op BE` (wait row) and `0x02 ‖ run ‖ name_len u16 ‖ name ‖ op BE` (FIFO index). `RunRecord.open_waits` counts live waits, so on_terminal skips the scans when it is 0.
- **Read-budget exceptions (asserted in tests, documented in storage-format.md):**
  - `SignalRun`: run record + waits-by-name prefix + SignalSeen point read when a `request_id` is present.
  - `RegisterWait`: inbox-by-name prefix.
  - Terminal transitions: the two Waits prefixes, only when `open_waits > 0`.
- **New RunRecord fields (05-02):** `schedule_to_start_ms`, `s2s_due_ms`, `retention_ms`, `idempotency_key`, `terminal_reason`, `resumable`, `resumed_count`, `open_waits`, `retry_of`, `continued_from`, `cancel_reason`.
- **Apply split.**
  - `apply/waits.rs` belongs to 05-07.
  - `apply/lifecycle.rs` and `apply/retention.rs` belong to 05-08.
  - `apply/inputs.rs`, `apply/terminal.rs` and `apply/outcome.rs` belong to 05-02. 05-02 also appends `RejectReason::RunCompleted` and `InvalidCode::NotImplemented`.
- **rebuild_digest generators.** `tests/common/gen_waits.rs` (05-07) and `tests/common/gen_lifecycle.rs` (05-08). Both are stubbed by 05-02.
- **Bench harness.** `crates/ostro-server/tests/perf.rs` (`#[ignore]`), with no new crate.
- **WIT tooling.** A new dev-tool crate, `crates/ostro-witcheck`, using `wit-component =0.261.0` (feature `semver-check`) and `wit-parser =0.261.0`.
- **CLI transport.** REST via `reqwest =0.13.5`, promoted to a normal dependency of `ostro-cli`.
- **Start request.** `schedule_to_start`, `retention` and `dry_run` are added to the domain, REST and proto start request in **05-12**, not 05-04, so proto literals in server tests don't break mid-wave.
- **Crash campaign.**
  - Every counted fault is an iteration that **ends in `kill -9`**. Some iterations kill at a random time; others inject failpoint or ENOSPC errors first and then kill.
  - The default layout is S=20 shards × 500 faults, with `OSTRO_CAMPAIGN_PARALLEL` shards concurrent, persisted per shard (resumable), and summed across shards.
  - The gate is **`kills ≥ 10,000`** across a local run of all shards, in batches or in the background.
  - The failpoint-enabled `ostro` binary is built with `--target-dir target/failpoints`.

## Standing rules (every Phase 5 plan)

- The build stays warning-free and clippy runs with `-D warnings`.
- `bash scripts/ci-local.sh` is the gate. `FAILPOINT_CRATES` in `scripts/ci-local.sh` stays in sync with the "Failpoint suites" step of `.github/workflows/ci.yml`.
- `Cmd` tags 1–7 are frozen. Journal kinds are append-only. No `EVENT_CODEC_VER` bump.
- No `chrono`, no `bincode`, no wasmtime `cache` feature.
- Never reference a Chronik clone path. No git remotes and no pushes; tags stay local.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Executors never run gsd-tools `state advance-plan`, `roadmap update-plan-progress`, `phase complete` or `requirements mark-complete`. Tracking files are edited by hand.
- Verify commands that pipe output use `set -o pipefail` (or grep for an explicit result line), so exit codes are never hidden.
