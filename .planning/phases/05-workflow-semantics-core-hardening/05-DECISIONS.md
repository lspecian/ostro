# Phase 5 decisions (adopted defaults; user may override)

**Status:** adopted by the planner on 2026-11-18 and revised on 2026-11-19 after the plan check. The project runs in YOLO + auto_advance mode and the user was not available, so no blocking decision checkpoint was used. For every decision D1–D18 in `05-RESEARCH.md` (plus D19–D21 added later from the Phase 6/8 research and plan checks) § "Open Questions / Decisions to Confirm Before the Freeze", the research's recommended option was adopted. Plan 05-01 Task 1 copies each one into `.planning/STATE.md` § Decisions by hand and adds user-review items to § Pending Todos.

**User-review items** (05-01 flags them in STATE.md Pending Todos):
- D7: step-local RNG semantics. These freeze with WIT 0.1.
- D8: the relaxed cold-start criterion, and P=16 as the frozen default.
- D14: the WIT pre-freeze review outcome (doc comments only, no record changes).
- The WIT freeze flip itself, done in 05-20. After it, `runtime.wit` changes only additively, through a new `0.1.N` package.

An override that lands before 05-20 can be folded in. An override after 05-20 is bound by the freeze rules.

| ID | Topic | Adopted option | Consequence in the plans |
|----|-------|----------------|--------------------------|
| D1 | Retry semantics | **Resume-in-place** for `resumable` terminal runs: executor-level failure (trap, fuel, memory, nondeterminism, attempt failures exhausted), Terminated and TimedOut. **Restart as a new linked run** (`StartRun.retry_of`) when the journal ends in a guest terminal event (`RunFailed`, or Cancelled derived from `RunFailed{cancelled}`). **409 `run_completed`** for Completed. On resume: `RunRecord.resumed_count` is incremented; the first activation of the resumed run **re-arms every unresolved op** from the replayed journal (ScheduleTimer for `TimerStarted`, RegisterWait for `SignalWaitStarted`/`AwaitTokenCreated` with the original due time; past-due fires immediately); a resumed run gets **no new run timeout**. | 05-09 `RetryRun`. 05-11 re-arm. 05-08 idempotent RegisterWait. 05-13 picks the mode from `RunRecord.resumable`. 05-15 adds e2e tests for resuming after a timeout. |
| D2 | Timeout semantics | Run timeout is a **hard stop** → `TimedOut`, no guest cleanup, reason `run_timeout`. Schedule-to-start applies to the **initial start only** (never-granted runs), reason `schedule_to_start`. The S2S timer is deleted by `RunRecord.s2s_due_ms`. | Reserved ops `RUN_TIMEOUT_OP`, `SCHEDULE_TO_START_OP` (05-01 constants; 05-02 record field; 05-09 apply). |
| D3 | Terminate | Distinct `Terminated` status. Children get a **graceful** `CancelRun` cascade. A **`RunTerminated{reason}` (kind 71)** history marker is journaled only when `next_seq > 0`, so "exactly one RunStarted at seq 0" holds. TimedOut uses the same marker with reason `run_timeout`/`schedule_to_start`. Every hard stop emits `Action::RunCompleted`. | 05-01 kind 71. 05-09 apply. |
| D4 | Wait timeouts | New input kind **`WaitTimedOut{op}` (code 70)**. `TimerFired` is not reused. Terminal transitions delete pending wait-timeout timer rows, so a stale timer can never surface as `Ok(Timer)` after a resume. | 05-01 kind. 05-03 engine maps it to `Err(Failure::Timeout)`. 05-02 cleanup in on_terminal. 05-08 apply. |
| D5 | cancel-op | `cancel-op` **completes the op with `Err(Cancelled)`**. The first completion in journal order wins. | 05-03. |
| D6 | patched | Memoised per change-id; unqueried `PatchMarker`s are skipped. The server-level proof uses a test-only `#[doc(hidden)] ExecutorHooks::module_override`. No re-pin API before Phase 13. | 05-03 (engine), 05-11 (hook), 05-15 (e2e). |
| D7 | Step-local RNG (03-11) | **Frozen as is**: randomness inside a live step comes from `sha256("ostro/step-rng/v1" ‖ seed ‖ seq)`, so each retry attempt gets a fresh stream. Documented in the WIT doc comment (05-05) and the SDK docs (05-06). Phase 9 SDKs must reseed at step boundaries the same way. **USER REVIEW.** | 05-05, 05-06. |
| D8 | Startup criterion, partition count | Keep the Phase 4 relaxed criterion: warm restart and `--ephemeral` < 1 s, cold first run measured and reported. **P=16 is the frozen single-node default**, so the dev and server formats are the same. **USER REVIEW.** | 05-01 records it. 05-20 re-measures. The STATE.md partition-count blocker is resolved pending review. |
| D9 | Retention defaults | `[runs] retention`: **`168h` (7 days) for server, `24h` for the dev profile**. Write durations as hours, because jiff `parse_unsigned_duration` rejects `7d`. Resolution order: request override > `WorkflowPolicy` > `QueuePolicy` > config. Resolved at start into `StartRun.retention_ms`. Policy changes affect future runs only. WAL bytes are not reclaimed until Phase 8 (`snapshot_floor = 0`). Blobs referenced by expired runs are not garbage-collected in Phase 5. | 05-11 (meta policies, resolver, config), 05-09 (EXPIRE_OP), 05-13 (API), 05-17 (docs). |
| D10 | Continue-as-new | **In scope for Phase 5.** It adds `TerminalStatus::ContinuedAsNew` and outbox `Kind::StartRun` (tag 6). | 05-01 contract, 05-09 apply, 05-11 derive/resolver, 05-13 long-poll follow. |
| D11 | SDK cancellation surfacing | **(b)**: `Op::wait()` observes cancellation by default (`wait([op, CANCELLATION], Any)` → `Err(Cancelled)`); code in `ctx.shielded(\|\| …)` does not. SDK-only, no WIT change. | 05-06. Existing SDK tests whose journals change shape are updated deliberately, with the reason in the SUMMARY. |
| D12 | Minimal deploy | **`ostro deploy <file.wasm> [--activate]`**, upload only. CLI-05 build-if-needed stays in Phase 10. | 05-14. |
| D13 | Signal dedupe | Optional **`request_id`** in a run-scoped **`SignalSeen`** keyspace, deleted with the run. | 05-02 keyspace, 05-08 apply. |
| D14 | WIT pre-freeze review | **No record/variant/enum shape changes in Phase 5.** Every candidate takes the "new function / new interface in `0.1.N` later" path. Candidates: object presign expiry and content-type, http redirect/size policy, child-options tags and S2S, run-info queue and tags, wait-signal metadata. Phase 5 WIT edits are **doc comments only**. **USER REVIEW.** | 05-05 writes `docs/wit-0.1-freeze-review.md`. |
| D15 | Perf gate environment | Native Linux NVMe, 8 vCPU, run by a human. WSL2 numbers are **advisory**: measured and reported, never a failure reason on their own. | 05-16 (harness, measurements) and 05-20 (re-measure, human_needed record). |
| D16 | Benchmark step definition | A step counts when its `StepCompleted` is acknowledged **and found on disk after a restart**. **Profile A** (10-step runs) is the gate; **profile B** (1-step runs) is reported. | 05-16. |
| D17 | Wasmtime 51 (~2026-12-20) | **No bump during Phase 5** once 05-05 starts. Bump after 05-20, in a post-freeze PR, together with `wit-component`/`wit-parser`, with the replay corpus green. | STATE.md Pending Todo (05-01). |
| D18 (amended 2026-11-19, Phase 8 F1) | On-disk freeze scope | Frozen sources of truth: the log (envelope, `Command` prost, journal event codec, command hash), `node.toml` (unknown keys are ignored and new keys are additive: Phase 8 F6), the blob layout, and the meta Raft log/snapshot encoding. **Partition state-store values are derived only while the full log is retained.** Once Phase 8 enables log trimming, snapshot rows become a source of truth. From then on, every `STATE_FORMAT`/`VALUE_FORMAT` change must ship a row migrator (vN → vN+1) applied at snapshot install and at store open. The snapshot carries `state_format`, and readers must accept every older snapshot format; this also covers Phase 12 follower install and Phase 13 N/N-1 upgrades. BlobRef invariant (Phase 8 F5): a `BlobRef` appears only in a `PayloadRef` field of a prost message or a documented postcard value, **never inside opaque payload bytes**. | 05-02 writes this policy to `docs/storage-format.md`. 05-20 marks it "FROZEN v0.1". |
| D19 (new, orchestrator 2026-11-19) | Object store implementation | Phases 6 and 8 share one in-house crate, **`ostro-objstore`**: local filesystem plus S3-compatible storage via `rusty-s3 =0.12.0` over reqwest/rustls. **No deny.toml exception for `object_store` or `chrono`.** The FX-05 requirement wording ("object_store crate") is superseded by this decision; Phase 6 research (b276dc5) agrees. The first of Phases 6/8 to run creates the crate. | Phase 5 builds nothing for it. The name is reserved in `.planning/RESERVATIONS.md`. |
| D20 (new, Phase 8 F2(b)) | Meta encoding evolution | Meta records and the `MetaState` snapshot are positional postcard. **Before the 05-20 freeze:** everything known lands now (05-07): run policies, the Phase 8 `Schedule` fields (timezone, overlap, catch-up, window, bounds, created_at, `ext`), `Webhook` with its `MetaState.webhooks` map, and one META_FORMAT bump with explicit rejection of the pre-Phase-5 dev format. **After the freeze:** any meta change is a **new `MetaRecord` variant appended in `.planning/RESERVATIONS.md` order, plus a `META_FORMAT` bump with decoders for every older format**. Fields are never added to existing meta structs. | 05-07 implements the pre-freeze part and the meta golden file. 05-20 pins the golden prefix. |
| D21 (new, Phase 6 plan-check finding) | Apply journals submitter-encoded event bytes verbatim | When apply journals an event that carries user-sized data, the **command carries the PayloadRef (inline ≤ 256 KiB, else blob) of the fully encoded journal event bytes** (`[EVENT_CODEC_VER] ‖ postcard((ts_nanos, body))`, ts included, no seq), built by the submitter. Apply writes `JournalEvent{seq: next_seq, kind, payload}` verbatim and never decodes, re-encodes or fetches it. This keeps "apply never decodes payloads" and the BlobRef invariant intact for payloads above 256 KiB. Phase 5 applies it to: **CompleteToken** (`event` = encoded TokenCompleted{op, payload}) and **signals**. The matched op is chosen in apply (FIFO), so the submitter cannot pre-encode SignalReceived{op, …}. Instead SignalRun's `event` is an encoded **SignalPayload{name, payload}** (kind 96), always journaled verbatim, and apply writes its own tiny **SignalMatched{op, payload_seq}** (kind 97, no user data) when a wait is matched, either immediately or later at RegisterWait from an inbox slot `{payload_seq, sent_at_ms}`. The engine completes the op with the payload of the referenced SignalPayload event. SignalReceived (66) remains decodable but is never produced from Phase 5 on. Outbox `Signal.payload` follows the same rule. Apply-built events without user data (TimerFired, WaitTimedOut, CancelRequested, RunTerminated, SignalMatched) are still encoded by apply. Phase 6's CompleteEffect follows D21. | 05-01 contract + kinds 96/97 (RESERVATIONS), 05-02 `journal_encoded`, 05-03 engine, 05-08 apply, 05-11 derive `at_ms` + blob journal read, 05-13 API encoding, 05-08/05-15 1 MiB tests. |

## Additional planner decisions (Claude's discretion, from research recommendations)

- **Contract tags.** **`.planning/RESERVATIONS.md` is the single registry** for every ordinal space, and every plan allocates only from it. The Phase 5 numbers below match it. 05-01 Task 1 verifies the code against it and records any discrepancy, editing RESERVATIONS first if needed.
  - Commands (`SignalRun.event` / `CompleteToken.event` = submitter-encoded event bytes, D21): `Cmd::SignalRun`=8, `RequestCancel`=9, `TerminateRun`=10, `RetryRun`=11, `CompleteToken`=12.
  - `StartRun` fields: 19 `schedule_to_start_ms`, 20 `retention_ms`, 21 `retry_of`, 22 `continued_from`, 23 `run_started`.
  - Intents: `Intent::RegisterWait`=7, `CancelWait`=8. Field: `CompleteRun.resumable`=4.
  - Statuses: `TerminalStatus::{Terminated=3, TimedOut=4, ContinuedAsNew=5}`. Outbox: `outbox_msg::Kind::StartRun`=6.
  - Journal kinds: `WaitTimedOut`=70, `RunTerminated`=71, `SignalPayload`=96, `SignalMatched`=97 (D21; 96/97 taken under RESERVATIONS rule 2).
- **Reserved timer ops.** `START_DELAY_OP = u64::MAX`, `RUN_TIMEOUT_OP = u64::MAX-1`, `SCHEDULE_TO_START_OP = u64::MAX-2`, `EXPIRE_OP = u64::MAX-3`. The range `u64::MAX-15..=u64::MAX` is reserved; guest timer intents in it are rejected with `BadIntent`.
- **Waits keyspace.** One keyspace with two key shapes: `0x01 ‖ run ‖ op BE` (wait row) and `0x02 ‖ run ‖ name_len u16 ‖ name ‖ op BE` (FIFO index). `RunRecord.open_waits` counts live waits, so on_terminal skips the scans when it is 0.
- **Read-budget exceptions (asserted in tests, documented in storage-format.md):**
  - `SignalRun`: run record + waits-by-name prefix + SignalSeen point read when a `request_id` is present.
  - `RegisterWait`: inbox-by-name prefix.
  - Terminal transitions: the two Waits prefixes, only when `open_waits > 0`.
- **New RunRecord fields (05-02):** `schedule_to_start_ms`, `s2s_due_ms`, `retention_ms`, `idempotency_key`, `terminal_reason`, `resumable`, `resumed_count`, `open_waits`, `retry_of`, `continued_from`, `cancel_reason`.
- **Apply split.**
  - `apply/waits.rs` belongs to 05-08.
  - `apply/lifecycle.rs` and `apply/retention.rs` belong to 05-09.
  - `apply/inputs.rs`, `apply/terminal.rs` and `apply/outcome.rs` belong to 05-02. 05-02 also appends `RejectReason::RunCompleted` and `InvalidCode::NotImplemented`.
- **rebuild_digest generators.** `tests/common/gen_waits.rs` (05-08) and `tests/common/gen_lifecycle.rs` (05-09). Both are stubbed by 05-02.
- **Phase 7 pre-freeze findings (07-RESEARCH F1–F4, adopted 2026-11-19).** These land before the freeze so that Phase 7 needs **0 keyspaces and 0 meta variants**:
  - F1: `RunRecord.pause: Option<PauseMark{reason, at_ms, detail}>` (05-02; stored, never set in Phase 5).
  - F3: `ReadyValue{concurrency_key, workflow, tenant}`, carried on `ReadyEntryRow` and `Action::EnqueueReady` (05-02).
  - F2: Phase 7 flow/placement/pause/auto-pause fields in `QueuePolicy` and `WorkflowPolicy`, placed before the trailing `ext` (05-07). No D20 bump is needed for Phase 7.
  - F7: `ReadyValue.first`, set from `next_seq == 0` and carried as `first_activation` on `ReadyEntryRow`/`Action::EnqueueReady` (05-02, tested).
  - F8: meta `ApiToken.expires_at_ms: Option<u64>`, appended last (05-07, meta golden). Stored only; the expiry check is Phase 7's, since no Phase 5 plan verifies API tokens.
  - F9 (from Phase 4 execution, 04-16): durable `RunRecord.attempt_failures: u32`. Apply increments it on `AttemptGranted.after_failure` (new field 3, recorded in RESERVATIONS by 05-01). The executor/dispatcher decision reads it (05-11), RetryRun resets it (05-09), and GET run exposes it as the retry count (05-13). A restart test proves the limit holds.
  - F10 (from Phase 4 execution, 04-18): `RunRecord.status_seq: Option<u64>`, set by apply from the StatusPublished event kind (no payload decode). GET run reads the published status in O(1) (05-13).
  - F4: the `run-info.attempt` doc comment corrected to say it is not replay-stable and is for logging only (05-05, mirrored in the SDK README in 05-06). D14 holds.
- **Phase 9 pre-freeze findings (09-RESEARCH PF-*, adopted 2026-11-19):**
  - PF-F1: `CompleteRun.parent_event` (field 5) and outbox `ChildCompleted.event` (field 6) carry the submitter-encoded ChildCompleted event bytes, so child completion follows D21 (05-01 contract and goldens; 05-02 forwards verbatim). Phase 9 wires it.
  - PF-F2: continue-as-new keeps parent linkage. The successor's `StartRun.parent` copies the predecessor's (05-11), `CompleteRun{ContinuedAsNew}` sends no ChildCompleted, `RunRecord.continued_to` is added (05-02, set in 05-09), and CancelRun/RequestCancel are forwarded along the chain (05-08). All have tests.
  - PF-F3: attached children get a graceful cancel when the parent reaches any terminal status. WIT doc only in 05-05; Phase 9 implements it.
  - PF-F5: Phase 9 block added to `.planning/RESERVATIONS.md`.
  - PF-W1..W9: WIT doc comments in 05-05 Task 3 (open-step rule, `wait` semantics, op-id/bigint, UTF-8 and lone surrogates, JSON payloads, retry/fuel portability, child contract, eviction-is-a-trap and pre-init, uuid and random streams), with verify grep markers. D14 holds.
- **Bench harness.** `crates/ostro-server/tests/perf.rs` (`#[ignore]`), with no new crate.
- **WIT tooling.** A new dev-tool crate, `crates/ostro-witcheck`, using `wit-component =0.261.0` (feature `semver-check`) and `wit-parser =0.261.0`.
- **CLI transport.** REST via `reqwest =0.13.5`, promoted to a normal dependency of `ostro-cli`.
- **Start request.** `schedule_to_start`, `retention` and `dry_run` are added to the domain, REST and proto start request in **05-13**, not 05-04, so proto literals in server tests don't break mid-wave.
- **Crash campaign.**
  - Every counted fault is an iteration that **ends in `kill -9`**. Some iterations kill at a random time; others inject failpoint or ENOSPC errors first and then kill.
  - The default layout is S=20 shards × 500 faults, with `OSTRO_CAMPAIGN_PARALLEL` shards concurrent, persisted per shard (resumable), and summed across shards.
  - The gate is **`kills ≥ 10,000`** across a local run of all shards, in batches or in the background.
  - The failpoint-enabled `ostro` binary is built with `--target-dir target/failpoints`.

## Standing rules (every Phase 5 plan)

- Ordinals (Cmd tags, StartRun fields, Derived intents, journal kinds, statuses, keyspaces, RejectReason/InvalidCode, outbox kinds, meta variants) and reserved names are allocated **only from `.planning/RESERVATIONS.md`**.
- The Phase 6 WIT doc-comment findings W1–W8 land in 05-05 Task 3 before the freeze. D14 still holds: no record changes.

- The build stays warning-free and clippy runs with `-D warnings`.
- `bash scripts/ci-local.sh` is the gate. `FAILPOINT_CRATES` in `scripts/ci-local.sh` stays in sync with the "Failpoint suites" step of `.github/workflows/ci.yml`.
- `Cmd` tags 1–7 are frozen. Journal kinds are append-only. No `EVENT_CODEC_VER` bump.
- No `chrono`, no `bincode`, no wasmtime `cache` feature.
- Never reference a Chronik clone path. No git remotes and no pushes; tags stay local.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Executors never run gsd-tools `state advance-plan`, `roadmap update-plan-progress`, `phase complete` or `requirements mark-complete`. Tracking files are edited by hand.
- Verify commands that pipe output use `set -o pipefail` (or grep for an explicit result line), so exit codes are never hidden.
