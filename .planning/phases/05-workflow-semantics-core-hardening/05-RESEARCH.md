# Phase 5: Workflow Semantics & Core Hardening - Research

**Researched:** 2026-11-18
**Domain:** User-facing workflow semantics on the single-node core (signals, cancel/terminate/retry, timeouts, await tokens, patching, retention). Also whole-server crash injection, the replay corpus, performance, the WIT 0.1 / command-set / on-disk-format freeze, and the client CLI.
**Confidence:**
- HIGH for code seams. They were read from the tree at `ba9aba9` and from the Phase 4 PLANs 04-09 and 04-12..04-20, which are not yet executed.
- MEDIUM for the new designs (waits registry, lifecycle commands, retention via timers). The planner must adopt these explicitly.
- MEDIUM for the WIT semver tooling. The API is verified on docs.rs, but its behaviour against Ostro's rules must be proven with fixtures.
- LOW for performance predictions. Nothing has been measured; the targets assume native NVMe and this machine is WSL2.

## Summary

Phase 5 builds on a core that Phase 4 will deliver:
- dispatcher + `EngineExecutor` + `PartitionSink`;
- timer inputs journaled by apply;
- `Suspend` CAS;
- `Running` index;
- server bootstrap and runs `ControlPlane` (start/get/history/list);
- `ostro dev|server|openapi`;
- e2e harnesses (`TestServer`, `start_with_hooks`, `ExecutorHooks`).

The engine already does most of the semantic work. Wait completion order is the journal seq of each completion input, and `WaitResolved` records that list, so gather/race replay identically by construction (`replay/wait.rs`). `patched`, `cancellation()`, await-token minting, `continue-as-new` and run deadlines in `run-info` all exist. **The missing half is storage and server-side:**
- Signals still land in an unconsumed `inbox`.
- Nothing knows which op a signal or token should complete.
- Signal and token timeouts are not scheduled.
- Cancel is not journaled as an input.
- Terminate, retry, run timeout, schedule-to-start, retention and continue-as-new have no command or apply representation.
- 04-15 fails runs that create await tokens or continue-as-new with "unsupported".

There are also **four engine-level semantic gaps** to close before the freeze:
1. `cancel-op` never completes the cancelled op. WIT says it "completes with failure::cancelled", but `completion_for` only looks at inputs, so a `wait` on a cancelled op hangs.
2. A `TimerFired` on a `wait-signal`/await-token op would be shown to the guest as `Ok(Timer)`, not `Err(Timeout)`.
3. `patched(id)` is not memoised, so a second call appends a second marker, and there is no rule for "marker present but never queried".
4. `completion_for` is a linear scan per op. That is O(n²) over long journals and matters for NFR-2.7.

Everything new can stay additive under the existing rules:
- new `Cmd` oneof tags (8..=63 reserved);
- new `StartRun` fields (19+);
- new `Derived` intents (7+);
- new `TerminalStatus` values;
- one new outbox kind;
- new journal `EventKind` codes appended at the end of `EventBody`, with golden lines appended and **no `EVENT_CODEC_VER` bump** (the Phase 4 rule).

Store-schema changes (keyspaces, `RunRecord` fields) are still allowed pre-freeze.

**Primary recommendation:**
- Start with one contract-first gate plan that lands every format addition (commands, intents, statuses, event kinds, keyspaces, `RunRecord` fields, apply dispatch stubs) and asks the user to confirm the open decisions below.
- Then fan out into parallel plans by file ownership: engine semantics, apply waits/inputs, apply lifecycle/timeouts/retention, the API surface, SDK/guests, WIT-freeze tooling, and replay-corpus tooling.
- Then server glue, CLI, e2e semantics, crash campaign and perf.
- End with a gate plan: 10k-fault campaign, corpus green on both arches, perf numbers, and the freeze flip (WIT hash lock, append-only goldens, storage-format doc).

<user_constraints>
## User Constraints

No `05-CONTEXT.md` exists, because `/gsd:discuss-phase` was not run for Phase 5. The constraints below come from ROADMAP.md, STATE.md, PROJECT.md, the Phase 2–4 decisions and the orchestrator prompt. The planner must honour them.

### Locked Decisions (project-level, inherited)
- **Toolchain and lints.** Rust 1.100.0 pinned. Builds are warning-free. The per-crate `unused_dependencies` lint is active, so a dep wired before use is anchored with `use x as _;`. `scripts/ci-local.sh` is the gate. No `chrono` (use `jiff`), no `bincode`. cargo-deny bans duplicate tonic/prost/hyper/axum/opentelemetry/wasmtime.
- **Wasmtime.** Wasmtime `=50.0.2`, pinned exactly. Pin policy: track monthly; Wasmtime 51 is due about 2026-12-20; one bump PR per release with the replay corpus green. Host imports stay thin (ENG-06 stack guard).
- **Freeze rules (Roadmap).** The format freeze happens at the END of Phase 5: WIT `ostro:runtime@0.1.0`, the partition command set and the on-disk format. After that, changes are additive only. Golden vectors (`crates/ostro-types/tests/golden/commands.txt`, `journal_v1.txt`, `crates/ostro-engine/tests/snapshots/determinism__golden_det_basic_journal.snap`) are never blindly re-blessed. A deliberate re-bless must be explained in the SUMMARY.
- **Command tags.** `Cmd` tags 1..=7 are frozen and 8..=63 are reserved. Field numbers are never reused. Apply outcome codes are frozen (Accepted 0, Duplicate 1, Rejected 16+idx). New `InvalidCode`/`RejectReason` variants are appended at the end.
- **Journal codec rule (Phase 4 decision).** A new journal event kind is additive and does **not** bump `EVENT_CODEC_VER`. It gets a new code, a new payload struct, an `EventBody` variant appended LAST, and a golden line appended. Bump the version only when an existing kind's encoding changes. Code ranges: 1–15 run record, 16–63 guest commands, 64–127 inputs, 128+ reserved.
- **WIT record rule.** WIT records, variants and enums can never gain fields or cases after the freeze. Additive means new functions, interfaces or types only (header of `wit/ostro-runtime-0.1.0/runtime.wit`).
- **Apply rules.** Apply never reads clocks, and every time value comes from the command. `crates/ostro-partition/clippy.toml` bans clocks and HashMap/HashSet. Validate the whole command, then write. The hot-path read budget is at most 1 keyspace read per command, with `range_prefix` counting as 1.
- **Derived intents.** `EventsAppended` carries opaque journal events plus submitter-computed `Derived` intents. Apply does not decode journal payloads. New storage consequences become new `Derived` intents or new top-level commands, never apply-side event decoding.
- **Fencing.** Fencing comes from the logged `AttemptGranted` plus the apply token check. Leases are soft. Executors self-fence on revoke or on their local deadline.
- **One code path.** One replication code path, RF=1 included. Ephemeral mode uses the same code path (tmpfs TempDir).
- **Step handle.** The step handle is a `resource step`. Never rely on `HostStep::drop`.
- **Guest builds.** Rust guests build with `cargo build --target wasm32-wasip2` and `wit-bindgen =0.63.0`, with no cargo-component. `guests/` is a separate workspace whose `Cargo.lock` is gitignored. Tests get wasm via `tests/common::guest_wasm(name)`.
- **Runs are pinned to `module_sha`** (WF-08, 04-09/04-12).
- **Partitions.** Fixed partition count, default P=16 single node. This must be confirmed before the freeze (STATE.md blocker).
- **Retention.** WAL retention stays inert until snapshots exist (Phase 8): `snapshot_floor = 0`, so `trim_prefix` never removes anything. Trimming the log while state rows are deleted would make "rebuild from log" impossible.
- **Phase 4 locked decision.** Effects, children and HTTP/SQL/object stores are bridged in Phase 6/9. Until then they complete with non-retryable `unsupported`. Phase 5 must keep that path for effects and children only.

### Claude's Discretion
Every Phase 5 design choice not listed above: command and intent shapes, keyspace layout, the retry/terminate/timeout semantics (subject to the open decisions below), the CLI client transport, crash-harness architecture, corpus format, and the WIT-check tooling. Recommendations follow.

### Deferred Ideas (OUT OF SCOPE for Phase 5)
- Effects bridge, secrets, egress (FX-*, Phase 6). Workers, flow-control enforcement, pause (Phase 7). Webhooks/cron firing, snapshots/backup, OTel/metrics (Phase 8).
- Python/TS SDKs, cross-language children, the saga helper WF-10 and the three-SDK conformance suite (Phase 9). `ostro init/build/deploy-with-build/replay/dev --watch` (Phase 10). Rerun-from-step, bulk ops, DLQ/`on_failure`, UI actions (Phase 11).
- `migrate-runs` / module GC / N/N-1 WIT linkers (ENG-10, CLUS-07; Phase 13). Auth enforcement and tenants (Phase 14).
- v2 items: debounce/singleton, streaming progress, Temporal-style updates and signal-with-start.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| WF-03 | Concurrent await (gather/all/join) and race, with completion order journaled and replayed identically | Already deterministic in `replay/wait.rs` (order = completion-event seq; `WaitResolved` replayed verbatim). Phase 5 adds server-side inputs (signals, tokens, cancel, wait timeouts) that all go through apply in log order, the cancel-op completion fix, an op→completion index, a replay-core proptest over random input interleavings, and a server e2e with concurrent signal senders plus a restart. See "Gather/race ordering" |
| WF-04 | `sleep` / `sleep_until`; a sleeping run holds no memory | Sleep exists (04-13 journals `TimerFired`, Suspend CAS, lazy timer window). Phase 5 adds `Runtime::live_activations()` and a scale test (1,000 sleepers → 0 live activations, 0 parked, budget ≈ 0, bounded RSS delta, all wake). See "Sleeping runs hold no memory" |
| WF-05 | Wait for a named signal with timeout; send signals via API | New `SignalRun` command, `RegisterWait`/`CancelWait` intents, `Waits` keyspace, inbox keyed `(run, name, n)`, `WaitTimedOut` input kind, `SignalReceived` journaled by apply. See "Signals and waits" |
| WF-08 | `patched(id)`; runs stay pinned to their module sha | Memoise per change-id, skip unqueried `PatchMarker`s, and prove at engine level by replaying a v1 journal against a v2 module. Server-level proof uses a test-only module override (no re-pin API until Phase 13). See "patched(id)" |
| WF-09 | Run timeout and schedule-to-start timeout | Reserved timer ops `RUN_TIMEOUT_OP` / `SCHEDULE_TO_START_OP` in the existing `Timers` keyspace (lazy window). `StartRun.schedule_to_start_ms` (new tag). A hard stop gives `TerminalStatus::TimedOut`. See "Timeouts" |
| API-05 | Signal, graceful cancel, terminate, retry | New top-level commands `SignalRun`, `RequestCancel`, `TerminateRun`, `RetryRun`. Domain/REST/gRPC/OpenAPI additions. `dry_run` validation. See "Lifecycle commands" and "API surface" |
| API-06 | Long-poll a run result | `GET /v1/runs/{run_id}/result?wait=30s` and gRPC `AwaitResult`, served by a node-wide `RunWatchers` hub fed by `Action::RunCompleted`. Subscribe before reading to avoid the race. See "Long-poll" |
| API-07 | Complete an await token | `POST /v1/tokens/complete {token, payload}`: verify the MAC (`ostro_engine` token module), route by the embedded partition, submit the `CompleteToken` command, apply journals `TokenCompleted` once. See "Await tokens" |
| CLI-06 | `ostro run/ls/inspect/history/signal/cancel/retry` with `--json` / `--dry-run` | `ostro-cli` crate (placeholder today) with a REST client (reqwest promoted from dev-dep), wired into `crates/ostro/src/main.rs`. `--json` prints exactly the REST response body. `--dry-run` maps to server-side `dry_run=true`. See "CLI" |
| OPS-06 | Terminal runs and journals deleted after TTL; dedupe entries have retention | `EXPIRE_OP` reserved timer row written at the terminal transition (`completed_at + retention_ms`). Apply deletes all run-scoped rows, including the dedupe row. `StartRun.retention_ms` is resolved at start from workflow/queue policy or the server default. WAL bytes are not reclaimed until Phase 8. See "Retention" |
| WIT-07 | WIT 0.1 frozen; later changes additive only | The `runtime.wit` sha256 is locked in a test, plus a `wit-component =0.261.0` `semver_check` test (feature `semver-check`) for any `0.1.x` package, validated by positive and negative fixtures. A pre-freeze WIT review checklist. A git tag (local; pushing is human). See "WIT freeze" |
| QA-01 | ≥ 10,000 injected faults (kill -9, failpoints, EIO/ENOSPC) under load: 0 lost accepted runs, 0 duplicate journal entries | Extend `ostro-crashtest` with a whole-server target (the `ostro` binary built with `--features failpoints`), a workflow load generator over HTTP, new failpoints (meta log, executor seam, ENOSPC variants), journal invariant checks and a sharded runner. See "Crash campaign" |
| QA-02 | Replay corpus replayed in CI across versions and x86_64/aarch64 | Committed corpus `tests/replay-corpus/<entry>/{module.wasm, journal.bin, entry.toml}`, never re-blessed, replayed by a test in the default `cargo test --workspace`. That test therefore runs on the existing linux-aarch64-musl and x86_64 legs. A recorder tool. See "Replay corpus" |
</phase_requirements>

## Dependency Status on Phase 4 (what Phase 5 may assume)

Only use what the 04-* PLANs deliver. **04-09 and 04-12..04-20 are not executed yet**, so the first Phase 5 plan must gate on their SUMMARY files.

| Capability | Delivered by | Phase 5 use |
|---|---|---|
| `StartRun` fields 14..18 (module_sha, tags, tenant, traceparent, run_timeout_ms), `Derived::Suspend` (6) | 04-09 | Next free tags: StartRun **19**, Derived **7**, Cmd **8** |
| `RunRecord` new fields, `Running` keyspace (14th table), `Action::InputAppended`, `query.rs` helpers | 04-12 | New keyspaces append as the 15th and later tables. `RunRecord` gains trailing fields (pre-freeze, no VALUE_FORMAT bump) |
| `TimerFired` journaled as input at `next_seq` plus wake (Ready or InputAppended) | 04-13 | Pattern for every new input: SignalReceived, TokenCompleted, CancelRequested, WaitTimedOut |
| Dispatcher: `Executor` trait, `Job`, `ExecOutcome`, leases, InputAppended forwarding, `RunCompleted` drops the lease | 04-14 | Terminate and timeouts rely on `RunCompleted` → **revoke** the leased job. Verify it sends `revoked=true`, not only a lease drop |
| `derive.rs`, `PartitionSink`, `run_started.rs`, `journal_io.rs`, "unsupported" completions | 04-15 | Extend derive with the new mappings. Remove "unsupported" for await tokens and continue-as-new, but keep it for effects and children |
| `EngineExecutor`, outcome state machine, `ExecutorHooks`, harness `TestNode` | 04-16 | Crash-seam failpoints and perf hooks go here |
| `Server::start`, `start_with_hooks`, `ServerControlPlane`, `MetaLeadership`, `TestServer` | 04-17 | New ControlPlane methods, RunWatchers hub, retention policy resolution |
| Runs ControlPlane start/get/history/list with idempotency | 04-18 | `retention_ms` / `schedule_to_start_ms` resolution at start; `dry_run` |
| `ostro dev / server / openapi / build-info`, spawn helpers | 04-19 | CLI client verbs are added to the same clap tree |
| e2e suites + ci-local SDK steps | 04-20 | Pattern for Phase 5 e2e and gate |

**Not delivered by Phase 4** (Phase 5 must build): any client CLI verb; signals, cancel, terminate, retry, long-poll and token APIs; the waits registry; continue-as-new; retention deletion; whole-server crash harness; corpus tooling; WIT check tooling; benchmark harness.

## Contract Additions (land all of them in the gate plan, 05-01)

These are the format items that freeze at the end of the phase. Landing them in one plan, as was done with 04-09, avoids concurrent edits to `command.rs`, `journal.rs`, `store/mod.rs` and `values.rs`.

### Partition commands (`ostro-types/src/command.rs`)

| Item | Tag | Fields | Purpose |
|---|---|---|---|
| `Cmd::SignalRun` | 8 | run_id, name, payload (PayloadRef), request_id (bytes, optional dedupe), sent_at_ms | API-05/WF-05 |
| `Cmd::RequestCancel` | 9 | run_id, reason, requested_at_ms | API-05 graceful cancel (local; the outbox `CancelRun` stays for cascades) |
| `Cmd::TerminateRun` | 10 | run_id, reason, terminated_at_ms | API-05 hard stop |
| `Cmd::RetryRun` | 11 | run_id, requested_at_ms | API-05 resume-in-place (see D1) |
| `Cmd::CompleteToken` | 12 | run_id, op, payload, completed_at_ms | API-07 |
| `StartRun.schedule_to_start_ms` | 19 | u64, 0 = none | WF-09 |
| `StartRun.retention_ms` | 20 | u64, 0 = server default resolved at API | OPS-06 |
| `StartRun.retry_of` | 21 | bytes (run id) | D1 restart-mode lineage |
| `StartRun.continued_from` | 22 | bytes (run id) | continue-as-new lineage (D10) |
| `StartRun.run_started` | 23 | optional `JournalEvent` (seq 0) | perf: the API pre-encodes `RunStarted` so the executor's separate append disappears (see Performance; optional, decide after measuring) |
| `derived::Intent::RegisterWait` | 7 | op, kind {signal(name) \| token}, timeout_due_ms (0 = none) | signal/token op registry |
| `derived::Intent::CancelWait` | 8 | op | `cancel-op` on a signal/token op |
| `CompleteRun.resumable` | 4 | bool | executor-level failure (no guest terminal event) → retry can resume in place |
| `TerminalStatus::{Terminated=3, TimedOut=4, ContinuedAsNew=5}` | — | additive enum values | API RunStatus parity |
| `outbox_msg::Kind::StartRun` | 6 | `StartRun` | continue-as-new into any partition (no parent linkage, unlike `StartChild`) |

Rules:
- All tag numbers above are recommendations; they only need to stay unique. Golden vectors are appended to `commands.txt` (`git diff | grep '^-[^-]'` empty). `Command::kind_name` gains the new arms.
- Reserved timer ops (apply-only, never journaled): `START_DELAY_OP = u64::MAX` (exists), `RUN_TIMEOUT_OP = u64::MAX-1`, `SCHEDULE_TO_START_OP = u64::MAX-2`, `EXPIRE_OP = u64::MAX-3`. Reserve `u64::MAX-15..=u64::MAX` and reject guest `ScheduleTimer`/`CancelTimer` intents in that range with `BadIntent`. Journal op ids are seqs, so there is no collision. `CANCELLATION_OP = u64::MAX` lives in the journal op namespace only.

### Journal event kinds (`ostro-types/src/journal.rs`)

| Kind | Code | Payload | Delivered to guest as |
|---|---|---|---|
| `WaitTimedOut` | 70 | `{ op }` | `Err(Failure::Timeout)` for a `wait-signal` or await-token op |
| `RunTerminated` | 71 | `{ reason }` | never delivered. History marker only; the run is terminal. Optional (D3) |

- `completes_op()` gains `WaitTimedOut`. `summary()` gains the new arms.
- Existing kinds are unchanged, so there is **no `EVENT_CODEC_VER` bump**.
- Reusing `TimerFired` for timeouts is rejected. It would force the engine to infer the semantics from the op's scheduling kind, and history would read "timer fired" for a signal timeout.

### State store (`ostro-partition/src/store`)

- **`Waits`** keyspace (appended to `Table`/`TABLES`): `wait_key(run, op)` → `{kind, name, timeout_due_ms}`, plus a secondary key shape `wait_name_key(run, name, op)` → `()` for FIFO matching by name. Two key shapes can share one keyspace with distinct prefixes, or use two keyspaces; pick one and document it.
- **`Inbox`** re-keyed for signals to `(run, name_len u16, name, n u64 BE)`. This is what docs/03 §2.4 specifies. Remove `InboxMsg::TimerFired` (dead since 04-13), `CancelRequested` (now a journal input) and `ChildCompleted` (Phase 9 journals it as an input; keep the variant only if 04-13 still references it).
- **`SignalSeen`** (optional, D13): `(run, request_id)` → `()` for signal dedupe, deleted with the run.
- **`RunRecord`** trailing fields:
  - `schedule_to_start_ms`, `retention_ms`, `idempotency_key` (needed to delete the dedupe row at expiry; today it is not stored);
  - `terminal_reason` and `resumable`, `retry_of`, `continued_from`;
  - `attempt_failures_reset_at`, if retry needs it.
- `RunStatus` gains `Terminated`, `TimedOut`, `ContinuedAsNew`.
- Pre-freeze positional postcard change: no VALUE_FORMAT bump. Document "delete pre-Phase-5 dev data dirs".
- Update `docs/storage-format.md`.

### Apply dispatch stubs
The gate plan adds `match` arms for every new `Cmd`/`Intent` that return `Rejected(Invalid(BadIntent))` (or a new `InvalidCode::NotImplemented` appended last). Later plans then fill in separate files (`apply/waits.rs`, `apply/lifecycle.rs`, `apply/retention.rs`) without touching each other. `tests/rebuild_digest.rs` generators are extended in the plans that implement each command.

## Architecture Patterns

### Recommended file layout (new files marked +)
```
crates/ostro-types/src/command.rs        # Cmd 8..12, StartRun 19..23, Intent 7..8, statuses, outbox kind 6
crates/ostro-types/src/journal.rs        # WaitTimedOut(70), RunTerminated(71)
crates/ostro-partition/src/apply/
  waits.rs        +  # RegisterWait/CancelWait, SignalRun, CompleteToken, wait-timeout timers, CancelRequested input
  lifecycle.rs    +  # TerminateRun, RetryRun, RUN_TIMEOUT/S2S timers, CompleteRun status mapping, continue-as-new outbox
  retention.rs    +  # EXPIRE_OP: delete run-scoped rows
  inputs.rs       +  # shared "journal an input at next_seq + wake" helper (extracted from 04-13 timer.rs)
crates/ostro-engine/src/replay/{cursor,wait}.rs   # cancel completion, WaitTimedOut, patched memo/skip, op index
crates/ostro-server/src/
  derive.rs          # new mappings
  control/lifecycle.rs +   # signal/cancel/terminate/retry/token/dry-run
  control/watch.rs     +   # RunWatchers long-poll hub
  retention.rs         +   # policy resolution (meta WorkflowPolicy/QueuePolicy + config default)
crates/ostro-api/src/{domain,rest,grpc,fake}.rs + proto runs.proto/tokens.proto
crates/ostro-cli/src/{lib,client,commands/*,output}.rs +
crates/ostro-crashtest/src/{server_target,workload,check}.rs + tests/server_campaign.rs +
crates/ostro-bench +   (or crates/ostro-server/tests/perf.rs #[ignore])
crates/ostro-witcheck + (dev tool crate)  OR  crates/ostro-engine/tests/wit_freeze.rs
tests/replay-corpus/<entry>/...  +  crates/ostro-engine/tests/replay_corpus.rs +
examples/order-saga/ +  (standalone cargo project, path dep on sdks/rust/ostro-sdk)
guests/{race-fuzz,signals,patch-v1,patch-v2,crash-saga,timeouts}/ +
```

### Signals and waits (WF-05, API-05)

The op registry lives in apply, populated through submitter-derived intents, so apply still never decodes journal payloads.

1. The guest calls `wait-signal(name, timeout)`. The engine journals `SignalWaitStarted{name, timeout_ms}` at seq `s` (op = s).
2. derive emits `RegisterWait{op: s, kind: Signal(name), timeout_due_ms: ceil((event_ts + timeout)/1e6) or 0}`.
3. Apply on `RegisterWait`:
   - It inserts the `Waits` rows.
   - If `timeout_due_ms > 0`, it inserts timer row `(due, run, s)`.
   - **If the inbox already holds a signal with that name** (one prefix read over `(run, name)`), it journals `SignalReceived{op: s, payload}` at `next_seq`, deletes the inbox row and the wait/timer rows, and emits `InputAppended`. The run is Running here, because the intent came from its own executor.
4. `SignalRun` apply:
   - terminal or absent run → `Rejected(RunTerminal/RunNotFound)`;
   - a seen `request_id` → `Duplicate`;
   - otherwise the oldest wait for `name` (prefix read on `wait_name_key(run, name)`) gets `SignalReceived` journaled at `next_seq`, its rows and timer are removed, and the run wakes (Scheduled → Ready + EnqueueReady; Running → InputAppended; Ready → nothing);
   - with no wait, the signal is pushed to the inbox `(run, name, inbox_next)`.
   - Payload: inline PayloadRef or blob, prepared by the API before submit.
5. TimerFired for op `s` while the wait row exists: apply journals **`WaitTimedOut{op: s}`** (not `TimerFired`), removes the wait rows, and wakes the run. If the wait row is gone (already satisfied), the timer row is simply removed and the outcome is Accepted. The existing "row absent = Duplicate" semantics stay.
6. `cancel-op(s)` → `OpCancelled{op: s}`. derive emits `CancelWait{op}` for signal/token ops and `CancelTimer` for timer ops (04-15 already handles timers). Apply removes the rows and timer. **The engine treats `OpCancelled` as the completion `Err(Cancelled)`** (see Engine gaps).
7. CompleteRun (any terminal transition) removes the run's `Waits` rows with one prefix scan. Pending timer rows fire later as no-ops; the existing behaviour handles that.

FIFO semantics:
- One signal satisfies exactly one wait.
- Signals sent before any wait are buffered per name, in arrival (log) order.
- Many waits on the same name are satisfied oldest-op first.
- This must be documented in the WIT doc comment before the freeze. Doc edits are allowed only pre-freeze.

The wake race is already solved. An input journaled between an activation's last flush and its `Suspend` makes the Suspend CAS fail with `SeqMismatch`, and the executor re-activates (04-12/04-13).

### Await tokens (API-07)
- Token = `ot1_` + base32(partition BE ‖ len ‖ run_id ‖ op BE ‖ HMAC[..10]), minted by the engine (`replay/token.rs`). The key is the cluster token key in meta (04-17).
- derive: `AwaitTokenCreated{timeout_ms}` → `RegisterWait{op, kind: Token, timeout_due_ms}`. This replaces 04-15's "fail run unsupported".
- API: `POST /v1/tokens/complete {token, payload}`. It parses and verifies the MAC (make `replay::token::{parse, verify}` public), takes partition and run from the token, prepares the payload, and submits `CompleteToken{run, op, payload}`.
- Apply:
  - wait row present with kind Token → journal `TokenCompleted{op, payload}`, remove the rows and timer, wake;
  - row absent and op < next_seq → `Duplicate` (already completed or timed out; the API returns **409 `token_already_completed`** with the current state);
  - run terminal → `RunTerminal`.
- Keep the token body out of URLs, so it never lands in access logs.
- Token failure completion ("reject") is **deferred**. Adding a `TokenFailed` input kind later is additive and needs no WIT change, because `wait` already returns `result<op-output, failure>`.

### Graceful cancel, terminate, retry (API-05)

**Graceful cancel (`RequestCancel`, and the inbound outbox `CancelRun` for cascades):**
- Apply sets `cancel_requested`.
- It journals the `CancelRequested{reason}` input (69, exists) at `next_seq`.
- It wakes the run and keeps the existing children cascade through the outbox.
- A repeat is a `Duplicate`.
- The guest observes it through the `cancellation()` op (CANCELLATION_OP), already supported by the engine (`wit_surface::cancel_cleanup_runs_after_cancel`).
- Final status:
  - derive maps `RunFailed{Failure::Cancelled}` to `CompleteRun{status: Cancelled}`;
  - guest `RunCompleted` after a cancel request stays `Completed` (the guest chose to finish);
  - a run that ignores cancellation keeps running (cooperative semantics). Add a `--terminate-after` follow-up later.

**Terminate (`TerminateRun`):**
- Apply, if non-terminal:
  - status `Terminated`;
  - removes the ready, running and waits rows;
  - stores the reason;
  - optionally journals `RunTerminated{reason}` (71) for history;
  - emits `Action::RunCompleted{status: Terminated}`;
  - sends `ChildCompleted` to the parent if any (same path as CompleteRun);
  - cascades graceful `CancelRun` to children.
- The dispatcher revokes the lease. The executor's next append gets `RunTerminal`, which maps to `Fenced`, so it stops.
- No guest code runs.

**Retry (`RetryRun`):**
- Valid only for terminal, non-Completed runs. The modes are D1:
  - **resume-in-place** when `resumable` (executor-level failure: trap, fuel, memory, nondeterminism, attempt failures exhausted, Terminated, TimedOut). Apply sets status Ready, clears `terminal_reason`, removes the expiry timer and emits EnqueueReady. The journal is untouched, so the run replays and continues live.
  - **restart** when the journal ends in a guest terminal event (`RunFailed`/`RunCompleted`). The server starts a new run with the same workflow, version and input, `retry_of = old`, and returns the new id with `mode: "restarted"`.
- Completed → `409 run_completed`. Rerun-from-step is Phase 11.

### Timeouts (WF-09)
**Run timeout:**
- Deadline = `created_at_ms + run_timeout_ms`. This is the same formula `run_started.rs` uses for `RunStarted.deadline_nanos`, and the guest sees it in `run-info.deadline`.
- At `StartRun` apply, insert timer `(deadline, run, RUN_TIMEOUT_OP)`.
- When it fires on a non-terminal run → `TimedOut`. This is a hard stop with the same mechanics as Terminate: no cleanup, reason `run_timeout`.
- CompleteRun removes the row by key. The due time is computable from the record: one point delete, no wasted later fire.
- Continue-as-new: the new run gets a fresh deadline (document it).

**Schedule-to-start:**
- Apply inserts timer `(ready_at_ms + schedule_to_start_ms, run, SCHEDULE_TO_START_OP)` at StartRun.
- The first `AttemptGranted` deletes it.
- When it fires and the run has never been granted (`current_attempt == None`) → `TimedOut`, reason `schedule_to_start`.
- It applies to the initial start only (D2).
- The value is resolved at the API from the request, else queue policy, else none.

Both ride the 02-14 lazy windowed timer service unchanged. The service fires every row, and apply interprets the reserved ops.

### Retention (OPS-06)
**When it is armed:** at every terminal transition (CompleteRun, Terminate, TimedOut, ContinuedAsNew), apply inserts timer `(completed_at_ms + retention_ms, run, EXPIRE_OP)`. `retention_ms` comes from the record.

**What firing deletes:** when `EXPIRE_OP` fires on a terminal run, apply deletes, in one batch:
- `Runs`;
- `Events` (prefix);
- `Payloads` (prefix);
- `Inbox` (prefix);
- `Waits` (prefix);
- `Children` (prefix);
- `Effects` (prefix);
- `SignalSeen` (prefix);
- `Running` (should already be gone);
- the `Dedupe` row (key rebuilt from the stored `idempotency_key` and workflow);
- any DLQ row.

A later StartRun with the same idempotency key creates a new run. That is "dedupe entries have retention", and the semantics must be documented.

**Policy resolution (server):**
- Order: request override (if allowed), else the workflow's `WorkflowPolicy.retention`, else `QueuePolicy.retention`, else `[runs] retention` config (default **7 days**; dev profile 24 h?). The defaults are D9.
- Add the policies as **new meta record variants appended to the `MetaRecord` enum**. Do not add fields to `QueueConfig`/`WorkflowVersion`, because meta's Raft log is the source of truth and postcard is positional.
- Policy changes affect future runs only (document it).

**What it does not do:**
- WAL bytes are not reclaimed: the retention service keeps `snapshot_floor = 0`.
- Rebuild from the log replays the expiry deterministically (TimerFired is in the log), so `rebuild_digest` stays valid.
- Document that "deleted" means gone from state and every API, but still present in WAL segments until Phase 8 snapshots allow trimming. This is a privacy and disk caveat for the release notes.

**Cost:** expiring a run with up to 50k events is a 50k-key WriteBatch inside one apply chunk.
- Measure it.
- If needed, bound the deletes per command. Apply deletes up to N rows, then re-arms `EXPIRE_OP` at the same due time with a continuation marker.
- Keep the first version simple and measure it in the perf plan.

### Continue-as-new (D10, recommended in scope)
- derive: `RunContinuedAsNew{continuation}` → `CompleteRun{status: ContinuedAsNew, result: {"next_run_id": …}}` preceded by `SendMessage{OutboxMsg::StartRun(StartRun{…, continued_from: run})}`.
- The new run id is generated by the submitter (uuid v7 → `partition_for_new_run`) and logged, so replay is deterministic.
- Module: workflow and version from the continuation, else the same version, then resolved to a sha. **The executor needs meta access** to resolve a different workflow; 04-16's executor has the Runtime only, so pass a resolver.
- Inbound apply of `StartRun` reuses `start_run` (dedupe by `(src, seq)` already exists).
- `continued_from` lineage fills `RunStarted.continued_from` (the field exists in the journal type).
- Long-poll `follow_continuations=true` follows the chain.

This must be settled before the freeze, because it adds a TerminalStatus and an outbox kind.

### Engine gaps (must close before freeze)
1. **`cancel-op` completion.** `completion_for(op)` also matches a guest `OpCancelled{op}` event (first in journal order wins). `completion()` maps it to `Err(Failure::Cancelled)`.
   - If an input completed the op before the cancel (lower seq), the input wins and the cancel is a no-op. This is deterministic.
   - Add a replay-core unit test. Check `wit_surface` and the det-basic golden: neither waits on a cancelled op today, so no re-bless is expected. Verify.
2. **`WaitTimedOut`** → `Err(Failure::Timeout)`.
3. **`patched` memo.** Add `BTreeMap<String, bool>` in `ReplayState`. The first evaluation per change-id decides (live: append marker, true; replay: consume a matching marker, true, else false); later calls return the memo with no journal effect.
4. **Unqueried marker skip.** When a guest command is matched and the next recorded guest commands are `PatchMarker`s the code never queried, consume them silently before matching. This is the 03-RESEARCH "patch removed" rule. Markers have no side effects, so skipping is safe.
5. **Op index.** `ReplayState` keeps `BTreeMap<OpId, seq>` of completions, maintained on load, on `deliver_inputs` and on live appends, so `completion_for` is O(log n). This is perf, not semantics.
6. **`Runtime::live_activations()`** gauge, an AtomicU64 incremented and decremented around `run_activation`, for WF-04.
7. **Hot-wait eligibility** (`park_or_evict`) stays timer-and-effect only. Signal and token waits always evict unless a timeout is due within `hot_wait`; document it.

### Gather/race deterministic ordering (WF-03)
- **Source of order.** Every completion input is journaled by apply, in log order:
  - TimerFired from the timer service;
  - SignalReceived from SignalRun or RegisterWait;
  - TokenCompleted;
  - WaitTimedOut;
  - CancelRequested;
  - EffectCompleted/ChildCompleted ("unsupported" in Phase 5);
  - OpCancelled, a guest command.

  Live `wait` sorts by completion seq. Replay returns the recorded `WaitResolved` list verbatim, even if more completions exist by then (`replay/wait.rs`).
- **Equal-due timers.** The timer service fires in `(due, run, op)` BTreeSet order. Even if two leaders differed, the log, not the timer service, is authoritative.
- **SDK.** `join!` = `wait(All)`. `select!` = `wait(Any)` returns *all* completed ops at that point in seq order, and the SDK takes the first. Losers stay pending and can be cancelled with `ctx.cancel(&op)`, which now completes them with `Err(Cancelled)`.
- **Proofs:**
  1. A replay-core proptest: random op sets, random input arrival interleavings and random `All`/`Any`/cancel sequences. Live run, then replay from the produced journal: identical `WaitResolved` lists and identical guest-visible results.
  2. A `race-fuzz` guest seeded from input: N sleeps of random equal and unequal durations, plus K signal waits and nested `select!`/`join!`, outputs the observed order.
  3. A server e2e: concurrent signal senders on tokio tasks, one forced restart mid-run, then re-replay the stored journal on a fresh Runtime with a recording sink. The output is byte-identical and no events are appended.

### Sleeping runs hold no memory (WF-04)
- Mechanism (Phase 3/4):
  - a wait longer than `hot_wait` evicts (Store dropped);
  - the executor submits Suspend, so the lease is released;
  - the dispatcher holds no in-memory entry for Scheduled runs;
  - the timer service loads only rows due within its window (`window_max` bounds memory).
- Test: start 1,000 runs that `sleep(1h)`. Once all are Scheduled, assert:
  - `runtime.live_activations() == 0`;
  - `runtime.parked().parked_count() == 0`;
  - `budget.used() == 0`;
  - dispatcher leases == 0;
  - timer window ≤ `window_max`;
  - process RSS delta per sleeper below a stated bound (read `/proc/self/statm`; Linux only, otherwise skip).
- Then fast-forward with short sleeps in a variant (10 s), or use the test clock if the TestServer clock is injectable, and assert all complete.

### patched(id) (WF-08)
- Runs are pinned to `module_sha`. In-flight runs keep executing their original module, so "continue on old code paths" only matters once a run is executed by a newer module. There is no re-pin API until `migrate-runs` (Phase 13).
- Proof plan:
  - **Engine level.** Record journals of `patch-v1` (steps A, sleep, B), including one parked mid-sleep. Replay them against `patch-v2` (`if ctx.patched("add-c") { step C }` inserted after A). The v1 journals finish on the old path (patched == false, no C) and fresh runs take the new path (marker journaled, C executed). Repeated `patched("add-c")` calls return the memo. A v3 that removes the `patched` call replays v2 journals (unqueried-marker skip).
  - **Server level.** A `#[doc(hidden)]` `ExecutorHooks::module_override(run → sha)` in the 04-16 executor runs the in-flight run on v2 after a restart. This is the minimal mechanism, and it is test-only. Phase 13's `migrate-runs` replaces it with a dry-run-gated re-pin command.
- The `run-info.module-hash` returned to the guest is the executing module's hash (`host/context.rs`). Document that.

### Lifecycle on the executor side (04-16 extensions)
- `RunCompleted` for a leased run (Terminate, TimedOut) must **revoke** the job, not just drop the lease. The activation is cancelled and the executor treats `Evicted{External}` after revoke as `Released`.
- `AttemptFailed` exhaustion writes `CompleteRun{Failed, resumable: true, reason}`. NonDeterminism and ProtocolViolation are `resumable: true` too: a fixed module plus re-pin could resume them later.
- Guest `RunFailed`/`RunCompleted` → `resumable: false`.

### Long-poll (API-06)
- `RunWatchers`: one task per partition subscribed to `machine().subscribe_actions()`. It forwards `RunCompleted{run, status}` to a `Mutex<BTreeMap<RunId, Vec<oneshot::Sender<…>>>>`. On `Lagged`, wake every waiter of that partition so they re-read.
- Request: register the waiter **first**, then read the RunRecord. Terminal → reply now. Otherwise `tokio::time::timeout(min(wait, 60 s))`, then re-read and reply.
- Response: always 200 with `{run_id, status, completed: bool, result?, failure?}`. gRPC `AwaitResult` mirrors it. Not-terminal-yet is not an error (parity-friendly).
- `follow_continuations` loops over `ContinuedAsNew`.
- Lifetime: drop the waiter on client disconnect (the axum handler future is dropped) and garbage-collect empty vectors.

### API surface (API-05..07)
Follow the existing path convention (`/v1/workflows/{name}/active`; no `:verb` suffixes):

| REST | gRPC (`ostro.api.v1`) | Notes |
|---|---|---|
| `POST /v1/runs/{run_id}/signals/{name}` body `{payload, request_id?, dry_run?}` | `Runs.Signal` | 200 `{accepted: true}`. 409 terminal, 404 unknown |
| `POST /v1/runs/{run_id}/cancel` `{reason?, dry_run?}` | `Runs.Cancel` | 200, idempotent (Duplicate → `already_requested: true`) |
| `POST /v1/runs/{run_id}/terminate` `{reason?, dry_run?}` | `Runs.Terminate` | 200. 409 if already terminal |
| `POST /v1/runs/{run_id}/retry` `{dry_run?}` | `Runs.Retry` | 200 `{mode: resumed\|restarted, run_id}` |
| `GET /v1/runs/{run_id}/result?wait=30s&follow_continuations=` | `Runs.AwaitResult` | see Long-poll |
| `POST /v1/tokens/complete` `{token, payload, dry_run?}` | `Tokens.Complete` (new service) | 409 already completed. 400 bad MAC |
| `POST /v1/workflows/{name}/runs` gains `schedule_to_start_timeout`, `retention` | `Runs.Start` fields | proto3 optional |

- `RunStatus` (domain, REST, proto) gains `TERMINATED`, `TIMED_OUT`, `CONTINUED_AS_NEW` as new proto enum values appended.
- `dry_run=true` runs the same validation as the real call: run exists, status allows the operation, payload size and JSON, token MAC, retry mode. It returns the would-be outcome without submitting.
- Extend `FakeControlPlane`, the OpenAPI insta snapshot (deliberate update) and the 04-11 parity suite (`tests/parity.rs`) scenario with every new call.
- Size limits mirror the existing `max_request_bytes`.

### CLI (CLI-06)
- **Crate.** `ostro-cli`, today a placeholder, owns the commands. `crates/ostro` adds them to the clap tree.
- **Transport.** REST through `reqwest =0.13.5` (already pinned; promote it to a normal dep, `default-features = false, features = ["json", "http2"]`, no TLS until SEC-01).
  - Rationale: `--json` output equals the REST bodies and the OpenAPI document.
  - The gRPC client is the alternative, kept for workers.
- **Global flags.** `--server/OSTRO_SERVER` (default `http://127.0.0.1:7700`), `--json`, `--timeout`.
- **Verbs:**
  - `run <workflow> [--input JSON|@file] [--version] [--queue] [--idempotency-key] [--conflict reject|return] [--delay] [--tag k=v]… [--wait[=30s]] [--dry-run]`
  - `ls [--workflow --status --queue --tag --limit --page]`
  - `inspect <run>`
  - `history <run> [--limit]`
  - `signal <run> <name> [--payload JSON|@file] [--request-id] [--dry-run]`
  - `cancel <run> [--reason] [--dry-run]`
  - `terminate <run> [--reason] [--dry-run]` (FR-3.4 lists it; terminate is separate from cancel)
  - `retry <run> [--dry-run]`
  - `result <run> [--wait]`
  - `token complete <token> [--payload]`
- **Exit codes:** 0 ok; 1 server or API error (error body printed, JSON under `--json`); 2 usage; with `run --wait` / `result`, 3 when the run ended non-Completed.
- **Tests.** Spawned binary against `ostro dev --ephemeral`, with insta snapshots of normalised `--json` output (ids and timestamps redacted). Human output is not snapshotted.
- **D12:** a minimal `ostro deploy <file.wasm> [--activate]` that only uploads (CLI-05's build-if-needed stays Phase 10). It makes the order-saga quickstart curl-free.

### Order-saga example (Success criterion 1)
`examples/order-saga/` is a standalone cargo project with a path dependency on `sdks/rust/ostro-sdk`. Built and run in CI by a test through `guest_wasm`-style helpers.

Five steps:
1. `reserve_inventory`
2. `charge_payment`, with a retry policy
3. `create_shipment`
4. `notify_customer`
5. `complete_order`

Plus:
- a timer, `ctx.sleep(payment_settle)`;
- a signal, `wait_signal("approve", timeout 24h)`, raced with `ctx.cancellation()`;
- hand-rolled compensations in reverse on failure or cancel: `release_inventory`, `refund_payment` (the WF-10 helper is Phase 9).

Steps are pure simulations because effects arrive in Phase 6. `README.md` quickstart:
1. `rustup target add wasm32-wasip2`
2. `cargo build --release --target wasm32-wasip2`
3. `ostro dev`
4. `ostro deploy target/…/order_saga.wasm --activate`
5. `ostro run order-saga --input '{…}'`
6. `ostro signal <run> approve --payload true`
7. `ostro result <run> --wait`

**"Clean machine < 10 min"** is measured by `examples/order-saga/quickstart.sh` in a fresh `rust:1.100` container (docker) from a clean clone, with the time printed. This is human_needed if docker is unavailable to the executor.

### WIT freeze (WIT-07)
1. **Pre-freeze WIT review (gate plan, human-confirmed).** Records, variants and enums cannot grow after the freeze. Review each one against the Phase 6–9 requirements. Candidates:
   - `effects.object-request`: no presign expiry, no put content-type. FX-05 needs presign.
   - `effects.http-request`: no redirect policy or response-size cap. FX-01 could take these from per-module egress config instead; decide.
   - `effects.child-options`: no tags or schedule-to-start.
   - `types.run-info`: no queue or tags.
   - `workflows.workflow-meta`: an export record; no retention or schedule-to-start, so those stay API/meta policy.
   - `wait-signal` returns no metadata.

   Decide per item: add the field now (cheap, pre-freeze) or accept "new function later". Also update doc comments now. FIFO signal semantics, cancel-op completion, patched memo, step-local RNG and timeout semantics are all frozen with the file bytes.
2. **Byte identity.** `wit/ostro-runtime-0.1.0/runtime.wit` sha256 is pinned in a test, `wit_freeze::frozen_0_1_0_is_byte_identical`. Any edit fails CI. Optionally commit `wit/FROZEN.toml` with the sha.
3. **Additive evolution path.** A future change is a new package dir `wit/ostro-runtime-0.1.N/` (package `ostro:runtime@0.1.N`) that must pass `wit_component::semver_check(resolve, prev=0.1.0 world, new=0.1.N world)`.
   - The function is gated by feature `semver-check` in `wit-component =0.261.0`, the version already in Cargo.lock via wasmtime 50, so there is no duplicate.
   - It encodes `prev` as a dummy component and validates instantiating it against the `new` world type. Per its docs, `new` may add imports and drop exports; otherwise types must match structurally.
   - **Caveats from the source:** both worlds must share the package name; it ignores versions; and a reported issue means two versions of the same package in one `Resolve` may fail to load (Zulip report).
   - **Prove the tooling with fixtures before trusting it:**
     - positive: add a function to `context`; add a new interface imported by the world;
     - negative, must fail: add a record field; add a variant case; change a param type; remove a function; rename an interface; add an export function to `workflows`.

     If a negative fixture passes `semver_check`, add an Ostro structural diff over `wit_parser::Resolve` for that class (wit-parser `=0.261.0`). Every existing interface, type and function in prev must exist in new with an identical resolved shape.
   - Wasmtime's linker resolves semver-compatible imports, so a guest importing `ostro:runtime/context@0.1.0` binds to a host definition `@0.1.1`. For `0.x`, the minor is the compatibility key (Wasmtime `Linker` docs, PR #7994). That makes `0.1.N` additive packages servable to old guests.
   - Bump `wit-component`/`wit-parser` together with every Wasmtime bump. Keep them dev-dependencies of a tooling crate or test only. New dev-deps `wat`/`wast` (pulled by `dummy-module`) must pass cargo-deny.
4. **Old guests still link.** Already covered by the replay corpus, which loads committed guest modules through the production linker and validation.
5. **Tag.** Create the local git tag `wit/ostro-runtime-v0.1.0` in the gate plan. Pushing tags is a human action.
6. **Goldens become append-only.** Add a test that pins `(line_count, sha256(lines))` of `commands.txt` and `journal_v1.txt` at freeze time, and of `routing.txt`. New lines may follow; the frozen prefix may never change. `wasi_probe_imports.snap` and the `describe_wasmtime_config` snapshot are not format, so they stay normal snapshots.

### Replay corpus (QA-02)

**Format:**
```
tests/replay-corpus/
  README.md                       # rules: never re-bless; add entries; how to record
  <entry>/entry.toml              # name, recorded_with (ostro version, wasmtime, target triple, date),
                                  # workflow, expected outcome kind, journal_events, sha256s
  <entry>/module.wasm             # exact bytes (Rust guests ~100–300 KB; commit them)
  <entry>/journal.bin             # length-prefixed JournalEvent protobufs (same bytes as the Events rows)
```
- **Replay test** (`crates/ostro-engine/tests/replay_corpus.rs`, in the default suite). For each entry:
  - verify the sha256s;
  - `Runtime::prepare` the module (AOT for the current target);
  - `activate` with the full recorded journal and a recording sink;
  - assert `Completed`/`Failed`/terminal outcome equal to the journaled terminal;
  - assert **zero new events** (pure replay);
  - assert no NonDeterminism.

  It runs in `cargo test --workspace` and `cargo-zigbuild test`, so it covers linux-x86_64-musl, linux-aarch64-musl (native arm runner), macos-aarch64 and linux-x86_64-gnu without new CI jobs. Remote legs stay human_needed until the user pushes (FOUND-05).
- **Recorder.** `cargo run -p ostro-engine --example record_corpus -- <guest> <spec>` for engine runs. Add `ostro-server` export of a stored run (`journal_io::read_journal` → `journal.bin`) so server-recorded runs (order-saga, race-fuzz with real signals, crash-campaign samples) can be committed.
- **Seed entries:**
  - det-basic (convert the existing golden);
  - wit-surface;
  - sdk-basic, sdk-retry exhaust/recover, sdk-native;
  - race-fuzz (several seeds);
  - signals + timeout;
  - cancel-cleanup;
  - patch-v1 replayed by v1;
  - continue-as-new;
  - order-saga happy, compensated and cancelled paths;
  - 2–3 crash-campaign journals.

  Record after the engine-semantics plan lands. **Never re-record an existing entry.** A change that breaks an entry is a determinism or compat bug.
- **Cross-version rule.** Entries carry `recorded_with`. Every Wasmtime bump PR must keep all entries green; this is the pin policy gate.
- **Cross-arch rule.** The corpus is recorded on x86_64 and replayed on aarch64 by CI. Optionally, a later CI job records on aarch64 and replays the artifact on x86_64.
- **Format corpus** (on-disk freeze, recommended). A tiny data dir created at freeze time (P=2, ~50 runs covering every command kind) committed as `tests/format-corpus/v0.1.tar.zst`, or as a directory if zstd would add a C dep; use plain tar.
  - A test unpacks it, opens it in Inspect mode, rebuilds from the log, and compares the digest, `state_hash` and LEO to the values recorded in `format.toml`.
  - Check the WAL segment file size first. If segments are preallocated large, generate the corpus with a small rotation size, or commit only the trimmed files.

### Crash campaign (QA-01)
**Target:** the real `ostro` binary built with a `failpoints` feature.
- Forward the feature to ostro-server, ostro-partition, ostro-log, ostro-wal, ostro-blob and ostro-meta.
- `main` calls `fail::FailScenario::setup()` when the feature is on. Its env var is `FAILPOINTS`.
- The harness spawns `ostro dev --data-dir <shard dir> --listen 127.0.0.1:<port>` or `ostro server`.

**Workload (`guests/crash-saga`, deterministic expected output from input + signals):**
- 3–8 steps, some with retry policies that fail deterministically;
- a short sleep (beyond hot_wait for some runs);
- `wait_signal` with timeout;
- a gather of 2–3 sleeps and a signal;
- optional graceful cancel handling.

**Load generator (harness process):**
- starts runs with idempotency keys (some duplicates) over HTTP;
- sends signals with `request_id`;
- cancels 5% of runs and terminates 2%.

**ACK rules:**
- A run is *accepted* when `start` returned 2xx with a run id.
- A signal is accepted on 2xx.
- Each is recorded in an append-only ack log, fsynced, in the harness.

**Faults per iteration (one is chosen):**
1. `kill -9` at a uniformly random time under load.
2. Failpoint injection (low probability per hit, e.g. `1%return`), then kill once the server reports a failed partition or after T.
3. ENOSPC variants.

Failpoint sites:

| Site | Exists today | Failure injected |
|---|---|---|
| `wal::write` | yes | EIO, or a partial write plus ENOSPC via arg `enospc` |
| `wal::fsync` | yes | — |
| `wal::dir_fsync` | yes | — |
| `state::commit` | yes | — |
| `state::persist` | yes | — |
| `blob::fsync` | yes | — |
| `blob::before_rename` | yes | — |
| `meta::log_append` | new, in the vendored raft log store | EIO/ENOSPC |
| `meta::fsync` | new | EIO/ENOSPC |
| `aot::write` | new | non-fatal: must just recompile |
| `executor::before_append` | new | crash-seam point (`panic`/`sleep` actions) |
| `executor::after_append_before_done` | new | crash-seam point |
| `dispatcher::before_grant` | new | crash-seam point |

Every new site gets an `enospc` arg variant where it writes.

**Checker (after each kill, and at the end):**
- (a) Restart. Every acked run exists (GET 200).
- (b) After a final settle with faults off and signals delivered, every acked run is terminal within T. For Completed runs the result equals the expected function of input and accepted signals; cancelled and terminated runs carry the right status.
- (c) **Journal invariants** (0 duplicate journal entries), per run, from history:
  - seqs contiguous from 0;
  - exactly one `RunStarted` at 0;
  - at most one terminal event, and it is last apart from history markers;
  - per step command position, at most one `StepCompleted`, and `StepAttemptFailed` attempts strictly 1..n;
  - every op completed at most once (one completion input per op);
  - every accepted `request_id` signal journaled at most once;
  - every TimerFired op appears once.
- (d) `rebuild_and_compare` on a copy (existing helper) every K iterations.
- (e) No acked start is lost even when the API answered just before the kill.

**Counting.** One fault = one kill, or one injected I/O error observed by the server (count from the server log line or the partition-failed event). Target ≥ 10,000.

**Scale problem.** 02-15 measured reopen time growing with segment count (≈1.3 s after 500 restarts, one new segment per start). 10k sequential restarts on one data dir is not viable unless 02-16 fixes segment growth. **Shard the campaign:** S independent data dirs (e.g. 20 × 500 iterations), run S/2 shards in parallel on a 12-core box. Fresh dirs also vary the starting state.
- Wall-clock estimate: 2–4 s per iteration → 10k iterations ≈ 6–11 h serial, ≈ 1–2 h with 8 parallel shards.
- Make it resumable: shard progress is persisted.

**Real ENOSPC (optional, Linux, root):** an extra `#[ignore]` leg on a size-limited tmpfs or loop device (`sudo mount -t tmpfs -o size=64m`) filled by a ballast file. It is human_needed locally; on GitHub Actions sudo is available. Failpoints remain the primary EIO/ENOSPC mechanism.

**CI:**
- smoke: 20 iterations × 1 shard in the failpoint step (`#[ignore]` off via an env var), or a dedicated short test;
- the full campaign is a gate command recorded in VALIDATION.md, like 02-15's 500-iteration gate.

### Performance (Success criterion 4)
**Targets:** ≥ 5,000 trivial Rust steps/s, fsync on, single node; start → first step p50 < 5 ms (docs/05 exit; NFR-2.5 says ≥ 10k, so report against both). The hardware definition is "8 vCPU NVMe". **This machine is WSL2, where Phase 4 measured ≈ 6.8 ms per fsync.** Numbers measured here are advisory, and the gate number must come from native Linux NVMe (human_needed, D15).

**Define the benchmark precisely (D16).** A step counts when its `StepCompleted` is acknowledged and found on disk after the run.
- Profile A: 10-step workflow, steps/s.
- Profile B: 1-step workflow, runs/s and steps/s.
- Load: N concurrent starters through the in-process ControlPlane and through HTTP.
- Afterwards: restart and count StepCompleted events through history. This verifies acknowledged work on disk (Chronik RP-11 / QA-04 rule).
- Report p50/p99 latency, CPU, fsyncs/s (`ostro_wal_batches_total`), and bytes on disk.

**Latency path today (04-16 design):**

| # | Step | Commit |
|---|---|---|
| 1 | `StartRun` | fsync 1 |
| 2 | apply → `EnqueueReady` action → dispatcher | — |
| 3 | `grant_attempt` (awaited) | fsync 2 |
| 4 | separate `RunStarted` append | fsync 3 |
| 5 | journal read; instantiate (~272 µs debug; less in release) | — |
| 6 | run the step; flush at activation end (`StepCompleted` + `RunCompleted` + `CompleteRun`) | fsync 4 |

The engine batches every live event until park or end of activation. It does not flush per step, so step throughput is dominated by per-run overhead, not per-step fsync.

Four sequential WAL commits on the start path. Even at ~1 ms per NVMe flush that is ~4 ms plus overheads, so **< 5 ms p50 needs fewer sequential commits.** Ranked optimisations (measure, then apply):
1. **Pipelined grant.** Submit `AttemptGranted` without awaiting its ack and start the activation. The Rf1Log sequencer assigns offsets under one lock in submission order (`rf1.rs` append), so the first flush always lands after the grant. If the grant is rejected, apply rejects the flush (token check). Often the grant and the flush share a WAL batch, which saves one fsync wait. This must be verified with a test that the dispatcher never issues two grants concurrently for one run.
2. **Drop the separate RunStarted append.** Either the API pre-encodes `RunStarted` into `StartRun.run_started` (tag 23; apply writes seq 0 and `next_seq = 1`; history shows it immediately), or the first activation flush carries it (an engine API change: an uncommitted prefix in `Activate`). Recommendation: the StartRun field, because it is simpler and makes 04-16's "next_seq == 0" path disappear.
3. **Replay index** (engine gap 5), for long journals.
4. **Dispatcher.** Avoid per-dispatch `read_run` after a grant by returning the record from the grant path.
5. **HTTP ingress.** If profile B over HTTP is the limit, measure JSON and axum overheads separately from the core.

**Likely throughput bottlenecks, in order:**
- per-run fixed commands (3–4) and their commit waits, when concurrency per partition is low;
- per-partition WAL worker fsyncs (16 independent fsync streams; on a single NVMe they parallelise, on WSL they do not);
- executor slots (semaphore default) and tokio task overhead per activation;
- `read_journal` decode for each activation;
- the HTTP load generator itself.

fjall apply (106.9k RMW cmds/s measured) is not the bottleneck: 4 commands per run gives a ceiling of 26k runs/s.

Bench harness: `crates/ostro-bench` (binary plus `#[ignore]` test) or `crates/ostro-server/tests/perf.rs #[ignore]`. No new deps: percentiles by sort; no hdrhistogram or criterion needed.

### Anti-Patterns to Avoid
- **Apply decoding journal payloads** to find signal names or timeouts. Use `RegisterWait` intents (the ownership rule).
- **A separate in-memory signal or token registry in the server.** It is lost on restart and diverges on rebuild. Everything goes through log + apply.
- **Reusing `TimerFired` for wait timeouts.**
- **Run timeout enforced by the executor or engine only.** A Scheduled (sleeping) run has no executor. It must be an apply-side timer.
- **Adding fields to existing meta structs.** Meta's Raft log is the source of truth and postcard is positional. Add new `MetaRecord` variants at the end.
- **Re-recording replay-corpus entries or re-blessing frozen golden lines.**
- **10k sequential kills on one data dir.** Segment growth makes it superlinear.
- **Long-poll that subscribes after reading the record.** That is a lost-wakeup race.
- **Benchmarks that count submitted rather than acknowledged-and-on-disk work.**

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| WIT compatibility diff | a text diff of `.wit` files | `wit_component::semver_check` (`=0.261.0`, feature `semver-check`) plus a byte-identity hash; wit-parser structural diff only for classes semver_check misses | Structural equality through component validation; text diffs miss type aliasing and reordering |
| Failpoints | ad-hoc env checks / `cfg(test)` hooks | `fail` crate (already used: `fail_point!`, `fail::eval`, `FailScenario::setup`, `FAILPOINTS` env) | Same registry across crates; the existing `enospc` arg pattern in `wal::write` |
| Durable timers for timeouts and retention | new timer services or keyspaces | the existing `Timers` keyspace + 02-14 lazy window + reserved ops | Already crash-safe, idempotent (`TimerFired` = row token), lazy |
| Cross-partition continue-as-new start | direct submit to another partition | outbox `SendMessage` + shuffler `(src, seq)` dedupe | Exactly-once across crashes is already proven (02-13) |
| Token MAC/format | new token code in the server | `ostro_engine::replay::token::{mint, parse, verify}` (make parse/verify pub) | Golden vectors already computed independently in Python |
| CLI HTTP client | hyper by hand | `reqwest =0.13.5` (already in Cargo.lock) | JSON, timeouts, h2 |
| JSON output snapshots | hand-written string compares | `insta` 1.49 (in the tree) with redactions | Deliberate review of output changes |
| Crash-check rebuild | new rebuild code | `ostro-crashtest::rebuild_and_compare`, `open_node`, `scan_log` | Proven over 500 kill iterations |

**Key insight:** every Phase 5 feature is a new *input* or a new *timer*. Phases 2–4 already built the crash-safe pipes for both: input = journal at `next_seq` + wake; timer = row + lazy window. The work is routing every new event through them, not inventing new mechanisms.

## Common Pitfalls

### Pitfall 1: Signal/timeout double completion
**What goes wrong:** a signal arrives and the timeout timer fires, so both `SignalReceived` and `WaitTimedOut` are journaled for one op.
**How to avoid:** the wait row is the token. Whichever apply runs first removes it, together with the timer row. The second finds no wait row and goes to the inbox (signal) or becomes a no-op (timer). The engine also takes the first completion in journal order, but never rely on that alone.
**Warning signs:** two completion inputs with the same op in a journal. The crash checker invariant (c) catches it.

### Pitfall 2: Lost wake-up for inputs journaled during RegisterWait
**What goes wrong:** apply satisfies a wait from the inbox inside the executor's own `EventsAppended`. The executor believes `next = from_seq + n`, and its next append conflicts.
**How to avoid:** this is correct by design. `Conflict` maps to re-activation, and `Suspend` fails its CAS. Also forward `InputAppended` so a hot activation receives the event. Test the "signal sent before wait" path explicitly.

### Pitfall 3: Terminate/timeout not stopping a hot activation
**What goes wrong:** the dispatcher drops the lease on `RunCompleted` but never revokes, so the activation keeps running until its next append.
**How to avoid:** revoke on `RunCompleted` (04-14 extension). Append after terminal → `RunTerminal` → `Fenced`.
**Warning signs:** the activations counter stays > 0 after terminate.

### Pitfall 4: Expiry deleting a non-terminal or resurrected run
**What goes wrong:** retry or resume makes the run non-terminal, but the old `EXPIRE_OP` row still fires.
**How to avoid:** RetryRun removes the expiry row by key (`completed_at + retention`). The `EXPIRE_OP` apply re-checks "terminal" and is a no-op otherwise.

### Pitfall 5: Retention vs WAL trimming
**What goes wrong:** someone enables `retention_floor` or trimming to reclaim disk. Rebuild from the log then becomes impossible ("snapshot required").
**How to avoid:** keep `snapshot_floor = 0` until Phase 8, and document that WAL bytes outlive TTL deletion.

### Pitfall 6: Freeze-blocking WIT omissions discovered in Phase 6/9
**What goes wrong:** the presign expiry or a child option is missing, and a v2 function is needed forever.
**How to avoid:** run the pre-freeze WIT review checklist with the user before the freeze plan.

### Pitfall 7: Crash campaign superlinear slowdown
**What goes wrong:** reopen and `rebuild_and_compare` times grow with log size and segment count.
**How to avoid:** shard, use fresh dirs per shard, rebuild every K iterations, and bound per-shard iterations.

### Pitfall 8: Benchmarks on tmpfs or WSL
**What goes wrong:** numbers that are meaningless for the gate.
**How to avoid:** the bench prints the filesystem type (`stat -f`) and refuses `tmpfs` unless `--allow-tmpfs` is given. The gate run happens on native NVMe, and the hardware is recorded.

### Pitfall 9: Golden drift through "harmless" changes
**What goes wrong:** appending a new `EventBody` variant anywhere other than last changes postcard tags for existing variants.
**How to avoid:** append-only goldens and the new frozen-prefix test.

### Pitfall 10: Meta format break
**What goes wrong:** adding `retention` to `QueueConfig` breaks decoding of existing meta Raft log entries in dev data dirs.
**How to avoid:** new `MetaRecord` variants appended at the end.

### Pitfall 11: Clock-dependent apply in timeouts
**What goes wrong:** computing the deadline in apply from wall time.
**How to avoid:** deadlines are `created_at_ms` (from the command) + `run_timeout_ms`, and fire time comes from `TimerFired.fired_at_ms`.

## Code Examples

### Input journaling + wake (shared helper, generalises 04-13)
```rust
// crates/ostro-partition/src/apply/inputs.rs (sketch; mirrors 04-13 timer path)
pub(crate) fn journal_input(
    b: &mut ApplyBatch<'_>, run: &RunId, rec: &mut RunRecord,
    ts_ms: u64, body: EventBody, actions: &mut Vec<Action>,
) -> Result<u64, StoreError> {
    let seq = rec.next_seq;
    let (kind, bytes) = encode_event(&Event { seq, ts_nanos: ts_ms.saturating_mul(1_000_000), body })?;
    b.insert(Table::Events, event_key(run, seq), encode_event_row(kind, PayloadRef::inline(bytes)));
    rec.next_seq += 1;
    match rec.status {
        RunStatus::Scheduled => make_ready(b, run, rec, ts_ms, actions),        // EnqueueReady
        RunStatus::Running   => actions.push(Action::InputAppended { run: *run, seq }),
        _ => {}                                                                // Ready: already queued
    }
    Ok(seq)
}
```

### SignalRun apply (sketch)
```rust
// 1 read: run record; 1 prefix read: waits by name; (optional) 1 read: SignalSeen
let Some(mut rec) = load_run(b, &run)? else { return reject(RunNotFound) };
if rec.status.is_terminal() { return reject(RunTerminal) }
match first_wait_for_name(b, &run, &s.name)? {           // range_prefix(wait_name_key(run, name))
    Some(op) => {
        remove_wait(b, &run, op, &mut rec);               // both key shapes + timer row (due from wait row)
        journal_input(b, &run, &mut rec, s.sent_at_ms,
            EventBody::SignalReceived(SignalReceived { op, payload }), actions)?;
    }
    None => push_signal_inbox(b, &run, &mut rec, &s.name, &s.payload),
}
store_run(b, &run, &rec);
```

### Engine: cancel-op as completion (sketch, `replay/wait.rs`)
```rust
pub fn completion_for(&self, op: OpId) -> Option<&Event> {
    // index: BTreeMap<OpId, u64> maintained on load / deliver_inputs / append_live
    self.completion_index.get(&op).map(|&seq| &self.journal[seq as usize])
}
// index insertion rule: first of { input with completes_op()==Some(op), guest OpCancelled{op} } wins
// host/effects.rs completion(): EventBody::OpCancelled(_) => Err(Failure::Cancelled),
//                               EventBody::WaitTimedOut(_) => Err(Failure::Timeout)
```

### WIT semver check (sketch; verify API on wit-component 0.261 docs.rs)
```rust
// dev-dependency: wit-component = { version = "=0.261.0", features = ["semver-check"] }
//                 wit-parser = "=0.261.0"
let mut resolve = wit_parser::Resolve::default();
let (prev_pkg, _) = resolve.push_dir("tests/wit-fixtures/base")?;     // ostro:runtime@0.1.0
let (new_pkg, _)  = resolve.push_dir("tests/wit-fixtures/add-func")?; // ostro:runtime@0.1.1
let prev = resolve.select_world(&[prev_pkg], Some("workflow"))?;      // signature varies by version: check
let new  = resolve.select_world(&[new_pkg],  Some("workflow"))?;
wit_component::semver_check(resolve, prev, new)?;                     // Err = not additive
```
`Resolve::push_dir` and `select_world` signatures have changed across wit-parser releases. The plan must read the 0.261 docs and must not copy this sketch blindly.

### Long-poll registration order
```rust
let rx = watchers.register(run);                 // 1. subscribe first
let rec = query::run_record(&reader, &run)?;     // 2. then read
if rec.status.is_terminal() { return Ok(done(rec)) }
match tokio::time::timeout(wait.min(MAX_WAIT), rx).await { _ => Ok(current(read_again()?)) }
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| Inputs in `inbox` (02-10) | Inputs journaled at `next_seq` + wake (04-13 timers; Phase 5 all) | Phase 4/5 | Inbox only buffers unmatched signals |
| `TerminalStatus {Completed, Failed, Cancelled}` | + Terminated, TimedOut, ContinuedAsNew | Phase 5 | API RunStatus parity with Temporal-like UX |
| WIT check by review | byte-identity hash + `semver_check` fixtures | Phase 5 | Mechanical WIT-07 |
| Phase 2 500-kill partition loop | sharded whole-server workflow campaign ≥ 10k faults | Phase 5 | QA-01 per phase gate from now on |
| Single golden det-basic snapshot | committed replay corpus (modules + raw journals) | Phase 5 | Wasmtime bump gate (pin policy) |

**Deprecated/outdated:**
- `InboxMsg::TimerFired` (dead after 04-13).
- `InboxMsg::CancelRequested` (becomes a journal input).
- docs/03 §2.2's `RunsExpired{runs[]}` command: replaced by `EXPIRE_OP` timers. Update docs/03 at freeze.
- docs/03 §2.2's `CancelRequested{run, mode}`: replaced by `RequestCancel` + `TerminateRun`. Update docs/03 at freeze.
- docs/03 §3's WAL path (`wal/p/<id>/…`, per 02-RESEARCH W11). Update docs/03 at freeze.

## Open Questions / Decisions to Confirm Before the Freeze

The planner should put these in the 05-01 gate plan as a `checkpoint:decision` task (or run `/gsd:discuss-phase 5`). Each has a recommendation.

1. **D1 Retry semantics.** Resume-in-place for `resumable` failures; restart as a new linked run for guest-terminal failures; 409 for Completed. *Alternative:* restart always (simpler, loses progress).
2. **D2 Timeout semantics.** Run timeout is a hard stop → `TimedOut`, no guest cleanup. Schedule-to-start applies to the initial start only. *Alternative:* a soft cancel at the deadline plus a hard stop after a grace period.
3. **D3 Terminate.** Distinct `Terminated` status; cascade *graceful* cancel to children; journal a `RunTerminated` history marker (kind 71). *Alternative:* no marker (history from RunRecord only).
4. **D4 Wait timeouts.** New input kind `WaitTimedOut(70)` rather than reusing `TimerFired`.
5. **D5 cancel-op** completes the op with `Err(Cancelled)` (engine change; matches the WIT doc).
6. **D6 patched.** Memoised per change-id, and unqueried markers skipped. Server-level proof via a test-only module override; no re-pin API before Phase 13.
7. **D7 Step-local RNG (03-11).** Randomness drawn inside a live step comes from `sha256("ostro/step-rng/v1" ‖ seed ‖ seq)`, so step-body randomness depends on the step's journal seq, and each retry attempt gets a fresh stream (new seq). Confirm this as a frozen semantic and document it in the WIT and SDK docs. Phase 9 Python/TS SDKs must reseed their language PRNGs at step boundaries the same way, or randomness inside steps leaks into the run-level stream.
8. **D8 Startup criterion and partition count.** Phase 4 relaxed CLI-01 to "warm restart and `--ephemeral` < 1 s; cold first run measured" (WSL2 cold open 1.5–1.7 s from ~2k fsyncs at P=16). Confirm. Confirm **P=16** as the frozen default (node.toml fixes P per data dir). Dev P=4 would cut cold start but makes the dev and server formats differ; recommend keeping 16 and accepting the relaxed criterion.
9. **D9 Retention defaults.** 7 days server, 24 h dev; resolved at start; policy changes affect future runs only; WAL not reclaimed until Phase 8.
10. **D10 Continue-as-new in Phase 5.** Recommended, because it adds a TerminalStatus and an outbox kind that freeze now.
11. **D11 SDK cancellation surfacing.** Options:
    - (a) explicit only: workflows `select!` on `ctx.cancellation()`;
    - (b) recommended: `Op::wait()` observes cancellation by default (`wait([op, CANCELLATION], Any)` → `Err(Cancelled)`), with `ctx.shielded(|| …)` for cleanup code. This is an SDK-only choice (no WIT change), but it shapes every Rust SDK journal (wait hashes include the op list).
12. **D12 Minimal `ostro deploy <wasm>`** (upload-only) in Phase 5 for the quickstart. CLI-05 full build/deploy stays Phase 10.
13. **D13 Signal dedupe.** An optional `request_id` with a run-scoped `SignalSeen` keyspace. *Alternative:* no dedupe (at-least-once signals from client retries).
14. **D14 WIT pre-freeze review outcomes:** object presign expiry, http redirect/size policy, child-options, run-info fields.
15. **D15 Perf gate environment.** Native Linux NVMe, 8 vCPU, human-run. WSL2 numbers are advisory.
16. **D16 Benchmark step definition.** Profile A (10-step runs) is the gate; profile B (1-step) is reported.
17. **D17 Wasmtime 51 (~2026-12-20)** may land during Phase 5. If bumped, bump `wit-component`/`wit-parser` together and re-run the corpus. Do not freeze WIT mid-bump.
18. **D18 What "on-disk format frozen" covers.** Recommendation: the log (envelope, `Command` prost, journal event codec, command hash), `node.toml`, the blob layout, and the meta Raft log/snapshot encoding. These are sources of truth. The **partition state-store value encoding is derived**, so a future `STATE_FORMAT` bump may trigger a rebuild from the log (or, after Phase 8, from snapshot + tail with a snapshot-format version). Write this policy into `docs/storage-format.md`, so post-freeze `RunRecord` evolution has a defined path (postcard cannot default missing trailing fields).

Other unresolved items:
- **WAL segment size for the format corpus.** Unknown whether segments are preallocated. Check before committing a data dir.
- **semver_check with two versions of one package in one `Resolve`.** A reported problem (Zulip, 2024). The fixtures will tell; the fallback is the wit-parser structural diff.
- **Child workflows remain "unsupported"** (Phase 9). Terminate/cancel cascades are exercised only with the outbox `CancelRun` already present.

## Proposed Plan Breakdown (waves and dependencies)

| Plan | Wave | Depends on | Scope (file ownership) | Requirements |
|---|---|---|---|---|
| **05-01** Gate + contract additions + decision checkpoint | 1 | Phase 4 complete (04-20 SUMMARY) | Gate check (04-* SUMMARYs, clean tree, ci-local baseline); **decision checkpoint D1–D18 + WIT review checklist**; command.rs (Cmd 8–12, StartRun 19–23, Intent 7–8, CompleteRun.resumable, statuses, outbox kind 6, reserved ops); journal.rs kinds 70/71; store keyspaces (Waits, SignalSeen), Inbox re-key, RunRecord fields, RunStatus; apply dispatch stubs; appended goldens; storage-format doc | WIT-07 (prep), all (contracts) |
| **05-02** Engine semantics | 2 | 05-01 | replay/cursor.rs, wait.rs, host/effects.rs, runtime.rs: cancel-op completion, WaitTimedOut, patched memo + skip, op index, live_activations, token parse/verify pub; replay-core proptest (gather/race interleavings) | WF-03, WF-08 (engine) |
| **05-03** Apply: waits, signals, tokens, cancel input | 2 | 05-01 | apply/waits.rs, apply/inputs.rs (+04-13 refactor), inbound.rs cancel path; tests apply_waits.rs; rebuild_digest generators | WF-05, API-05 (signal/cancel), API-07 (apply) |
| **05-04** Apply: lifecycle, timeouts, continue-as-new, retention | 2 | 05-01 | apply/lifecycle.rs, apply/retention.rs, timer.rs reserved ops, run.rs CompleteRun mapping; tests apply_lifecycle.rs, apply_retention.rs; rebuild_digest generators | WF-09, API-05 (terminate/retry), OPS-06 (apply) |
| **05-05** API surface (adapters only) | 2 | 05-01 | ostro-api domain/rest/grpc/fake, ostro-proto runs.proto + tokens.proto, OpenAPI snapshot, parity scenario extension (against FakeControlPlane) | API-05, API-06, API-07 (surface) |
| **05-06** WIT freeze tooling | 2 | 05-01 | tooling crate or ostro-engine test: byte hash (pre-freeze mode: report only), semver_check fixtures (positive/negative), structural fallback if needed, ci-local + ci.yml step, cargo-deny for wat/wast | WIT-07 |
| **05-07** Rust SDK + test guests + order-saga | 3 | 05-02 | sdks/rust (await token API, cancellation surfacing per D11, select! loser cancel, docs for patched/step RNG); guests race-fuzz, signals, timeouts, patch-v1/v2/v3, crash-saga; examples/order-saga (code + README) | WF-03, WF-05, WF-08 (guests) |
| **05-08** Server seam: derive + executor + meta policies | 3 | 05-02, 05-03, 05-04 | derive.rs mappings (RegisterWait/CancelWait/Cancelled/ContinuedAsNew/resumable), remove token/CAN "unsupported", executor revoke on RunCompleted, module resolver for CAN, ExecutorHooks::module_override, meta WorkflowPolicy/QueuePolicy records, retention/s2s resolution | WF-04/05/08/09, OPS-06 |
| **05-09** Server ControlPlane lifecycle + long-poll | 4 | 05-05, 05-08 | control/lifecycle.rs, control/watch.rs (RunWatchers), dry_run, token verify/route; tests lifecycle_api.rs over REST+gRPC | API-05, API-06, API-07 |
| **05-10** Replay corpus tooling + seed entries | 4 | 05-02, 05-07 | tests/replay-corpus/*, replay_corpus.rs, recorder example + server export, format-corpus generator/test (entries finalised in 05-16) | QA-02 |
| **05-11** CLI verbs | 5 | 05-09 | ostro-cli + crates/ostro main: run/ls/inspect/history/signal/cancel/terminate/retry/result/token (+ deploy per D12), --json/--dry-run, spawned-binary insta tests | CLI-06 |
| **05-12** Workflow semantics e2e | 5 | 05-07, 05-09 | ostro-server tests/semantics_e2e.rs: gather/race with concurrent signals + restart + re-replay; 1,000 sleepers no memory; signal timeout; patched old/new path; run timeout + s2s; cancel cleanup; terminate; retry both modes; token complete/duplicate/expired; continue-as-new; TTL expiry incl. dedupe reuse | WF-03/04/05/08/09, API-05/06/07, OPS-06 |
| **05-13** Whole-server crash campaign harness | 5 | 05-07, 05-09 | ostro-crashtest server_target/workload/check, new failpoints (meta, executor, dispatcher, aot, ENOSPC args), `failpoints` feature on `ostro`, sharded runner, smoke loop in CI | QA-01 |
| **05-14** Performance harness + optimisations | 5 | 05-09 | ostro-bench (profiles A/B, on-disk verification), profile, pipelined grant, StartRun.run_started path, dispatcher record reuse; WSL numbers recorded | Success criterion 4 (perf) |
| **05-15** Order-saga quickstart + docs | 6 | 05-11, 05-12 | examples/order-saga e2e test via the CLI, quickstart.sh container timing, docs (signals/cancel/timeouts/patched/retention semantics), docs/03 updates | Success criterion 1 |
| **05-16** Phase gate + freeze flip | 7 | all | 10k-fault campaign run (sharded), final corpus entries (order-saga, crash samples) on x86_64 + aarch64 (remote legs human_needed), WIT freeze: hash lock enforce, local tag, append-only golden prefix test, format corpus committed, storage-format doc "FROZEN v0.1", perf numbers (native run human_needed), ci-local full, VALIDATION update | QA-01, QA-02, WIT-07, all gates |

Parallelism notes:
- Wave 2 plans touch disjoint files thanks to the 05-01 stubs: engine / apply-waits / apply-lifecycle / api / tooling. Waves 5–6 split server tests, CLI, crashtest and bench crates.
- 05-03 and 05-04 both extend `tests/rebuild_digest.rs` generators. Assign one generator file each, or sequence the generator edits: 05-04 after 05-03, as a small final task.
- Wasmtime 51 (D17) should be bumped either before 05-06/05-10 or after 05-16, never in between.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test harness (`cargo test`, Rust 1.100.0) + proptest 1.12 + insta 1.49 + trybuild (SDK); cargo-nextest available locally; spawned-binary tests via `CARGO_BIN_EXE_ostro`; reqwest/tonic clients; `fail` failpoints (single-threaded suites) |
| Config file | none beyond Cargo.toml; `scripts/ci-local.sh` (`FAILPOINT_CRATES` gains ostro-server/ostro-meta/ostro if they get the feature); guests workspace `guests/Cargo.toml`; SDK workspace `sdks/rust/Cargo.toml`; new `examples/order-saga/Cargo.toml` |
| Quick run command | `cargo test -p <touched crate> --locked` (e.g. `cargo test -p ostro-partition --test apply_waits --locked`) |
| Full suite command | `bash scripts/ci-local.sh --no-musl` |
| Phase gate commands | `bash scripts/ci-local.sh`; `OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored --nocapture`; `cargo test -p ostro-engine --test replay_corpus` (all CI legs); `cargo run --release -p ostro-bench -- --profile a` (native NVMe, human-run) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| WF-03 | Random interleavings: live vs replay `WaitResolved` identical; cancel-op loser completes Err(Cancelled) | proptest (replay core) | `cargo test -p ostro-engine --lib replay::tests::gather_race_prop` | ❌ Wave 0 |
| WF-03 | race-fuzz guest through server with concurrent signals + restart; re-replay byte-identical, 0 appended events | e2e (in-process server) | `cargo test -p ostro-server --test semantics_e2e gather_race` | ❌ Wave 0 |
| WF-04 | 1,000 sleepers → 0 live activations, 0 parked, budget 0, bounded RSS; all wake | e2e | `cargo test -p ostro-server --test semantics_e2e sleepers_hold_no_memory -- --test-threads=1` | ❌ Wave 0 |
| WF-05 | Signal before/after wait (FIFO), timeout → WaitTimedOut → Err(Timeout), signal after timeout goes to inbox, cancel-op on wait | unit (apply) + integration | `cargo test -p ostro-partition --test apply_waits` | ❌ Wave 0 |
| WF-05 | Signal via REST and gRPC wakes a sleeping run; dedupe by request_id | e2e | `cargo test -p ostro-server --test lifecycle_api signal` | ❌ Wave 0 |
| WF-08 | v1 journals replayed by v2 take the old path; new runs the new path; memo; v3 skips unqueried markers | integration (engine) | `cargo test -p ostro-engine --test patching` | ❌ Wave 0 |
| WF-08 | In-flight run continues the old path on v2 via module_override after restart | e2e | `cargo test -p ostro-server --test semantics_e2e patched_in_flight` | ❌ Wave 0 |
| WF-09 | RUN_TIMEOUT fires → TimedOut (sleeping and running runs); S2S fires only if never granted; rows removed on completion | unit (apply) + e2e | `cargo test -p ostro-partition --test apply_lifecycle timeouts && cargo test -p ostro-server --test semantics_e2e timeouts` | ❌ Wave 0 |
| API-05 | cancel (guest cleanup runs, status Cancelled), terminate (no guest code, activation revoked), retry resume vs restart, 409s, dry_run no-op | integration (apply) + e2e REST/gRPC parity | `cargo test -p ostro-partition --test apply_lifecycle && cargo test -p ostro-server --test lifecycle_api && cargo test -p ostro-api --test parity` | ❌ Wave 0 / ✅ parity (extend) |
| API-06 | Long-poll returns at completion (not after timeout), returns `completed:false` at timeout, no lost wakeup under race, follows continuations | e2e | `cargo test -p ostro-server --test lifecycle_api long_poll` | ❌ Wave 0 |
| API-07 | Token complete once; duplicate 409; bad MAC 400; expired (WaitTimedOut) 409; wrong partition routing impossible | unit (apply) + e2e | `cargo test -p ostro-partition --test apply_waits token && cargo test -p ostro-server --test lifecycle_api token` | ❌ Wave 0 |
| CLI-06 | Each verb against `ostro dev --ephemeral`; `--json` snapshots; `--dry-run` submits nothing (history unchanged); exit codes | e2e (spawned binary) + insta | `cargo test -p ostro --test cli_verbs` | ❌ Wave 0 |
| OPS-06 | EXPIRE_OP deletes every run-scoped row incl. dedupe; same idempotency key creates a new run after expiry; retry removes the expiry row; rebuild digest equal | unit (apply) + proptest + e2e | `cargo test -p ostro-partition --test apply_retention && cargo test -p ostro-partition --test rebuild_digest && cargo test -p ostro-server --test semantics_e2e retention` | ❌ Wave 0 / ✅ rebuild_digest (extend) |
| WIT-07 | Frozen file byte-identical; semver_check passes positive fixtures and rejects every negative fixture | unit (tooling) | `cargo test -p ostro-witcheck` (or `-p ostro-engine --test wit_freeze`) | ❌ Wave 0 |
| WIT-07 | Golden frozen prefix unchanged (commands, journal, routing) | unit | `cargo test -p ostro-types --test golden_frozen` | ❌ Wave 0 |
| QA-01 | Smoke: 20 server iterations (kill -9 + failpoints) green; checker negative controls catch a lost ack, a duplicate completion and a wrong result | integration (`#[ignore]` smoke enabled in the CI step) | `OSTRO_CAMPAIGN_FAULTS=20 cargo test --release -p ostro-crashtest --features failpoints --test server_campaign -- --ignored` | ❌ Wave 0 |
| QA-01 | ≥ 10,000 faults, 0 lost accepted runs, 0 duplicate journal entries, all terminal or resumed | gate (long-running) | see Phase gate commands | ❌ Wave 0 |
| QA-02 | Every corpus entry replays with 0 appended events and no divergence, on every CI leg | integration (default suite) | `cargo test -p ostro-engine --test replay_corpus` | ❌ Wave 0 |
| QA-02 | Format corpus opens, rebuilds and matches the recorded digest/LEO | integration | `cargo test -p ostro-partition --test format_corpus` | ❌ Wave 0 |
| SC-1 | order-saga: happy, compensated and cancelled paths via the CLI against spawned `ostro dev` | e2e | `cargo test -p ostro --test order_saga` | ❌ Wave 0 |
| SC-1 | Clean machine < 10 min | manual/containerised | `bash examples/order-saga/quickstart.sh` (docker; human_needed if unavailable) | ❌ Wave 0 |
| SC-4 perf | ≥ 5k steps/s (profile A) acknowledged and on disk; start→first step p50 < 5 ms | benchmark (human-run on native NVMe) | `cargo run --release -p ostro-bench -- --profile a --verify-on-disk` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test -p <crate> --locked` plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`. For apply changes, also `cargo test -p ostro-partition --features failpoints --locked -- --test-threads=1`.
- **Per wave merge:** `cargo test --workspace --locked` + SDK workspace tests + `cargo test -p ostro-engine --test replay_corpus` (from 05-10 on).
- **Phase gate:** full `bash scripts/ci-local.sh` green; 10k campaign green; corpus green locally (x86_64 gnu and musl). aarch64/macOS legs are human_needed until the remote exists. Native-NVMe perf numbers recorded (human_needed). All of this before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `crates/ostro-partition/tests/{apply_waits,apply_lifecycle,apply_retention,format_corpus}.rs` (05-03/05-04/05-16)
- [ ] `crates/ostro-engine/tests/{patching,replay_corpus}.rs` + `replay::tests::gather_race_prop` (05-02/05-10)
- [ ] `crates/ostro-server/tests/{lifecycle_api,semantics_e2e}.rs` (05-09/05-12)
- [ ] `crates/ostro/tests/{cli_verbs,order_saga}.rs` (05-11/05-15)
- [ ] `crates/ostro-types/tests/golden_frozen.rs` (05-06/05-16)
- [ ] WIT tooling crate or test + `tests/wit-fixtures/*` (05-06)
- [ ] `crates/ostro-crashtest/tests/server_campaign.rs` + `src/{server_target,workload,check}.rs` (05-13)
- [ ] `crates/ostro-bench` (05-14)
- [ ] Guests: race-fuzz, signals, timeouts, patch-v1/v2/v3, crash-saga; `examples/order-saga` (05-07)
- [ ] Dev-deps: `wit-component =0.261.0` (feature semver-check), `wit-parser =0.261.0`, transitive `wat`/`wast` (cargo-deny check); reqwest promoted to a normal dep in ostro-cli
- [ ] `failpoints` feature chain on `ostro` / `ostro-server` / `ostro-meta`; add the crates to `FAILPOINT_CRATES` and the ci.yml failpoint step

## Sources

### Primary (HIGH confidence)
- Repository at `ba9aba9`:
  - `wit/ostro-runtime-0.1.0/runtime.wit`;
  - `crates/ostro-types/src/{command,journal}.rs`;
  - `crates/ostro-engine/src/{replay/{cursor,wait,token}.rs, host/effects.rs, runtime.rs, limits.rs, activation.rs}`;
  - `crates/ostro-partition/src/{apply/{inbound,timer,mod}.rs, store/{mod,values,batch}.rs, handle.rs, leader/retention.rs}`;
  - `crates/ostro-log/src/rf1.rs`; `vendor/ostro-wal/src/group_commit.rs`;
  - `crates/ostro-crashtest`; `crates/ostro-meta/src/records.rs`; `crates/ostro-api/src/{domain,rest}.rs`; `sdks/rust/ostro-sdk/src/ctx.rs`;
  - `scripts/ci-local.sh`; `.github/workflows/ci.yml`; `Cargo.lock` (wit-component/wit-parser 0.261.0).
- Planning docs:
  - `.planning/{ROADMAP,REQUIREMENTS,PROJECT,STATE}.md`;
  - Phase 2 summaries 02-15 (crash harness results, reopen growth) and 02-14 (timers/retention);
  - Phase 3 03-RESEARCH (patched/time rules), 03-11 SUMMARY (step-local RNG);
  - Phase 4 04-RESEARCH (contract gaps, derive table, startup measurements) and 04-09, 04-12..04-20 PLANs.
- docs/02-prd.md (FR-1/FR-3/FR-6, NFR-2/3); docs/03-architecture.md §2, §5, §6, §7; docs/05-roadmap.md Phase 1 exit criteria.
- docs.rs `wit-component` 0.261.0 feature list (`semver-check` → `dummy-module` → `wat`): https://docs.rs/crate/wit-component/0.261.0/features

### Secondary (MEDIUM confidence)
- `wit_component::semver_check` doc and source (algorithm: dummy component for prev, validated against new world type; new may add imports and drop exports; same package name required; weak error messages): https://docs.rs/wit-component/latest/wit_component/fn.semver_check.html and https://docs.rs/crate/wit-component/latest/source/src/semver_check.rs
- Wasmtime component `Linker` semver-compatible lookup (compat key for nonzero major or minor; exact match first): https://docs.wasmtime.dev/api/wasmtime/component/struct.Linker.html; PR #7994 discussion: https://bytecodealliance.github.io/zulip-archive/stream/225357-git-wasmtime/topic/wasmtime.20.2F.20PR.20.237994.20Make.20.60component.3A.3ALinker.60.20semver-aware.html

### Tertiary (LOW confidence, flagged)
- Zulip report that `semver-check` fails when old and new worlds are different package IDs in one Resolve (2024; unclear whether fixed): https://bytecodealliance.github.io/zulip-archive/stream/223391-wasm/topic/using.20semver-check.html. Fixtures in 05-06 decide.
- Performance predictions (fsync counts per start path, bottleneck ranking): derived from code reading, not measured.
- Crash-campaign wall-clock estimates: extrapolated from 02-15 (≈13.6 s per iteration for the partition loop including rebuild; whole-server iterations differ).

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH. No new runtime deps except promoting reqwest. Dev-only wit-component/wit-parser at the lockfile version.
- Architecture: MEDIUM-HIGH. It extends proven Phase 2–4 patterns (input journaling, Derived intents, reserved timer ops), but the new designs need planner adoption and user decisions D1–D18.
- Pitfalls: HIGH for storage and fencing (grounded in code and prior phase findings). MEDIUM for WIT tooling and perf.
- Phase 4 assumptions: MEDIUM. They are based on unexecuted PLANs, so 05-01 must gate on the actual SUMMARYs and adjust tag numbers if 04-* used any.

**Research date:** 2026-11-18
**Valid until:** about 2026-12-18, or the Wasmtime 51 release (~2026-12-20), whichever comes first. Re-verify the `wit-component` version pairing at that bump.
