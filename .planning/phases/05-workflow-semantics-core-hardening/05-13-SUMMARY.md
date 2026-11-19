---
phase: 05-workflow-semantics-core-hardening
plan: 13
subsystem: api
tags: [control-plane, signals, cancel, terminate, retry, await-tokens, long-poll, dry-run, policies, retention, d1, d9, d21, d23, f9, f10, f11, w14, w15, api-05, api-06, api-07]

requires:
  - phase: 05-04
    provides: "Lifecycle API surface (REST + gRPC, domain types, error reasons, FakeControlPlane)"
  - phase: 05-08
    provides: "Apply SignalRun / RequestCancel / CompleteToken and their outcomes"
  - phase: 05-09
    provides: "Apply TerminateRun / RetryRun, RunRecord.resumable, continued_to"
  - phase: 05-11
    provides: "retention::resolve_run_policy, derive token/signal waits, resumable executor failures, CAN successor {\"next_run_id\"}"
  - phase: 05-22
    provides: "RunRecord.attempt_failures (F9) and status_seq (F10)"
  - phase: 05-24
    provides: "D23 names (request_tenant, qualify_request_name, split_qualified), qualified start path"
  - phase: 05-03
    provides: "ostro_engine::token::{parse, verify, mint}"
provides:
  - "ServerControlPlane: signal_run, cancel_run, terminate_run, retry_run, await_result, complete_token, set/get_workflow_policy, set/get_queue_policy (all ten 05-04 methods)"
  - "dry_run on every lifecycle call and on start (W15), with the real validation and no effect"
  - "Start fields: StartRunRequest.schedule_to_start_timeout_ms/retention_ms/dry_run (proto 13..15), StartRunResponse.dry_run (3), REST retention / schedule_to_start_timeout / dry_run"
  - "D9 resolution at start (request > WorkflowPolicy > QueuePolicy > [runs]) into StartRun.retention_ms / schedule_to_start_ms"
  - "GET run: attempt_failures, retention_ms, schedule_to_start_ms (proto RunInfo 18..20); published status read O(1) at status_seq"
  - "control/watch.rs RunWatchers long-poll hub + await_result (follow_continuations)"
  - "control/policy.rs workflow/queue policies in meta (CAS, D23 keys)"
  - "tests/lifecycle_api.rs (13 scenarios over REST and gRPC)"
affects: [05-14, 05-15, 05-16, 05-17, 05-20, phase-7, phase-12, phase-14]

tech-stack:
  added: []
  patterns:
    - "Lifecycle op = pre-read for clean errors + one partition command; the apply outcome stays authoritative for races"
    - "Long-poll: register the waiter, then read; waiters only (no state), woken by Action::RunCompleted"
    - "Transport-parameterised server tests: one Api type, identical normalised outputs for REST and gRPC"

key-files:
  created:
    - crates/ostro-server/src/control/lifecycle.rs
    - crates/ostro-server/src/control/watch.rs
    - crates/ostro-server/src/control/policy.rs
    - crates/ostro-server/tests/lifecycle_api.rs
  modified:
    - crates/ostro-proto/proto/ostro/api/v1/runs.proto
    - crates/ostro-proto/src/lib.rs (outside files_modified, see Deviations)
    - crates/ostro-api/src/domain.rs
    - crates/ostro-api/src/rest.rs
    - crates/ostro-api/src/grpc.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/tests/common/mod.rs
    - crates/ostro-server/tests/start_run_api.rs

key-decisions:
  - "A dry-run start that would create a run returns run_id \"\" (not null) with created=true, dry_run=true, status 200: keeps StartRunResponse.run_id a required string (OpenAPI strictly additive). An existing idempotency key returns its run id (created=false); reject policy is 409 with existing_run_id"
  - "REST StartRunResponse.dry_run is omitted unless true, so every pre-05-13 start response is byte-identical"
  - "GET run exposes retention_ms / schedule_to_start_ms as null when the record holds 0 (keep forever / none)"
  - "Policy routes take one name, bare or tenant-qualified (acme/w, %2F over REST); the tenant comes from a valid prefix, then request_tenant + qualify_request_name give the meta key. Workflow policies require the workflow to exist (404, same as the fake); queues are implicit. PUT replaces retention/s2s and preserves the Phase 7 F2 fields and ext of an existing record"
  - "Retry: Completed and ContinuedAsNew -> 409 run_completed (fake choice kept); resumable -> RetryRun; anything else terminal -> restart as a new run on a fresh partition with no idempotency key and no parent, copying the stored workflow/queue, version, module, input ref, priority, concurrency key, run timeout, retention, s2s, tenant, tags, traceparent; retry_of = old; dry run returns the original id"
  - "Token on a terminal run -> 409 run_terminal (apply checks terminal first); the fake now checks terminal before duplicates. Duplicate / expired -> 409 token_already_completed with details.status. A MAC-valid token whose op has no token wait (BadIntent) -> 400 token_invalid. Partition mismatch -> 400 token_invalid. MAC-valid token for a missing run -> 404"
  - "await_result failure shape: the stored failure JSON ({reason_code, kind, message}) parsed as JSON (text falls back to a JSON string, as in the fake); a Terminated/TimedOut run without a payload gets {\"reason_code\": terminal_reason}. A ContinuedAsNew run's {\"next_run_id\"} is a result (also on GET run, which used to show it as failure text); the fake's continue_as_new now sets it too"
  - "follow_continuations polls (10 ms) for a successor that does not exist yet: it is created by the predecessor's outbox StartRun, after continued_to is set. At the deadline the predecessor is answered"
  - "RunWatchers subscribes once per partition at control-plane construction: on RF=1 the apply machine (and its broadcast sender) lives as long as the node, so no re-subscribe is needed; Phase 12 must re-subscribe on leader change. Lagged and Closed wake the whole partition"
  - "The F10 read probe is a per-control-plane counter (ServerControlPlane::status_rows_read, doc(hidden)); a process-wide static raced between parallel tests"

requirements-completed: [API-05, API-06, API-07, WF-05]

duration: ~3h
completed: 2026-11-19
---

# Phase 5 Plan 13: Server ControlPlane Summary

**Signal, cancel, terminate, retry (resume vs restart), long-poll and await-token completion now work against the real server over REST and gRPC, each with a `dry_run` that validates and changes nothing. Starts carry retention and schedule-to-start resolved through workflow and queue policies, which are settable over both transports and persist in meta. GET run reads the published status in one row and shows the durable retry count.**

## Performance

- **Duration:** about 3 h (one full gate run included).
- **Tasks:** 3 of 3.
- **Files:** 4 created, 13 modified (one outside files_modified).
- **Commits:** 2 code commits plus this docs commit.

## Task Commits

1. **Task 1, start fields + policies:** `c2c12f7` (feat). Proto, domain, REST, gRPC, fake, parity, OpenAPI snapshot, `runs.rs`, `policy.rs`, `mod.rs`, `start_run_api.rs`, `ostro-proto/src/lib.rs`.
2. **Tasks 2 + 3, lifecycle + long-poll:** `27c2fb1` (feat). `lifecycle.rs`, `watch.rs`, `mod.rs`, `runs.rs`, `server.rs`, `lifecycle_api.rs`, `common/mod.rs`, `fake.rs`. Tasks 2 and 3 share `control/mod.rs` and `lifecycle_api.rs` (the long-poll scenarios use the lifecycle harness), so they went into one commit; see Deviations.

Both were committed from the main checkout through a private index (`GIT_INDEX_FILE` seeded with `read-tree HEAD`, blobs hashed from the scratch worktree), then the shared index was reset for exactly those paths and `git show --stat HEAD` checked.

## What landed

### Task 1: start fields, policies, GET additions

- **Proto (additive):** `StartRunRequest` 13 `optional uint64 schedule_to_start_timeout_ms`, 14 `optional uint64 retention_ms`, 15 `bool dry_run`; `StartRunResponse` 3 `bool dry_run`; `RunInfo` 18 `uint32 attempt_failures`, 19 `optional uint64 retention_ms`, 20 `optional uint64 schedule_to_start_ms`.
- **Domain:** `StartRunRequest` gains the three fields and derives `Default`; `StartRunResult.dry_run`; `RunInfo.{attempt_failures, retention_ms, schedule_to_start_ms}`.
- **REST:** `StartRunBody.{schedule_to_start_timeout, retention}` (friendly durations, jiff, `168h` ok, `7d` = 400 with `details.field`) and `dry_run`. A dry run always answers 200. `RunDto` gains the three GET fields.
- **Server start (`runs.rs`):** after the D23 block, `resolve_run_policy(meta, [runs], qualified workflow, qualified queue, request)` fills `StartRun.retention_ms` / `schedule_to_start_ms` (the 05-11 open item "a plain start submits retention 0" is closed). Dry run: names, version, input JSON, options, policy, partition availability, then the dedupe row read; no blob write, no command.
- **GET run (F9/F10):** the published status is the single row at `RunRecord.status_seq`; the backward journal scan is gone.
- **`policy.rs`:** see key-decisions.

### Task 2: lifecycle (`lifecycle.rs`)

The outcome table is at the top of the module. Signals and tokens encode the full D21 journal event (`SignalPayload` / `TokenCompleted`, ts = server clock, no seq) and pass it through `prepare_payload`, so 1 MiB payloads become blob refs. `*Request::validate()` runs first. Errors use the 05-04 helpers; no message echoes a token.

### Task 3: long-poll (`watch.rs`)

`RunWatchers` (waiters only) plus `await_result`: register, read, wait `min(wait, 60 s)`, re-read; always 200/OK for a known run; continuation following within the same deadline.

## Struct-literal sites fixed (W14)

| File | Site | Fix |
|---|---|---|
| `crates/ostro-api/src/fake.rs` | test helper `start()` | `..Default::default()` |
| `crates/ostro-api/src/fake.rs` | `RunInfo` in `start_run` | new fields (resolved policy) |
| `crates/ostro-api/src/grpc.rs` | `From<RunInfo>`, `TryFrom<StartRunRequest>`, `start` | new fields mapped |
| `crates/ostro-api/src/rest.rs` | `start_run` handler, `From<RunInfo> for RunDto` | new fields mapped |
| `crates/ostro-api/tests/parity.rs` | gRPC `start` literal | new fields from the scenario |
| `crates/ostro-proto/src/lib.rs` | `start_run_request_all_fields_roundtrip` | new fields set (Deviation 1) |
| `crates/ostro-server/src/control/runs.rs` | `StartRunResult` literals in `map_outcome` and its unit test | `dry_run: false` |

`e2e_dev.rs`, `start_run_api.rs`, `tenancy_api.rs`, `sdk_e2e.rs` and `runs_api.rs` already used `..Default::default()` on proto literals and needed no change.

## OpenAPI snapshot diff (deliberate re-bless)

`git diff --diff-algorithm=patience`: **46 insertions, 0 deletions**. Added properties only: `RunDto.{attempt_failures, retention_ms, schedule_to_start_ms}`, `StartRunBody.{dry_run, retention, schedule_to_start_timeout}`, `StartRunResponse.dry_run`. No description, path or required list changed.

## Verification

- `cargo test -p ostro-api --locked`: lib 31, http_smoke 1, lifecycle_surface 8, openapi 2, parity 3 (new steps: four dry-run starts, two policy-resolved starts with GET), rest 11.
- `cargo test -p ostro-server --test start_run_api`: 10 (4 new: policies over both transports + restart persistence, dry-run start leaves run list/blobs/dedupe untouched, tenant run completes with a tenant-qualified policy over `%2F`, GET reads one status row and a 10,000-event run reads none in < 1 s).
- `cargo test -p ostro-server --test lifecycle_api`: **13 passed** (run twice in a row, ~4 s each):
  - `signal_rest_and_grpc`: wake, dry run unchanged, 409 run_terminal (+status), 404, 257-byte name 400, request_id dedupe = one SignalPayload + one SignalMatched, FIFO result.
  - `signal_1mib_payload_is_blob_offloaded`: result equals the 1 MiB payload; the journaled row is a `Blob` ref.
  - `cancel_rest_and_grpc`: waiting run ends Cancelled, repeat 409, 404, sdk-cancel shielded cleanup completes `"cleaned"`.
  - `cancel_before_start_is_flag_only_and_idempotent` (`executor.slots = 0`): dry run unchanged, first false, repeat `already_requested`, `next_seq == 0`.
  - `terminate_rest_and_grpc`: sleeping run Terminated with reason, history ends `RunTerminated`, repeat 409; a busy `busy_steps` activation stops (`live_activations() == 0`).
  - `retry_resume_restart_and_completed_rest_and_grpc`: Completed 409 run_completed, live 409 run_not_terminal, 404; Terminated → resumed same id → completes `"slept"` with `resumed_count == 1`; guest failure → restarted new id with `retry_of`, tenant `acme`, tags, traceparent, qualified workflow/queue, same version/module/retention/input (F11).
  - `token_completion_rest_and_grpc`: dry run unchanged; tampered / garbage / non-token text 400 token_invalid (never echoed); partition-mismatched but signed token 400; signed token for a missing run 404; two concurrent 1 MiB completions: one wins, the other 409; result equals the payload; one `TokenCompleted`; expired token 409.
  - `token_text_never_reaches_the_logs`: TRACE-level capture of the whole process; neither the token, its body nor the tampered token appears.
  - `long_poll_returns_on_completion_not_at_the_timeout`: 300 ms sleep answered in 250 ms..2 s with a 30 s wait.
  - `long_poll_times_out_with_completed_false`: 200 ms wait → `completed: false` in 190..1500 ms; wait 0 immediate; 404 details identical on both transports; failed run's failure JSON.
  - `long_poll_no_lost_wakeup_when_completion_races_registration`: 200 start-then-poll iterations (10 lanes, alternating transports), none near the 10 s timeout, no leaked waiter.
  - `long_poll_follows_continuations_with_transport_parity`: can-chain max 3, follow → result 3 from the last successor; no follow → `continued_as_new` with `{"next_run_id"}`; REST == gRPC.
  - `long_poll_waiter_is_dropped_with_its_request`: an aborted in-process poll and a disconnected HTTP client both leave 0 waiters; a terminate wakes three concurrent polls.
- **Negative control:** with the `RunCompleted` wake disabled in `watch.rs`, `long_poll_returns_on_completion_not_at_the_timeout` fails after 30.0 s (restored afterwards).
- Unit tests: `policy::tests::policy_keys_follow_d23`, `lifecycle::tests::{retry_mode_follows_d1, token_done_carries_status}`, `watch::tests::{completion_wakes_and_drop_unregisters, lagged_wakes_the_whole_partition}`.
- `cargo test -p ostro-server -p ostro-api -p ostro-proto --locked --no-fail-fast`: all binaries green (incl. tenancy_api, sdk_e2e, runs_api, modules_api, bootstrap).
- `cargo clippy --workspace --all-targets --locked -- -D warnings` clean; `cargo fmt --all --check` clean; workspace build warning-free.

**Real-server parity (05-04 hand-off).** The 05-04 parity scenario drives fake-only helpers (`set_run_outcome`, `mint_token`, `continue_as_new`), so it cannot run unchanged against the server. `lifecycle_api.rs` is its real-server counterpart: every scenario runs through one transport-neutral `Api` type over REST and then gRPC with the same assertions, and two tests compare the normalised REST and gRPC answers directly. This is the proof used to mark API-05/06/07 complete.

## Gate

`bash scripts/ci-local.sh --no-musl` on committed HEAD `27c2fb1`, run in a detached scratch worktree (`CARGO_TARGET_DIR=target/wt-05-13`): **all steps passed on the first run, exit 0.** toolchain, fmt, clippy `-D warnings`, warning-free build, SDK workspace fmt/clippy/test/wasm, guests build, order-saga example, `cargo test` (482 s), failpoint suites (563 s), cargo deny, check-vendored, check-deny-fixtures, forbidden crates, workflow validation; musl steps skipped (`--no-musl`). No flake occurred, so no reruns were needed.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] `crates/ostro-proto/src/lib.rs` edited (not in files_modified)**
- **Found during:** Task 1 (`cargo build --workspace --all-targets`).
- **Issue:** the `start_run_request_all_fields_roundtrip` unit test builds `StartRunRequest` with every field and no `..Default::default()`, so the three new proto fields break it. No running plan owns the file.
- **Fix:** the literal sets the new fields (`schedule_to_start_timeout_ms: Some(0)`, `retention_ms`, `dry_run`) and asserts that `Some(0)` presence survives the round trip. No other hunk. **Commit:** `c2c12f7`.

**2. [Rule 1 - Bug] GET run showed a continued-as-new run's `{"next_run_id"}` as failure text**
- `get_run` treated every non-Completed result payload as a failure. ContinuedAsNew is now a result on GET and in `await_result`. The fake's `continue_as_new` sets the same payload so the parity transcript stays truthful. **Commit:** `27c2fb1`.

**3. [Rule 1 - Bug] Continuation following could 404**
- The successor of a continue-as-new run is created by an outbox `StartRun` after `continued_to` is committed, so reading it right away can miss. `await_result` polls for it within the deadline (found by `long_poll_follows_continuations_with_transport_parity`). **Commit:** `27c2fb1`.

**4. Tasks 2 and 3 in one commit.** They share `control/mod.rs` (`ControlPlane` impl, `watchers` field) and `lifecycle_api.rs`; splitting would have meant committing an intermediate tree that was never built or tested.

**5. Dry-run start `run_id` is `""`, not `null`** (see key-decisions): keeps the OpenAPI change strictly additive. 05-14 should print "would create a new run" when `dry_run && created`.

**6. Busy-run terminate history.** The plan expects history to end with `run_terminated`. A `busy_steps` run journals nothing until its first flush, and D3 journals `RunTerminated` only when `next_seq > 0`, so a busy run terminated mid-activation can have an empty history. The test accepts empty or `RunTerminated`-last; the sleeping-run case asserts `RunTerminated` last.

**7. Token duplicate test is race-tolerant.** A deterministic `Duplicate` would need the run to stay live after its token completes. The test fires two concurrent completions and accepts 409 `token_already_completed` (with `details.status`) or 409 `run_terminal`; the details shape is pinned by a unit test.

### Plan items resolved differently

- **The F10 "10k-event run, single event read" check** uses a per-control-plane counter of status rows read: exactly 1 for a run that published a status, 0 for a 10,000-event `trivial-steps` run (no status). The read count no longer depends on journal length.
- **Leadership re-subscribe** is not needed on RF=1 (documented in `watch.rs`).

## Known limitations

- Two concurrent retries of the same guest-failed run start two linked runs (no dedupe on `retry_of`).
- `PUT /v1/workflows/{name}/active` still cannot address a tenant workflow except via `%2F` (05-24 limitation, unchanged).
- OPS-06 and WF-09 are not marked complete: the API half is done and tested here, but run expiry after its resolved retention and schedule-to-start firing are not proven end to end through the server (05-15).

## Hand-off notes

### 05-14 (CLI)
- REST bodies: start gains `retention` / `schedule_to_start_timeout` (friendly, hours max unit) and `dry_run`. Dry-run start answers 200 `{run_id: "" | existing, created, dry_run: true}`; `dry_run` is absent from normal start responses.
- Lifecycle: `POST /v1/runs/{id}/signals/{name}` `{payload, request_id, dry_run}`, `/cancel` `{reason, dry_run}`, `/terminate`, `/retry` `{dry_run}` (empty bodies ok), `GET /v1/runs/{id}/result?wait=30s&follow_continuations=true` (always 200; `completed` false at the timeout), `POST /v1/tokens/complete` `{token, payload, dry_run}`.
- Error reasons for messages: `run_terminal` (+`status`), `run_completed`, `run_not_terminal` (+`status`), `token_already_completed` (+`run_id`, `status`), `token_invalid`.
- Policies: `PUT/GET /v1/workflows/{name}/policy`, `/v1/queues/{name}/policy`; tenant names as `acme%2Fw`. Unknown workflow 404.
- GET run now has `attempt_failures` (show this as "retries"), `retention_ms`, `schedule_to_start_ms`.

### 05-15 (e2e)
- `lifecycle_api.rs` already covers 1 MiB signal and token payloads, cancel before start, resume after terminate, restart with F11, long-poll race. Still open for 05-15: expiry after the resolved retention (OPS-06), schedule-to-start firing through the API (`schedule_to_start_timeout` + `executor.slots = 0`, WF-09), signal right after start (B1) over the API, resume after TimedOut over the API.
- Harness tips: `executor.slots = 0` makes pre-start states deterministic; set `engine.hot_wait` low (50 ms) when a test needs a short sleep to evict (status `Scheduled`); `TestServer::{run_record, wait_record, wait_status, history_kinds, published_token}` are in `tests/common/mod.rs`; `ServerControlPlane::watchers().waiters()` and `status_rows_read()` are test probes.

### 05-16 (perf / start path)
- The API start path now does one meta read for policy resolution (`resolve_run_policy`, in-memory view) and nothing else new; dry run adds one dedupe point read only when asked.
- If 05-16 writes `RunStarted` at `StartRun`, `lifecycle::restart` builds its `StartRun` by hand (retry_of path): keep it in step with the API start (same fields, `run_started` included).
- `await_result` reads the record plus the result payload per wake; the hub costs one broadcast receiver task per partition.

### 05-17 (docs)
- Dry-run semantics per call, the retry mode table (D1), `{"next_run_id"}` as the continue-as-new result, token errors, policy resolution order, long-poll 60 s cap.

### Phase 12
- `RunWatchers` must re-subscribe to the new leader's action broadcast on leadership change (`watch_partition` per partition).

## STATE notes (for the orchestrator; STATE.md not edited, no gsd-tools mutators run)

- 05-13 complete (3/3 tasks), commits `c2c12f7`, `27c2fb1`.
- Decisions: dry-run start `run_id ""`; ContinuedAsNew payload is a result; token on a terminal run → run_terminal (fake aligned); policy routes accept qualified names, workflow must exist, Phase 7 fields preserved; RunWatchers subscribe once on RF=1.
- Requirements: API-05, API-06, API-07, WF-05 complete. OPS-06 and WF-09 stay Pending (end-to-end expiry / s2s via 05-15).

## Self-Check: PASSED

- FOUND: control/lifecycle.rs, control/watch.rs, control/policy.rs, tests/lifecycle_api.rs (in HEAD)
- FOUND: commits c2c12f7, 27c2fb1
- FOUND key-link patterns: `RunWatchers`, `subscribe_actions` (watch.rs/mod.rs), `token::verify` (lifecycle.rs), `resolve_run_policy` (runs.rs)
