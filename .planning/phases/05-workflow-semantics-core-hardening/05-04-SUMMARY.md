---
phase: 05-workflow-semantics-core-hardening
plan: 04
subsystem: api
tags: [rest, grpc, tonic, utoipa, parity, signals, cancel, terminate, retry, long-poll, await-tokens, policies, api-05, api-06, api-07]

requires:
  - phase: 05-01
    provides: "Run status parity (Terminated/TimedOut/ContinuedAsNew) across domain/REST/proto; OpenAPI snapshot baseline"
  - phase: 04-11
    provides: "One-port REST+gRPC router, to_status/status_details, parity suite harness"
  - phase: 04-08
    provides: "ControlPlane trait, ApiError tables, FakeControlPlane, utoipa routes"
provides:
  - "ostro.api.v1 Runs.{Signal,Cancel,Terminate,Retry,AwaitResult}, Workflows.{SetPolicy,GetPolicy,SetQueuePolicy,GetQueuePolicy}, new service ostro.api.v1.Tokens.Complete"
  - "Domain types SignalRequest/Response, CancelRequest/Response, TerminateRequest/Response, RetryRequest/Mode/Response, AwaitResultRequest/Response, CompleteTokenRequest/Response, RunPolicy (+ validate() helpers and limits)"
  - "Ten ControlPlane methods with default bodies returning ApiError::unimplemented (ostro-server compiles unchanged)"
  - "ErrorCode::Unimplemented (HTTP 501 / gRPC UNIMPLEMENTED) and details.reason refinements: run_terminal, run_completed, run_not_terminal, token_already_completed, token_invalid"
  - "REST routes for every call, rest::parse_friendly_ms / rest::friendly_ms (jiff friendly, same parser as ServerConfig)"
  - "FakeControlPlane lifecycle semantics + test helpers (mint_token, set_resumable, continue_as_new, clock_ticks, signals, cancel_requested, token_completed)"
  - "tests/lifecycle_surface.rs and 60 new REST-vs-gRPC parity steps"
affects: [05-12, 05-13, 05-14, 05-15, 05-24]

tech-stack:
  added: []
  patterns:
    - "Several conditions sharing one transport status are told apart by details.reason (HTTP/gRPC tables stay bijective); both transports carry the same details JSON"
    - "New ControlPlane methods ship with default Unimplemented bodies so adapters land before the server implementation"
    - "dry_run proof in tests: every fake mutation advances the fake clock; clock_ticks() unchanged = nothing mutated"
    - "Lifecycle POSTs with all-optional fields accept an empty body (optional_json_body)"

key-files:
  created:
    - crates/ostro-proto/proto/ostro/api/v1/tokens.proto
    - crates/ostro-api/tests/lifecycle_surface.rs
  modified:
    - crates/ostro-proto/proto/ostro/api/v1/runs.proto
    - crates/ostro-proto/proto/ostro/api/v1/workflows.proto
    - crates/ostro-proto/build.rs
    - crates/ostro-proto/src/lib.rs
    - crates/ostro-api/src/domain.rs
    - crates/ostro-api/src/error.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/src/grpc.rs
    - crates/ostro-api/src/rest.rs
    - crates/ostro-api/src/lib.rs
    - crates/ostro-api/tests/openapi.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap

key-decisions:
  - "Specific 409/400 conditions are details.reason values on the existing Conflict/InvalidArgument codes, not new ErrorCodes: the HTTP and gRPC inverse tables must stay bijective (several conditions share 409)"
  - "unimplemented maps to a new ErrorCode::Unimplemented = HTTP 501 / gRPC UNIMPLEMENTED (bijective, so 501 rather than the plan's 503 fallback)"
  - "Retry of a live (non-terminal) run is 409 reason run_not_terminal; retry of ContinuedAsNew is 409 run_completed, like Completed (05-13 may revisit)"
  - "Signal/cancel/terminate/token on a terminal run is 409 run_terminal with details.status; the fake checks terminal before request_id dedupe"
  - "Retry dry_run returns the original run id even for mode restarted (no id is allocated)"
  - "Await-result result/failure are omitted from REST JSON when absent (skip_serializing_if) so a completed run with a JSON null result is distinguishable; gRPC uses optional bytes"
  - "AwaitResult wait is capped at 60 s in both adapters (domain::effective_wait_ms); longer waits are capped, not rejected; invalid REST wait = 400"
  - "PUT policy replaces the whole policy (omitted fields are cleared); unknown workflow = 404; queues are implicit (any non-empty name)"
  - "Fake await tokens are ot1_<run_id>.<op> and exist only after mint_token; the real server verifies the MAC (05-13)"
  - "CompleteTokenRequest/CompleteTokenBody Debug impls redact the token; error messages never echo it"
  - "API-05/06/07 left Pending in REQUIREMENTS.md: proven against the fake only, as with API-01 in 04-11; 05-13 proves them against the real server"

patterns-established:
  - "Parity normalisation for optional payloads: has_result/has_failure flags plus the value"

requirements-completed: []

duration: ~75min
completed: 2026-11-19
---

# Phase 5 Plan 04: Lifecycle API surface (REST + gRPC) Summary

**Signal, cancel, terminate, retry, long-poll result, await-token completion and workflow/queue run policies now exist on both transports on the one-port router. They are backed by new `ControlPlane` methods whose default bodies answer 501 Unimplemented, so ostro-server compiles unchanged. `FakeControlPlane` implements D1/D3/D13 semantics in memory, and a REST-vs-gRPC parity transcript shows identical results, including error codes and details.**

## Performance

- **Duration:** about 75 min, including one full `ci-local --no-musl` run.
- **Completed:** 2026-11-19.
- **Tasks:** 2/2.
- **Files:** 15 (2 created, 13 modified) over 2 code commits.

## Accomplishments

- **Protos (additive only).**
  - `runs.proto` gains `Signal`, `Cancel`, `Terminate`, `Retry` and `AwaitResult`, with messages `SignalRunRequest/Response`, `CancelRunRequest/Response`, `TerminateRunRequest/Response`, `RetryRunRequest/Response`, `AwaitResultRequest/Response` and an enum `RetryMode {UNSPECIFIED, RESUMED, RESTARTED}`. JSON travels as `payload_json`, `result_json` and `failure_json` bytes, where empty = null. `request_id` and `reason` are proto3 optional.
  - The new `tokens.proto` defines `service Tokens { rpc Complete }`, compiled in the same protox call.
  - `workflows.proto` gains the four policy RPCs and `RunPolicy {optional uint64 retention_ms = 1; optional uint64 schedule_to_start_ms = 2}`.
  - **`StartRunRequest` is untouched** (05-13 owns it). No existing field was renumbered.
- **Domain.** All types match the plan's `<interfaces>` block, plus:
  - `validate()` on Signal/Cancel/Terminate requests. The limits mirror `ostro_types::command`: name 1..=256, request_id ≤ 64, reason ≤ 1024. A failure is InvalidArgument with `details.field`.
  - `TOKEN_PREFIX = "ot1_"`, `MAX_RESULT_WAIT_MS = 60_000` and `effective_wait_ms`.
- **Errors.**
  - New `ErrorCode::Unimplemented` (HTTP 501 / gRPC UNIMPLEMENTED, wire name `unimplemented`).
  - New `ApiError::{unimplemented, run_terminal, run_completed, run_not_terminal, token_already_completed, token_invalid, reason()}` and an `error::reason` constants module.
- **gRPC.**
  - `GrpcRuns` and `GrpcWorkflows` gain the new methods, and the new `GrpcTokens` serves `Tokens`.
  - `router()` mounts `TokensServer` with `max_decoding_message_size(cfg.max_request_bytes)`. Over-limit messages answer RESOURCE_EXHAUSTED through the existing 04-11 rewrite layer.
- **REST.** New routes:

  | Method and path | Body / query | Success |
  |---|---|---|
  | `POST /v1/runs/{run_id}/signals/{name}` | `{payload?, request_id?, dry_run?}` | 200 `{accepted, duplicate, dry_run}` |
  | `POST /v1/runs/{run_id}/cancel` | `{reason?, dry_run?}` (body optional) | 200 `{already_requested, dry_run}` |
  | `POST /v1/runs/{run_id}/terminate` | `{reason?, dry_run?}` (body optional) | 200 `{status, dry_run}` |
  | `POST /v1/runs/{run_id}/retry` | `{dry_run?}` (body optional) | 200 `{mode, run_id, dry_run}` |
  | `GET /v1/runs/{run_id}/result` | `?wait=30s&follow_continuations=true` | always 200 `{run_id, status, completed, result?, failure?}` |
  | `POST /v1/tokens/complete` | `{token, payload?, dry_run?}` | 200 `{run_id, accepted, dry_run}` |
  | `PUT`/`GET /v1/workflows/{name}/policy` and `/v1/queues/{name}/policy` | `{retention?: "168h", schedule_to_start_timeout?: "30s"}` | 200 `{retention, retention_ms, schedule_to_start_timeout, schedule_to_start_ms}` |

  - Every new body has `deny_unknown_fields` and the router-wide `max_request_bytes` limit (413 when exceeded).
  - The token only ever appears in a request body. The OpenAPI test asserts that no path or query parameter is called `token`.
- **Fake semantics.**
  - Signals: the run must exist and must not be terminal (else `run_terminal`). A repeated `request_id` gives `duplicate: true` and is not re-delivered.
  - Cancel: idempotent; a repeat answers `already_requested`.
  - Terminate: sets Terminated with completed_at.
  - Retry (D1):
    - non-terminal → `run_not_terminal`;
    - Completed or ContinuedAsNew → `run_completed`;
    - Terminated or TimedOut (or a run set by `set_resumable(true)`) → Resumed, same id, back to Ready;
    - anything else → Restarted as a new run id that copies the workflow, queue, tags and tenant.
  - await_result: subscribes to a `tokio::sync::Notify` **before** reading, so no wake-up is lost between the read and the wait. It waits up to the capped wait_ms and follows continue-as-new chains when asked.
  - Tokens: a garbage or unminted token → `token_invalid`; a second completion → `token_already_completed`.
  - Policies: stored in maps; PUT replaces the whole policy.
  - `dry_run` never mutates anything.

## Task Commits

1. **Task 1: protos, domain, ControlPlane defaults, fake, gRPC adapters**: `e37c76e` (feat)
2. **Task 2: REST routes, Tokens mount, OpenAPI, lifecycle_surface + parity**: `ea33859` (feat)

Both were committed through a private index (`GIT_INDEX_FILE` seeded from HEAD), and `git show --stat HEAD` was checked after each.

## OpenAPI snapshot diff (deliberate re-bless)

`git diff --diff-algorithm=patience` on the snapshot: **937 insertions, 1 deletion**. The default diff algorithm reports 45 deletions only because it misaligns repeated JSON braces.

- **Added paths (8):** `/v1/runs/{run_id}/signals/{name}` (post), `/cancel` (post), `/terminate` (post), `/retry` (post), `/result` (get), `/v1/tokens/complete` (post), `/v1/workflows/{name}/policy` (put, get) and `/v1/queues/{name}/policy` (put, get).
- **Added schemas:** SignalBody, SignalResponseDto, StopRunBody, CancelResponseDto, TerminateResponseDto, RetryBody, RetryModeDto, RetryResponseDto, RunResultDto, CompleteTokenBody, CompleteTokenResponseDto, RunPolicyBody and RunPolicyDto.
- **Added tags:** `tokens` and `queues`.
- **The one changed line** is the doc comment on `ErrorDetail.code`, which now lists `unimplemented`. I left it in so the description stays accurate. Two other description edits (the API info text and the `runs` tag) were reverted to keep the diff additive.

## Verification

- `cargo test -p ostro-api -p ostro-proto --locked`:
  - ostro-api: 31 lib, 1 http_smoke, 8 lifecycle_surface, 2 openapi, 3 parity and 11 rest tests;
  - ostro-proto: 6 lib tests;
  - all green.
- `cargo clippy -p ostro-api -p ostro-proto --all-targets --locked -- -D warnings`: clean. `cargo fmt --check`: clean.
- `cargo build --workspace --all-targets --locked`, run in a detached scratch worktree at the Task 1 tree: warning-free. ostro-server needed no change.
- Parity negative control: with gRPC `Signal` hard-coded to `duplicate: false`, `rest_and_grpc_transcripts_are_equal` fails at step "signal r1 duplicate". The edit was then reverted.
- `bash scripts/ci-local.sh --no-musl` on committed HEAD `ea33859`, run in the detached worktree (`CARGO_TARGET_DIR=target/wt-05-04`): 
  - **PASS:** toolchain, fmt, clippy -D warnings, warning-free build, SDK workspace fmt/clippy/test/wasm, guests build, cargo deny, check-vendored, check-deny-fixtures, the chronik/tokio-uring/prometheus check and workflow validation. musl was skipped (`--no-musl`).
  - **cargo test:** failed on the first pass at `ostro::dev_startup::warm_restart_under_1s` (`/readyz not 200 within 30s`). The machine was at load average 14-17 from parallel wave-2 executors. `cargo test --workspace` stops at the first failing binary, so I reran it with `--no-fail-fast`. 170 test binaries passed. Only two spawned-binary timing tests failed: `warm_restart_under_1s` (warm restart 1.93 s against a 1 s budget) and `ostro::server_config::live_start_with_toml` (`/readyz` not up within 30 s). **Both pass rerun alone**: 11.5 s and 2.6 s.
  - **Failpoint suites:** failed on the first pass at `ostro-wal` `group_commit::queue_creation_tests::the_first_write_to_every_new_partition_commits` (a 20 s commit timeout under load, in vendored code). **It passes rerun alone** (15 s). The full failpoint loop was then rerun with `--no-fail-fast` (clippy + tests for ostro-wal, ostro-blob, ostro-log, ostro-partition and ostro-crashtest): **all exit 0**.
  - **Flaky under load (report, not fixed; outside this plan's files):** `dev_startup::warm_restart_under_1s`, `server_config::live_start_with_toml`, and ostro-wal `the_first_write_to_every_new_partition_commits`. None touches ostro-api or ostro-proto.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] The gRPC adapters landed in the Task 1 commit**
- **Found during:** Task 1
- **Issue:** Adding RPCs to the existing `Runs` and `Workflows` services changes the generated tonic traits, so `GrpcRuns`/`GrpcWorkflows` stop compiling until the new methods exist. Task 1's verify step (`cargo build --workspace`) could not pass without them.
- **Fix:** `grpc.rs` (the new methods and `GrpcTokens`) was committed with Task 1. Task 2 kept REST, the router mount, OpenAPI and the tests.
- **Commit:** e37c76e

**2. [Rule 2 - Missing critical] `error.rs` edited (not in files_modified)**
- **Issue:** The plan asks for the `run_terminal`/`run_completed`/`token_*`/`unimplemented` error codes, but `ErrorCode` lives in `error.rs`.
- **Fix:** `ErrorCode::Unimplemented` was added, plus the reason helpers (see key-decisions). The file is inside ostro-api, which this wave owns.

**3. [Rule 2] `run_not_terminal` reason added**
- **Issue:** The plan does not say what retrying a live run returns, but it has to be rejected.
- **Fix:** 409 with reason `run_not_terminal`.

**4. `tests/rest.rs` unchanged**
- All REST lifecycle behaviour is covered in the new `tests/lifecycle_surface.rs`, as the plan's action text suggests. The existing Phase 4 REST tests pass unmodified.

## Hand-off notes

### 05-13 (server ControlPlane implementation)
- Implement the ten methods on `ServerControlPlane`. Keep these semantics so the parity suite and the fake stay truthful:
  - Errors are built with `ApiError::run_terminal(run_id, status)`, `run_completed`, `run_not_terminal`, `token_already_completed(run_id)`, `token_invalid(why)` and `unimplemented`. Never echo the token in a message.
  - Call `SignalRequest::validate()`, `CancelRequest::validate()` and `TerminateRequest::validate()` first. Their limits equal `ostro_types::command::MAX_*`. If those constants change, update `domain::MAX_*` too.
  - The adapters already cap `wait_ms` at 60 s. Register the RunWatchers waiter **before** reading the RunRecord (research § Long-poll registration order). A known non-terminal run returns `Ok` with `completed: false`.
  - The `failure` in `AwaitResultResponse` is a JSON value; the fake maps the RunInfo failure string to a JSON string. Decide the real shape (for example the RunFailed failure JSON) and update the fake if it differs.
  - Choices you may revisit, documented in the fake:
    - ContinuedAsNew retry → `run_completed`.
    - A token on a terminal run → `run_terminal`. The real apply may instead see Duplicate → `token_already_completed`; pick one and align the fake.
    - Retry dry_run returns the original id.
  - `set_workflow_policy` on an unknown workflow → 404. Queues are implicit.
- Delete nothing in ostro-api. Override the defaults in ostro-server only. Once the server overrides all ten, the `unimplemented_control_plane_answers_501` test keeps the default path covered.
- The start-request additions (`schedule_to_start`, `retention`, `dry_run` on StartRun) are still yours. No `StartRunRequest` proto, domain or REST field was added here.
- Rerun the parity scenario against the real server, as 04-20 did for API-01. API-05/06/07 can then be marked complete.

### 05-12
- No proto field was added to `StartRunRequest`, so server-test struct literals are unaffected. The new RPCs and messages are additive. External code that constructs `RunsClient`/`WorkflowsClient` compiles unchanged.

### 05-24 (D23 tenancy and names)
- The policy routes take `{name}` for workflows and queues with no grammar validation in the adapters. Apply the D23 name grammar and tenant qualification in the ControlPlane, as the rest of the API does. The fake only rejects an empty queue name.
- The signal-name grammar is not D23 (signal names are not resource names). Only the 1..=256-byte limit applies.

### 05-14 (CLI)
- REST bodies for `ostro signal/cancel/retry`: cancel, terminate and retry accept an empty body. `--dry-run` maps to `"dry_run": true`. `--json` should print the response bodies above verbatim.

## Notes for STATE.md (owned by 05-07)

- Decision: lifecycle error refinements travel as `details.reason` (`run_terminal`, `run_completed`, `run_not_terminal`, `token_already_completed`, `token_invalid`) on Conflict/InvalidArgument. `ErrorCode::Unimplemented` = 501/UNIMPLEMENTED.
- Decision: ControlPlane Phase 5 methods have Unimplemented default bodies until 05-13.
- API-05/06/07 remain Pending (proven against the fake only).

## Self-Check: PASSED

Files: tokens.proto, lifecycle_surface.rs and this SUMMARY are present. Commits e37c76e and ea33859 are present on main.
