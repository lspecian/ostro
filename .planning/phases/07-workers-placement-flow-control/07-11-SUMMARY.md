---
phase: 07-workers-placement-flow-control
plan: 11
subsystem: api
tags: [rest, grpc, openapi, proto, queues, workers, join, pause, flow-control, selector, parity]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-02 ErrorCode::RateLimited (429 / RESOURCE_EXHAUSTED + retry_after_ms), RESERVATIONS §11 Phase 7 names"
  - phase: 08-triggers-bottomless-observability
    provides: "08-10 router_with / ApiExtensions, append-only OpenAPI snapshot, parity pattern"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-08 ControlPlane-defaults pattern for new admin methods"
provides:
  - "ostro.api.v1.Queues (queues.proto) and ostro.api.v1.Workers (workers.proto); Runs.PauseRun/ResumeRun and Workflows.PauseWorkflow/ResumeWorkflow appended"
  - "ostro_api::phase7 wire/domain types, QueueFlow::validate (05-07 rules + selector syntax), Selector (k8s-style label selectors) for 07-12"
  - "15 ControlPlane methods with Unavailable(\"phase 7: lands in 07-14\") defaults"
  - "REST routes for every Phase 7 path in RESERVATIONS §11 (in rest::api_routes, so router/router_with mount them)"
  - "FakeControlPlane Phase 7 implementation (+ queue rate_limit -> RateLimited) and helpers add_worker / worker_token_secret / run_pause"
  - "parity transcript (49 steps), phase7_surface suite incl. openapi_superset (W9)"
affects: [07-12, 07-14, 07-15, 07-16, 07-17, 07-20, 08-10]
tech-stack:
  added: []
  patterns:
    - "Phase 7 types double as REST DTOs and ControlPlane types (08-10 pattern); proto conversions in grpc.rs"
    - "Semantic OpenAPI superset check against a recorded base snapshot (JSON-value equality per base path/method and schema)"
key-files:
  created:
    - crates/ostro-api/src/phase7.rs
    - crates/ostro-api/tests/phase7_surface.rs
    - crates/ostro-proto/proto/ostro/api/v1/queues.proto
    - crates/ostro-proto/proto/ostro/api/v1/workers.proto
  modified:
    - crates/ostro-proto/proto/ostro/api/v1/runs.proto
    - crates/ostro-proto/proto/ostro/api/v1/workflows.proto
    - crates/ostro-proto/build.rs
    - crates/ostro-proto/src/lib.rs
    - crates/ostro-api/src/{lib,domain,rest,grpc,fake}.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
key-decisions:
  - "New routes are ControlPlane methods (plan must-have, 06-08 pattern), mounted in rest::api_routes and router_with's tonic Routes; ApiConfig unchanged (still Copy + Eq), ApiExtensions untouched"
  - "create_worker_token takes CreateWorkerTokenRequest { ttl_ms } instead of a bare ttl (extensible, validated: ttl_ms > 0)"
  - "Durations and times are *_ms on both transports (ttl_ms, connected_at_ms, expires_at_ms, created_at_ms), like the rest of the API"
  - "QueueFlowInfo / PutQueueFlowBody are flat JSON (serde flatten): {queue, version, <flow fields>} and {<flow fields>, expected_version?}; If-Match accepted as an alternative (both given must agree, else 400)"
  - "A refused join (unknown, revoked or expired token) is 400 invalid_argument with details.reason = join_token_invalid; the message never echoes the token. 07-14 may keep or change this, the parity suite pins only the fake"
  - "Pause/resume answer {name|run_id, paused, changed}; changed = false on an idempotent repeat. Pausing or resuming a terminal run is 409 run_terminal"
  - "Placement / ExhaustAction protos use UNSPECIFIED = 0 for 'absent' (no optional enums); percentages are uint32 on the wire, values > 255 rejected in the conversion, 101..=255 by validate (same code as REST)"
  - "prost Debug is skipped for WorkerTokenCreated and JoinWorkerRequest (build.rs skip_debug) and hand-written with <redacted>; the domain types redact too"
requirements-completed: []
duration: ~1h45m
completed: 2026-11-21
---

# Phase 7 Plan 11: Phase 7 API surface Summary

**Every Phase 7 operation (queue flow, queue/workflow/run pause and resume, demand, worker list and drain, join tokens, join) is defined once in `ostro_api::phase7` + 15 `ControlPlane` methods, served over REST and over gRPC (`ostro.api.v1.Queues`, `ostro.api.v1.Workers`, plus RPCs appended to `Runs` and `Workflows`), implemented in-memory by `FakeControlPlane`, proven equal across transports by a 49-step parity transcript, and documented in an OpenAPI document that is a semantic superset of the base (116 base operations and schemas unchanged, 15 operations and 20 schemas added). ostro-server builds and passes unchanged on the Unavailable defaults; 07-14 only implements the methods and 07-15 only adds CLI verbs.**

## Task commits

1. `88c7653` feat(07-11): Phase 7 API domain, protos and ControlPlane methods
2. `28093db` feat(07-11): Phase 7 REST/gRPC handlers, OpenAPI, parity

## ControlPlane methods (crates/ostro-api/src/domain.rs)

Every default body is `Err(ApiError::unavailable(P7_PENDING))` with `P7_PENDING = "phase 7: lands in 07-14"` (HTTP 503 / gRPC UNAVAILABLE). Types are from `ostro_api::phase7`.

```rust
async fn get_queue_flow(&self, queue: String) -> Result<QueueFlowInfo, ApiError>;
async fn put_queue_flow(&self, queue: String, flow: QueueFlow, expected_version: Option<u64>) -> Result<QueueFlowInfo, ApiError>;
async fn pause_queue(&self, queue: String) -> Result<PauseState, ApiError>;
async fn resume_queue(&self, queue: String) -> Result<PauseState, ApiError>;
async fn pause_workflow(&self, workflow: String) -> Result<PauseState, ApiError>;
async fn resume_workflow(&self, workflow: String) -> Result<PauseState, ApiError>;
async fn pause_run(&self, run_id: String, detail: Option<String>) -> Result<RunPauseState, ApiError>;
async fn resume_run(&self, run_id: String) -> Result<RunPauseState, ApiError>;
async fn queue_demand(&self, queue: String) -> Result<DemandSnapshot, ApiError>;
async fn list_workers(&self) -> Result<Vec<WorkerInfo>, ApiError>;
async fn drain_worker(&self, worker_id: String) -> Result<DrainResult, ApiError>;
async fn create_worker_token(&self, req: CreateWorkerTokenRequest) -> Result<WorkerTokenCreated, ApiError>;
async fn list_worker_tokens(&self) -> Result<Vec<WorkerTokenInfo>, ApiError>;
async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError>;
async fn join_worker(&self, req: JoinRequest) -> Result<JoinResponse, ApiError>;
```

Implementations call the validators: `QueueFlow::validate`, `PauseRunRequest::validate` (detail at most 1024 bytes), `CreateWorkerTokenRequest::validate` (ttl_ms > 0), `JoinRequest::validate` (token non-empty, PEM CSR, worker id, labels), and `phase7::require_name` for queue names. The fake shows the expected semantics.

## phase7 types (serde + utoipa, also the REST DTOs)

| Type | Fields |
|---|---|
| `Placement` | `embedded` \| `workers` \| `auto` (`as_str`, `parse`) |
| `ExhaustAction` | `fail` \| `pause` |
| `RateSpecDto` | `limit: u32, period_ms: u64, burst: u32 (default 0)` |
| `SpillPolicyDto` | `cpu_pct, slots_pct, mem_pct: u8, lag_ms, sustain_ms, cooldown_ms: u64, hysteresis_pct: u8` |
| `QueueFlow` | `placement?, selector: Vec<String>, spill?, concurrency?, weight?, key_concurrency?, throttle?, key_throttle?, rate_limit?, key_rate_limit?, on_retries_exhausted?, paused` |
| `QueueFlowInfo` | `queue, version` + flattened `QueueFlow` (version 0 = never stored) |
| `PutQueueFlowBody` | flattened `QueueFlow` + `expected_version?` |
| `PauseState` | `name, paused, changed` |
| `PauseRunRequest` | `detail?` (deny_unknown_fields) |
| `RunPauseState` | `run_id, paused, changed` |
| `DemandSnapshot` | `queue, demand, backlog, running_on_workers, placement: Placement, spilling, workers, worker_slots_free` (D-W12; the server's `workers::demand::DemandSnapshot.placement` String maps through `Placement::parse`) |
| `WorkerInfo` | `worker_id, labels: BTreeMap, slots, free_slots, leases: u32, connected_at_ms, node_id: u64, draining` |
| `WorkerList` / `WorkerTokenList` | `{workers}` / `{tokens}` |
| `DrainResult` | `worker_id, already_draining` |
| `CreateWorkerTokenRequest` | `ttl_ms?` |
| `WorkerTokenCreated` | `token_id, token, join_command, expires_at_ms`; Debug redacts `token` and `join_command` |
| `WorkerTokenInfo` | `token_id, created_at_ms, expires_at_ms?` |
| `JoinRequest` | `token, csr_pem, worker_id, labels` (the 07-07 contract); Debug redacts `token` |
| `JoinResponse` | `client_cert_pem, ca_cert_pem, endpoints, renew_after_ms, cluster_id` (the 07-07 contract) |

Constants: `SELECTOR_MAX_ENTRIES = 32`, `SELECTOR_ENTRY_MAX = 128`, `WORKER_TOKEN_PREFIX = "wkr_"`, `MAX_WORKER_ID_LEN = 128` (07-07's worker accepts 64 without `:`, a subset), `MAX_WORKER_LABELS = 64`, `MAX_PAUSE_DETAIL_LEN = 1024`, `REASON_JOIN_TOKEN_INVALID = "join_token_invalid"`.

`QueueFlow::validate` mirrors 05-07's meta `queue_policy` check: at most 32 selector entries of at most 128 bytes, every percentage at most 100, every `period_ms > 0`. On top of that, each selector entry must parse. Placement and `on_retries_exhausted` are range-checked by their enums: serde rejects unknown names, and the gRPC conversion rejects unknown values. Both answer InvalidArgument.

## Selector (for 07-12): `ostro_api::phase7::Selector`

Location: `crates/ostro-api/src/phase7.rs`. It is a pure parser with no dependencies.

```text
selector    = requirement *( "," requirement )      ; ANDed; commas inside (..) do not split
requirement = key "=" value | key "==" value | key "!=" value
            | key ws "in" ws? "(" values ")" | key ws "notin" ws? "(" values ")"
            | key | "!" key
key         = 1*( ALPHA / DIGIT / "-" / "_" / "." / "/" )
value       = *( ALPHA / DIGIT / "-" / "_" / "." )   ; may be empty for = / == / !=
values      = value *( "," value )                   ; set members non-empty
```

- API: `Selector::parse(&str) -> Result<Selector, SelectorError>`, `Selector::from_entries(&[S: AsRef<str>])` (ANDs every entry; this is what `QueueFlow::selector` means), `matches(&BTreeMap<String, String>) -> bool`, `requirements() -> &[Requirement]`, `is_empty()`, and `QueueFlow::compiled_selector()`.
- `Requirement` is `Eq | NotEq | In | NotIn | Exists | NotExists`.
- Semantics are Kubernetes': `k!=v` and `k notin (..)` also match a worker that has no `k` label; `!k` matches only when `k` is absent; the empty selector matches every worker.
- Worker labels go through the same key/value grammar (`validate_labels`).
- Table tests: `phase7::tests::phase7_selector_table` (14 selectors × 3 label sets) and `phase7_selector_rejects_bad_syntax` (16 inputs).

## Proto (ostro.api.v1)

| Service | RPCs (new messages) |
|---|---|
| `Queues` (queues.proto) | `GetQueueFlow(GetQueueFlowRequest{name}) -> QueueFlowInfo{queue, version, flow}`, `PutQueueFlow(PutQueueFlowRequest{name, flow, optional expected_version})`, `PauseQueue / ResumeQueue({name}) -> QueuePauseState{queue, paused, changed}`, `GetQueueDemand({name}) -> QueueDemand{queue, demand, backlog, running_on_workers, placement, spilling, workers, worker_slots_free}` |
| `Workers` (workers.proto) | `ListWorkers -> ListWorkersResponse{workers: WorkerInfo}`, `DrainWorker({worker_id}) -> {worker_id, already_draining}`, `CreateWorkerToken({optional ttl_ms}) -> WorkerTokenCreated`, `ListWorkerTokens -> {tokens: WorkerTokenInfo}`, `RevokeWorkerToken({token_id}) -> {}`, `JoinWorker(JoinWorkerRequest{token, csr_pem, worker_id, labels}) -> JoinWorkerResponse` |
| `Runs` (appended) | `PauseRun(PauseRunRequest{run_id, optional detail}) -> RunPauseState`, `ResumeRun(ResumeRunRequest{run_id}) -> RunPauseState` |
| `Workflows` (appended) | `PauseWorkflow / ResumeWorkflow({name}) -> WorkflowPauseState{workflow, paused, changed}` |

Supporting messages:
- `QueueFlow`: `Placement placement = 1`, `repeated string selector = 2`, `SpillPolicy spill = 3`, optional `concurrency`/`weight`/`key_concurrency` = 4–6, `RateSpec throttle/key_throttle/rate_limit/key_rate_limit` = 7–10, `ExhaustAction on_retries_exhausted = 11`, `bool paused = 12`.
- `enum Placement { UNSPECIFIED, EMBEDDED, WORKERS, AUTO }` and `enum ExhaustAction { UNSPECIFIED, FAIL, PAUSE }`.
- `RateSpec{limit, period_ms, burst}`.
- `SpillPolicy` has uint32 percentages.

No existing message changed. Every service is mounted in `router_with` with `max_decoding_message_size(max_request_bytes)`.

## REST routes

| Method | Path | Tag | Result |
|---|---|---|---|
| GET / PUT | `/v1/queues/{name}/flow` | queues | `QueueFlowInfo`. PUT body `PutQueueFlowBody` or `If-Match`. Stale version = 412, invalid = 400 |
| POST | `/v1/queues/{name}/pause`, `/resume` | queues | `PauseState` |
| GET | `/v1/queues/{name}/demand` | queues | `DemandSnapshot` |
| POST | `/v1/workflows/{name}/pause`, `/resume` | workflows | `PauseState` (404 for an unknown workflow) |
| POST | `/v1/runs/{run_id}/pause` (optional body `{detail}`), `/resume` | runs | `RunPauseState` (404 unknown, 409 `run_terminal`) |
| GET | `/v1/workers` | workers | `{workers}` |
| POST | `/v1/workers/{id}/drain` | workers | `DrainResult` (404 unknown) |
| POST | `/v1/workers/tokens` (optional body `{ttl_ms}`) | workers | **201** `WorkerTokenCreated`, `Cache-Control: no-store` |
| GET | `/v1/workers/tokens` | workers | `{tokens}` (no secrets) |
| DELETE | `/v1/workers/tokens/{id}` | workers | 204 (404 unknown) |
| POST | `/v1/workers/join` | workers | `JoinResponse` (400 `join_token_invalid`) |

- Run paths use `{run_id}` like every other run route (RESERVATIONS writes `{id}`, as 08-10 noted for logs).
- A paused-run `/retry` alias is server behaviour (07-14).
- `RateLimited` (from a queue `rate_limit`) answers 429 with `Retry-After = ceil(retry_after_ms / 1000)` on REST and `RESOURCE_EXHAUSTED` with the same details on gRPC. Both are asserted in `phase7_rate_limited_on_rest_and_grpc`.

## OpenAPI (W9: semantic superset)

- The base was recorded before the first edit: `target/07-11-base.sha` = `30bf49c` and `target/07-11-base-openapi.snap`.
- `openapi_superset` (in `tests/phase7_surface.rs`, base path from `OSTRO_OPENAPI_BASE`) checks that **116** base operations and schemas are present and equal as JSON values. The test prints `SKIP` only when the variable is unset.
- Added operations (15):
  - `GET|PUT /v1/queues/{name}/flow`, `POST /v1/queues/{name}/pause`, `POST /v1/queues/{name}/resume`, `GET /v1/queues/{name}/demand`;
  - `POST /v1/workflows/{name}/pause|resume`, `POST /v1/runs/{run_id}/pause|resume`;
  - `GET /v1/workers`, `POST /v1/workers/{id}/drain`, `POST|GET /v1/workers/tokens`, `DELETE /v1/workers/tokens/{id}`, `POST /v1/workers/join`.
- Added schemas (20): CreateWorkerTokenRequest, DemandSnapshot, DrainResult, ExhaustAction, JoinRequest, JoinResponse, PauseRunRequest, PauseState, Placement, PutQueueFlowBody, QueueFlow, QueueFlowInfo, RateSpecDto, RunPauseState, SpillPolicyDto, WorkerInfo, WorkerList, WorkerTokenCreated, WorkerTokenInfo, WorkerTokenList.
- One tag was added: `workers`, inserted before `health`.
- Snapshot diff: +1507 lines. `git diff --minimal` and `--patience` show **0** removed lines, and the old snapshot is a line subsequence of the new one. Git's default Myers diff shows 191 realigned `-` lines (the same artefact 08-10 recorded).
- The `ErrorDetail` doc comment, which is part of the base schema, still omits `rate_limited`. Changing it would break the superset rule, so it is left for a later deliberate re-bless.

## FakeControlPlane (07-11 additions)

- **Flow:** CAS per queue. The version starts at 0 and every put adds 1. pause/resume flip `paused` and bump the version only on a change.
- **Workflow pause:** NotFound for an unknown workflow, as the fake's workflow policies. **Run pause:** detail stored, `RunPaused`/`RunResumed` history events, terminal runs answer 409.
- **Demand:** backlog = Ready runs of the queue that are not in a paused run or workflow (0 when the queue is paused). `workers` placement demands the backlog; `embedded` and `auto` demand 0 (the fake never spills). `worker_slots_free` counts non-draining workers only.
- **Tokens:** `wkr_<nnnnnn>.<32 hex>`, deterministic. Expiry runs on the fake clock (1 s per mutating call). The secret only appears in the create answer; `worker_token_secret()` exposes it to tests.
- **Join:** returns fixed PEMs (`FAKE_CLIENT_CERT_PEM`, `FAKE_CA_CERT_PEM`), `FAKE_CLUSTER_ENDPOINT`, `FAKE_RENEW_AFTER_MS` and `FAKE_CLUSTER_ID`.
- **Rate limit (D-W21):** `start_run` charges a queue's `rate_limit`, `limit + burst` per fixed window of the fake clock. The excess answers `RateLimited` with `retry_after_ms` and does not tick.
- **Helpers:** `add_worker(WorkerInfo)`, `worker_token_secret(id)`, `run_pause(run)`.

## Verification

All runs were in the worktree with `CARGO_TARGET_DIR=target/wt-07-11`.
- Task 1 verify: `cargo test -p ostro-proto -p ostro-api --locked -- phase7` ok. `cargo build --workspace --all-targets --locked` was warning-free (base f271792 at that point).
- Task 2 verify:
  - `cargo test -p ostro-api -p ostro-proto --locked`: all ok. lib 52, bridge_surface 7, http_smoke 1, lifecycle_surface 8, openapi 2, **parity 6** (new `phase7::phase7_rest_and_grpc_transcripts_are_equal`, 49 steps), **phase7_surface 10**, phase8_surface 9, rest 11, ostro-proto 9.
  - `OSTRO_OPENAPI_BASE=target/07-11-base-openapi.snap cargo test -p ostro-api --test phase7_surface -- openapi_superset`: 1 passed, no SKIP.
  - `cargo test -p ostro-server --locked`: all ok, 43 result lines, 331 passed, 0 failed (perf and semantics_sleepers ignored by design), on base 3d8fbde. `outbound_traceparent` (passed) logs an "apply thread panicked" ERROR for partition 2 during the run; it is unrelated to this plan (no ostro-api code on that path) and recorded here as an observation.
  - `cargo clippy -p ostro-api -p ostro-proto --all-targets --locked -- -D warnings`: clean.
  - `cargo build --workspace --all-targets --locked`: warning-free.
  - `cargo fmt --check` on the touched crates: clean.
- Cargo.toml and Cargo.lock are unchanged.

**Not run:** the full `bash scripts/ci-local.sh`. Several Phase 7 executors and the Phase 8 campaign were running in parallel, and the brief caps concurrent builds. The checks above cover the touched crates (ostro-api, ostro-proto) and their main dependent (ostro-server).

## Deviations from Plan

1. **[Rule 3] gRPC adapters landed in the Task 1 commit.** Appending RPCs to `Runs`/`Workflows` makes the generated server traits require them, so `grpc.rs` (a Task 2 file) had to implement them before the crate compiled. The Queues/Workers adapters came with them. Task 2 then mounted the services and added REST, OpenAPI and parity.
2. **[Plan detail] `create_worker_token(req: CreateWorkerTokenRequest)`** instead of `create_worker_token(ttl)`: same information, validated, extensible.
3. **[Plan detail] `*_ms` naming** (`ttl_ms`, `connected_at_ms`, `expires_at_ms`) instead of the interface block's `ttl` / `connected_at` / `expires_at`, matching the rest of the API. `WorkerInfo` gained `draining`.
4. **[Rule 2] Secret hygiene beyond the domain types.** prost-generated `Debug` for `WorkerTokenCreated` and `JoinWorkerRequest` would print the token, so build.rs skips it and ostro-proto implements a redacting `Debug`. `POST /v1/workers/tokens` answers with `Cache-Control: no-store`.
5. **[Note] Join error code.** A refused join is InvalidArgument + `join_token_invalid`, because the ErrorCode set has no Unauthenticated. 07-14 picks its own mapping and records it.
6. **[Note] Snapshot diff check.** As in 08-10: `--minimal`/`--patience` show 0 removed lines; the default Myers diff shows realigned lines.

## Follow-ups

- **07-14:** implement the 15 methods over `P7Shared`/meta. `put_queue_flow` writes only the F2 fields, mapping `Placement` to `PLACEMENT_*` (embedded 0, workers 1, auto 2) and `ExhaustAction` to `ON_EXHAUSTED_*` (fail 0, pause 1). Map `workers::demand::DemandSnapshot` to `phase7::DemandSnapshot`. The server's `workers::join::JoinRequest{csr}` corresponds to `phase7::JoinRequest{csr_pem}`.
- **07-12:** use `ostro_api::phase7::Selector::from_entries(&policy.selector)` and `matches(&labels)`.
- **07-15:** the CLI uses the REST routes and types above (`ostro_api::phase7`).

## Self-Check: PASSED
