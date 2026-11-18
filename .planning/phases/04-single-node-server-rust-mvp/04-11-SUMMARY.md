---
phase: 04-single-node-server-rust-mvp
plan: 11
subsystem: api
tags: [grpc, tonic, parity, one-port, api-01]
requires:
  - phase: 04-03
    provides: ostro.api.v1 Modules/Workflows/Runs protos and generated servers/clients
  - phase: 04-08
    provides: ControlPlane trait, ApiError gRPC/HTTP tables, FakeControlPlane, router(cp, cfg)
provides:
  - ostro_api::grpc::{GrpcModules, GrpcWorkflows, GrpcRuns} adapters + proto<->domain conversions
  - ostro_api::grpc::{to_status, status_details} (ApiError <-> tonic::Status with JSON details)
  - router(cp, cfg) serving REST + HTTP/2 + gRPC (Health, Modules, Workflows, Runs) on one port
  - tests/parity.rs REST-vs-gRPC transcript suite
affects: [04-17, 04-18, 04-19, 04-20]
tech-stack:
  added: []
  patterns:
    - "gRPC adapters are tuple/struct wrappers over Arc<dyn ControlPlane>; every proto<->domain conversion is a From/TryFrom in grpc.rs"
    - "Parity suite: one async scenario generic over a private Client trait (RestClient, GrpcClient), outcomes normalised to JSON or (ErrorCode, details)"
    - "tonic decode-limit OUT_OF_RANGE rewritten to RESOURCE_EXHAUSTED by an axum map_response layer on the gRPC router"
key-files:
  created:
    - crates/ostro-api/src/grpc.rs
    - crates/ostro-api/tests/parity.rs
  modified:
    - crates/ostro-api/src/lib.rs
key-decisions:
  - "Status details = serde_json bytes of ApiError.details (status_details() decodes them); no google.rpc.Status wrapping"
  - "Empty input_json = JSON null (REST default); non-empty must be valid JSON and passes through unchanged; RUN_STATUS_UNSPECIFIED / unknown enum values -> InvalidArgument"
  - "proto3 optional fields map 1:1 to Option (Some(\"\") stays Some(\"\"), same as REST)"
  - "gRPC limits mirror REST: Modules decode limit = max_module_bytes + 4 KiB headroom with an exact module-bytes check in GrpcModules (ResourceExhausted); Workflows/Runs decode limit = max_request_bytes; tonic's OUT_OF_RANGE for over-limit messages is rewritten to RESOURCE_EXHAUSTED"
  - "list/history limit normalised with domain::effective_limit before calling the ControlPlane (same as REST)"
requirements-completed: []
duration: 25min
completed: 2026-11-18
---

# Phase 4 Plan 11: gRPC adapters + one-port REST/gRPC parity Summary

**tonic `Modules`/`Workflows`/`Runs` services are thin adapters over the same `ControlPlane` and `ApiError` table as REST. They are merged into `router()`, so one port serves HTTP/1.1 JSON, HTTP/2 and gRPC. A transcript suite shows the same scenario gives identical results over REST and gRPC, size limits included.**

## Performance

- **Duration:** about 25 min
- **Tasks:** 2 (plus one fix commit found while writing the parity suite)
- **Files:** 3 (2 created, 1 modified)

## Accomplishments

- `grpc.rs`:
  - `GrpcModules::new(cp, max_module_bytes)`, `GrpcWorkflows(cp)` and `GrpcRuns(cp)` implement `modules_server::Modules`, `workflows_server::Workflows` and `runs_server::Runs`.
  - Conversions: `From<domain::*>` for ModuleInfo, WorkflowInfo, RunInfo, RunHistoryResponse, ListRunsResponse and RunStatus, plus `TryFrom<pb::StartRunRequest>` and `TryFrom<pb::ListRunsRequest>`.
  - `to_status` takes the code from `ApiError::grpc_code()`, the message, and the details as JSON bytes. `status_details` is the client-side inverse.
- `router(cp, cfg)` builds `Routes::new(Health).add_service(Modules/Workflows/Runs).into_axum_router()` and merges it with the REST router. The REST `DefaultBodyLimit` and `TraceLayer` stay on the REST routes only.
- `tests/parity.rs` (3 tests):
  - `rest_and_grpc_transcripts_are_equal` runs 34 steps per transport against two fresh fakes and compares them step by step. The steps cover:
    - upload, re-upload, sha mismatch (with `actual_sha256` details), version conflict, get, and unknown module
    - list workflows; activate with a stale CAS, OK, unknown workflow and unknown version
    - start with an idempotency key: created, idempotent, rejected (details `existing_run_id`), other tenant, delayed/pinned/queued, unknown workflow/version, invalid input JSON
    - get run with result and published status, and an unknown run
    - two history pages via `next_from_seq`, and history of an unknown run
    - two run-list pages via the token, plus status and workflow+tag filters and a bad token
    - module just over and far over `max_module_bytes`, and a start over `max_request_bytes`
  - `http2_prior_knowledge_rest_on_the_same_port`: reqwest `http2_prior_knowledge` PUT and GET both report `HTTP_2`, and gRPC works on the same port.
  - `grpc_upload_above_tonic_default_reaches_control_plane`: a 6 MiB gRPC upload with the wrong digest gets InvalidArgument with `actual_sha256` (it was decoded, not size-rejected). With the right digest it is stored.

## Task Commits

1. **Task 1: gRPC adapters and one-port router**: `4c90bea` (feat)
2. **Fix: gRPC size limits answer RESOURCE_EXHAUSTED like REST 413**: `7f1b6e3` (fix)
3. **Task 2: REST vs gRPC parity suite on one port**: `2580827` (test)

## Verification

- `cargo test -p ostro-api --locked`: 20 lib + 1 http_smoke + 2 openapi + 3 parity + 11 rest, all green. The OpenAPI snapshot is unchanged.
- `cargo clippy -p ostro-api --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-api`: clean.
- `cargo check -p ostro --locked` / `-p ostro-server`: ok.
- Negative control: with the `map_size_limit_status` layer removed, the parity test fails at "upload far over max_module_bytes" (REST 413 vs tonic OUT_OF_RANGE). The rewrite is needed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] gRPC over-limit messages did not match REST 413**
- **Found during:** Task 2 (designing the 413 <-> ResourceExhausted parity step)
- **Issue:** tonic rejects messages over its decode limit with `OUT_OF_RANGE`, but the shared table maps an over-limit body to `RESOURCE_EXHAUSTED`. A decode limit of exactly `max_module_bytes` would also reject a module of exactly the limit, because protobuf framing adds bytes.
- **Fix:**
  - `GrpcModules` now carries `max_module_bytes` and checks the module bytes exactly, returning PayloadTooLarge.
  - The decode limit is `max_module_bytes + 4 KiB`.
  - Workflows and Runs use `max_request_bytes` instead of tonic's fixed 4 MiB, which also equals the default.
  - An axum `map_response` layer on the gRPC router rewrites `grpc-status: 11` to `8`. No `ApiError` maps to OUT_OF_RANGE, so the rewrite cannot change a real error.
- **Files modified:** crates/ostro-api/src/grpc.rs, crates/ostro-api/src/lib.rs
- **Commit:** 7f1b6e3

**2. `GrpcModules` is a struct with `new(cp, max_module_bytes)` rather than the plan's `GrpcModules(Arc<dyn ControlPlane>)` tuple**, because of fix 1. Workflows and Runs stay tuple structs.

**3. TDD: the suite went green on its first run.** The adapters (Task 1) existed before the test, as the plan's task order requires. The negative control above shows the suite can fail.

## Notes for later plans

- 04-17/04-18: the real ControlPlane plugs into the same `router()`. The parity suite only needs `FakeControlPlane`, so keep the fake's semantics in line with the real implementation.
- 04-19/04-20: gRPC clients (CLI, e2e tests) should read error details with `ostro_api::grpc::status_details`. The e2e suite should rerun a REST/gRPC parity scenario against the real server. That run is the remaining proof for API-01.
- API-01 is **not** marked complete. The plan's success criteria say "proven against the fake; proven against the real server in 04-20".

## Self-Check: PASSED
