---
phase: 04-single-node-server-rust-mvp
plan: 03
subsystem: api
tags: [grpc, protobuf, protox, tonic, prost, api-contract]

requires:
  - phase: 01-foundation
    provides: ostro-proto crate with protox + tonic-prost-build pipeline (health.v1)
provides:
  - ostro.api.v1 protos (common, modules, workflows, runs)
  - ostro_proto::api::v1 generated messages, enums, Modules/Workflows/Runs server + client
affects: [04-08 REST DTOs, 04-11 gRPC adapters + parity suite]

tech-stack:
  added: []
  patterns:
    - "JSON payloads as bytes *_json (UTF-8 JSON) so REST and gRPC share domain JSON validation"
    - "Timestamps as uint64 *_at_ms; proto3 optional for presence-sensitive scalars"
    - "All protos compiled in one protox::compile call"

key-files:
  created:
    - crates/ostro-proto/proto/ostro/api/v1/common.proto
    - crates/ostro-proto/proto/ostro/api/v1/modules.proto
    - crates/ostro-proto/proto/ostro/api/v1/workflows.proto
    - crates/ostro-proto/proto/ostro/api/v1/runs.proto
  modified:
    - crates/ostro-proto/build.rs
    - crates/ostro-proto/src/lib.rs

key-decisions:
  - "Plan left ListWorkflows/GetRun messages unspecified: ListWorkflowsRequest {} / ListWorkflowsResponse { repeated WorkflowInfo workflows = 1 }, GetRunRequest { string run_id = 1 }"
  - "RunStatus values are unprefixed (SCHEDULED..CANCELLED) per plan; prost strips RUN_STATUS_ prefix so Rust names are RunStatus::Unspecified/Scheduled/..."

patterns-established:
  - "ostro.api.v1 is the gRPC half of API-01; 04-11 maps it onto ControlPlane"

requirements-completed: []

duration: 6min
completed: 2026-11-18
---

# Phase 4 Plan 03: ostro.api.v1 gRPC Contract Summary

**gRPC services ostro.api.v1 Modules (Upload/Get), Workflows (List/Activate) and Runs (Start/Get/History/List), generated through protox + tonic-prost-build with no protoc, plus round-trip tests**

## Performance

- **Duration:** about 6 min
- **Tasks:** 2
- **Files modified:** 6

## Accomplishments
- Four proto files that match the plan's message spec field for field (JSON payloads as `bytes *_json`, `uint64 *_at_ms` timestamps, `optional` scalars where presence matters)
- build.rs compiles health plus the api/v1 protos in a single protox call, with rerun-if-changed for each file
- `ostro_proto::api::v1` exposes the messages, the enums and both client and server stubs
- 5 unit tests pass: StartRunRequest with every optional set (including zero/empty values) survives a round trip with presence intact, RunInfo/ListRunsRequest round-trip, RunStatus/ConflictPolicy numbering is pinned, and all six server/client types can be named

## Task Commits

1. **Task 1: Write ostro.api.v1 protos** - `53127c9` (feat)
2. **Task 2: Codegen wiring + round-trip test** - `3cbc844` (feat)

## Files Created/Modified
- `crates/ostro-proto/proto/ostro/api/v1/common.proto` - RunStatus, ConflictPolicy, WorkflowRef
- `crates/ostro-proto/proto/ostro/api/v1/modules.proto` - service Modules
- `crates/ostro-proto/proto/ostro/api/v1/workflows.proto` - service Workflows
- `crates/ostro-proto/proto/ostro/api/v1/runs.proto` - service Runs
- `crates/ostro-proto/build.rs` - single protox compile list
- `crates/ostro-proto/src/lib.rs` - `pub mod api::v1` + tests

## Decisions Made
- The plan named three messages without defining them. Added `ListWorkflowsRequest {}`, `ListWorkflowsResponse { repeated WorkflowInfo workflows = 1; }` and `GetRunRequest { string run_id = 1; }`.
- The existing lints did not require `#[allow(missing_docs)]` on the generated module.

## Deviations from Plan

None. The plan was executed as written; the minimal definitions for the unspecified request/response messages are listed under Decisions.

## Issues Encountered
None. Verification ran only against `-p ostro-proto` because other wave-1 plans were editing the workspace at the same time.

## Next Phase Readiness
- 04-11 can implement `modules_server::Modules`, `workflows_server::Workflows` and `runs_server::Runs` on top of ControlPlane.
- API-01 stays Pending until the REST half (04-08) and the one-port parity suite (04-11) are done.

## Self-Check: PASSED
