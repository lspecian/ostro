---
phase: 04-single-node-server-rust-mvp
plan: 08
subsystem: api
tags: [rest, openapi, utoipa, axum, control-plane, api-01, api-02, api-03, api-04]
requires:
  - phase: 04-02
    provides: utoipa/utoipa-axum/tower-http/jiff/serde_json pins anchored in ostro-api, http_smoke test
  - phase: 04-03
    provides: ostro.api.v1 message shapes (REST DTOs mirror the field set)
provides:
  - ostro_api::domain::ControlPlane trait + transport-neutral domain types (exact 04-08 interface block)
  - ostro_api::error::{ErrorCode, ApiError} with HTTP and gRPC mapping tables + JSON error envelope
  - ostro_api::fake::FakeControlPlane (feature "fake"), deterministic ids/clock
  - ostro_api::router(cp, ApiConfig) one-port router (REST + /openapi.json + /healthz + /readyz + / + gRPC health)
  - ostro_api::openapi() (no ControlPlane needed) + insta snapshot
affects: [04-11, 04-17, 04-18, 04-19]
tech-stack:
  added: []
  patterns:
    - "REST DTOs (serde + ToSchema) separate from domain types; handlers take State<Arc<dyn ControlPlane>> only"
    - "Per-route DefaultBodyLimit on the upload MethodRouter (inner layer wins over the router-wide max_request_bytes)"
    - "Extractor rejections taken as Result<_, *Rejection> and mapped to ApiError JSON (413 -> payload_too_large, else invalid_argument)"
    - "OpenApiRouter built without state; split_for_parts used by both router() and openapi()"
key-files:
  created:
    - crates/ostro-api/src/domain.rs
    - crates/ostro-api/src/error.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/src/rest.rs
    - crates/ostro-api/src/openapi.rs
    - crates/ostro-api/src/ui_shell.html
    - crates/ostro-api/tests/rest.rs
    - crates/ostro-api/tests/openapi.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
  modified:
    - crates/ostro-api/Cargo.toml
    - crates/ostro-api/src/lib.rs
    - crates/ostro-api/tests/http_smoke.rs
    - Cargo.lock
key-decisions:
  - "REST error envelope {error:{code,message,details?}} with snake_case codes; HTTP 400/404/409/412/413/503/500, gRPC InvalidArgument/NotFound/AlreadyExists/FailedPrecondition/ResourceExhausted/Unavailable/Internal"
  - "REST JSON emits null for absent optional fields (stable shape); timestamps as RFC 3339 strings plus *_ms; result/published_status/history details embedded as JSON values (invalid JSON text falls back to a JSON string)"
  - "Fake module format 'ostro-fake:name@v,...' registers workflows; first registered version auto-activates; idempotency scope (tenant, workflow, key); run ids run-NNNNNN; fake clock FAKE_EPOCH_MS + n*1000 per mutation"
  - "Upload language hint is the ?language= query parameter; activation accepts expected_version in the body or If-Match (\"7\", W/\"7\", 7); both present and different -> 400"
  - "list/history limit: 0 or absent -> 100, capped at 1000 (domain::effective_limit) before calling the ControlPlane"
  - "/healthz, /readyz, /openapi.json and / are documented in the OpenAPI document too (tags health/meta)"
  - "sha2 is an optional dep enabled only by feature fake (plus dev-dep for lib unit tests)"
requirements-completed: []
duration: 30min
completed: 2026-11-18
---

# Phase 4 Plan 08: ControlPlane contract, REST + OpenAPI surface Summary

**One `ControlPlane` trait with transport-neutral domain types and a single `ApiError` mapping table; utoipa-annotated axum REST handlers served by `router(cp, cfg)` with `/openapi.json` (insta-pinned), `/healthz`, `/readyz` and a UI shell, all tested over real HTTP against a deterministic in-memory `FakeControlPlane`.**

## Performance

- **Duration:** about 30 min
- **Tasks:** 2
- **Files:** 13 (9 created, 4 modified)

## Accomplishments

- `domain.rs` implements the plan's interface block field for field (plus `RunStatus::{ALL, as_str, parse, is_terminal}` and `effective_limit`). There are no serde or utoipa derives on domain types.
- `error.rs`: `ErrorCode` maps to HTTP and gRPC through one table each, with inverse lookups. A table test checks both directions. `IntoResponse for ApiError` renders `{ "error": { "code", "message", "details"? } }`.
- `fake.rs` (cfg `test` or feature `fake`). Ostro-api's integration tests turn it on through a self dev-dependency. Behaviour:
  - A sha mismatch or a non-lowercase sha returns InvalidArgument; a re-upload returns `created=false`.
  - The same (name, version) from a different module returns Conflict, and nothing is applied.
  - Activation uses CAS on `record_version` (PreconditionFailed).
  - Idempotency keys support ReturnExisting and Reject (Reject returns Conflict with `details.existing_run_id`).
  - NotFound covers unknown workflows, versions, runs and modules.
  - History and run listing page with `next_from_seq` / `next_page_token`.
  - A `ready` flag and a `set_run_outcome` helper exist for tests.
- `rest.rs` has the routes listed below, built with `OpenApiRouter::with_openapi(ApiDoc).routes(routes!(..))`:

  | Method | Path |
  |---|---|
  | PUT, GET | `/v1/modules/{sha256}` |
  | GET | `/v1/workflows` |
  | PUT | `/v1/workflows/{name}/active` |
  | POST | `/v1/workflows/{name}/runs` |
  | GET | `/v1/runs` |
  | GET | `/v1/runs/{run_id}` |
  | GET | `/v1/runs/{run_id}/history` |
  | GET | `/healthz`, `/readyz`, `/openapi.json`, `/` |

  The upload body limit applies only to the upload route. Every other route gets `max_request_bytes`, and a `TraceLayer` is added.
- `lib.rs`: `ApiConfig` (64 MiB modules / 4 MiB requests by default), `router(cp, cfg)` (REST plus the existing gRPC Health merge), `openapi()` and `ui_shell_html()`. All 04-02 `use x as _;` anchors are gone; every one of those deps is now really used.
- OpenAPI 3.1 document: title "Ostro API", version = crate version, every route, and the schemas for the DTOs, `ErrorBody` and `WasmBytes` (string/binary for `application/wasm` and `application/octet-stream`). It is pinned in `tests/snapshots/openapi__openapi_document.snap`, which was reviewed before it was accepted.

## Task Commits

1. **Task 1: Domain types, ControlPlane trait, ApiError mapping and FakeControlPlane**: `8fbd93c` (feat)
2. **Task 2: REST handlers with utoipa, router(), /openapi.json, UI shell and snapshot**: `37e4da4` (feat)

## Verification

- `cargo test -p ostro-api --locked`: 17 lib + 1 http_smoke + 2 openapi + 11 rest, all green.
- Pitfall 7 coverage:
  - A 20 MB upload under the 64 MiB limit reaches the fake and gets a 400 sha mismatch, not a 413.
  - A 2 MiB upload over a 1 MiB limit gets a 413 `payload_too_large`.
  - A start-run body over `max_request_bytes` gets a 413, while a module larger than `max_request_bytes` still uploads.
- `cargo clippy -p ostro-api --all-targets --locked -- -D warnings`: clean. `cargo fmt`: clean.
- `cargo deny check bans`: ok (no new duplicates).
- `cargo build -p ostro --locked`: ok.

## Deviations from Plan

**1. [Rule 3 - Blocking] `tests/http_smoke.rs` updated to the new `router(cp, cfg)` signature.** The test is not in the plan's file list, but the contract changed `router()`. It now passes a `FakeControlPlane` and `ApiConfig::default()`.

**2. Tests use a real listener + reqwest instead of `tower::ServiceExt::oneshot`.** The plan allowed this. It avoids adding a direct `tower` dev-dep and exercises the body limits over real HTTP.

**3. `insta` added as an ostro-api dev-dependency.** It is an existing workspace pin and the snapshot test needs it.

**4. TDD: no separate RED commits.** Tests and implementation were committed together for each task, as in 04-02: a RED commit would have left unused-anchor and dependency churn at HEAD. The OpenAPI snapshot test did fail first (there was no snapshot); the snapshot was then generated and reviewed.

## Notes for later plans

- 04-11: use `ApiError::grpc_code()` / `ErrorCode::from_grpc_code`, `RunStatus::as_str`, and `FakeControlPlane` (deterministic ids/clock) for the parity suite. Merge the gRPC services into `router()` next to the Health route.
- 04-17/04-18: the real ControlPlane must keep the fake's semantics, or the fake must be updated with it.
  - Code mapping: sha mismatch -> InvalidArgument; (name, version) taken by another sha -> Conflict; stale `expected_version` -> PreconditionFailed; no active version -> PreconditionFailed.
  - Idempotency scope (tenant, workflow, key).
  - The fake auto-activates the first registered version. Confirm or drop that when the real modules/workflows ControlPlane lands.
- 04-19: `ostro openapi` can print `serde_json::to_string_pretty(&ostro_api::openapi())`.
- No requirement is marked complete. API-01 needs gRPC parity (04-11), and API-02/03/04 need the real ControlPlane (04-17/04-18).

## Self-Check: PASSED
