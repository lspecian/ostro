---
phase: 14-multi-tenancy-security-1-0-release
plan: 02
subsystem: ostro-api / ostro-proto (Phase 14 wire surface + authentication front door)
tags: [p14, api, grpc, rest, openapi, auth, bearer, tenants, tokens, audit, signing, webhooks]
requires:
  - phase: 14-01
    provides: "ostro_api::auth (Principal, PRINCIPAL, Authenticator, AllowDev, AuthError), ostro_api::phase14 traits + DTOs, ApiExtensions slots, ApiError 401/403 + reasons"
  - phase: 14-01b
    provides: "ApiConfig.authenticator (AllowDev default), UploadModule.signatures, guarded REST/gRPC/UI wiring in the server (p11::ApiFront)"
provides:
  - "crates/ostro-proto/proto/ostro/api/v1/security.proto: Tenants, ApiTokens, Audit, Security services; UploadModuleRequest.signatures = 6, ListRunsRequest.tenant = 12; ostro_proto::GRPC_METHODS"
  - "crates/ostro-api/src/authn_layer.rs: bearer authentication layer around every REST and gRPC route of router_with (PRINCIPAL scope), UNAUTHENTICATED route list, classify, spawn_scoped / scoped"
  - "crates/ostro-api/src/phase14_adapters.rs: D-P14-29 REST routes + gRPC services over the four slots, ModuleSignatures extractor, shared validation (check::*), pb <-> domain conversions"
  - "Ostro-Module-Signature carriage (REST header, gRPC field) into UploadModule.signatures; ListRunsRequest.tenant; POST /hooks/{tenant}/{name}"
  - "fakes FakeTenants / FakeTokens / FakeAudit / FakeSecurity (FakeP14), FakeControlPlane::last_upload_signatures"
  - "OpenAPI: Phase 14 paths/schemas/tags, global bearerAuth, explicit empty security on the unauthenticated operations"
affects: [14-03, 14-15, 14-16, 14-17, 14-18, 14-19, 14-21, 14-26]
tech-stack:
  added: []
  patterns:
    - "one axum middleware covers REST and gRPC (tonic services are mounted on the same axum router), so PRINCIPAL wraps the whole service future"
    - "requests carrying request_meta::TrustedNode (cluster listener) skip the bearer layer; 14-15 ingests their principal"
    - "upload signatures decoded by a FromRequestParts extractor, so limits are enforced before the body is read"
key-files:
  created:
    - crates/ostro-proto/proto/ostro/api/v1/security.proto
    - crates/ostro-proto/tests/p14_wire.rs
    - crates/ostro-api/src/authn_layer.rs
    - crates/ostro-api/src/phase14_adapters.rs
    - crates/ostro-api/tests/p14_authn.rs
    - crates/ostro-api/tests/p14_routes.rs
  modified:
    - crates/ostro-proto/proto/ostro/api/v1/{modules,runs}.proto, crates/ostro-proto/{build.rs,src/lib.rs}
    - crates/ostro-api/src/{lib,domain,rest,grpc,grpc_client,phase8,fake,openapi}.rs
    - crates/ostro-api/tests/{parity,openapi}.rs, crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-server/src/services/bulk.rs (one field), crates/ostro-server/tests/{modules_api,tenancy_api}.rs (literals)
key-decisions:
  - "Authentication is one axum layer in router_with (no separate tonic interceptor): it covers both transports and holds the task-local for the whole call"
  - "Presigned object URLs (/v1/objects/*) and gRPC Workers/JoinWorker are unauthenticated (System), like the REST join routes; GET / (shell / UI redirect) runs with no scope"
  - "ostro's gRPC health service is ostro.health.v1.Health; both it and grpc.health.v1.Health/* are on the list"
  - "TrustedNode requests (node API on the cluster listener) are never authenticated here"
  - "REST 401 carries WWW-Authenticate: Bearer realm=\"ostro\" (the layer overrides ApiError's plain \"Bearer\")"
  - "Tenant webhooks reuse validate_qualified_name; a decoded '/' inside either path segment is 400"
  - "since/until of GET /v1/audit accept Unix ms digits or RFC 3339"
requirements-completed: []
duration: ~2h40m
completed: 2026-11-26
---

# Phase 14 Plan 02: API wire surface + bearer authentication Summary

**Tenants / ApiTokens / Audit / Security protos, REST routes and gRPC services over the four Phase 14 slots (404 / UNIMPLEMENTED while absent), a single bearer-authentication layer that runs every REST and gRPC request inside `PRINCIPAL` (401 `WWW-Authenticate: Bearer realm="ostro"` / `UNAUTHENTICATED` before any handler; webhooks and joins as `System`, health / UI with no scope), the `Ostro-Module-Signature` carriage, the run-list tenant filter, tenant webhooks, in-memory fakes, REST/gRPC parity and an additive OpenAPI snapshot with a global `bearerAuth` scheme.**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 | 20b112f | `feat(14-02): Phase 14 protos` |
| 2 | 73a435b | `feat(14-02): bearer authentication layer` |
| 3 | d182989 | `feat(14-02): Phase 14 REST/gRPC adapters + OpenAPI` |

## Field numbers allocated (appended after the highest existing number)

| Message | Field | Number | Type |
|---------|-------|--------|------|
| `ostro.api.v1.UploadModuleRequest` | `signatures` | **6** | `repeated bytes` |
| `ostro.api.v1.ListRunsRequest` | `tenant` | **12** | `optional string` |

Recorded in `crates/ostro-proto/tests/p14_wire.rs` (`UPLOAD_SIGNATURES_FIELD`, `LIST_RUNS_TENANT_FIELD`). New messages live in `security.proto` (numbered from 1). No partition/meta ordinal, ext tag or WIT change.

## Unauthenticated route list (`ostro_api::authn_layer::UNAUTHENTICATED`)

| Method | Pattern | Principal |
|--------|---------|-----------|
| GET | `/healthz`, `/readyz`, `/metrics` | none (Anonymous) |
| GET | `/` (shell page / redirect to `/ui/`) | none |
| any | `/ui`, `/ui/*` (UiAuth's, 14-21) | none |
| POST | `/hooks/*` (`/hooks/{name}`, `/hooks/{tenant}/{name}`) | `Principal::system()` |
| POST | `/v1/workers/join`, `/v1/cluster/join` | `Principal::system()` |
| any | `/v1/objects/*` (Phase 6 presigned URLs) | `Principal::system()` |
| POST | `/grpc.health.v1.Health/*`, `/ostro.health.v1.Health/*` | none |
| POST | `/ostro.api.v1.Cluster/Join`, `/ostro.api.v1.Workers/JoinWorker` | `Principal::system()` |

Everything else authenticates, including `/openapi.json`, the demand endpoint and `/v1/admin/*`. Requests with `request_meta::TrustedNode` (the cluster listener's node API) skip the layer. `p14_authn::every_route_and_grpc_method_is_listed_or_authenticated` sends every documented REST operation and every method of `ostro_proto::GRPC_METHODS` (built from the descriptor set) through the router and asserts exact equality of the unauthenticated sets (W12).

With `AllowDev` (`[auth] mode = "none"`, the `ApiConfig` default and the server stub until 14-03) every authenticated route runs as `Principal::dev()`, so server behaviour is unchanged until 14-03 switches the authenticator.

## Error mapping (REST ↔ gRPC, parity-tested)

| ApiError code | REST | gRPC | Phase 14 reasons |
|---------------|------|------|------------------|
| InvalidArgument | 400 | INVALID_ARGUMENT | `signature_invalid` |
| Unauthenticated | 401 + `WWW-Authenticate: Bearer realm="ostro"` | UNAUTHENTICATED | `unauthenticated` |
| PermissionDenied | 403 | PERMISSION_DENIED | `missing_scope`, `forbidden_tenant`, `tenant_suspended`, `signature_required` |
| NotFound | 404 (also: absent slot) | NOT_FOUND (absent slot: UNIMPLEMENTED) | — |
| Conflict | 409 | ALREADY_EXISTS | `tenant_not_empty` |
| PreconditionFailed | 412 | FAILED_PRECONDITION | — |
| Unavailable | 503 | UNAVAILABLE | `audit_unavailable`; authenticator store down |

## REST surface (D-P14-29)

`GET|POST /v1/tenants` (create 201), `GET|PATCH|DELETE /v1/tenants/{id}`, `POST /v1/tenants/{id}/{suspend,resume}`, `GET|POST /v1/auth/tokens` (create 201 + `no-store`), `DELETE /v1/auth/tokens/{id}`, `POST /v1/auth/tokens/{id}/rotate` (201 + `no-store`, optional body), `GET /v1/auth/whoami`, `GET /v1/audit?since&until&tenant&action&actor&limit&cursor`, `GET|PUT /v1/security/modules/policy`, `POST /v1/security/modules/keys`, `DELETE /v1/security/modules/keys/{key_id}`, `POST /v1/modules/{sha}/signatures` (body `{signatures: [base64]}`; mounted on `{sha256}` like the replay route), `POST /hooks/{tenant}/{name}` (webhook `"<tenant>/<name>"`, same body limit), `GET /v1/runs?tenant=` (`_` = default tenant, empty = none). Upload: up to 4 `Ostro-Module-Signature` headers, each ≤ 8 KiB decoded (base64, padding optional), else 400 `signature_invalid` before the body is read; gRPC `signatures` has the same limits.

## Verification

- Task 1 verify (plan command): PASS (`p14_wire` 6 passed; greps).
- Task 2 verify (plan command): PASS (`p14_authn` 9 passed; grep `WWW-Authenticate` in `authn_layer.rs`).
- Task 3 verify, adapted to the real layout (see deviations): `p14_routes` 8, `parity` 11 (incl. `phase14::rest_and_grpc_transcripts_are_equal`, `phase14::unauthenticated_maps_to_401_and_unauthenticated`), `openapi` 3 passed; greps `Ostro-Module-Signature` in `rest.rs`, `/v1/auth/tokens` in `phase14_adapters.rs`. OpenAPI additivity: see deviation 4.
- `cargo test -p ostro-api -p ostro-proto --locked`: all green (lib 85; every integration target ok). `cargo clippy -p ostro-api -p ostro-proto --all-targets -- -D warnings`: clean. `cargo check --workspace --tests --locked` on the merged main: clean. rustfmt clean. `scripts/p12-grep-gates.sh`, `scripts/p13-grep-gates.sh` and CLOCK_GATE_SCOPED: clean (no clock reads added).
- Plan gate: `ostro-k8s-ci --parallel` on the worktree at d182989 (run p173711-29483, wall 203 min): **not fully green, no test failure attributable to 14-02.** PASS: `cargo test` shard test1 (809 s), polyglot block (python/ts SDKs, polyglot conformance, polyglot clippy, clients python/ts/go), failpoint suites (fp1+fp2+fp3), replication mutant matrix, server campaign smoke (20 kills), zig present, musl release, musl binary size limit. FAIL: `musl zigbuild test`, only `backup_cli::bucket_online_and_offline_round_trips` (`/readyz not 200 within 60s`, the known load-only test); it **passes alone** (musl, slot 0, 2 passed in 115 s, log `20261126-220847-s0-d182989c-cmd.log`). **No result (cluster infrastructure on dell-1, not tests):** groups lint and obuild ("ci container not started 10 min after the upload"), test2 (exit 137, "API unreachable", killed while compiling). A targeted rerun of those steps on slot 0 (dell-1; log `20261126-233916-s0-d182989c-cmd.log`) completed toolchain, `cargo fmt --check`, `cargo clippy -D warnings`, warning-free `cargo build`, SDK workspace fmt + clippy + tests + wasm, guests build and the order-saga example with no error, then was killed (exit 137, "API unreachable" again) while compiling `cargo test`. **Still unverified on the cluster:** the test2 shard of `cargo test`, the lint-group steps from "semantics sleepers" onward (failpoint list sync, Phase 13 mutant matrix, wit-next fixture suite, Phase 13 tier C, phase 7 failpoint legs, cargo deny, vendored/deny fixture checks, grep gates, workflow validation) and "ostro-build polyglot". Locally: the P12/P13 grep gates and the clock gate are clean, and `cargo check --workspace --tests` passes.

## Deviations from Plan

### File layout (plan allows real files)

The plan's `rest/{p14,mod,modules,hooks}.rs`, `grpc/{p14,mod}.rs`, `authn_layer.rs`/`router.rs`, `tests/snapshots/openapi.json` do not exist in this layout. Used instead: `src/phase14_adapters.rs` (REST + gRPC adapters, like `phase13_adapters.rs`), `src/rest.rs` (upload header, list filter, tags, `wildcard_routes`/`parse_rfc3339_ms` made `pub(crate)`), `src/grpc.rs` (upload field, list filter), `src/phase8.rs` (tenant hook), `src/lib.rs` (router wiring, the layer), `src/authn_layer.rs`, `src/openapi.rs`, `tests/snapshots/openapi__openapi_document.snap` (insta). `src/domain.rs` gained `ListRunsRequest.tenant` (allowed by the orchestrator notes).

### Auto-fixed / added

**1. [Rule 3 - Blocking] Exhaustive literals of the appended fields outside the plan's files.** `pb::UploadModuleRequest` literals in `crates/ostro-server/tests/{modules_api,tenancy_api}.rs` and `ostro-api/tests/parity.rs` gained `signatures: vec![]`; `ListRunsRequest` in `crates/ostro-server/src/services/bulk.rs::list_request` gained `tenant: None` (one field + comment; tenant scoping of bulk jobs stays 14-16's). `grpc_client.rs` forwards `signatures` and `tenant` (forwarded uploads keep their signatures). Commits 20b112f, d182989.

**2. [Rule 2 - Missing critical] Presigned object route left unauthenticated (System).** The plan's list omits `/v1/objects/*`, but the Phase 6 presign route is authorised by its query signature and is consumed without a bearer; authenticating it would break presigned URLs. Added as System (like webhooks). **Orchestrator / 14-15 / 14-24 should confirm** (it is in the route × scope matrix as unauthenticated).

**3. [Rule 2] gRPC `Workers/JoinWorker` unauthenticated (System), `ostro.health.v1.Health/*` listed, `GET /` unauthenticated (no scope).** JoinWorker is the gRPC twin of `POST /v1/workers/join` (join-token credential); ostro's health service is `ostro.health.v1.Health` (the plan names `grpc.health.v1.Health`, also listed); `GET /` only serves the shell page / the redirect to `/ui/` and calls no control-plane method (authenticating it would answer browsers 401 instead of sending them to the UI login). `/openapi.json` stays authenticated as planned.

**4. OpenAPI additivity check adapted.** The plan's `git diff -U0 base -- <snapshot> | grep "^-[^-]"` reports 1904 lines with git's default (Myers) alignment because large blocks are inserted mid-document; with `--diff-algorithm=patience|histogram|minimal` it reports exactly **8** lines, each a closing `}` that became `},` because `"security": []` was appended after `responses` on the 8 existing unauthenticated operations (`/`, `/healthz`, `/readyz`, `POST /hooks/{name}`, `POST /v1/cluster/join`, `POST /v1/workers/join`, `GET|PUT /v1/objects/{connection}/{key}`). The explicit empty security the plan asks for cannot be added to existing operations without that punctuation change. A semantic check (old document is a recursive, order-preserving subset of the new one) passes with no removed or changed value; 43 added keys (paths, schemas, `securitySchemes`, root `security`, the upload's 403 response, per-operation `security`), plus the upload's header parameter and four tags appended to arrays.

**5. W4 streaming case.** ostro-api has no server-streaming RPC, SSE or spawned producer today (run results are awaited inside the handler). `authn_layer::{spawn_scoped, scoped}` exist for server-side producers; `p14_authn::long_poll_producers_keep_the_callers_principal` checks a long-poll result (REST `?wait=` and gRPC `AwaitResult`) whose control plane spawns its producer with `spawn_scoped` and observes the caller's principal; a unit test shows plain `tokio::spawn` sees Anonymous.

**6. `/ui/**` handler principal** is observed with a router built by `authn_layer::apply` (ostro-api does not serve `/ui`; the server merges the UI router outside `router_with`, so `/ui` never reaches this layer in production either).

## Notes for later plans

- 14-03: switching `p14::authenticator` to the meta-backed one turns authentication on for the server profile; ostro-server tests that call the API without a bearer under `[auth] mode = "token"` will then need tokens.
- 14-15: node-API principal ingest is yours (`TrustedNode` requests bypass this layer and run Anonymous); the unauthenticated list is `authn_layer::UNAUTHENTICATED` / `classify`.
- 14-16: `ListRunsRequest.tenant` reaches the control plane as given (`_` = default tenant); force/validate it for tenant principals. Bulk selection passes `tenant: None`.
- 14-17/14-18/14-19: implement the traits; adapters already validate ids, tenant grammar, sha syntax and signature limits. `AttachSignaturesRequest.signatures` arrives as standard base64 of the raw bytes (REST and gRPC alike).
- 14-21: `/ui` and `/ui/*` are exempt here; UiAuth owns them.
- No ci.yml change needed (new tests are in existing crates).

## Self-Check: PASSED
