---
phase: 06-effects-bridge-connectors-secrets
plan: 08
subsystem: api
tags: [rest, grpc, openapi, secrets, grants, connections, egress, presign, parity]

requires:
  - phase: 06-effects-bridge-connectors-secrets
    provides: 06-02 contract (names, grant id convention); 06-05 secret-name grammar + origin patterns (duplicated minimal checks); 06-12 local HMAC presign route shape
  - phase: 08-triggers-bottomless-observability
    provides: 08-10 router_with / ApiExtensions, append-only OpenAPI snapshot
provides:
  - "ostro.api.v1.Secrets + ostro.api.v1.Connectors (bridge.proto)"
  - "ControlPlane bridge methods (15) with Unimplemented defaults; domain types + validation helpers"
  - "REST /v1/secrets, /v1/grants, /v1/connections, /v1/egress; GET|PUT /v1/objects/{connection}/{key} outside the REST middleware stack (rest::object_routes)"
  - "FakeControlPlane bridge impl + test helpers secret_value / last_presigned_object"
affects: [06-14, 06-16, 06-19]

tech-stack:
  added: []
  patterns:
    - "wildcard_routes: mount a utoipa route at an axum `{*x}` path while documenting it as `{x}`"
    - "Unauthenticated capability routes are a separate router merged after the REST layers in router_with"

key-files:
  created:
    - crates/ostro-proto/proto/ostro/api/v1/bridge.proto
    - crates/ostro-api/tests/bridge_surface.rs
  modified:
    - crates/ostro-proto/build.rs
    - crates/ostro-proto/src/lib.rs
    - crates/ostro-api/src/domain.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/src/rest.rs
    - crates/ostro-api/src/grpc.rs
    - crates/ostro-api/src/openapi.rs
    - crates/ostro-api/src/lib.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap

key-decisions:
  - "Validation lives in domain.rs (PutSecret/GrantInfo/ConnectionInfo/EgressInfo::validate) and is called by the ControlPlane implementation, as SignalRequest::validate is; 06-16's server must call them too. Duplicated from ostro-bridge (no dependency)"
  - "Secret names, grant ids, connection names and egress modules are wildcard path captures (they contain `/`); OpenAPI documents them as {name}/{id}/{module}"
  - "The presign route is mounted outside the REST middleware stack and has no TraceLayer (its query carries the signature); ostro-api has no auth layer yet, so the 401 half of the presign test cannot run"
  - "value_b64 decoded by a 30-line in-crate standard-base64 decoder: ostro-api has no base64 dependency and Cargo files are 06-02's"
  - "List responses are wrapped objects ({secrets}, {grants}, {connections}, {policies}), deletes answer 204"

requirements-completed: []

duration: ~55 min
completed: 2026-11-20
---

# Phase 6 Plan 08: Bridge admin API surface Summary

**Secrets (write-only), grants, connections and egress policies now exist on REST and gRPC (`ostro.api.v1.Secrets`, `ostro.api.v1.Connectors` in `bridge.proto`), backed by 15 new `ControlPlane` methods. Each method defaults to Unimplemented, so ostro-server builds unchanged until 06-16. The local presign route `GET|PUT /v1/objects/{connection}/{key}` is mounted outside the REST middleware stack. Tests prove that a canary secret never comes back over REST, gRPC or the OpenAPI document. The REST/gRPC parity transcript is equal across 36 steps, and the OpenAPI snapshot diff is additive only.**

## ControlPlane methods (all with default `Err(ApiError::unimplemented("<method>"))`)

```rust
async fn put_secret(&self, req: PutSecret) -> Result<SecretInfo, ApiError>;
async fn list_secrets(&self, tenant: Option<String>) -> Result<Vec<SecretInfo>, ApiError>;
async fn delete_secret(&self, name: String, tenant: Option<String>) -> Result<(), ApiError>;
async fn put_grant(&self, g: GrantInfo) -> Result<GrantInfo, ApiError>;          // id = GrantInfo::expected_id()
async fn list_grants(&self, module: Option<String>) -> Result<Vec<GrantInfo>, ApiError>;
async fn delete_grant(&self, id: String) -> Result<(), ApiError>;
async fn put_connection(&self, c: ConnectionInfo) -> Result<ConnectionInfo, ApiError>;
async fn get_connection(&self, name: String) -> Result<ConnectionInfo, ApiError>;
async fn list_connections(&self) -> Result<Vec<ConnectionInfo>, ApiError>;
async fn delete_connection(&self, name: String) -> Result<(), ApiError>;
async fn put_egress(&self, e: EgressInfo) -> Result<EgressInfo, ApiError>;
async fn get_egress(&self, module: String) -> Result<EgressInfo, ApiError>;
async fn list_egress(&self) -> Result<Vec<EgressInfo>, ApiError>;
async fn delete_egress(&self, module: String) -> Result<(), ApiError>;
async fn presigned_object(&self, req: PresignedObjectRequest) -> Result<PresignedObjectResponse, ApiError>;
```

The domain types match the plan's `<interfaces>` exactly:
- `PutSecret` and `PresignedObjectRequest` have redacting `Debug` impls (value, signature and body).
- A bad presign signature is `Ok(PresignedObjectResponse { status: 403, .. })`. `ErrorCode` has no 403.

Validation helpers in `domain.rs`:
- `PutSecret::validate`: name grammar plus a value of at least 8 bytes. A short value is 400 with `details.reason = "secret_too_short"`.
- `validate_secret_name`: `[a-z0-9][a-z0-9_.\-/]{0,127}`, no `..`, no empty or `.` segment. Names under `__ostro/` are 400 with `reason = "secret_reserved"`.
- `GrantInfo::validate`: module name without `:`, a valid secret name, at least one origin, and an id that, when given, equals `secret:<module>:<secret>`.
- `validate_origin_pattern`: `http(s)://host[:port]` or `http(s)://*.domain[:port]`, an optional trailing `/`, and IPv6 in brackets. A bare `*`, a `*` in any other position, paths, queries and userinfo are refused.
- `ConnectionInfo::validate`: kind is one of `sql-postgres|sql-mysql|object|http-target`, and config is a JSON object with `"v": 1`.
- `EgressInfo::validate`: each entry is an origin pattern or `connection:<name>`.

These duplicate the minimal checks of ostro-bridge (`secrets::template::validate_name`, `origin::OriginPattern::parse`, `secrets::grants`), because ostro-api does not depend on ostro-bridge. **06-16's server must call the `validate` helpers** and may validate more strictly, for example by using `OriginPattern::parse` itself.

## REST paths

| Method | Path | ControlPlane | Notes |
|---|---|---|---|
| PUT | `/v1/secrets/{*name}` | `put_secret` | body `{"value": "..."}` xor `{"value_b64": "..."}` (unknown fields rejected), `?tenant=` → 200 `SecretInfoDto {name, key_id, tenant}` |
| GET | `/v1/secrets` | `list_secrets` | `?tenant=` → `{secrets: [...]}` |
| DELETE | `/v1/secrets/{*name}` | `delete_secret` | `?tenant=` → 204 |
| PUT | `/v1/grants` | `put_grant` | `GrantDto {id?, module, secret, origins, body?}` → stored grant with id |
| GET | `/v1/grants` | `list_grants` | `?module=` → `{grants: [...]}` |
| DELETE | `/v1/grants/{*id}` | `delete_grant` | 204 |
| PUT | `/v1/connections/{*name}` | `put_connection` | body `{kind, config}`; name percent-decoded (`https%3A%2F%2Fapi.example.com`) |
| GET | `/v1/connections` | `list_connections` | `{connections: [...]}` |
| GET / DELETE | `/v1/connections/{*name}` | `get_connection` / `delete_connection` | 204 on delete |
| PUT | `/v1/egress/{*module}` | `put_egress` | body `{allow}` |
| GET | `/v1/egress` | `list_egress` | `{policies: [...]}` |
| GET / DELETE | `/v1/egress/{*module}` | `get_egress` / `delete_egress` | 204 on delete |
| GET / PUT | `/v1/objects/{connection}/{*key}?exp=<unix>&sig=<hex>` | `presigned_object` | **outside the REST middleware stack**; missing/empty `sig` or missing/unparsable `exp` → 400; the response passes through `status`, `body` and `content_type`; PUT passes the raw body and `Content-Type` |

OpenAPI documents the wildcard paths as `{name}`, `{id}`, `{module}` and `{key}`. A test asserts that no `*` appears in any OpenAPI path. Two tags are new: `secrets` (secrets and grants) and `connectors` (connections, egress, objects).

## gRPC

| Service | RPCs |
|---|---|
| `ostro.api.v1.Secrets` | PutSecret, ListSecrets, DeleteSecret, PutGrant, ListGrants, DeleteGrant |
| `ostro.api.v1.Connectors` | PutConnection, GetConnection, ListConnections, DeleteConnection, PutEgress, GetEgress, ListEgress, DeleteEgress |

- Both services are always mounted, with decode limit `max_request_bytes`.
- A missing `grant`, `connection` or `egress` sub-message is `INVALID_ARGUMENT`.
- `config_json` must be JSON. An empty `config_json` decodes to `null`, which validation rejects.
- Default methods return `UNIMPLEMENTED`.
- The presign route is REST-only.

## Presign route and authentication

ostro-api has **no** bearer-auth layer today. In `router_with`, the object router (`rest::object_routes()`) is built separately:
- It has its own `DefaultBodyLimit(max_request_bytes)`.
- It is merged after the REST stack's `DefaultBodyLimit` and `TraceLayer`, so any auth layer added to that stack later cannot wrap it.
- It deliberately has no `TraceLayer`, because the URL query carries the signature.

`bridge_surface::presign_route_needs_no_credentials` checks the route with no `Authorization` header: `sig=valid` gives 200 with the body and content type, anything else gives 403, and missing parameters give 400. The connection, key, method, exp, body and content type reach the control plane, including keys with `/`. The plan's "`/v1/secrets` without a token gets 401" check applies only "if auth is enabled in that test harness". It is not, so that check does not exist.

## Write-only secrets (SC3)

`secret_values_are_write_only` writes two canaries through three paths: REST `value`, REST `value_b64` (with a tenant) and gRPC `PutSecret`. It then reads every REST list, `/openapi.json` and every gRPC list, and checks that:
- neither canary appears, in text or in base64 form, in any response;
- the `SecretInfoDto` and `SecretListResponse` schemas have no `value` property;
- `PutSecretBody.value` and `PutSecretBody.value_b64` are documented `writeOnly`.

Error messages never echo a value: tested for the short-value error, in both the unit test and the REST test. `PutSecretBody`'s `Debug` also redacts.

## OpenAPI diff

The snapshot gained 979 lines and lost none.
- `git diff --minimal`, `--patience` and `--histogram` show **0 removed lines**.
- A script confirmed that the old snapshot is a line subsequence of the new one.
- Git's default Myers diff shows 36 realigned `-` lines. This is the same artefact noted in 08-10.

New paths:
- `/v1/connections`, `/v1/connections/{name}`
- `/v1/egress`, `/v1/egress/{module}`
- `/v1/grants`, `/v1/grants/{id}`
- `/v1/objects/{connection}/{key}`
- `/v1/secrets`, `/v1/secrets/{name}`

New schemas: ConnectionDto, ConnectionListResponse, EgressDto, EgressListResponse, GrantDto, GrantListResponse, ObjectBytes, PutConnectionBody, PutEgressBody, PutSecretBody, SecretInfoDto, SecretListResponse.

New tags: `secrets` and `connectors`, inserted before `health`.

## Task Commits

1. **Task 1: bridge.proto + domain types + ControlPlane defaults + FakeControlPlane**: `78650f4`
2. **Task 2: REST routes, gRPC services, presign route outside the stack, parity + OpenAPI**: `c3e0f8f`

## Verification

The worktree was at main `c3e0f8f`, with `CARGO_TARGET_DIR=target/wt-06-08`.
- `cargo test -p ostro-proto -p ostro-api --locked`: all ok. The new tests:
  - ostro-api lib: 46, including 5 domain `bridge_tests`, the fake `bridge_admin_round_trip` and rest `base64_decoding`;
  - bridge_surface: 7;
  - parity: 5, including `bridge::bridge_rest_and_grpc_transcripts_are_equal` (36 steps);
  - ostro-proto lib: 8, including `bridge_messages_roundtrip`;
  - the existing suites (http_smoke, lifecycle_surface, openapi, phase8_surface, rest) are unchanged and green.
- `cargo clippy -p ostro-api -p ostro-proto --all-targets --locked -- -D warnings`: clean.
- `cargo build --workspace --all-targets --locked`: warning-free. ostro-server compiles unchanged.
- `cargo fmt --all --check`: clean.
- Cargo.toml and Cargo.lock are unchanged.

**Not run:** the full `bash scripts/ci-local.sh`. Four other executors were building in parallel, and the brief caps concurrent builds. Only ostro-api and ostro-proto changed. Every dependent crate is covered by the warning-free workspace all-targets build.

## Deviations from Plan

**1. [Rule 3] In-crate base64 decoder for `value_b64`.** ostro-api has no base64 dependency, and only 06-02 edits Cargo files. Rather than stop, a strict standard-alphabet decoder (padding optional, non-canonical trailing bits rejected, unit-tested) was added in rest.rs. No dependency changed.

**2. [Note] No auth layer exists in ostro-api.** The "outside the bearer-auth layer" requirement is met structurally: the presign route is mounted outside the REST middleware stack, as documented in `router_with`. The 401 assertion is skipped because the plan makes it conditional on auth being enabled in the harness.

**3. [Rule 2] Wildcard capture for grant ids, connection names and egress modules as well as secret names and keys.** Grant ids (`secret:acme/billing:stripe/live`), module names (`<tenant>/<name>`, D23) and http-target connection names all contain `/`. The `wildcard_routes` helper keeps the OpenAPI paths in `{x}` form.

**4. [Rule 2] `PresignedObjectRequest` has a redacting `Debug`** (signature and body). The signature is a bearer capability. The type's fields are exactly as specified.

**5. [Note] Delete responses are 204 for every bridge resource**, not only secrets, and lists are wrapped objects, following the 08-10 convention of extensible list shapes.

## Follow-ups for 06-16

- Implement the 15 methods in ostro-server. Call `PutSecret::validate`, `GrantInfo::validate`, `ConnectionInfo::validate` and `EgressInfo::validate` first.
- Store grants as `Grant{id: expected_id(), subject: "module:<module>", scopes: origin:..., body}` (ostro-bridge `GrantSpec::to_record`).
- Wire `presigned_object` to ostro-bridge `serve_local`.
- The CLI verbs `ostro secret|grant|connection|egress` can use these REST routes. The fake helpers `FakeControlPlane::secret_value` and `last_presigned_object` are available to CLI tests.

## Self-Check: PASSED

- `crates/ostro-proto/proto/ostro/api/v1/bridge.proto` and `crates/ostro-api/tests/bridge_surface.rs` exist on main.
- Commits `78650f4` and `c3e0f8f` are on main.
