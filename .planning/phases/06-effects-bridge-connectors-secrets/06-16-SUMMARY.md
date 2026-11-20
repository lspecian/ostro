---
phase: 06-effects-bridge-connectors-secrets
plan: 16
subsystem: server-admin
tags: [secrets, grants, connections, egress, presign, cli, sc3, test-helpers]
requires:
  - "06-08 ControlPlane bridge methods, domain types and validate helpers, REST/gRPC surface, rest::object_routes"
  - "06-14 BridgeServices (sealed_store, object_connector), ServerControlPlane::bridge, test helpers"
  - "06-05 SealedStore, GrantSpec; 06-04 parse_rule; 06-10/11/12 http-target/sql/object config parsers; 06-12 serve_local"
provides:
  - "ostro_server::control::bridge: the 15 bridge admin ControlPlane methods (sealed secrets, grants, connections, egress, presigned_object)"
  - "ostro_meta::MetaState::{secrets, connections, egress_policies} read-only iterators"
  - "CLI verbs ostro secret|grant|connection|egress (ostro_cli::commands::bridge)"
  - "ostro-server tests/common/fx.rs: configure(..).apply(), deploy_fx, start_fx_run, wait_terminal, history, events_of"
affects: [06-17, 06-18, 06-19, 06-20]
tech-stack:
  added: []
  patterns:
    - "Admin writes are create-or-replace CAS loops on the current meta version that keep the record's ext bytes"
    - "Secret values enter the CLI only through stdin / file / env and leave the server never"
key-files:
  created:
    - crates/ostro-server/src/control/bridge.rs
    - crates/ostro-server/tests/bridge_admin.rs
    - crates/ostro-server/tests/common/fx.rs
    - crates/ostro-cli/src/commands/bridge.rs
    - crates/ostro/tests/cli_bridge.rs
  modified:
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/tests/common/mod.rs
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-cli/src/lib.rs
    - crates/ostro-meta/src/state.rs
key-decisions:
  - "No sealing key: put_secret answers 412 precondition_failed with details.reason = sealing_key_missing (ErrorCode is a closed enum; the plan's 'code sealing_key_missing' is carried as the reason, like every other ostro reason code)"
  - "Secret records are named <tenant|_>/<name>; listings strip the prefix of the requested tenant, so __ostro/* (no tenant prefix) never appears, and every secret method refuses __ostro/ names through validate_secret_name"
  - "http-target connection names must equal their normalised origin (OriginPattern round trip, no wildcard); configs are also parsed by HttpTarget::from_connection / SqlConnConfig::parse / ObjectConnConfig::parse and errors surface as 400 naming the field"
  - "presigned_object reads the connection record first only to choose 404 (unknown / not object) or 400 (backend not local); everything else (signature first, then expiry, then the store) is ObjectConnector::serve_local; error bodies are fixed short texts"
  - "CLI: stdin and --from-file drop one trailing newline (echo-friendly), --from-env is verbatim; non-UTF-8 values are sent as value_b64 through an in-crate encoder (ostro-cli has no base64 dependency and only 06-02 edits Cargo files)"
requirements-completed: [FX-05, FX-06, FX-07, FX-08, FX-10]
duration: ~1h
completed: 2026-11-20
---

# Phase 6 Plan 16: Server bridge admin + CLI verbs Summary

**The server now implements every Phase 6 admin method over meta and `BridgeServices`. Secrets are sealed with the node's `SealedStore` as `<tenant|_>/<name>` before the meta write. Without a key the call is refused with `sealing_key_missing`. Values are never returned, and `__ostro/` names and values under 8 bytes are refused. Grants, connections and egress policies go through the bridge parsers and are stored under the 06-DECISIONS conventions. The local presign route is served by `ObjectConnector::serve_local`. `ostro secret|grant|connection|egress` drive the same REST surface and take secret values only from stdin, a file or an env var. Tests prove the canaries never appear in any API response, CLI output, process listing, server log or file in the data directory.**

## Server (`crates/ostro-server/src/control/bridge.rs`)

`control/mod.rs` only gained the delegation lines plus `pub mod bridge`.

| Method | Behaviour |
|---|---|
| `put_secret` | `PutSecret::validate` (grammar, `__ostro/` → 400 `secret_reserved`, < 8 bytes → 400 `secret_too_short`), tenant check (`names::request_tenant`), no key → **412 `precondition_failed`, `details.reason = "sealing_key_missing"`**, message "no sealing key is configured, so secrets cannot be stored: set OSTRO_SEALING_KEY or [secrets] sealing_key_file". Otherwise `seal("<tenant|_>/<name>", value)` → CAS put of `MetaRecord::Secret` → `SecretInfo{name, key_id, tenant}` |
| `list_secrets(tenant)` | records under `<tenant|_>/`, prefix stripped, `__ostro/*` skipped; names and key ids only |
| `delete_secret` | `validate_secret_name` (refuses `__ostro/*`), 404 when absent |
| `put_grant` | `GrantInfo::validate`, each origin through `OriginPattern::parse`, `GrantSpec::validate`, stores `to_record()` (id `secret:<module>:<secret>`, subject `module:<module>`, scopes `origin:<normalised>` + `body`); returns the normalised origins |
| `list_grants(module)` | `secret:` grants, filtered by subject `module:<module>`, parsed with `GrantSpec::from_record` |
| `delete_grant(id)` | only `secret:` ids (else 400), 404 when absent |
| `put_connection` | `ConnectionInfo::validate`; `http-target`: name must equal its normalised origin + `HttpTarget::from_connection`; `object`: `ObjectConnConfig::parse` with `[connectors]` defaults; `sql-*`: `SqlConnConfig::parse` (e.g. a literal password → 400). Config stored as JSON bytes |
| `get/list_connection(s)` | config verbatim (templates, not secrets) |
| `put_egress` | `EgressInfo::validate` + `parse_rule` on every entry (incl. `connection:<name>`); stores `EgressPolicy{name: module, allow}` |
| `presigned_object` | unknown / non-object connection → 404; backend not `local` → 400; then `serve_local(conn, key, GET|PUT, exp, sig, body)`; `Forbidden` (bad / tampered / expired / no key) → 403, `NotFound` → 404, `BadRequest` → 400, `TooLarge` → 413, `Backend` → 500 (logged without the URL) |

Every replace keeps the existing record's `ext` bytes, and all errors name the field without echoing a value. `ostro-meta` gained the read-only iterators `MetaState::{secrets, connections, egress_policies}` (16 lines, additive, no format change), because listing needs them.

## CLI (`crates/ostro-cli/src/commands/bridge.rs`)

| Verb | REST |
|---|---|
| `ostro secret set <name> [--tenant T] [--from-file P \| --from-env V]` | `PUT /v1/secrets/{name}` (stdin by default; a TTY stdin is refused with a hint; a value on argv is a clap usage error, exit 2) |
| `ostro secret ls [--tenant T]` / `rm <name> [--tenant T]` | `GET /v1/secrets` / `DELETE /v1/secrets/{name}` |
| `ostro grant add --module M --secret S --origin O... [--body]` / `ls [--module M]` / `rm <id>` | `PUT /v1/grants` / `GET /v1/grants` / `DELETE /v1/grants/{id}` |
| `ostro connection set <name> --kind K --config-file PATH\|-` / `ls` / `get <name>` / `rm <name>` | `/v1/connections[/{name}]` |
| `ostro egress set <module> --allow RULE...` / `ls` / `get <module>` / `rm <module>` | `/v1/egress[/{module}]` |

`--json` prints the REST body verbatim, or `null` for a delete's 204. Human output is a table for `ls` and `key: value` lines otherwise. No verb prints a secret value, and the server never returns one.

## API-based e2e helpers (`crates/ostro-server/tests/common/fx.rs`, for 06-17..06-19)

```rust
use common::fx::{configure, deploy_fx, start_fx_run, wait_terminal, history, events_of, RunInfo};

let module = deploy_fx(&s, "fx-http").await;          // deploy_named(guest, guest, None) -> "fx-http"
configure(&s)                                           // REST PUTs, in order, panics on non-200
    .secret("stripe/live", CANARY)                      // PUT /v1/secrets/{name} (value / value_b64)
    .tenant_secret("acme", "stripe/live", CANARY)       // ?tenant=acme
    .grant(&module, "stripe/live", &[&mock.base_url()], /*body*/ false) // PUT /v1/grants
    .connection("orders-db", "sql-postgres", json!({"v": 1, ..}))       // PUT /v1/connections/{name}
    .egress(&module, &[&mock.base_url(), "connection:orders-db"])       // PUT /v1/egress/{module}
    .apply().await;
let run: RunId = start_fx_run(&s, "fx-http", "<workflow>", json!({..})).await; // deploys, POST .../runs (201)
let info: RunInfo = wait_terminal(&s, &run, Duration::from_secs(30)).await;  // RunInfo = ostro_api::rest::RunDto
let events: Vec<HistoryEventDto> = history(&s, &run).await;                   // GET .../history, every page
let done = events_of(&events, "effect_completed");
```

- `seg(s)` percent-encodes a path segment. `is_terminal(&RunInfo)` is also exported.
- The raw journal (`Vec<ostro_types::journal::Event>`) is still `TestServer::journal(&run)`.
- Grants and egress policies are keyed by the deployed module name, so deploy with a name (`deploy_fx` / `deploy_named`) before relying on them.

## Task Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 8ed589b | feat(06-16): server bridge admin: sealed secrets, grants, connections, egress, presign route |
| 2 | 831ba6e | feat(06-16): ostro secret\|grant\|connection\|egress verbs and API-based fx test helpers |

## Verification

The worktree was detached at main, rebased onto e50ba14 before the first commit, with `CARGO_TARGET_DIR=target/wt-06-16`. Every run was sequential.

- `cargo test -p ostro-server --test bridge_admin --locked -- --test-threads=1`: **6 passed**:
  - `crud_round_trip_over_rest_and_grpc`;
  - `secret_values_are_sealed_at_rest_and_never_returned`: REST and gRPC canaries are absent from every list response, `/openapi.json` and every gRPC list. Each meta record is `aes256gcm:` ciphertext that opens to the canary. A scan of **every file under the data dir** after shutdown (meta log and snapshots included) finds no canary;
  - `reserved_names_and_short_values_are_refused`: `__ostro/x` → 400 `secret_reserved` over REST and `INVALID_ARGUMENT` over gRPC; 7 bytes → 400 `secret_too_short`, value not echoed; deleting `__ostro/token-key` → 400;
  - `server_profile_without_a_sealing_key_refuses_secrets`: 412 `sealing_key_missing`;
  - `local_presign_route_serves_signed_urls_without_auth`: an unauthenticated client gets 200 for the PUT and the GET. A PUT signature used for a GET, a tampered signature, a different key and an expired URL each get 403. An absent object and an unknown connection get 404. An s3-backend connection gets 400;
  - `fx_helpers_configure_and_run_over_rest`: `configure().apply()`, `start_fx_run("timeouts")`, `wait_terminal` reaching completed, and `history`.
- `cargo test -p ostro-server --lib --locked -- control::bridge`: 2 passed.
- `cargo test -p ostro-cli --locked`: 36 lib tests passed (including 5 new `bridge` tests: RFC 4648 base64 vectors, a value on argv is a usage error, from-file/from-env, parsing), plus 6 in phase8_verbs.
- `cargo test -p ostro --test cli_bridge --locked -- --test-threads=1`: **1 passed**. Every verb ran with `--json` against `ostro dev --ephemeral` at debug log level. The three canaries (stdin, `--from-env`, `--from-file`) were checked against the CLI's `/proc/<pid>/cmdline` while it waited on stdin, every CLI stdout/stderr, and the server's log; none appeared.
- `cargo clippy -p ostro-cli -p ostro -p ostro-server -p ostro-meta --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean.
- No ostro-api file changed, so the OpenAPI snapshot is untouched. Cargo.toml and Cargo.lock are unchanged.

**Not run:**
- `bash scripts/ci-local.sh`: a full ci-local run and two other executors (08-13, 08-14) were running, and the brief asked for modest load.
- The failpoints clippy variant: no failpoint site was touched.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `crates/ostro-meta/src/state.rs` (outside files_modified)**
- **Issue:** `MetaState` had no way to enumerate secrets, connections or egress policies; only `grants()` existed (added by 06-14). Its maps are private.
- **Fix:** three read-only iterators, same shape as `grants()`. No format, command or digest change.
- **Commit:** 8ed589b

**2. [Plan detail] `sealing_key_missing` is a reason, not an `ErrorCode`**
- `ErrorCode` is a closed enum that drives the HTTP and gRPC status. The refusal is `PreconditionFailed` (412 / `FAILED_PRECONDITION`) with `details.reason = "sealing_key_missing"`. This is the same constant (`bridge_services::SEALING_KEY_MISSING`) the presign key store uses.

**3. [Plan detail] fx helper return types**
- `wait_terminal` returns `RunInfo = ostro_api::rest::RunDto`, the REST body.
- `history` returns the REST `Vec<HistoryEventDto>`. The plan wrote `Vec<Event>`, but the helpers are REST-based by design. Raw journal `Event`s remain available through `TestServer::journal`.
- `deploy_fx` and `events_of` were added, because egress and grants need a named deploy.

**4. [Rule 1] `/proc/<pid>/cmdline` race in the CLI test**
- Read right after `spawn`, the child had not exec'd yet and showed an empty argv, so the first run failed.
- The test now polls until the CLI's own arguments are visible, then writes stdin.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/control/bridge.rs, crates/ostro-server/tests/bridge_admin.rs, crates/ostro-server/tests/common/fx.rs, crates/ostro-cli/src/commands/bridge.rs, crates/ostro/tests/cli_bridge.rs on main
- FOUND: 8ed589b, 831ba6e on main
