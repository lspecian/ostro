---
phase: 06-effects-bridge-connectors-secrets
plan: 09
subsystem: rust-sdk
tags: [sdk, effects, idempotency, guests, fx-02, fx-04, fx-05]
requires:
  - "04-05/04-06 Rust SDK (Ctx, Op, export!, #[workflow])"
  - "WIT ostro:runtime@0.1.0 http-request / sql-request / object-request (unchanged)"
provides:
  - "ostro_sdk::effects: HttpClient, HttpBuilder, SqlBuilder, ObjectClient, ObjectPutInfo, ObjectHead, idempotency_key(run_id, name), IDEMPOTENCY_KEY_DOMAIN"
  - "Ctx::http(), Ctx::sql(conn), Ctx::object(conn), Ctx::idempotency_key(name)"
  - "guests fx-http, fx-sql, fx-object, fx-canary, fx-ssrf (workflows fx-http/fx-sql/fx-object/fx-canary/fx-ssrf v1; wasm fx_http.wasm ... fx_ssrf.wasm)"
affects: [06-15..06-19 e2e plans (guest I/O contracts below), 06-20 docs]
tech-stack:
  added: ["sha2 =0.10.9 in sdks/rust/ostro-sdk (same pin as guests/)"]
  patterns:
    - "Builders are thin over the WIT records and expose build()/request() so records are unit-testable on the host without a component"
    - "Test guests take their whole script from the run input and report per-item results; a cancelled wait ends the run instead of being reported"
key-files:
  created:
    - sdks/rust/ostro-sdk/src/effects.rs
    - sdks/rust/ostro-sdk/tests/effects.rs
    - guests/fx-http/Cargo.toml
    - guests/fx-http/src/lib.rs
    - guests/fx-sql/Cargo.toml
    - guests/fx-sql/src/lib.rs
    - guests/fx-object/Cargo.toml
    - guests/fx-object/src/lib.rs
    - guests/fx-canary/Cargo.toml
    - guests/fx-canary/src/lib.rs
    - guests/fx-ssrf/Cargo.toml
    - guests/fx-ssrf/src/lib.rs
  modified:
    - sdks/rust/ostro-sdk/Cargo.toml
    - sdks/rust/ostro-sdk/src/ctx.rs
    - sdks/rust/ostro-sdk/src/lib.rs
key-decisions:
  - "Workflow names follow the existing guest convention (crate name, hyphenated): fx-http, fx-sql, fx-object, fx-canary, fx-ssrf; the wasm artifacts are fx_http.wasm etc."
  - "ctx.http() returns an HttpClient (get/post/put/delete/request) that yields an HttpBuilder; the call chain is the one in the plan"
  - "HttpBuilder::json returns Result<Self, StepError> (invalid_json), so a chain uses `.json(&v)?`"
  - "fx-http parallel=true issues every request before the first wait, then waits on each op in order (all in flight at once; public Op::wait only, no hidden join API)"
  - "Guest error objects carry an extra `retryable` bool next to kind/message"
  - "sdks/rust/Cargo.lock is gitignored (04-20 policy), so no lock was committed"
requirements-completed: [FX-02, FX-04, FX-05]
metrics:
  duration: ~25 min
  completed: 2026-11-20
---

# Phase 6 Plan 09: Rust SDK effect builders and Phase 6 test guests Summary

The Rust SDK now has thin builders over the unchanged WIT records: `ctx.http().post(url).secret_header(..).json(..)?.idempotency_key(..).send()`, `ctx.sql(conn).exactly_once().query(stmt, params)` and `ctx.object(conn).get/put/head/delete/presign_get/presign_put`. It also has the P-11 semantic key `ctx.idempotency_key(name)`, pinned by vectors. Five input-scripted guests (fx-http, fx-sql, fx-object, fx-canary, fx-ssrf) are ready for the e2e plans.

## Tasks

| Task | Name | Commit |
| ---- | ---- | ------ |
| 1 | SDK effect builders + idempotency_key | f70a1f0 |
| 2 | Five Phase 6 test guests | f04d668 |

## SDK surface

- `idempotency_key(run_id, name)` returns `"ostro-" + hex(sha256(b"ostro/idem/v1/name" || run_id || 0x00 || name))[..32]`, 38 characters. Six vectors are pinned in `tests/effects.rs`; they were computed with Python hashlib and include two run ids for the same name and the `("ab","c")` / `("a","bc")` separator case. `Ctx::idempotency_key` feeds in `context::info().run_id`.
- `HttpBuilder::{new, header, secret_header, body, json, idempotency_key, timeout, retry, redact, build, send}`:
  - `secret_header` panics unless the value contains `{{secret:`.
  - `json` replaces any existing content-type header with `content-type: application/json`.
  - `retry` takes the SDK `RetryPolicy`.
- `SqlBuilder::{new, exactly_once, request, query}`. Without `exactly_once()`, `exactly_once` is false.
- `ObjectClient::{new, request, raw, get, put, head, delete, presign_get, presign_put}`:
  - `put` decodes to `ObjectPutInfo {e_tag}`.
  - `head` decodes to `ObjectHead {size, last_modified, e_tag, version}`. Both match the 06-12 connector JSON.
  - `presign-*` returns a `String`.
  - Failures (including `unsupported_op`) come back as a `StepError` from `wait`.
- The module docs give the W1-W8 semantics, one line each.
- Re-exported at the crate root: `HttpBuilder, HttpClient, ObjectClient, ObjectHead, ObjectPutInfo, SqlBuilder, SqlResult, SqlValue`.

## Guest I/O contracts (for the e2e plans)

Build all of them with `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests`. Each guest exports one workflow, version 1. Every error item is `{"error":{"kind","message","retryable"}}`. If a wait is cancelled, the run ends cancelled.

**fx-http** (`fx_http.wasm`, workflow `fx-http`)
- Input: `{"requests":[{"method","url","headers":[[k,v]],"body_b64"?,"idempotency_key"?,"semantic_key"?,"timeout_ms"?,"retry"?,"redact":[..]}],"parallel":bool}`.
  - `retry` = `{"max_attempts","initial_interval_ms"?,"max_interval_ms"?,"backoff_coefficient"? (default 1.0),"non_retryable_kinds"?}`.
  - `headers`, `redact` and `parallel` default to empty/false.
- `semantic_key`: the override becomes `ctx.idempotency_key(semantic_key)`, and the key is written into the body as `"idempotency_key"` with `content-type: application/json`.
  - If `body_b64` decodes to a JSON object, the key is merged into it. Otherwise the body becomes `{"idempotency_key": key}`.
  - `semantic_key` takes precedence over `idempotency_key`.
- `parallel=true`: all requests are issued first, then each op is awaited in order. Otherwise each request is issued and awaited in turn.
- Output: `{"results":[{"status","headers":[[k,v]],"body_b64"} | {"error":{..}}]}`, in request order.

**fx-sql** (`fx_sql.wasm`, workflow `fx-sql`)
- Input: `{"connection","statement","params":[..],"exactly_once":bool,"repeat":n}`.
  - `params` defaults to `[]`, `exactly_once` to false and `repeat` to 1.
  - Each repetition is a separate effect, run sequentially.
- Params can be plain JSON: `null`, a bool, an integer (int8), any other number (float8) or a string (text). They can also be a one-key tagged object, as in the output.
- Output: `{"results":[{"columns":[..],"rows":[[cell..]..],"rows_affected":n} | {"error":{..}}]}`.
- Output cells are always tagged: `{"null":null}`, `{"boolean":b}`, `{"int8":n}`, `{"float8":f}`, `{"numeric":"s"}`, `{"text":"s"}`, `{"bytea":"<base64>"}`, `{"json":"s"}` or `{"timestamptz":{"seconds":n,"nanoseconds":n}}`.

**fx-object** (`fx_object.wasm`, workflow `fx-object`)
- Input: `{"connection","ops":[{"op","key","body_b64"?}]}`.
  - Ops run in order, one effect each.
  - The op string is passed through verbatim, so unknown ops reach the host and fail `unsupported_op`.
- Output: `{"results":[{"bytes_b64"} | {"error":{..}}]}`. `bytes_b64` is the raw W7 output:
  - `get`: the object bytes;
  - `put` / `head`: JSON;
  - `delete`: empty;
  - `presign-*`: the URL.

**fx-canary** (`fx_canary.wasm`, workflow `fx-canary`)
- Input: `{"echo_url","secret":"<name>","header":"X-Api-Key","in_body":bool,"redact"?:bool}`.
  - `header` defaults to `X-Api-Key`; `in_body` and `redact` default to false.
- It POSTs to `echo_url` with `<header>: {{secret:<name>}}`.
  - With `in_body`, the body is `{"secret":"{{secret:<name>}}"}`. Bodies are never expanded (W1), so the echo should show the literal template.
  - With `redact`, the header name is added to `redact`.
- Output: `{"observed":{"status","headers":[[k,v]],"body_b64"} | {"error":{..}},"env":[[k,v]],"args":[..],"random":"<32 hex>"}`.
  - `env` is `std::env::vars()` and `args` is `std::env::args()`, both read after the response, in shim order.
  - `random` is `ctx.random_bytes(16)`.
- The guest holds only the template and the secret name, never a secret value.

**fx-ssrf** (`fx_ssrf.wasm`, workflow `fx-ssrf`)
- Input: `{"urls":[..]}`. One GET per URL, run sequentially.
- Output: `{"results":[{"url","status"} | {"url","error":{..}}]}`.

## Verification

- `cargo test --manifest-path sdks/rust/Cargo.toml`: ostro-sdk lib 29 passed, tests/effects.rs 9 passed, macros 7 passed + trybuild 1 passed, doctests ok.
- `cargo clippy --manifest-path sdks/rust/Cargo.toml --all-targets -- -D warnings` and `--target wasm32-wasip2 -- -D warnings` are clean. `cargo fmt --check` is clean. `RUSTDOCFLAGS=-D warnings cargo doc` is clean.
- Guest release build for wasm32-wasip2: all five `fx_*.wasm` artifacts are produced (alongside `log_spammer.wasm`), with 0 warnings.
- `cargo clippy --manifest-path guests/Cargo.toml --target wasm32-wasip2 -- -D warnings` is clean, and so is guests `cargo fmt --check`.
- `examples/order-saga` (depends on ostro-sdk): fmt check and wasm clippy `-D warnings` are clean.
- `git diff 4968a13 main -- wit/` is empty.
- Builds used `--offline` and per-agent target dirs (`target/wt-06-09`, `target/wt-06-09/guests`).
- The local base64 helper was checked against coreutils `base64` with a standalone roundtrip.
- The full `bash scripts/ci-local.sh` (whole root workspace) was not run. This plan touches only the separate SDK and guests workspaces, and every SDK, guest and order-saga step of ci-local was run individually.

## Deviations from Plan

- **Workflow name format.** The plan says "named after the crate (`fx_http`, …)". The workflows are registered as `fx-http`, etc., the crate names, matching every existing guest (`sdk-basic`, ...). `fx_http` is the wasm file name.
- **TDD commits.** Task 1 was written test-first: the vectors were computed independently before the implementation existed. Tests and implementation landed in one commit rather than separate RED/GREEN commits, which keeps the number of CAS commits down while other plans run in parallel.
- **sdks/rust/Cargo.lock.** It is in `.gitignore` (04-20 policy), so the plan's conditional "commit if it changes" does not apply.

Otherwise the plan was executed as written.

## Self-Check: PASSED

- Files: all 15 files in key-files exist at main f04d668.
- Commits: f70a1f0 and f04d668 are on main.
