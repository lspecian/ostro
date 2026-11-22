---
phase: 10-developer-cli-test-kits
plan: 09
subsystem: client
tags: [ostro-client, rest, typed-client, openapi, contract, sdk-07]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-02 ostro-client crate (bundleable manifest, testing feature stub); 10-05 dev_time/dev_skip/replay_module client methods and REST routes"
  - phase: 11-operations-ui
    provides: "11-14 delimited Phase 11 client block (rerun, rerun-cuts, dlq, bulk, limits) in ostro-cli/src/client.rs"
provides:
  - "crates/ostro-client/src/client.rs: the raw REST client (Client, Response, CallError, parse_server, ca_certs, DEFAULT_SERVER, MAX_POLL, REPLAY_EXTRA_TIMEOUT), moved verbatim from ostro-cli"
  - "crates/ostro-client/src/error.rs: ClientError { status: Option<u16>, code, message, details } + From<CallError> + reason()"
  - "crates/ostro-client/src/typed.rs: TypedClient (Client::into_typed / TypedClient::new), serde DTOs with flattened extra fields, ROUTES (33 entries)"
  - "crates/ostro-cli/src/client.rs: `pub use ostro_client::client::*;` shim"
  - "tests/contract.rs: Rust ROUTES + clients/{python,ts,go} ROUTES tables vs `ostro openapi`; OSTRO_REQUIRE_ALL_CLIENTS=1 for the 10-20 gate"
  - "tests/typed.rs + tests/common_p10.rs: recording-server route check, spawned `ostro dev --ephemeral` e2e, ready-line parser (D-P10-9 amended regex)"
affects: [10-08, 10-11, 10-13, 10-14, 10-16, 10-17, 10-20, phase-11-followups]
tech-stack:
  added: []
  patterns:
    - "Typed surface on a wrapper type (TypedClient) so the raw Client keeps its method names (CLI unchanged)"
    - "Response DTOs keep unknown fields in `#[serde(flatten)] extra` (additive server changes never break clients)"
    - "Route coverage: one `(METHOD, path)` table per client, path params normalised to `{}`, text-parsed for polyglot tables"
key-files:
  created:
    - crates/ostro-client/src/client.rs
    - crates/ostro-client/src/error.rs
    - crates/ostro-client/src/typed.rs
    - crates/ostro-client/tests/contract.rs
    - crates/ostro-client/tests/typed.rs
    - crates/ostro-client/tests/common_p10.rs
  modified:
    - crates/ostro-client/src/lib.rs
    - crates/ostro-cli/src/client.rs
key-decisions:
  - "Typed methods live on `TypedClient`, not on `Client`: the raw methods already use the names start_run/get_run/history/signal/cancel/... (an inherent duplicate does not compile; an extension trait would be shadowed). `client.into_typed()` / `TypedClient::new()`, `typed.raw()` for the raw client"
  - "ROUTES covers the raw named methods plus the typed-only Phase 11 inventory/failure-policy wrappers (modules, queues, partitions, workflow/queue failure-policy GET/PUT), so every Phase 11 route the coordinator listed is guarded. Generic `Client::call` routes built in CLI command files (Phase 6-8 secrets/grants/schedules/webhooks/...) are not enumerated"
  - "StartRun has no `run_id` field: StartRunBody has none (ids are server-assigned); repeatable starts use idempotency_key. `conflict` serialises as `conflict_policy` (the wire name)"
  - "RunResult.output is the wire field `result`; HistoryPage.next is `next_from_seq`"
  - "Contract test gets the document from the built `ostro openapi` (ostro-client cannot dev-depend on ostro-api: Cargo files are 10-02-only and the crate is bundleable)"
  - "Unknown-run ClientError: a malformed id is 400 invalid_argument (run ids are 29-char Crockford base32); a well-formed unknown id is 404 not_found. The test checks both"
  - "The user agent stays `ostro-cli/<version>` (verbatim move; both crates are 0.1.0)"
requirements-completed: []
duration: ~1h10m
completed: 2026-11-22
---

# Phase 10 Plan 09: ostro-client crate Summary

**The REST client now lives in `ostro-client` (CLI re-exports it, every snapshot byte-identical), with a typed `TypedClient` surface, a single `ClientError`, and a contract test that checks every route of the Rust client (and, once 10-14 lands, the Python/TS/Go ROUTES tables) against `ostro openapi`.**

## Moved methods (verbatim, `crates/ostro-cli/src/client.rs` at f007a07)

- Free items: `DEFAULT_SERVER`, `MAX_POLL`, `CallError` (+`message`), `Response`, `Client`, `parse_server`, `ca_certs`.
- 05-14: `Client::new`, `with_ca`, `base`, `url`, `upload_module`, `list_workflows`, `activate`, `start_run`, `list_runs`, `get_run`, `history`, `signal`, `cancel`, `terminate`, `retry`, `result_once`, `complete_token`; Phase 8 generic `call`.
- Phase 11 (11-14) block, as-is: `rerun`, `rerun_cuts`, `list_dlq`, `requeue_dlq`, `dismiss_dlq`, `create_bulk`, `list_bulk`, `get_bulk`, `cancel_bulk`, `limits`. (`ls --tag/--newest/--dlq` uses `list_runs` with query pairs; no separate method.)
- Phase 10 (10-05) block: `REPLAY_EXTRA_TIMEOUT`, `dev_time`, `dev_skip`, `replay_module` (+ private `replay_url`).
- Unit tests `server_urls`, `segments_are_encoded`, `phase10_urls`, `api_error_message`.

`crates/ostro-cli/src/client.rs` is now a 2-line doc comment + `pub use ostro_client::client::*;`. No command file changed; `crate::client::{Client, CallError, MAX_POLL, DEFAULT_SERVER, parse_server, ca_certs}` all resolve through the shim.

**Base:** f007a07. client.rs did not move during the plan (main gained 10-07 and 10-08 commits, neither touched `ostro-cli/src/client.rs` or `ostro-client/src`). 11-14 has a SUMMARY (not in progress).

## Typed API (`ostro_client::typed`, re-exported `TypedClient`, `StartRun`)

| Method | Returns |
|---|---|
| `upload_module(wasm, name?, tenant?)` (sha computed) | `UploadOutcome { created, module: ModuleInfo { sha256, size_bytes, workflows: [WorkflowVersion] } }` |
| `activate(workflow, version)` | `WorkflowInfo { name, active_version, versions, record_version }` |
| `workflows()` | `Vec<WorkflowInfo>` |
| `start_run(&StartRun { workflow, input, idempotency_key, conflict, tags, retention, version, queue, extra })` | `StartedRun { run_id, created, dry_run }` |
| `get_run(id)` | `RunInfo { run_id, workflow, version, module_sha256, queue, status, tags, result, failure, created_at_ms, completed_at_ms }` |
| `list_runs(&RunFilter { workflow, status, queue, tags, limit, page_token })` | `RunList { runs, next_page_token }` |
| `history(id, from_seq?, limit?)` | `HistoryPage { events, next }` |
| `result(id, wait: Option<Duration>)` (loops long polls up to `wait`) | `RunResult { run_id, completed, status, output, failure }` |
| `signal(id, name, payload, request_id?)` | `SignalOutcome { accepted, duplicate, dry_run }` |
| `cancel(id, reason?)` / `terminate(id, reason?)` / `retry(id)` | `CancelOutcome` / `TerminateOutcome` / `RetryOutcome` |
| `complete_token(token, payload)` | `TokenOutcome { run_id, accepted, dry_run }` |
| `dev_time()` / `dev_skip_by(Duration)` / `dev_skip_to(ms)` | `DevTime { now_ms, offset_ms, mode, idle, next_due_ms }` / `DevSkip { now_ms, offset_ms, skipped_ms }` |
| `replay(sha, &ReplaySelect::{Runs, Sample{n, workflow, status}}, allow_missing)` | `ReplayReport { module_sha256, verdicts: [ReplayVerdict], summary: ReplaySummary }` |
| Phase 11: `rerun`, `rerun_cuts`, `dlq`, `requeue_dlq`, `dismiss_dlq`, `create_bulk`, `bulk_jobs`, `bulk_job`, `cancel_bulk`, `limits`, `modules`, `queues`, `partitions`, `failure_policy(PolicyTarget, name)`, `set_failure_policy(target, name, policy, version_tag?)` | `serde_json::Value` (raw body) |

Every response struct has `#[serde(flatten)] extra: Map<String, Value>`. All methods return `Result<T, ClientError>`; `ClientError.code` is the server code, `unreachable` (transport), `usage` (local input) or `decode` (2xx body of the wrong shape).

## Contract coverage table (`cargo test -p ostro-client --test contract -- --nocapture`)

84 `/v1` operations in `ostro openapi`; Rust ROUTES = 33, all present. Python/TS/Go: **pending (10-14)** — `clients/` does not exist yet (10-04 scaffold not landed; the ROUTES tables are 10-14's). Missing tables print `pending` and do not fail; `OSTRO_REQUIRE_ALL_CLIENTS=1` makes them fail (10-20 gate).

Rust-covered operations: PUT `/v1/modules/{}`, GET `/v1/workflows`, PUT `/v1/workflows/{}/active`, POST `/v1/workflows/{}/runs`, GET `/v1/runs`, GET `/v1/runs/{}`, GET `/v1/runs/{}/history`, GET `/v1/runs/{}/result`, POST `/v1/runs/{}/signals/{}`, POST `/v1/runs/{}/{cancel,terminate,retry}`, POST `/v1/tokens/complete`, GET `/v1/dev/time`, POST `/v1/dev/time/skip`, POST `/v1/modules/{}/replay`, POST `/v1/runs/{}/rerun`, GET `/v1/runs/{}/rerun-cuts`, GET `/v1/dlq`, POST `/v1/dlq/{}/{requeue,dismiss}`, POST/GET `/v1/bulk`, GET `/v1/bulk/{}`, POST `/v1/bulk/{}/cancel`, GET `/v1/limits`, GET `/v1/{modules,queues,partitions}`, GET/PUT `/v1/{workflows,queues}/{}/failure-policy`.

Not in Rust ROUTES (reached via `Client::call` from CLI command files, or UI/worker-only): connections, egress, grants, secrets, schedules, webhooks, workers/tokens/join/drain, queues/{}/{demand,flow,policy,pause,resume}, workflows/{}/{policy,pause,resume}, runs/{}/{logs,pause,resume}, objects, admin/backup, GET `/v1/modules/{}`.

The checker itself is tested: an injected `GET /v1/nope` and `DELETE /v1/runs/{id}` are reported by name; the pair parser accepts the Python tuple, TS array and Go brace forms.

## Commits

- 77725ab refactor(10-09): move the REST client into ostro-client; ostro-cli re-exports it
- e3d9b18 feat(10-09): typed ostro-client surface and four-client OpenAPI contract test

## Verification

- `cargo test -p ostro-client --locked -- --test-threads=1`: lib 10, common_p10 1, contract 5, typed 4, doc 1 — all pass. Spawned e2e: upload/re-upload, activate, workflows, start, get, result (completed), history (step_completed), list by tag, signal → result = payload, cancel → cancelled, terminate, 400 invalid_argument / 404 not_found, dev time 409 `time_skipping_disabled`, replay 501 `unimplemented` (adapts once 10-10/10-11 land); `--time-skipping` test prints pending until the flag exists.
- `cargo test -p ostro --test cli_verbs --test cli_p11 --test cli --test p10_scaffold -- --test-threads=1`: 7 + 6 + 5 + 7 pass; `git diff --exit-code -- crates/ostro/tests/snapshots/` clean (re-run after rebasing onto 10-08).
- `cargo test -p ostro-cli --locked`: 66 + 6 pass. `cargo test -p ostro-build --test sdk_bundle` (bundles ostro-client/src): 6 pass.
- `cargo clippy -p ostro-client -p ostro-cli --all-targets --all-features --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean. `RUSTDOCFLAGS=-D warnings cargo doc -p ostro-client --no-deps`: clean. `cargo check --workspace --all-targets --locked` after the final rebase: clean.
- Full `scripts/ci-local.sh --no-musl` was NOT run (shared machine with parallel builds).

## Deviations from Plan

- **[Rule 3] Typed methods on `TypedClient` instead of an extension impl on `Client`.** Name collision with the raw methods the CLI uses; renaming those would edit command files (forbidden).
- **[Rule 1] No `run_id` in `StartRun`.** The REST start body has no such field.
- **[Rule 2] ROUTES also lists the raw methods' routes and typed wrappers for Phase 11 inventory/failure-policy**, so the contract covers every Phase 11 route (coordinator request), not just the plan's typed list.
- **Unknown-run test uses a well-formed id.** `no-such-run` is 400 invalid_argument; the 404 path needs a valid 29-character id.
- **Worktree / target dir:** the coordinator's scratchpad worktree and `target/slot-6`, not `/tmp/ostro-wt-10-09`.
- Pending label is "pending (10-14 …; 10-04 is only the scaffold)".

## Open issues / notes for later plans

- 10-14: tables at `clients/python/ostro_client/routes.py`, `clients/ts/src/routes.js`, `clients/go/ostro/routes.go`; any `"METHOD", "/path"` pair is read (either quote char, any bracket). An existing file with zero pairs fails.
- 10-17: `tests/common_p10.rs` has the spawn + ready-line helpers to lift into `ostro_client::testing`.
- Bundled copy (10-08): `src/lib.rs` keeps `#[cfg(test)]` dev-dep anchors (axum) and client.rs unit tests use tempfile; the bundle strips dev-deps, so `cargo test` *of the bundled crate itself* would not build (unchanged from 10-02's pattern; using it as a dependency is fine).
- Phase 11 follow-ups add client methods in a new `crates/ostro-client/src/<module>.rs` and append to `typed::ROUTES`.

## Self-Check: PASSED

- Files exist: crates/ostro-client/src/{client,error,typed}.rs, crates/ostro-client/tests/{contract,typed,common_p10}.rs.
- Commits 77725ab and e3d9b18 are on main.
