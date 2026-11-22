---
phase: 10-developer-cli-test-kits
plan: 14
subsystem: clients
tags: [sdk-07, clients, python, typescript, go, route-contract, time-skipping, e2e]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-04 clients/{python,ts,go} scaffolds; 10-09 ostro-client typed::ROUTES + tests/contract.rs (table formats); 10-10 DevClock (GET /v1/dev/time, POST /v1/dev/time/skip, ostro dev --time-skipping=manual) + guests/p10-time; 10-11 POST /v1/modules/{sha}/replay"
provides:
  - "clients/python: ostro_client.Client (urllib/json), OstroError, ROUTES (routes.py), py.typed, README; pytest unit + e2e (conftest fixtures stub / ostro_server / p10_time_wasm)"
  - "clients/ts: Client / OstroError / ROUTES / parseDuration / sha256Hex (src/client.js, src/routes.js), hand-written src/index.d.ts, README; node:test unit + e2e"
  - "clients/go/ostro: NewClient, context-aware methods, *ostro.Error, Routes (routes.go), README; go test unit + e2e"
affects: [10-17, 10-20]
tech-stack:
  added: []
  patterns:
    - "One literal ROUTES table per client, each method passes its (METHOD, template) to the transport; a unit test calls every public method and asserts used routes == ROUTES"
    - "Long-poll: per-request timeout = client timeout + wait, wait capped at 60 s per poll and looped (mirrors ostro-client)"
    - "Spawned e2e: OSTRO_BIN (else <repo>/target/debug/ostro), env scrubbed of OSTRO_*, D-P10-9 ready-line regex with (.+?) name group, guest via OSTRO_E2E_WASM else cargo build into <repo>/target/guests"
key-files:
  created:
    - clients/python/README.md
    - clients/python/ostro_client/client.py
    - clients/python/ostro_client/errors.py
    - clients/python/ostro_client/routes.py
    - clients/python/ostro_client/py.typed
    - clients/python/tests/conftest.py
    - clients/python/tests/test_client_unit.py
    - clients/python/tests/test_e2e.py
    - clients/ts/README.md
    - clients/ts/src/client.js
    - clients/ts/src/routes.js
    - clients/ts/test/client.test.js
    - clients/ts/test/e2e.test.js
    - clients/go/README.md
    - clients/go/ostro/client.go
    - clients/go/ostro/errors.go
    - clients/go/ostro/routes.go
    - clients/go/ostro/client_test.go
    - clients/go/ostro/e2e_test.go
  modified:
    - clients/python/pyproject.toml
    - clients/python/ostro_client/__init__.py
    - clients/ts/src/index.js
    - clients/ts/src/index.d.ts
key-decisions:
  - "Transport failures: code unreachable (no connection) and timeout (client timeout / deadline) in all three; Go adds cancelled (context cancelled) and Unwrap to the context error"
  - "start_run has no run_id parameter: StartRunBody has no run_id field (frozen REST contract); further body fields pass through **extra / opts.extra / StartRunRequest.Extra"
  - "The e2e accepts get_run status running|scheduled|ready for the live run: a run suspended on its signal wait reads scheduled (Intent::Suspend -> RunStatus::Scheduled), racing with running during activation"
metrics:
  duration: "about 1h45m"
  completed: 2026-11-23
---

# Phase 10 Plan 14: Python/TS/Go clients Summary

Three hand-written, zero-dependency REST clients (Python `urllib`, TS global `fetch`, Go `net/http`) covering the D-P10-8 core surface. Each has a literal route table that the 10-09 contract test checks against `ostro openapi` (all four clients covered, none pending), unit tests on local stubs, and the shared time-skipping e2e scenario against a spawned `ostro dev --ephemeral --time-skipping=manual`.

## Method tables

| Route | Python (`Client`) | TS (`Client`) | Go (`*ostro.Client`) |
| --- | --- | --- | --- |
| `PUT /v1/modules/{sha256}` | `upload_module(wasm, name=None, tenant=None)` | `uploadModule(bytes, {name, tenant})` | `UploadModule(ctx, wasm, name, tenant)` |
| `GET /v1/workflows` | `workflows()` | `workflows()` | `Workflows(ctx)` |
| `PUT /v1/workflows/{name}/active` | `activate(workflow, version)` | `activate(workflow, version)` | `Activate(ctx, workflow, version)` |
| `POST /v1/workflows/{name}/runs` | `start_run(workflow, input=None, *, idempotency_key, tags, conflict_policy, version, queue, **extra)` | `startRun(workflow, input, {idempotencyKey, tags, conflictPolicy, version, queue, extra})` | `StartRun(ctx, StartRunRequest)` |
| `GET /v1/runs/{run_id}` | `get_run(run_id)` | `getRun(runId)` | `GetRun(ctx, runID)` |
| `GET /v1/runs` | `list_runs(*, workflow, status, queue, tags, limit, page_token)` | `listRuns({...})` | `ListRuns(ctx, RunFilter)` |
| `GET /v1/runs/{run_id}/history` | `history(run_id, *, from_seq, limit)` | `history(runId, {fromSeq, limit})` | `History(ctx, runID, fromSeq, limit)` |
| `GET /v1/runs/{run_id}/result` | `result(run_id, wait=None)` ("30s" or seconds) | `result(runId, {wait})` ("30s" or ms) | `Result(ctx, runID, wait time.Duration)` |
| `POST /v1/runs/{run_id}/signals/{name}` | `signal(run_id, name, payload=None, *, request_id)` | `signal(runId, name, payload, {requestId})` | `Signal(ctx, runID, name, payload, requestID)` |
| `POST /v1/runs/{run_id}/cancel` | `cancel(run_id, reason=None)` | `cancel(runId, reason)` | `Cancel(ctx, runID, reason)` |
| `POST /v1/runs/{run_id}/terminate` | `terminate(run_id, reason=None)` | `terminate(runId, reason)` | `Terminate(ctx, runID, reason)` |
| `POST /v1/runs/{run_id}/retry` | `retry(run_id)` | `retry(runId)` | `Retry(ctx, runID)` |
| `POST /v1/tokens/complete` | `complete_token(token, payload=None)` | `completeToken(token, payload)` | `CompleteToken(ctx, token, payload)` |
| `GET /v1/dev/time` | `dev_time()` | `devTime()` | `DevTime(ctx)` |
| `POST /v1/dev/time/skip` | `dev_skip(by=None, *, to_ms=None)` | `devSkip({by} \| {toMs})` | `DevSkipBy(ctx, d)` / `DevSkipTo(ctx, toMs)` |
| `POST /v1/modules/{sha}/replay` | `replay(sha, *, runs \| sample, workflow, status, allow_missing)` | `replay(sha, {runs \| sample, ...})` | `Replay(ctx, sha, ReplayRequest)` |

Errors: `OstroError` (Python, TS) and `*ostro.Error` (Go) carry status, code, message and details from `{"error": {...}}`. Connection failures give `unreachable`, timeouts `timeout`, and non-JSON error bodies `http_<status>`. Path values are percent-encoded one segment at a time, so `acme/order` becomes `acme%2Forder`.

## Shared e2e scenario (wall times)

The scenario runs the same way in each language:

1. Upload and activate p10-time.
2. Start `signal_then_sleep` and check that `get_run` shows the run live.
3. Send the signal `go {ok: true}`.
4. Poll history until `timer_started` appears.
5. Call `dev_skip("24h")`, which reports `skipped_ms >= 1 d`.
6. Call `result(wait 30 s)`: it is completed and returns `{ok: true}`.
7. Check that history shows `timer_fired` and that `dev_time` reports an offset of at least 1 d.
8. Call `get_run` on an unknown well-formed id and check that it gives 404.

| Language | Wall time (scenario, after the server is ready) |
| --- | --- |
| Python | 0.24 s |
| TS | 0.47–0.69 s |
| Go | 0.46–0.56 s |

Flake check: the Go e2e passed 30 times in a row and the Python and TS e2e 15 times each, after the status fix below.

## Packaging dry runs (nothing published)

- `uv build --out-dir target/dist/clients-python` produces two artefacts:
  - `ostro_client-0.1.0-py3-none-any.whl` containing `ostro_client/{__init__,client,errors,routes}.py`, `ostro_client/py.typed` and dist-info (METADATA, WHEEL, RECORD). There is no `Requires-Dist`; `Requires-Python: >=3.9`.
  - `ostro_client-0.1.0.tar.gz` containing `ostro_client/**`, README.md, pyproject.toml, PKG-INFO and the VCS-root `.gitignore` (hatchling adds it, as for `sdks/python`).
- `npm pack --dry-run --json` gives `ostro-client 0.1.0` with `README.md`, `package.json`, `src/client.js`, `src/index.d.ts`, `src/index.js` and `src/routes.js` only.
- Go: `go vet ./...` is clean, and `go.mod` has no `require` lines.

## Contract coverage (`OSTRO_REQUIRE_ALL_CLIENTS=1 cargo test -p ostro-client --test contract -- --nocapture`)

```
test rust_client_routes_are_in_openapi ... ok
client python: 16 routes, 0 missing
client ts: 16 routes, 0 missing
client go: 16 routes, 0 missing
route coverage (84 /v1 operations; clients: rust, python, ts, go)
test polyglot_client_routes_are_in_openapi ... ok
test result: ok. 5 passed; 0 failed
```

## Tests

- Python `pytest tests -q` (3.12, pytest 8.4.2): 17 passed. That is 16 unit tests (15 of them stub-based plus a route-introspection test) and the e2e test. The unit tests also pass on 3.9.
- TS `node --test 'clients/ts/test/**/*.test.js'`: 18 pass, 0 fail. This includes the route-introspection test and a test that every export and Client method appears in `index.d.ts`.
- Go `go vet ./... && go test ./...`: ok, with 14 unit tests, the e2e test and the scaffold test.
- `cargo test -p ostro-build --test sdk_bundle` (after `touch build.rs`): 6 passed, and the bundle picks up the new client files.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] e2e "get_run shows running" was flaky.**
- **Found during:** Task 2, Go e2e. The status stayed `scheduled` for 30 s while history showed `[run_started, signal_wait_started]`. TS had one unexplained failure that was probably the same thing.
- **Issue:** a run suspended on a wait is stored as `RunStatus::Scheduled` (`Intent::Suspend` in `ostro-partition/src/apply/run.rs`). The REST status of a parked run is therefore `scheduled`, or `running` while an activation is in flight. This is a server property, not a client bug.
- **Fix:** all three e2e tests accept `running|scheduled|ready` and check the workflow name.
- **Commit:** 480c7f2

### Other deviations

- **Start route:** the plan's illustrative `("POST", "/v1/runs")` route does not exist. The real start route is `POST /v1/workflows/{name}/runs`, as in typed.rs and OpenAPI.
- **No `run_id` on `start_run`:** the plan's Python signature had `run_id=None`, but `StartRunBody` has no such field. It was left out rather than silently ignored.
- **Extra error codes:** the plan named only `unreachable`. The clients also use `timeout`, and Go also uses `cancelled`.
- **Workspace and commits:** work happened in a scratchpad worktree with CARGO_TARGET_DIR slot-6, as the brief says, not `/tmp/ostro-wt-10-14`. Commits used the worktree index and were landed with CAS.
- **Stale comment:** `clients/go/ostro/doc.go` still says "Scaffold (plan 10-04) ... arrive with plan 10-14". The file is not in files_modified, so it was left as is. 10-17 can refresh it.

## Deferred / open

- The full `scripts/ci-local.sh --no-musl` was not run. The client steps it runs (`check_clients_python/ts/go`) were run as the same commands and are green.
- The OpenAPI description of `RunStatusDto::Scheduled` ("Waiting for its start delay") does not match the server's use of it for suspended runs. That is a doc mismatch owned by the API, not a client issue.

## Self-Check: PASSED

- The commits exist: 91b0b44 and 480c7f2.
- All files in key-files.created are present on main.
