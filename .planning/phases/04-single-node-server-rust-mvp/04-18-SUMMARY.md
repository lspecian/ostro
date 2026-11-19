---
phase: 04-single-node-server-rust-mvp
plan: 18
subsystem: control-plane-runs
tags: [api, runs, idempotency, history, list, pagination, control-plane]
requires:
  - phase: 04-09
    provides: StartRun fields 14..18 (module_sha, tags, tenant, traceparent, run_timeout_ms)
  - phase: 04-12
    provides: RunRecord Phase 4 fields, query::{run_record, run_payload, journal_rows, runs_page}
  - phase: 04-16
    provides: EngineExecutor (input as application/json, result payload, failure JSON)
  - phase: 04-17
    provides: ServerControlPlane helpers, TestServer harness, WorkflowVersion records
provides:
  - ostro_server::control::runs (start_run, get_run, outcome/partition error mapping, DEFAULT_QUEUE)
  - ostro_server::control::history (run_history, kind_name, details, payload_json, base64)
  - ostro_server::control::list (list_runs, list_runs_with_budget [doc(hidden)], SCAN_BUDGET, page token)
affects: [04-20, 05-*, 08-triggers]
tech-stack:
  added: []
  patterns:
    - "Idempotent starts route by partition_for_key(workflow, key) so the dedupe row and every retry share a partition"
    - "Global (uuid v7, partition) merge order lets a single run id serve as the cross-partition page cursor"
    - "Published status found by a backwards scan of journal event kinds; only the match is decoded"
key-files:
  created:
    - crates/ostro-server/src/control/history.rs
    - crates/ostro-server/src/control/list.rs
    - crates/ostro-server/tests/start_run_api.rs
    - crates/ostro-server/tests/runs_api.rs
  modified:
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/tests/modules_api.rs
key-decisions:
  - "Run listing is oldest first in (uuid v7, partition) order; the store has no reverse scan, and new runs only ever append to later pages"
  - "Page token = c1.<run id of the last examined row>; partition q resumes after (q, uuid) when q <= p, at uuid when q > p; malformed or out-of-range tokens are 400"
  - "Scan budget 10,000 examined rows per call (REFILL 128 rows per partition per read); budget-stopped pages carry a token unless every partition is exhausted"
  - "RunInfo.attempt = current_attempt.n, the grant counter of the current epoch (one per dispatch, including resumes after a suspend); not a retry count, since failures are counted only in dispatcher memory"
  - "Failed/cancelled runs: result_json = None, failure = the stored failure JSON text {reason_code, kind, message}"
  - "Workflow with versions but no active version: 412; unknown workflow/version: 404; option sizes checked against the StartRun limits up front (400)"
  - "PartitionError mapping: NotLeader/Failed/Log/AckLost and StaleEpoch/WrongPartition = 503; Store/Blob = 500; PayloadTooLarge = 413; other rejections = 400"
requirements-completed: [API-03, API-04, WF-01]
duration: 40min
completed: 2026-11-19
---

# Phase 4 Plan 18: Runs ControlPlane (start, get, history, list) Summary

**The real server now starts runs over REST and gRPC with every API-03 option. Idempotency keys are durable: `return_existing` gives 200 with the same id, `reject` gives 409 with `existing_run_id`, and both still hold after a restart. GET returns the typed JSON result, the failure JSON, and the latest published status. History decodes the journal into per-kind JSON. Listing merges all P partitions oldest first, with in-memory filters, a single-run-id page token and a 10,000-row scan budget.**

## Performance

- **Duration:** about 40 min (03:25 - 04:05 UTC, excluding ci-local)
- **Tasks:** 2 (both TDD)
- **Files:** 4 created, 3 modified

## Accomplishments

- **runs.rs: start_run**
  - Version: explicit or active. Unknown workflow or version is 404; versions without an active one is 412.
  - Input JSON check (400). Tags, tenant and traceparent are checked against the `MAX_*` constants in ostro-types (400) before apply ever sees them.
  - Partition: `partition_for_key(workflow, key, P)` with an idempotency key, `partition_for_new_run(uuid v7, P)` without one. The run id is `RunId::new(p, uuid)`.
  - Input goes through `prepare_payload`: inline up to 256 KiB, larger inputs become a durable blob before the append.
  - `StartRun` carries:
    - `module_sha` from the `WorkflowVersion`
    - `requested_at_ms = now` and `ready_at_ms = now + delay_ms`
    - a 32-byte `getrandom` seed
    - `run_timeout_ms` from describe()
    - queue = request, else the version's `default_queue`, else `"default"`
    - priority, concurrency key, tags, tenant and traceparent
    - idempotency key and conflict policy
- **runs.rs: get_run**
  - A bad id is 400. A partition outside the count or a missing record is 404.
  - Returns status, version, module sha (hex), queue, options, created and completed ms, and `attempt` (the grant counter).
  - Completed runs: `result_json` is `PAYLOAD_RESULT`. Failed or cancelled runs: `failure` is the failure JSON text.
  - `published_status_json`: the latest `StatusPublished`, found by scanning journal kinds backwards in pages of 64 rows. Only the matching row's payload is fetched and decoded.
- **history.rs**
  - `journal_rows(from_seq, limit + 1)`, then fetch, then `decode_event`.
  - Each event: a snake_case `kind`, `ts_ms`, `EventBody::summary`, and per-kind details:
    - `run_started`: workflow, version, module, input, deadline
    - `step_completed`: name, `outcome.ok` or `outcome.error`, fuel
    - `step_attempt_failed`, `timer_started.fire_at_ms`, `status_published.status`, `run_completed.output`, `run_failed.failure`, and the rest
  - JSON payloads are embedded. Other payloads become `{content_type, base64}`, using an in-crate RFC 4648 encoder (no new dependency).
- **list.rs**
  - One reader stream per partition, refilled 128 rows at a time in a single `spawn_blocking` per round.
  - Min-heap-free k-way merge by `(uuid, partition)`, with filters applied in memory: workflow, status, queue, tag (contains), and `created_after <= created < created_before`.
  - Stops at `limit` (default 100, max 1000) or after 10,000 examined rows.
  - Returns token `c1.<run id>` only if some stream still has rows.
  - `list_runs_with_budget` is `#[doc(hidden)] pub` so the integration test can drive the budget path with a small budget.
- **mod.rs**: `pub mod history; pub mod list;`, trait delegates updated. The `PENDING` const and the "runs API lands in 04-18" string are gone (grep clean).

## Task Commits

1. **Task 1 RED: API-03 start/get suite**: `b3dd0a3` (test)
2. **Task 1 GREEN: start_run + get_run**: `b95c0c3` (feat)
3. **Task 2 tests: API-04 history/list suite**: `0c975fe` (test)
4. **Task 2 GREEN: run_history + list_runs**: `7ade927` (feat)

## Verification

- `cargo test -p ostro-server --locked`: all green.
  - lib 24, bootstrap 5, config 16, executor 8, leases 4, modules_api 3, runs_api 2, sink 15, start_run_api 6
- **start_run_api (6 tests):**
  - REST start/get with every option and typed result
  - unknown workflow or version 404, bad id 400, unknown id 404, workflow failure rendered
  - explicit version pinned across activations, with v2 status `{"stage":"done","version":2}`
  - delay 1500 ms: `scheduled` at +200 ms, completed at 1.5 s or later
  - idempotency: return_existing, reject 409, a different workflow is independent, and both hold after a restart
  - 300 KiB input offloaded to blobs and echoed back; 1.1 MiB body over `max_request_bytes = 1 MiB` is 413
  - gRPC: start/get, ALREADY_EXISTS with details, invalid JSON, NotFound, bad id
- **runs_api (2 tests):**
  - history over REST and gRPC: identical events, paging with limit 3, 404/400
  - P=16 with 60 runs: every filter over both transports with limit 7, exactly once in start order; a token continues correctly after 3 new runs; garbage token 400; budget 25 with a no-match filter takes exactly 3 calls; budget-limited pages of a matching filter return every match
- Unit tests: outcome mapping (201/200/409/413/400), partition errors map to 503, token round trip and garbage, resume positions, base64 RFC 4648 vectors, payload rendering.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings` clean; `cargo fmt --all --check` clean.
- `! grep -rn "lands in 04-18" crates/ostro-server/src`: clean.
- `bash scripts/ci-local.sh --no-musl` with the private `CARGO_TARGET_DIR=target/wt-04-18`, run at 7ade927. **PASS:** toolchain, fmt, clippy, warning-free build, `cargo test` (583 s), failpoint suites (680 s), deny, check-vendored, deny-fixtures, forbidden deps, workflow validation. **SKIP:** the musl legs. No `Node::open timed out` flake. No new dependencies, and Cargo.lock is unchanged.
- RED: the start_run_api suite failed 6/6 against the placeholder (503) before GREEN. The runs_api suite was committed before the implementation and fails to compile against that commit (`list_runs_with_budget` is absent). It was not run against the placeholder separately.

## Deviations from Plan

### Auto-fixed / adjusted

**1. [Rule 1 - Test expectation] `attempt` is the grant counter, not 1 for a completed sdk-basic run.**
- **Found during:** Task 1 GREEN
- **Issue:** The plan expected `attempt Some(1)`. `RunRecord.current_attempt` is the `AttemptGranted` token, and its `n` increments on every dispatch in an epoch. sdk-basic suspends on its sleeps and is dispatched more than once, so `n` is 2 or more. Retry failures exist only in dispatcher memory (`Job::failures`), so no persisted retry count exists to report.
- **Fix:** `attempt = current_attempt.n` is documented as the grant counter. Tests assert `>= 1` when running or terminal and `None` while scheduled. A persisted per-run attempt count would be a RunRecord change in ostro-partition, which is not 04-18's file and is before the Phase 5 freeze. Flagged for the orchestrator.
- **Commit:** b95c0c3

**2. [Rule 3 - Test fixture] sdk-echo ships in the sdk-basic module.** The test deploys `sdk-basic` to get `sdk-echo`; no `sdk_echo.wasm` exists.

**3. modules_api placeholder assertion.** 04-17's suite asserted 503 for `POST /runs`. It now asserts 201 (commit b3dd0a3).

### Design choices within the plan's latitude

- **Page token** is `c1.<run id>` instead of base64url(postcard(per-partition cursors)). The global `(uuid, partition)` order makes one position sufficient, and it avoids adding postcard and base64 to ostro-server. It is still opaque, versioned and validated.
- **Oldest-first order**, as the plan allowed. The store has no reverse scan.
- **No created-range seek.** Created-range filters are evaluated in memory; the scan budget bounds the cost. A future optimisation could seek the cursor to a uuid v7 built from `created_after_ms - slack`.
- **412** for a workflow with no active version (the REST OpenAPI already documents 412). This cannot happen today, because the first upload auto-activates.

## Known gaps / notes

- **Concurrent start vs page boundary:** a run whose uuid was generated before a served page's cursor but committed after it can be missed by that pagination. This is single-node with sequential uuid v7, so the window is the length of one start call. Documented in list.rs.
- **Published status cost:** the backwards scan is O(journal length) in kinds when a run never published a status. A `status_seq` pointer on RunRecord would make it O(1). Suggested for the Phase 5 freeze.
- **Concurrency key** is persisted and returned but not enforced by the dispatcher (Phase 7).

## Hand-off: 04-20 (e2e)

- Start: `POST /v1/workflows/{name}/runs` with body `{input, version?, queue?, delay_ms?, priority?, tags?, traceparent?, tenant?, idempotency_key?, conflict_policy?: "return_existing"|"reject", concurrency_key?}`. Responses: 201 created, 200 existing, 409 `error.details.existing_run_id`, 404, 412, 413.
- Get: `GET /v1/runs/{id}`.
  - `status` is one of scheduled, ready, running, completed, failed, cancelled.
  - `result` is the workflow's JSON output (completed only).
  - `failure` is a JSON **string** `{"reason_code","kind","message"}`; parse it.
  - `published_status` is embedded JSON.
  - `attempt` is the grant counter; don't assert `== 1`.
- History: `GET /v1/runs/{id}/history?from_seq&limit`. Kinds are snake_case, starting with `run_started` and ending with `run_completed` or `run_failed`. Step details are `{name, outcome: {ok}|{error}, fuel_used}`.
- List: `GET /v1/runs?workflow&status&queue&tag&created_after&created_before&limit&page_token`, oldest first. Keep requesting while `next_page_token` is set.
- gRPC `ostro.api.v1.Runs` has the same semantics. `input_json` empty means `null`. Conflict is ALREADY_EXISTS, with `ostro_api::grpc::status_details` giving `existing_run_id`.
- Polling for terminal status at 20 ms intervals works well. An sdk-basic run completes in about 100 ms on tmpfs in a debug build.
- Use a `TestServer` with `ServerOpts { partitions: 16, .. }` for multi-partition behaviour.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/control/{runs,history,list}.rs (`partition_for_key`, `runs_page`)
- FOUND: crates/ostro-server/tests/{start_run_api,runs_api}.rs
- FOUND: commits b3dd0a3, b95c0c3, 0c975fe, 7ade927
