---
phase: 11-run-operations-embedded-ui
plan: 09
subsystem: api surface (REST + gRPC + OpenAPI)
tags: [api, rest, grpc, openapi, fakes, limits, search]
requires:
  - 11-04 (ostro_api::phase11 traits + DTOs, ApiExtensions.ops/.inventory, ApiConfig.max_payload_bytes/.root_redirect)
provides:
  - Phase 11 REST routes (phase11::api_routes / router), 404 while a slot is absent
  - gRPC ostro.api.v1.RunOps + ostro.api.v1.Inventory (ops.proto), mounted only when the slot is present
  - ListRunsRequest.{tags, order, dlq} (domain, REST, gRPC) and HistoryEvent.attempt (domain, REST, gRPC)
  - phase11::{limit_error, reason_error, check_payload, check_json_payload, validate_user_tags, search_tags, validate_qualified_name, rerun_cut_unsafe, dlq_not_member, bulk_too_many_jobs, reason::*, MAX_SEARCH_TAGS, SYSTEM_TAG_PREFIX}
  - ostro_api::fake::{FakeOps, FakeInventory, FAKE_MAX_BULK_JOBS}; FakeControlPlane::{last_list_runs, hide_from_dlq, set_history_attempt}
  - GET / -> 303 ApiConfig.root_redirect
affects: [11-10, 11-11, 11-12, 11-13, 11-14, 11-15, 11-16, 11-17, 11-18]
tech-stack:
  added: []
  patterns:
    - "Phase 11 REST handlers read ApiConfig through an axum Extension<RestOptions> installed by router_with (absent = ApiConfig defaults), because the core router state is only Arc<dyn ControlPlane>"
    - "repeatable query params (tags) are read with a second Query<Vec<(String, String)>> extractor; serde_urlencoded cannot fill a Vec field"
    - "proto <-> DTO conversions in both directions live in phase11.rs (like phase8.rs), so clients (CLI, parity) reuse them"
key-files:
  created:
    - crates/ostro-proto/proto/ostro/api/v1/ops.proto
    - crates/ostro-api/tests/phase11_surface.rs
  modified:
    - crates/ostro-api/src/{phase11,lib,domain,rest,grpc,fake,openapi}.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-proto/{build.rs,src/lib.rs,proto/ostro/api/v1/runs.proto}
    - crates/ostro-server/src/control/history.rs (one line, attempt: None)
    - crates/ostro-server/tests/p11_contract.rs (root assertion)
    - crates/ostro/tests/dev_startup.rs (root assertion)
decisions:
  - "bulk_too_many_jobs reuses Phase 7's ErrorCode::RateLimited (HTTP 429 + Retry-After, gRPC RESOURCE_EXHAUSTED with details.retry_after_ms), as D-P11-21 allows when the code exists at HEAD"
  - "Absent slot: REST 404 not_found, gRPC service not mounted (UNIMPLEMENTED), the Phase 8 convention"
  - "domain ListRunsRequest keeps `tag`; the adapters set it to the first entry of the merged `tags`, so the server's existing scan (control/list.rs, filters on `tag` only) keeps filtering until 11-11 serves `tags`/`order`/`dlq`"
  - "rerun_cut_unsafe details are {reason, seq, detail, nearest_safe_seq}: the 'why' is under `detail` because `reason` is the details.reason discriminator"
  - "GrpcRuns / GrpcTokens are no longer tuple structs: GrpcRuns::new(cp, max_payload_bytes), GrpcTokens::new(cp, max_payload_bytes)"
  - "proto RerunStep.occurrence 0 means 1 (proto3 cannot tell unset from 0); REST rejects an explicit occurrence 0"
metrics:
  duration: "~1h45m"
  completed: 2026-11-22
---

# Phase 11 Plan 09: API surface Summary

This plan adds the whole Phase 11 API surface in `ostro-api` and `ostro-proto`:

- REST routes for rerun, DLQ, bulk jobs, limits, failure policies and inventory.
- gRPC services `RunOps` and `Inventory`.
- Search fields on `GET /v1/runs` / `Runs.List` and the history `attempt` field.
- API-side payload and system-tag caps on start, signal and token completion, with the same details on both transports.
- In-memory fakes for downstream server, UI and CLI plans, a parity transcript, and a re-blessed OpenAPI snapshot.
- `GET /` answering `303` to the UI.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 416a600 | `feat(11-09)`: Phase 11 REST surface, search params, limit validation and fakes |
| 2 | 9834dfb | `feat(11-09)`: RunOps and Inventory gRPC services, parity and OpenAPI |

## REST routes

All routes are in `phase11::api_routes()`, merged by `router_with`. Bodies are the 11-04 DTOs, serialized as snake_case.

| Method | Path | Trait method | Status |
|---|---|---|---|
| POST | `/v1/runs/{run_id}/rerun` (body `RerunBody {from, version, idempotency_key, dry_run}`) | `OpsApi::rerun_run` | 201 new run; 200 dry run or `existing` |
| GET | `/v1/runs/{run_id}/rerun-cuts` | `analyze_rerun` | 200 |
| GET | `/v1/dlq?workflow&queue&reason&tags*&include_dismissed&limit&page_token` | `list_dlq` | 200 |
| POST | `/v1/dlq/{run_id}/requeue` (body optional `{mode, dry_run}`) | `requeue_dlq` | 200 |
| POST | `/v1/dlq/{run_id}/dismiss` (`{reason}`) | `dismiss_dlq` | 200 |
| GET / POST | `/v1/bulk` | `list_bulk` (`{"jobs": [...]}`) / `create_bulk` | 201 job; 200 dry run |
| GET | `/v1/bulk/{job_id}` | `get_bulk` | 200 |
| POST | `/v1/bulk/{job_id}/cancel` | `cancel_bulk` | 200 |
| GET | `/v1/limits` | `limits` | 200 |
| GET / PUT | `/v1/workflows/{name}/failure-policy` | `get/set_failure_policy(Workflow)` | 200 |
| GET / PUT | `/v1/queues/{name}/failure-policy` | `get/set_failure_policy(Queue)` | 200 |
| GET | `/v1/modules?limit&page_token` | `InventoryApi::list_modules` | 200 |
| GET | `/v1/queues` | `list_queues` (`{"queues": [...]}`) | 200 |
| GET | `/v1/partitions` | `list_partitions` (`{"partitions": [...]}`) | 200 |

- **Absent slots.** Every route answers 404 `not_found` while its slot (`ops` or `inventory`) is `None`. The existing `GET /v1/modules/{sha256}` is unaffected.
- **Reservations.** `/v1/runs/{id}/rerun-cuts` is a sub-path of the reserved rerun path; **11-18 adds it to RESERVATIONS §11**. In the router the path parameter is spelled `{run_id}`, because axum requires one parameter name per segment.
- **Failure-policy PUT.** The `version_tag` comes from the body, the `?version_tag=` query or `If-Match`; if they disagree the answer is 400. Validation before the trait call:
  - `on_failure.workflow` and `on_failure.queue` must match the D23 grammar (`[<tenant>/]<name>`, the same as `ostro_meta::is_qualified_name`).
  - The scope name is checked with the same grammar.
  - A `dlq` policy on a workflow scope is 400.
- **RunPolicy.** It is untouched. `/v1/workflows/{name}/policy` behaves as before and still rejects unknown fields such as `on_failure`.
- **Validation shared with gRPC** (`phase11::check`):
  - Step cuts need a non-empty name and occurrence ≥ 1.
  - An `idempotency_key`, when set, must not be empty.
  - Reasons are at most 1024 bytes: `limit_error(InvalidArgument, "reason_bytes")`.
  - Filter tags are at most 8: `search_tags`.
  - `rate_per_sec` and `max_in_flight` must be > 0 when set.
  - Bulk signal names are 1..=256 bytes.

## Search, history, caps (core routes)

- **`GET /v1/runs` search fields.**
  - `tags` is repeatable (AND).
  - The legacy `tag` is merged in first, and duplicates are dropped.
  - More than 8 tags is 400 with `details {reason: limit_exceeded, limit: search_tags, max: 8, actual}`.
  - `order=oldest|newest` defaults to oldest; `dlq=true` is also accepted.
  - Domain `ListRunsRequest` gains `tags: Vec<String>`, `order: ListOrder {Oldest (default), Newest}` and `dlq: bool`. `tag` is kept and set to `tags.first()`.
  - The gRPC `Runs.List` path runs the same merge and check.
- **History `attempt`.** Domain `HistoryEvent` and REST `HistoryEventDto` gain `attempt: Option<u64>`; REST omits it when `None`. The server's `control/history.rs` literal gets `attempt: None`; 11-11 maps the stored value (0 → None).
- **Payload cap.** It is checked before proposal. REST measures the compact JSON encoding of `input` / `payload`. gRPC measures the raw `input_json` bytes for start, and the re-encoded payload for signal and token completion. A payload over `ApiConfig.max_payload_bytes` gets:
  - REST: 413 `payload_too_large` with `details == {"reason":"limit_exceeded","limit":"payload_bytes","max":M,"actual":N}`.
  - gRPC: `RESOURCE_EXHAUSTED` with the same details JSON.
- **System tags.** A user tag starting with `__ostro/` is REST 400 / gRPC `INVALID_ARGUMENT`, with details `{field: "tags", tag}` on both.
- **`GET /`.** It answers `303 See Other` to `ApiConfig.root_redirect` when that is set, else the existing shell. The OpenAPI operation documents both 200 and 303.

## Proto field numbers (ostro.api.v1, append-only, nothing renumbered)

- `runs.proto`:
  - `ListRunsRequest`: `repeated string tags = 9`, `ListOrder order = 10`, `bool dlq = 11`.
  - New enum `ListOrder {LIST_ORDER_UNSPECIFIED = 0 (= oldest), LIST_ORDER_OLDEST = 1, LIST_ORDER_NEWEST = 2}`.
  - `HistoryEvent`: `optional fixed64 attempt = 6`.
  - An ostro-proto test pins the wire tags (0x4a, 0x50, 0x58, 0x31) and old-encoder compatibility.
- `ops.proto` (new):
  - `service RunOps {Rerun, RerunCuts, ListDlq, RequeueDlq, DismissDlq, CreateBulk, ListBulk, GetBulk, CancelBulk, GetLimits, GetFailurePolicy, SetFailurePolicy}`.
  - `service Inventory {ListModules, ListQueues, ListPartitions}`.
  - Enum DTOs are oneofs:
    - `RerunFrom {seq | step}`
    - `RerunVersion {active | source | version}` (unset = active)
    - `RequeueMode {auto | resume | restart | rerun}` (unset = auto)
    - `BulkAction {cancel | terminate | retry | rerun | requeue | pause | resume | signal}`
    - `scope {workflow | queue}`
  - JSON travels as `*_json` bytes (`BulkSignal.payload_json`, `QueueSummary.flow_json`).
  - `PartitionSummary.id` is `uint32`; on decode it is checked to fit `u16`.

## Absent-slot gRPC convention

`phase11::add_grpc_services` mounts `RunOps` and `Inventory` only when `ext.ops` / `ext.inventory` is `Some`, so an absent slot answers `UNIMPLEMENTED`. This is the same as Phase 8 (`Schedules`, `Webhooks`, `RunLogs`). `parity::phase11::absent_slots_are_rest_404_and_grpc_unimplemented` pins the pair: REST 404, gRPC UNIMPLEMENTED, and the core `Runs` service still mounted.

## Error-details mapping (D-P11-21)

There is no new `ErrorCode`. Constructors and constants are in `phase11::{reason, limit_error, reason_error, ...}`.

| Cause | Code | REST | gRPC | details |
|---|---|---|---|---|
| `run_not_terminal`, `rerun_cut_unsafe`, `rerun_nondeterministic`, `dlq_not_member` | Conflict | 409 | ALREADY_EXISTS | `{reason, ...}`; cut: `{seq, detail, nearest_safe_seq}` |
| `rerun_prefix_too_large`, `payload_bytes` | PayloadTooLarge | 413 | RESOURCE_EXHAUSTED | `{reason: limit_exceeded, limit, max, actual}` for limits |
| `search_tags`, `reason_bytes` (count caps), `bulk_too_many_matches` | InvalidArgument | 400 | INVALID_ARGUMENT | `{reason: limit_exceeded, limit, max, actual}` |
| `bulk_too_many_jobs` | **RateLimited** (Phase 7's code, reused) | 429 + Retry-After | RESOURCE_EXHAUSTED | `{reason, retry_after_ms, limit: bulk_jobs, max}` |
| stale failure-policy tag | Conflict | 409 | ALREADY_EXISTS | `{reason: version_mismatch, current_version_tag}` (Phase 8 helper) |

`ErrorCode::from_grpc_code_and_details` tells RateLimited apart from PayloadTooLarge by `retry_after_ms`. The parity transcript uses it.

## Fakes (`ostro_api::fake`, feature `fake`)

- **`FakeOps`**: deterministic ids (`run-rerun-NNNNNN`, `run-requeue-NNNNNN`, `blk_<16 hex>`) and a fake clock. It provides:
  - `fail(method, Option<ApiError>)` for injected errors.
  - Seeding with `set_cuts`, `push_dlq` and `set_bulk_matches`.
  - `set_bulk_progress`.
  - The recorded requests, `reruns()` and `bulk_requests()`.
  - `install(&mut ApiExtensions)`.
  - A cap of `FAKE_MAX_BULK_JOBS` = 4 unfinished bulk jobs, after which it answers `bulk_too_many_jobs`.
- **`FakeInventory`**: `set_modules` / `set_queues` / `set_partitions`, `fail` and `install`.
- **`FakeControlPlane`**:
  - Now records the last `list_runs` request (`last_list_runs()`).
  - Honours `tags` (AND), `order` and `dlq`. `dlq` means Failed or TimedOut and not hidden; `hide_from_dlq` hides a run.
  - `set_history_attempt(run, seq, attempt)` sets an event's attempt.

## OpenAPI diff (re-blessed in Task 1; Task 2 left it unchanged)

The diff is additive: new Phase 11 paths and schemas, the `tags`/`order`/`dlq` parameters on `GET /v1/runs`, the `attempt` property, tags `operations` and `inventory`, and the 303 response on `/`.

With `git diff --diff-algorithm=histogram` (patience and minimal agree) there are **7 removed lines**, all description texts:
1. The `/` summary: "Web UI shell (placeholder until the UI phase)." became "Web UI shell, or a redirect to the embedded UI when it is enabled (D-P11-17)."
2. The `tag` parameter description: "Tag filter." became "Tag filter (same as one `tags` value)."
3. The `GET /v1/runs` 400 description gained "; > 8 tags: details.limit = search_tags".
4. to 6. The 413 descriptions of start, signal and token completion now mention `limits.max_payload_bytes`.
7. The start 400 description notes that the `__ostro/` prefix is reserved.

**Verify-command deviation:** git's default Myers diff misaligns this large insertion into the pretty JSON and reports 279 "removed" lines, which fails the plan's `test "$removed" -le 12`. The semantic diff was checked with the histogram algorithm instead.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Root-redirect consequences in tests outside `files_modified`**
- **Found during:** Task 2 verification.
- **Issue:** `ostro-server/tests/p11_contract.rs` (11-04) asserted "root shell until 11-09". `ostro/tests/dev_startup.rs` expected the shell at `/`. Both run servers with the UI enabled (loopback), so `/` now redirects to `/ui/`.
- **Fix:** Both assertions now expect the final URL `/ui/` (status 200). There were no other edits to those files.
- **Commit:** 9834dfb

**2. [Rule 3 - Blocking] gRPC Runs/Tokens need the cap**
- `GrpcRuns` and `GrpcTokens` became structs with `::new(cp, max_payload_bytes)`. The only constructor site is `lib.rs`.
- `grpc::run_status_from_proto` and `payload_from_proto` became `pub(crate)` so phase11.rs can reuse them.

**3. Placement**
- The gRPC adapter (services and conversions) is in `phase11.rs`, as Phase 8 did in `phase8.rs`. `grpc.rs` only got the core Runs/Tokens/ListRuns/history hunks.
- `FakeOps` and `FakeInventory` are in `fake.rs`, matching the key_link `impl OpsApi for`.

**4. OpenAPI re-bless in Task 1**
- The snapshot was re-blessed in the Task 1 commit so that each commit is green on its own. Task 2 changed only gRPC, and the snapshot stayed identical.

**5. Parity step**
- An explicit `occurrence: 0` is REST-only. Proto3 cannot express it: 0 = unset = 1, as documented in ops.proto. The parity transcript therefore checks an empty step name instead. `phase11_surface` covers the REST rejection.

**6. Workflow per the executor brief**
- Work was done in worktree `wt-11-09` with slot-5 and CAS `update-ref`, instead of the plan's `/tmp/ostro-wt-11-09` and private index.

## Tests

- `cargo test -p ostro-api --test phase11_surface --locked`: **14 passed**. Covered:
  - absent slots
  - slot independence
  - rerun (dry run, 201, idempotent 200, `rerun_cut_unsafe` 409 details)
  - cuts
  - DLQ (120 entries, limit 50 → 3 pages; filters; repeatable tags; dismiss hides; `dlq_not_member`; `reason_bytes`; requeue auto/resume/restart dry run/rerun)
  - bulk (dry run with estimate and ≤ 20 samples; `blk_` job; progress; cancel; validation; job cap 429)
  - limits and inventory (pagination)
  - failure policy (validation, round trip, CAS through query and If-Match; RunPolicy unchanged)
  - search fields reaching `list_runs` (`tag` → `tags`, 9 tags → `search_tags`) and fake semantics
  - history attempt omitted or present
  - payload cap 1 KiB exact details on start, signal and token completion; exact-at-cap accepted
  - `__ostro/` tag rejected
  - default 4 MiB cap
  - `/` 303 vs shell
  - OpenAPI path list
- `cargo test -p ostro-api --test parity --locked`: **8 passed**, including `phase11_rest_and_grpc_transcripts_are_equal` (about 60 steps across both transports) and the absent-slot pair.
- `cargo test -p ostro-api -p ostro-proto --locked`: all green. This includes the OpenAPI snapshot and the new `phase11_messages_roundtrip_and_numbering`.
- `cargo test -p ostro-server --test runs_api --test p11_contract --locked`: 2 + 10 passed. `cargo test -p ostro-server --lib history`: 2 passed (the server history tests are unchanged).
- `cargo test -p ostro --test dev_startup --locked`: 4 passed (1 ignored, measurement only).
- `cargo test -p ostro-cli -p ostro-ui --locked`: green.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean. `cargo check --workspace --all-targets --locked`: clean after rebasing onto 8cf611a (11-06 and 09-19 had landed).
- Not run: the full `cargo test --workspace` and `scripts/ci-local.sh`. The machine is shared with 4 other executors, and the crates this plan touches were run individually.

## Open items for later plans

- **11-15:** `ostro-ui`'s router is still empty, so on UI-enabled servers `GET /` now redirects to a blank `/ui/` until 11-15 registers the pages.
- **11-11:** Serve `tags` / `order` / `dlq` in the server's `list_runs`; today the scan honours only `tag = tags[0]`. Map the stored `JournalEvent.attempt` into `HistoryEvent.attempt` (0 → None).
- **11-18:** Add `/v1/runs/{id}/rerun-cuts` to RESERVATIONS §11.
- **Clients:** Bulk jobs at the cap answer 429 `rate_limited`, so they should honour `retry_after_ms`.

## Self-Check: PASSED

- FOUND: crates/ostro-proto/proto/ostro/api/v1/ops.proto, crates/ostro-api/tests/phase11_surface.rs
- FOUND: 416a600, 9834dfb on main
