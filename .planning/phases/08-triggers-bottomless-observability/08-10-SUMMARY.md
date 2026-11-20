---
phase: 08-triggers-bottomless-observability
plan: 10
subsystem: api
tags: [rest, grpc, openapi, schedules, webhooks, run-logs, backup, traceparent, server-stubs]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 RESERVATIONS §11 names (REST paths, gRPC Schedules/Webhooks); 08-04 services seam, GuestLogSink, one injectable clock, [triggers] webhook_body_limit, testkit OTLP Collector; 08-05/08-06 ostro-triggers cron policies and webhook verifiers
provides:
  - "ostro_api::phase8: ScheduleApi, WebhookApi, HookIngress, RunLogsApi, AdminApi traits + DTOs + REST handlers + gRPC services; ApiExtensions slots; router_with(cp, cfg, ext); FakePhase8"
  - "ostro.api.v1.Schedules + ostro.api.v1.RunLogs (schedules.proto), ostro.api.v1.Webhooks (webhooks.proto)"
  - "Inbound W3C traceparent on start-run: REST header / gRPC metadata when the body field is empty, validated (reason invalid_traceparent)"
  - "OpenAPI: Phase 8 paths, schemas and tags (additive snapshot diff)"
  - "ostro-server ControlCtx (complete) + controller stubs control/{schedules,webhooks,run_logs,admin}.rs wired once in server.rs via phase8_extensions"
  - "tests/common/phase8.rs: Phase8Server builder (collector, backup dir, snapshot thresholds, guest logs, triggers), Stripe/GitHub/Slack signers, schedule/webhook/hook/run-status REST helpers"
affects: [08-12, 08-14, 08-15, 08-17, 08-18, 08-21, 06-08, 07-11, 07-14]

tech-stack:
  added: []
  patterns:
    - "Optional API slots: REST routes always mounted (complete OpenAPI), absent slot = 404 not_found, gRPC service not mounted (UNIMPLEMENTED)"
    - "Phase 8 traits take the wire DTOs directly (serde + utoipa); proto <-> DTO conversion lives next to them; shared op_* functions give REST and gRPC identical validation"
    - "Controller stubs answer Unavailable 'not yet implemented (08-NN)'; feature plans fill one controller file each"

key-files:
  created:
    - crates/ostro-api/src/phase8.rs
    - crates/ostro-api/tests/phase8_surface.rs
    - crates/ostro-proto/proto/ostro/api/v1/schedules.proto
    - crates/ostro-proto/proto/ostro/api/v1/webhooks.proto
    - crates/ostro-server/src/control/schedules.rs
    - crates/ostro-server/src/control/webhooks.rs
    - crates/ostro-server/src/control/run_logs.rs
    - crates/ostro-server/src/control/admin.rs
    - crates/ostro-server/tests/common/phase8.rs
    - crates/ostro-server/tests/phase8_smoke.rs
  modified:
    - crates/ostro-api/src/{lib,rest,grpc,openapi,fake}.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-proto/build.rs
    - crates/ostro-proto/src/lib.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/tests/common/mod.rs
    - .planning/RESERVATIONS.md

key-decisions:
  - "GetRunLogs is a sibling service ostro.api.v1.RunLogs in schedules.proto, not an RPC appended to Runs: runs.proto is outside this plan's files and shared with 06-08 / 07-11; recorded in RESERVATIONS §11"
  - "ApiConfig stays Copy and unchanged; the extensions and hook_body_limit live in ApiExtensions, passed to the new ostro_api::router_with (router(cp, cfg) = router_with(.., ApiExtensions::default())). Adding fields to ApiConfig would break struct literals in tests outside the plan's files"
  - "Stale version_tag on PUT/DELETE is 409 conflict with details.reason = version_mismatch (the plan's 409), unlike activate_workflow's 412"
  - "ControlCtx holds the node as Weak<Node> (accessor node() -> Unavailable after shutdown) so Node::shutdown's Arc::try_unwrap cannot fail on a lingering connection; the partition client is Arc<ServerControlPlane>; ServerHandle keeps owning Services, so ControlCtx carries the GuestLogSink Arc (the same instance Services::guest_log_sink returns) instead of Arc<Services>"
  - "Times and durations in the schedule DTO are *_ms integers on both transports (exact parity); schedule input is JSON (meta stores JSON bytes)"
  - "AdminApi::backup returns BackupStream { body: axum::body::Body, filename }: ostro-api has no futures dependency and the lock may not change, so the server builds the body (Body::from_stream) in 08-18"

requirements-completed: []

duration: ~1h30m
completed: 2026-11-20
---

# Phase 8 Plan 10: Phase 8 API surface Summary

**Schedules, webhooks, hook ingress, run logs and the admin backup now exist on REST and gRPC (`Schedules`, `Webhooks`, `RunLogs`) with a parity transcript, an additive OpenAPI diff and an in-memory fake. Start-run accepts a validated W3C `traceparent` header or gRPC metadata. The server fills every slot with a controller stub that names its owning plan, wired once through a complete `ControlCtx`, and `tests/common/phase8.rs` gives later plans a Phase 8 test-server builder.**

## Why this plan is wide (W5)

About 22 files, most of them registration lines: a proto pair plus two build.rs lines, `pub mod`/re-export lines, the router merge, the OpenAPI merge, the tag list, the `server.rs` wiring and `pub mod phase8;` in tests/common. This plan is the single serialisation point (P8-D21(b)) for the registries every trigger/log/backup feature would otherwise edit concurrently: the OpenAPI snapshot, the parity suite, the proto build list, `server.rs` and `tests/common`. The new logic is in `ostro-api/src/phase8.rs` (traits, DTOs, handlers, gRPC services, validation, fake) and the four stub controllers.

## Route table

| Method | Path | Trait method | Notes |
|---|---|---|---|
| GET | `/v1/schedules` | `ScheduleApi::list_schedules` | `{schedules: [..]}` |
| POST | `/v1/schedules` | `create_schedule` | 201; 409 on an existing name |
| GET | `/v1/schedules/{name}` | `get_schedule` | |
| PUT | `/v1/schedules/{name}` | `update_schedule` | CAS via body `version_tag` or `If-Match` (must agree, else 400); stale = 409 `version_mismatch` |
| DELETE | `/v1/schedules/{name}` | `delete_schedule` | CAS via `?version_tag=` or `If-Match`; `{name, deleted: true}` |
| POST | `/v1/schedules/{name}/pause` | `pause_schedule` | |
| POST | `/v1/schedules/{name}/resume` | `resume_schedule` | |
| POST | `/v1/schedules/{name}/trigger` | `trigger_schedule` | optional body `{request_id?}` (1..=64 bytes) -> `{run_id}` |
| GET | `/v1/webhooks` | `WebhookApi::list_webhooks` | |
| POST | `/v1/webhooks` | `create_webhook` | 201 |
| GET / PUT / DELETE | `/v1/webhooks/{name}` | `get_webhook` / `update_webhook` / `delete_webhook` | same CAS rules |
| POST | `/hooks/{name}` | `HookIngress::deliver` | raw `Bytes` + `HeaderMap`, no JSON extractor, route-level `DefaultBodyLimit(hook_body_limit)` -> 413; no auth layer (the signature is the authentication); any status / content type (`HookResponse`) |
| GET | `/v1/runs/{run_id}/logs` | `RunLogsApi::run_logs` | `?limit=` (default 100, max 1000) `&level=` (minimum) |
| GET | `/v1/admin/backup` | `AdminApi::backup` | 200 `application/x-tar`, `Content-Disposition: attachment; filename="ostro-backup.tar"` |

API-layer validation, identical for both transports:
- names `[A-Za-z0-9_.-]{1,128}` for schedules and webhooks;
- schedule `workflow` and `cron` required, timezone at most 64 bytes, `start_at_ms <= end_at_ms`;
- webhook actions (required fields, JSON pointers start with `/`, start `input` is `"body"` or a pointer), `secret_ref` required unless the verifier is `none`, idempotency `header:<name>|json:/<pointer>|none`;
- `request_id` length.

Cron and time-zone semantics belong to the `ScheduleApi` implementation (08-14). A path whose slot is `None` answers 404 `not_found`.

## gRPC services

| Service (proto) | RPCs |
|---|---|
| `ostro.api.v1.Schedules` (`schedules.proto`) | List, Get, Create, Update, Delete, Pause, Resume, Trigger |
| `ostro.api.v1.RunLogs` (`schedules.proto`) | GetRunLogs |
| `ostro.api.v1.Webhooks` (`webhooks.proto`) | List, Get, Create, Update, Delete |

- `OverlapPolicy`/`CatchupPolicy` proto numbering equals the meta `OVERLAP_*`/`CATCHUP_*` constants.
- Hook ingress and the backup are REST-only: a webhook signature covers the raw HTTP body and headers, and a tar stream is an HTTP download.
- A service is mounted only when its slot is present. Decode limit: `max_request_bytes`.

## Inbound traceparent

- REST `POST /v1/workflows/{name}/runs` reads the `traceparent` header, gRPC `Runs.Start` reads `traceparent` metadata.
- The body field wins when it is non-empty; otherwise the header/metadata value is used. The chosen value is validated: `00-<32 lowercase hex, not all 0>-<16 lowercase hex, not all 0>-<2 lowercase hex>`. Invalid gives 400 / `INVALID_ARGUMENT` with `details.reason = "invalid_traceparent"`.
- The value reaches `StartRunRequest.traceparent`, and from there `StartRun.traceparent`.
- Tests cover header-only, body-only, both (body wins), an empty body field with a header, and invalid values, over REST and gRPC. The real server is checked as well: an invalid header is rejected in `phase8_smoke`.
- The OpenAPI start-run operation documents the header parameter.

## ControlCtx (crates/ostro-server/src/control/mod.rs)

| Field | Type | Purpose |
|---|---|---|
| `node` (private) + `node()` | `Weak<Node>` -> `Result<Arc<Node>, ApiError>` | node access; `Unavailable` once shutdown released it |
| `meta` | `Arc<MetaHandle>` | schedules/webhooks/secrets CAS + reads |
| `config` | `Arc<ServerConfig>` | `[triggers]`, `[guest_logs]`, `[backup]` ... |
| `blobs` | `Option<Arc<BlobStore>>` | node blob store (None if no partition started) |
| `guest_logs` | `Arc<GuestLogSink>` | the executor's sink, the same instance as `Services::guest_log_sink()` |
| `partitions` | `Arc<ServerControlPlane>` | partition client: start/signal/complete-token via the API code path (submit + wait for apply), partition handles, run watchers |
| `secrets` | `SecretsAccess` | `sealed(name) -> Option<SealedSecret>` from meta |
| `clock` | `Arc<dyn Clock>` | the one injectable clock (same instance as NodeConfig/ServiceCtx) |
| `data_dir` | `PathBuf` | data directory |

`phase8_extensions(&ctx)` fills every slot. `hook_body_limit` comes from `[triggers] webhook_body_limit`. `server.rs` builds the context right after the control plane and calls `ostro_api::router_with`.

## Stub -> owning plan

| File | Traits | Owner |
|---|---|---|
| `control/schedules.rs` | `ScheduleApi` (+ `overlap_/catchup_{to,from}_meta` helpers, tested against `ostro_meta::records` and `ostro_triggers::cron`) | 08-14 |
| `control/webhooks.rs` | `WebhookApi`, `HookIngress` | 08-15 |
| `control/run_logs.rs` | `RunLogsApi` | 08-17 |
| `control/admin.rs` | `AdminApi` | 08-18 |

Every stub method returns `ApiError::unavailable("not yet implemented (08-NN)")`: HTTP 503, gRPC `UNAVAILABLE`.

## OpenAPI diff summary

The snapshot gains 1431 lines and loses none:
- 10 paths: `/hooks/{name}`, `/v1/admin/backup`, `/v1/runs/{run_id}/logs`, `/v1/schedules`, `/v1/schedules/{name}`, `/v1/schedules/{name}/{pause,resume,trigger}`, `/v1/webhooks`, `/v1/webhooks/{name}`;
- 18 schemas: BackupTar, CatchupPolicyDto, DeletedDto, HookPayload, LogLevelDto, OverlapPolicyDto, RunByDto, RunLogLine, RunLogsResponse, ScheduleDto, ScheduleListResponse, TriggerScheduleBody, TriggerScheduleResponseDto, VerifierDto, VerifierKindDto, WebhookActionDto, WebhookDto, WebhookListResponse, plus the start-run `traceparent` header parameter, inserted before `name`;
- 3 tags (`schedules`, `webhooks`, `admin`), inserted before `health`.

The old snapshot is a line subsequence of the new one (checked by script). `git diff --minimal`, `--patience` and `--histogram` all show 0 removed lines. Git's default Myers heuristic shows 82 spurious `-` lines, from realigning the repeated JSON blocks around `/v1/tokens/complete`. See Deviations.

## Task Commits

1. **Task 1: ostro-api/proto Phase 8 surface, traceparent, fake, parity, OpenAPI, RESERVATIONS** – `2eae480`
2. **Task 2: server ControlCtx + controller stubs + wiring + tests/common/phase8.rs + smoke** – `f690670`

## Verification

All runs used the worktree with `CARGO_TARGET_DIR=target/wt-08-10`. Task 2 was re-checked after rebasing onto main 8962059.

Plan checks:
- `cargo test -p ostro-proto -p ostro-api --locked`: **ok**.
  - ostro-api lib 39;
  - http_smoke 1, lifecycle_surface 8, openapi 2;
  - parity 4, including the new `phase8::phase8_rest_and_grpc_transcripts_are_equal` (44 steps);
  - phase8_surface 9, rest 11;
  - ostro-proto lib 7.
- Snapshot additivity:
  - the old snapshot is a line subsequence of the new one;
  - `git diff --minimal|--patience|--histogram HEAD~1 HEAD -- <snap> | grep '^-[^-]' | grep -v '^---'` is empty;
  - the default Myers diff reports 82 realigned lines (Deviation 6).
- `cargo build -p ostro-server --all-targets --locked`: warning-free.
- `cargo test -p ostro-server --locked`: **all ok**:
  - lib 50;
  - bootstrap 5, config 29, data_dir_lock 4, derive_phase5 16;
  - executor 8, executor_phase5 12, leases 4, lifecycle_api 13;
  - metrics_endpoint 2, modules_api 3, otlp_export 1;
  - **phase8_smoke 1**;
  - resume_rearm 4, runs_api 2, sdk_e2e 9, semantics_e2e 18, sink 15;
  - start_run_api 11, tenancy_api 3.
  - perf and semantics_sleepers are ignored by design.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean.

Wider checks:
- `cargo clippy --workspace --all-targets --locked --exclude ostro-raft-rs --exclude ostro-raft-proto --exclude ostro-raft-driver --exclude ostro-wal -- -D warnings` (the ci-local form): clean.
- `cargo clippy -p ostro-server --features failpoints --all-targets --locked -- -D warnings`: clean.
- `cargo build --workspace --locked`: warning-free.
- `cargo fmt --all --check`: clean.
- `cargo test -p ostro -p ostro-cli --locked`: all ok (5, 7, 1, 4 (+1 ignored), 3, 4, 4, 23 passed).
- Cargo.toml and Cargo.lock are unchanged.

**Not run:** the full `bash scripts/ci-local.sh --no-musl` (about 50 min). Four other executors were building in parallel, and the brief caps concurrent builds. The checks above cover every crate this plan touched (ostro-api, ostro-proto, ostro-server), plus their dependents ostro and ostro-cli, the workspace clippy and build, and fmt.

## Deviations from Plan

**1. [Rule 3] `ApiExtensions` + `router_with` instead of an `ApiConfig.ext` field**
- `ApiConfig` is `Copy + Eq`, and `tests/rest.rs` and `tests/lifecycle_surface.rs` (outside this plan's files) build it with exhaustive struct literals.
- The extensions and `hook_body_limit` live in `ApiExtensions`, passed to `ostro_api::router_with`. `router()` is unchanged for existing callers.

**2. [Plan option] `GetRunLogs` lives in the sibling service `ostro.api.v1.RunLogs` (schedules.proto)**
- runs.proto is not in files_modified, and the brief keeps the Phase 8 additions self-contained (06-08 and 07-11 share ostro-proto).
- RESERVATIONS §11 row updated in the Task 1 commit (docs hunk rebuilt from the main blob).

**3. [Deviation] `ControlCtx` field shapes**
- `Weak<Node>` instead of `Arc<Node>`: shutdown must unwrap the node, and an aborted drain could leave a connection holding the router.
- `Arc<GuestLogSink>` instead of `Arc<Services>`: `Services::shutdown(self)` consumes the services, which `ServerHandle` owns.
- `partitions: Arc<ServerControlPlane>` is the partition client.
- `SecretsAccess` reads sealed meta secrets only. Unsealing goes through the Phase 6 `SecretResolver` once the server wires one (06-05 landed it in ostro-bridge; wiring is 06-14's).

**4. [Deviation] Phase 8 traits use the wire DTOs** (serde + utoipa) rather than separate serde-free domain types, as allowed by "put all new traits, DTOs and handlers in phase8.rs". Documented in the module docs.

**5. [Deviation] Smoke test in a new file `tests/phase8_smoke.rs`**
- A test placed in `tests/common/phase8.rs` would run in every test binary that includes `mod common;`.
- The new file holds one test function, because the OTLP collector installs the process-global subscriber.
- It also checks the Stripe/GitHub/Slack signers against `ostro_triggers::webhook::verify`.

**6. [Note] Snapshot diff check and git's default diff algorithm**
- The plan's literal check (`git diff … | grep '^-[^-]'`) uses git's default Myers diff, which reports 82 realigned lines even though nothing was removed.
- Additivity is shown with `--minimal`/`--patience`/`--histogram` (0 removed lines) and a subsequence check.
- No repo git config was changed.

**7. [Note] REST `/v1/runs/{run_id}/logs`** uses `{run_id}` (like every other run path) for RESERVATIONS' `{id}`.

## Follow-ups for feature plans

- **08-14:** implement `ScheduleController` (meta CAS, `next_fire_at_ms` via ostro-triggers, trigger = `ScheduleFire{Manual}`), and set `schedule_source` in `server.rs` (its one allowed edit).
- **08-15:** implement `WebhookController` (CRUD + `deliver` with `VerifierSpec::from_meta`, `ctx.secrets`, and `ctx.clock` for tolerance).
- **08-17:** implement `RunLogsController` from `ctx.guest_logs`.
- **08-18:** implement `AdminController`, building `BackupStream::new(Body::from_stream(..))`.
- **08-12 (CLI):** consume the REST routes above.
- The fake (`ostro_api::fake::FakePhase8`) is available to CLI tests.

## Self-Check: PASSED

- Every created file listed under key-files exists on main (phase8.rs, phase8_surface.rs, both protos, the four controllers, tests/common/phase8.rs, phase8_smoke.rs).
- Commits 2eae480 and f690670 are on main.
