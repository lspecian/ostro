---
phase: 04-single-node-server-rust-mvp
verified: 2026-11-19T05:20:00Z
status: passed
score: 5/5 must-haves verified
---

# Phase 4: Single-Node Server & Rust MVP Verification Report

**Phase Goal:** A developer runs `ostro dev`, uploads a Rust workflow component through the API, starts runs and sees them complete with steps, retries and recorded results. Storage, engine, metadata and dispatch are joined in one binary.
**Verified:** 2026-11-19
**Status:** passed
**Re-verification:** No, initial verification

Verification ran on the main checkout at HEAD `d9893c0` with a private target dir (`target/wt-verify4`, removed afterwards). There are no code changes between the 04-20 gate commit `b0b4e58` and HEAD. The commits after it (`8973b27`, `f1b8f4f`, `d9893c0`) touch only `.planning/`.

Runs done for this verification:

- `cargo test -p ostro-server -p ostro -p ostro-api -p ostro-meta --locked` (debug): 25 test binaries, **156 passed, 0 failed, 1 ignored**. The ignored test is `cold_first_run_measurement`, which only measures by design.
  - ostro: cli 5, dev_startup 4, e2e_dev 3 (7.7 s), server_config 4.
  - ostro-api: lib 20, http_smoke 1, openapi 2, parity 3, rest 11.
  - ostro-meta: meta_persist 1, meta_records 3, meta_versions 6, single_voter 1.
  - ostro-server: lib 24, bootstrap 5, config 16, executor 8, leases 4, modules_api 3, runs_api 2, sdk_e2e 9 (9.8 s), sink 15, start_run_api 6.
- `cargo test -p ostro-partition --locked --lib --test apply_core --test apply_cross --test apply_hw --test apply_phase4 --test apply_timer_input --test fencing --test timers --test dispatcher --test ack_after_apply`: **108 passed, 0 failed**.
- `cargo test -p ostro --test dev_startup -- --nocapture` (debug, spawned binary), measured /readyz after spawn:
  - `--ephemeral`: 108 ms.
  - Warm restart: 508 ms.
  - Cold first run: 2812 ms (reported only).

Not re-run. These are taken from the 04-20 SUMMARY (full `bash scripts/ci-local.sh` at `b0b4e58`, all 17 steps green):

- The workspace-wide `cargo test`.
- The failpoint suites.
- The SDK workspace: fmt, clippy (host and wasm32-wasip2), test including trybuild, and the wasm build.
- The guests build.
- deny, vendored and deny-fixture checks.
- The musl static release build, and the musl zigbuild test leg (e2e_dev 3/3, sdk_e2e 9/9, dev_startup 4 on musl).
- The release startup numbers from 04-19: ephemeral 75–76 ms, warm restart 292 ms quiet, cold min 1557 / median 1624 ms.

## Observable Truths

| # | Truth (ROADMAP success criterion) | Status | Evidence |
|---|-------|--------|----------|
| 1 | `ostro dev` starts server, executor and UI shell with zero config in under 1 s, and `--ephemeral` keeps state in memory. `ostro server` starts a single node from defaults, with TOML and env overrides applied. | VERIFIED (with the recorded startup decision) | **Binary:** `crates/ostro/src/main.rs`. `dev` uses `ServerConfig::dev_defaults()` with no file, then `OSTRO_*` env, then flags. `server` uses `server_defaults()`, then an optional `-c` TOML, then env, then flags, and supports `--print-config`.<br>**Bootstrap:** `ostro-server/src/server.rs::start_with_hooks` runs meta, InitCluster, Runtime, `Node::open` with the dispatcher running `EngineExecutor`, `SetPartitionLeader`, then one axum router.<br>**One port:** `ostro_api::router` merges REST, `/openapi.json`, `/healthz`, `/readyz`, the UI shell at `/` and the tonic Routes.<br>**Ephemeral:** a `TempDir` on `/dev/shm`, removed on shutdown. It is tmpfs, not process memory, because fjall has no in-memory mode (research D5).<br>**Tests:** `dev_startup` asserts < 1 s for ephemeral and warm restart through the spawned binary (UI shell `text/html` plus gRPC health SERVING on the same port). `server_config::toml_then_env_then_flags` and `live_start_with_toml` pass, as do the 16 layering tests in `ostro-server/tests/config.rs`.<br>**Cold start:** cold first run is above 1 s and is reported only, per the STATE decision (Ruling 3). |
| 2 | A Rust workflow built with `cargo build --target wasm32-wasip2` using `#[workflow]`/`#[step]` is uploaded by sha256 (re-upload is a no-op), activated, and started over both HTTP/JSON and gRPC on the same port. The OpenAPI document is served. | VERIFIED | **Guests:** `guests/sdk-basic` is a plain cdylib on `ostro-sdk` with `#[step]`/`#[workflow]`, `join!`/`select!` and `export!`. The test helper builds it with `cargo build --release --target wasm32-wasip2`.<br>**Upload:** `control/modules.rs` checks that the path sha equals `Sha256::digest(body)`. A module already in meta returns `created:false`.<br>**Activation:** `PUT /v1/workflows/{name}/active` is a CAS on `ActiveVersion`.<br>**gRPC:** `GrpcModules`, `GrpcWorkflows` and `GrpcRuns` are served on the same router.<br>**Tests:** `e2e_dev::upload_start_rest_and_grpc_history_list` covers `/openapi.json`, upload 201 then re-upload 200 `created:false`, a REST start plus a tonic `RunsClient` start on one address, and field-by-field gRPC/REST parity. `activate_v2_new_runs_use_v2` passes. The `ostro-api` openapi snapshot and parity tests pass. |
| 3 | A step that fails is retried per its policy (backoff, max attempts, non-retryable kinds). After eviction or restart, replay returns recorded results without re-executing completed steps. Native time/random calls in the guest replay identically. | VERIFIED | **Retry decision:** `ostro-engine/src/retry.rs::decide` is pure. It applies max_attempts, `is_retryable` (non-retryable kinds), and a delay of `min(initial*coeff^(k-1), max)`, choosing InActivation or Requeue. `StepAttemptFailed` (kind 29) is journaled.<br>**Retry tests (sdk_e2e):**<br>- `retry_exhaust`: exactly 3 SAFs with backoffs of exactly 50/100/200 ms, then the run fails.<br>- `retry_non_retryable`: 0 SAFs.<br>- `retry_recover_hot` passes.<br>- `retry_restart_between_attempts`: 3 attempts in total across a restart, and `before` is not re-run.<br>**Replay:** `replay_after_eviction` has `greet`/`add` completed once each over 2 or more grants. `e2e_dev::kill9_mid_run_restart_completes_and_metadata_persists` (SIGKILL) passes.<br>**Native determinism:** `native_determinism` uses `guests/sdk-native` (`SystemTime::now`, std `HashMap` order, `rand::random`, both in the body and in a step). It re-executes the recorded journal on a fresh `Runtime` with a different token key: the output is byte-identical and 0 events are appended. |
| 4 | Starting a run twice with the same idempotency key returns the existing run or rejects, depending on the conflict policy. Run status, history and filtered run lists come back through the API. | VERIFIED | **Start path:** `control/runs.rs` routes keyed starts by `partition_for_key(workflow, key, P)`. `ConflictPolicy::{ReturnExisting, Reject}` maps to the partition command. `Duplicate` gives 200 with the existing id, and `IdempotencyConflict` gives 409 / `ALREADY_EXISTS` with `existing_run_id`.<br>**Read paths:** `control/history.rs` and `control/list.rs` (cross-partition list with workflow/status/tag filters).<br>**Tests:**<br>- `e2e_dev` over REST and gRPC: idempotency 201 then 200 with the same id, 409, and ALREADY_EXISTS. History starts with `run_started` and ends with `run_completed`. Lists filtered by workflow, status and tag.<br>- `start_run_api` (6), `runs_api` (2, P=16) and `ostro-partition` `apply_phase4` (19) pass. |
| 5 | Metadata (modules, workflows, queues, tokens) persists across restart through the single-voter Raft group. A stale metadata record never overwrites a newer one. When an executor loses its lease, the run is re-dispatched after `lease_ttl` and the zombie's late completion is fenced out. | VERIFIED | **Meta Raft group:** `ostro-meta/src/handle.rs` runs `RaftDriver::start(dir, ..., &[node_id], MetaSm, NullOutbox)`, then `campaign()`, then waits for leader and applied >= commit.<br>**Persistence:** `meta_persist::records_survive_restart_and_versions_never_regress` puts Module, Workflow, **Queue** (CAS chain to v2) and **Token** records, restarts on the same dir with a fresh SM, and checks the replayed state is identical. A stale put or delete is then rejected with `StaleVersion`.<br>**Versioning:** `state.rs::put/delete` validate before mutating. `meta_versions` (proptest) and `sdk_e2e::meta_stale_cas_rejected` pass (REST 412, `GET /v1/workflows` byte-identical). Through the binary, modules, workflows and active versions survive SIGKILL (`e2e_dev`). `bootstrap::stale_partition_leader_is_rejected` passes.<br>**Leases:** in `dispatcher.rs`, a lease not renewed within `lease_ttl` is revoked and re-granted as `AttemptGranted n+1`. Apply rejects a stale token with `RejectReason::Fenced` (`apply/run.rs`). In `executor.rs`, the executor self-fences.<br>**Lease tests:** `leases::stalled_attempt_is_fenced_redispatched_and_zombie_rejected`: `lease_expired >= 1`, attempt 2 owns the run, the zombie append returns `AppendError::Fenced` both before and after completion, and the zombie writes no events. `executor_self_fences_before_dispatcher_expiry`, `restart_fences_pre_restart_tokens` and `sdk_e2e::lease_zombie` also pass. |

**Score:** 5/5

## Rulings on the Specific Points

### 1. Requirement status

**META-01: SATISFIED for its stated scope. Recommend marking it Complete.**

- **The requirement text:** "Metadata state machine runs on the vendored Raft driver with Ostro's own transport; single voter on a single node".
- **Each part is met in code:**
  - The vendored driver (`ostro-raft-driver`) is used.
  - It runs as a single voter (`&[node_id]`) on a single node.
  - The transport is Ostro's own. It is the `Outbox` seam implemented in `ostro-meta`, not Chronik's transport, which was never vendored.
- **Why NullOutbox is enough:** for a lone voter, raft never addresses a peer, so a message sink that drops everything is the complete and correct transport (04-RESEARCH META-01 row).
- **Where the network transport lives:** a networked tonic transport is needed only once there is a second voter. That is CLUS-05 ("multi-voter metadata Raft (3/5 nodes)") in Phase 13, whose ROADMAP entry carries the research flag "Raft transport on tonic 0.14".
- **Why not leave it Pending:** META-01 is mapped only to Phase 4. Keeping it Pending would orphan it, because no later phase claims it.
- **Suggested traceability note:** "Complete (single voter, NullOutbox); the networked transport ships with CLUS-05 (Phase 13)".

**WF-07: SATISFIED for Rust. The Python/TS clauses belong to Phase 9, but the traceability does not say so yet. This is a planning fix, not a Phase 4 gap.**

- **What the requirement names:** `time.time()`, `Date.now()`, `Math.random()` and `HashMap`.
- **The Rust part is proven** at server level by `native_determinism`: `SystemTime::now`, `HashMap`, `rand`, a byte-identical replay on a fresh runtime. SC3 asks only for "native time/random calls in the guest", which is met.
- **Mapping:** WF-07 is **not** mapped to Phase 9. The ROADMAP Phase 9 requirement list is SDK-02..05, WF-06 and WF-10, and the REQUIREMENTS traceability row says only "Phase 4".
- **Phase 9 already plans the work:** 09-08 makes Python `time.time()` logical and 09-09 covers JS `Date.now`/`Math.random`, under D-P9-15.
- **Action for the orchestrator:**
  - Add WF-07 to the Phase 9 requirement list.
  - Change the traceability row to "Phase 4 (Rust) / Phase 9 (Python, TS)".
  - Keep it Pending until Phase 9 verifies.

**SDK-01: Complete is correct.**

- **The requirement text:** "Rust SDK: `#[workflow]`, `#[step]`, ctx, effects, `join!`/`select!`, built with `cargo build --target wasm32-wasip2` + wit-bindgen".
- **It describes the SDK surface.** The SDK exposes effect ops: `Ctx::http_fetch -> Op<HttpResponse>`, `sleep`/`sleep_until`, and `set_status` (`sdks/rust/ostro-sdk/src/ctx.rs`). These are used end to end from a plain cargo wasm32-wasip2 build.
- **Host-side effect execution is not part of this requirement.** It belongs to the Phase 6 EFF requirements. Until then, the server completes effects with the non-retryable `unsupported` result ("effects arrive in Phase 6", `sink.rs`), which is a locked decision.

### 2. SC5 "queues": properly out of scope, not a gap

SC5 asks that queue metadata **persist through the single-voter Raft group**. It does not ask for a queue API.

- `QueueConfig` is a META-02 record kind.
- `meta_persist` writes a queue record, CAS-updates it to v2, restarts, and asserts it is replayed at v2. It then rejects a stale v1 update.
- Tokens are covered the same way (`ApiToken`, plus the token-key `SealedSecret`).
- No Phase 4 requirement asks for queue CRUD over the API. API-03 takes a queue *name* on start, and that is implemented and tested. Queue configuration and flow control are Phase 7.

### 3. Startup criterion: the relaxed reading is properly recorded

- **The ROADMAP text is not amended.** SC1 still reads "starts ... with zero config in under 1 s", and CLI-01 reads "< 1 s".
- **The decision record:** STATE.md "[Phase 4 research decisions, orchestrator] (2)" says: "warm `ostro dev` restart and `--ephemeral` ... must be < 1 s; cold first-run is measured and reported, optimised best-effort (≈2k fsyncs at P=16 on WSL2 ≈1.6 s)". The same decision is listed as a manual-only row in 04-VALIDATION.md and in 04-19-SUMMARY.
- **Measured values, release build (04-19):** ephemeral 75 ms, warm about 292 ms, cold median 1624 ms.
- **Measured values, debug build (here):** ephemeral 108 ms, warm 508 ms, cold 2812 ms.
- **Ruling:** I accept this as a recorded deviation. The cold first run is a one-off per data dir and is dominated by WSL2 fsync cost while creating 16 partitions.
- **Suggestion:** amend the SC1 and CLI-01 wording to match the decision, so a later reader does not see a contradiction.

### 4. Logical time inside a retried step: a Phase 5 finding, not a Phase 4 gap

- **Confirmed in code:** `ReplayCursor::logical_now_nanos()` returns `last_cmd_ts` (`ostro-engine/src/replay/cursor.rs:220`). A requeued attempt therefore sees the previous SAF's timestamp, not `retry_at`.
- **No SC is violated:**
  - SC3 requires retries per policy, which holds: backoffs are exact and attempt counts capped.
  - It also requires identical replay, which holds: the value comes from the journal.
- **It is a semantics question.** It is already owned by Phase 5: `05-DECISIONS.md` D22 ("now() >= retry_at on requeued step attempts", commit `d9893c0`), implemented in 05-10 Task 3 with a replay-identity test and expectation fixes.

### 5. Flaky timing test `warm_restart_under_1s`: harden it (advisory, not blocking)

- **The threshold:** a single sample of spawn-to-/readyz with a hard 1 s limit. `OSTRO_STARTUP_BUDGET_MS` can override it.
- **Headroom:** in a debug build the warm restart takes about 500 ms here, which is only about 2x headroom.
- **Why it failed:** about 140 ms of a warm restart is 16 sequential fsynced `SetPartitionLeader` proposals, and `Node::open` also fsyncs. A writeback storm, like the 55 GB target-dir copy in 04-20, therefore shows up directly in the measurement.
- **The criterion itself is sound:** the release build sits at about 290 ms. The test, not the product, is what needs hardening.
- **Recommended hardening (any one of these is enough), assigned to the Phase 5 perf/CI plan:**
  - (a) Assert on the minimum of 3 warm restarts, or retry once before failing.
  - (b) Batch the 16 `SetPartitionLeader` proposals into one meta proposal. This is the speed-up 04-19 already proposed, and it also removes most of the fsync exposure.
  - (c) Run `dev_startup` in its own ci-local step after the heavy suites.
- I do not recommend raising the 1 s budget: that would weaken the CLI-01 contract.

### 6. Known Phase 5 gaps: none contradicts a Phase 4 SC

| Gap | Phase 4 SC touched? | Ruling |
|---|---|---|
| Signals, children and cancel do not wake a suspended run | No. No SC mentions signals, children or cancel. Timers do wake runs (04-13, `apply_timer_input`). | Deferred. Owned by Phase 5 plans (05-08, 05-13). |
| The run-level attempt-failure count is held in memory | No. SC3's "max attempts" is the **step** retry policy. Step attempts are journaled as SAFs, and `retry_restart_between_attempts` proves the 3-attempt cap survives a restart. The in-memory count is the executor's run-level `max_attempt_failures`, for activation crashes. | Deferred. Owned by Phase 5 plans (05-01, 05-02). |
| Concurrency keys are not enforced | No. DISP-02 says entries "carry a concurrency key from day one". The key is stored in `RunRecord`, carried through apply and `query.rs`, and present on every dispatcher ready entry (`dispatcher.rs:260/523`). Enforcement is not a Phase 4 criterion. | Deferred to Phase 5 and Phase 7 flow control (05-13, 07-05). |

## Required Artifacts

| Artifact | Status | Details |
|---|---|---|
| `crates/ostro` (binary: dev, server, openapi, build-info) | VERIFIED | `run_server` calls `Server::start`. The ready line is printed, and signals trigger graceful shutdown. 4 spawned-binary test files pass. |
| `crates/ostro-server` (config, server bootstrap, derive, sink, executor, control/{modules,runs,history,list}, leadership) | VERIFIED | The real `ServerControlPlane` serves the API. `EngineExecutor` is wired as the dispatcher `Executor`, and `MetaLeadership` is the epoch floor. 10 test binaries pass. |
| `crates/ostro-api` (ControlPlane, REST + utoipa OpenAPI, gRPC adapters, UI shell, error mapping) | VERIFIED | `router()` merges REST and tonic on one router. The openapi snapshot and REST/gRPC parity tests pass. |
| `crates/ostro-proto` (ostro.api.v1 Modules, Workflows, Runs) | VERIFIED | Used by `grpc.rs` and by the tonic clients in the tests. |
| `crates/ostro-meta` (records, versioned CAS state, MetaSm, MetaHandle) | VERIFIED | Used by `server.rs` (start, InitCluster, SetPartitionLeader) and `control/modules.rs`. |
| `crates/ostro-partition` Phase 4 additions (Running keyspace, Suspend CAS, InputAppended, TimerFired input, dispatcher, query helpers, StartRun 14..18) | VERIFIED | 108 apply/dispatcher/timer tests pass here. The dispatcher is enabled from `server.rs` with `DispatcherConfig`. |
| `crates/ostro-engine` Phase 4 additions (retry::decide, StepAttemptFailed, step retry, timeout, fuel attribution, `Runtime::prepared`) | VERIFIED | Exercised end to end by sdk_e2e. Engine suites were green in the 04-20 ci-local run (not re-run here). |
| `sdks/rust/ostro-sdk` + `ostro-sdk-macros` | VERIFIED | Used by guests sdk-basic, sdk-basic-v2, sdk-retry and sdk-native, which are built for wasm32-wasip2 in the server and binary tests. trybuild and SDK tests ran in the 04-20 ci-local run. |

## Key Link Verification

| From | To | Via | Status |
|---|---|---|---|
| `ostro dev` / `ostro server` | `Server::start` | `run_server` in main.rs | WIRED |
| Server bootstrap | MetaHandle (single voter) | `MetaHandle::start(<data>/meta)`, then `init_cluster` | WIRED |
| Server bootstrap | Node + dispatcher + EngineExecutor | `node_cfg.leader.dispatcher = Some(...)`, `Arc<dyn Executor>` | WIRED |
| Node epochs | meta | `MetaLeadership` floor plus `SetPartitionLeader` (lower epoch rejected) | WIRED |
| HTTP/gRPC | ServerControlPlane | `ostro_api::router(Arc<dyn ControlPlane>)` | WIRED |
| Upload | meta Module/Workflow records + blob | sha check, then create-only put (`Conflict` means no-op) | WIRED |
| Start run | partition `StartRun` | `partition_for_key`, conflict policy, then `map_outcome` | WIRED |
| Engine appends | partition apply | `PartitionSink` with the attempt token; stale token returns `Fenced` | WIRED |
| Dispatcher lease expiry | re-grant | revoke, then `AttemptGranted n+1` | WIRED |
| Step failure | retry | `retry::decide`, then SAF, then in-activation sleep or StepBackoff requeue | WIRED |

## Requirements Coverage

| Requirement | Source Plan(s) | Status | Evidence |
|---|---|---|---|
| META-01 | 04-01, 04-17 | SATISFIED (single node; see Ruling 1) | handle.rs, meta_persist, single_voter, bootstrap. The REQUIREMENTS checkbox and traceability row are still Pending. Recommend Complete. |
| META-02 | 04-01 | SATISFIED | records.rs has every record kind. meta_records passes. |
| META-03 | 04-01, 04-17 | SATISFIED | `Versioned<T>` with CAS. meta_versions (proptest), meta_stale_cas_rejected and stale_partition_leader_is_rejected pass. |
| DISP-01 | 04-12, 04-13, 04-14 | SATISFIED | The dispatcher rebuilds from the Running/ready state on leadership and wakes on actions, with no polling. dispatcher.rs (12 tests) passes. |
| DISP-02 | 04-14 | SATISFIED | The concurrency key is carried on ready entries. Enforcement is deferred (Ruling 6). |
| DISP-07 | 04-14, 04-16 | SATISFIED | Covered by leases.rs (4 tests) and sdk_e2e::lease_zombie. |
| API-01 | 04-02, 04-03, 04-08, 04-11 | SATISFIED | One port serves REST, gRPC and OpenAPI. parity and e2e_dev pass. |
| API-02 | 04-17 | SATISFIED | modules_api and e2e_dev pass. |
| API-03 | 04-08, 04-09, 04-12, 04-18 | SATISFIED | The StartRun fields cover queue, delay, priority, tags, version, traceparent, tenant and the idempotency policy. start_run_api passes. |
| API-04 | 04-08, 04-18 | SATISFIED | runs_api and e2e_dev cover status (including published status), history and filtered lists. |
| CLI-01 | 04-17, 04-19 | SATISFIED (startup decision, Ruling 3) | dev_startup passes. |
| CLI-02 | 04-02, 04-19 | SATISFIED | config.rs (16) and server_config (4) pass. |
| SDK-01 | 04-05, 04-06 | SATISFIED | See Ruling 1. |
| WF-01 | 04-06, 04-15, 04-18 | SATISFIED | sdk_e2e::typed_json_rest_and_grpc passes, and versioned names work. |
| WF-02 | 04-04, 04-07, 04-10, 04-15, 04-16 | SATISFIED | The retry suites pass. Step timeout and fuel are covered by engine suites (04-20 ci-local). |
| WF-07 | 04-06, 04-13 | SATISFIED for Rust. The Python/TS part is not yet mapped to Phase 9 (Ruling 1). | native_determinism passes. |

All 16 IDs in the ROADMAP Phase 4 list appear in at least one plan's `requirements` field. REQUIREMENTS.md maps no other ID to Phase 4. There are no orphaned requirements.

## Anti-Patterns

There are no blockers.

- I found no `todo!`, `unimplemented!`, TODO, FIXME or HACK markers in ostro, ostro-server, ostro-api, ostro-meta, the dispatcher or the SDK sources.
- The one TODO in `ostro-engine/src/validate.rs:49` is tagged Phase 9 (componentize-py exports) and is informational.
- The "unsupported" completions in `sink.rs` (effects, children, await tokens) are a documented, locked Phase 4 decision that Phase 6 replaces. They complete non-retryably and are journaled, not silently dropped.

## Accepted Deviations

- **Startup budget.** The < 1 s startup budget applies to warm restart and `--ephemeral`. The cold first run is reported only (STATE decision 2; Ruling 3).
- **Ephemeral storage.** `--ephemeral` keeps state on tmpfs (`/dev/shm` TempDir), not in process memory, because fjall has no in-memory mode (research D5). Observable behaviour matches the SC: nothing persists, and the directory is removed on exit.
- **Meta transport.** Meta uses `NullOutbox` as the transport for a single voter (Ruling 1).

## Human Verification

None required for sign-off. The remote CI legs (aarch64 musl, macOS) remain the project-wide FOUND-05 manual item, because there is no git remote yet. They are not specific to Phase 4: the local musl leg was green in 04-20.

## Gaps Summary

No gaps. The phase goal is achieved. Through one `ostro dev` binary, a Rust SDK component is uploaded by sha256, activated, and started over REST and gRPC on one port. Its runs complete with steps, retries, recorded results and deterministic replay, with metadata on the single-voter Raft group and leases fenced.

Follow-ups for the orchestrator (planning hygiene, not gaps):

1. Mark META-01 Complete in REQUIREMENTS.md, with a note that the networked transport ships with CLUS-05 (Phase 13).
2. Add WF-07 to the Phase 9 requirement list and set its traceability row to "Phase 4 (Rust) / Phase 9 (Python, TS)". Phase 9 plans 09-08 and 09-09 already implement it.
3. Optionally amend the SC1 and CLI-01 wording to the recorded startup decision.
4. Harden `dev_startup::warm_restart_under_1s` (min of N, or batched `SetPartitionLeader`, or a separate CI step).
5. The logical-time finding is already owned by Phase 5 D22 (05-10).

_Verified: 2026-11-19_
_Verifier: Claude (gsd-verifier)_
