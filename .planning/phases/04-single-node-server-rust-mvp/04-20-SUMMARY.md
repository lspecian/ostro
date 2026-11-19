---
phase: 04-single-node-server-rust-mvp
plan: 20
subsystem: e2e-gate
tags: [e2e, binary, sdk, retries, replay, determinism, leases, zombie, meta-cas, ci]
requires:
  - phase: 04-06
    provides: SDK guests sdk-basic / sdk-basic-v2 / sdk-retry / sdk-native
  - phase: 04-10
    provides: step timeout / fuel attribution (WF-02 engine side)
  - phase: 04-16
    provides: EngineExecutor, ExecutorHooks (stall / heartbeat suppression / jobs), DISP-07 in-process suite
  - phase: 04-17
    provides: start_with_hooks, TestServer harness, modules/workflows ControlPlane
  - phase: 04-18
    provides: runs ControlPlane (start / get / history / list) over REST + gRPC
  - phase: 04-19
    provides: ostro dev / server binary, spawned-binary harness (Spawned, SERIAL, dev())
provides:
  - crates/ostro/tests/e2e_dev.rs (success criteria 2, 4, 5 + restart replay through the spawned binary)
  - crates/ostro-server/tests/sdk_e2e.rs (WF-01/02/07, DISP-07, META-03 against the full in-process server)
  - crates/ostro/tests/common: guest_wasm, sha_hex, Api (REST + tonic Runs) helpers
  - crates/ostro-server/tests/common: TestServer::start_with, handle_of, journal, result_bytes, jobs_of
  - ci-local + ci.yml: Rust SDK workspace fmt/clippy (host + wasm32-wasip2), test, wasm build; explicit guests build step
affects: [05-*, 09-sdks, 10-cli-build, 14-release]
tech-stack:
  added: [sha2 (dev-dep of crates/ostro)]
  patterns:
    - "Binary e2e: one spawned `ostro dev` per test under SERIAL, P=4, OSTRO_LOG__LEVEL=warn, guests built before the lock"
    - "Fault injection only in-process (start_with_hooks); the binary has no test hooks"
    - "WF-07 proof: re-execute the server's recorded journal on a fresh Runtime and compare bytes with the stored result"
key-files:
  created:
    - crates/ostro/tests/e2e_dev.rs
    - crates/ostro-server/tests/sdk_e2e.rs
  modified:
    - crates/ostro/tests/common/mod.rs
    - crates/ostro/Cargo.toml
    - crates/ostro-server/tests/common/mod.rs
    - Cargo.lock
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - .planning/phases/04-single-node-server-rust-mvp/04-VALIDATION.md
key-decisions:
  - "Restart-mid-run uses SIGKILL during sdk-retry long_backoff's 3 s requeue; the history shows 2 requeued `slow` failures, not 1, because logical time inside a step is the timestamp of the last journaled command (see Findings)"
  - "replay_after_eviction runs sdk-basic with engine.hot_wait = 1 ms: its longest awaited sleep is 20 ms, so the plan's 100 ms window would never evict it"
  - "native_determinism replays the full recorded journal (read from the partition after a server restart) on a fresh Runtime with a different token key via ostro_engine::testkit::replay_journal; output bytes equal the stored PAYLOAD_RESULT and nothing is appended"
  - "dev_startup stays in the parallel `cargo test` step: it holds its own SERIAL lock and has passed there since 04-19, so no separate serial step was added"
  - "SDK workspace steps run without --locked (sdks/rust/Cargo.lock is not committed by policy)"
requirements-completed: [SDK-01, API-01]
duration: 60min
completed: 2026-11-19
---

# Phase 4 Plan 20: End-to-end success-criteria tests + phase gate Summary

**Every Phase 4 success criterion now has an automated end-to-end test. Through the spawned `ostro dev` binary, a Rust SDK component built with plain `cargo build --target wasm32-wasip2` is uploaded by sha256, started over HTTP/JSON and gRPC on one port, and completes with typed JSON results. The binary tests also cover idempotency, history and lists, survive SIGKILL mid-run with metadata intact, and switch active versions. Against the full in-process server, the tests prove step retries with exact attempt counts and backoffs (including a restart between attempts), replay after eviction, byte-identical native-nondeterminism replay on a fresh runtime, lease-stall re-dispatch with zombie fencing, and stale meta CAS rejection. ci-local and ci.yml now cover the SDK workspace, and the full gate, musl included, is green.**

## Performance

- **Duration:** about 60 min (04:15 - 05:15 UTC), most of it the two full ci-local runs
- **Tasks:** 3
- **Files:** 2 created, 7 modified

## Accomplishments

### Task 1: `crates/ostro/tests/e2e_dev.rs` (3 tests, ~7.5 s, spawned binary)

- **`upload_start_rest_and_grpc_history_list`** (`--ephemeral`, P=4):
  - `/openapi.json`: an OpenAPI 3 document listing `/v1/modules/{sha256}` and `/v1/workflows/{name}/runs`.
  - `PUT /v1/modules/{sha}` with sdk-basic gives 201 with `[sdk-basic v1, sdk-echo v1]`. Re-uploading gives 200 with `created:false`.
  - Run #1 starts over REST and run #2 over the tonic `RunsClient` on the same address. Both complete in about 70 ms, with typed results, published status `{"stage":"done"}` and the module sha.
  - gRPC `Get` of the REST-started run matches REST field by field: workflow, version, sha, queue, status, attempt, tags, priority, created/completed ms, result and published status.
  - Idempotency over REST: the same key gives 201, then 200 with the same id. `reject` gives 409 with `error.details.existing_run_id`.
  - Idempotency over gRPC: `return_existing` gives the same id. `reject` gives `ALREADY_EXISTS`, and `status_details` returns `existing_run_id`.
  - History, REST and gRPC: starts with `run_started` (with input), ends with `run_completed` (with output). Exactly one `step_completed` each for `greet` and `add`, and the same kind sequence over gRPC.
  - Lists:
    - `workflow=sdk-basic&status=completed` returns all 3 runs.
    - `tag=http` returns only run #1 and `tag=grpc` only run #2. `tag=e2e` returns both.
    - `workflow=sdk-echo` is empty.
    - gRPC `List` with workflow, status and tag filters returns the same 2 runs.
- **`kill9_mid_run_restart_completes_and_metadata_persists`** (`--data-dir` on tmpfs, P=4):
  - Uploads sdk-basic and sdk-retry, then completes one sdk-basic run.
  - Starts sdk-retry `long_backoff` and waits until history shows `before` completed and `slow`'s first `step_attempt_failed` (3 s requeue).
  - **SIGKILL**, then restarts `ostro dev` on the same dir and a new port.
  - The run completes with `{"mode":"long_backoff","before":1,"attempts":null}`, with exactly one `step_completed` for `before` and one for `slow`. That is 2 requeued SAFs (see Findings), one `run_started` and one `run_completed`.
  - `GET /v1/workflows` still shows sdk-basic, sdk-echo and sdk-retry, each with active version 1 and the uploaded shas. `GET /v1/modules/{sha}` returns 200 for both modules.
  - A re-upload is a no-op. The pre-kill run is unchanged, and new runs complete.
- **`activate_v2_new_runs_use_v2`**:
  - Uploading v2 leaves active at 1 (2 versions listed). `PUT active {2}` changes it to 2.
  - A new run without a version runs v2: `version 2`, the v2 sha, and `result.version == 2`.
  - The earlier run still reports v1 and the v1 sha. An explicit `version: 1` start still runs v1.
- **`tests/common/mod.rs`** (additive): `guest_wasm` (copy of the engine/server helper), `sha_hex`, `is_terminal`, and `Api`:
  - REST: get, upload, workflow, activate, start_http/start_new, run, wait_run/wait_terminal, paged history, paged list
  - tonic: runs, start_grpc, get_grpc
  - Also `events_of` and `step_completions`.
- `crates/ostro/Cargo.toml`: dev-dep `sha2.workspace = true` (Cargo.lock: one line).

### Task 2: `crates/ostro-server/tests/sdk_e2e.rs` (9 tests, ~9.6 s, `start_with_hooks` on a real port)

| Test | Proves |
|------|--------|
| `typed_json_rest_and_grpc` | sdk-echo echoes a nested JSON document (unicode, escaped quotes, negative and float numbers, 2^53-1, null, empty object) over REST and gRPC. sdk-basic maps typed structs on both transports. A type-mismatched input fails non-retryably with `invalid_json`. (WF-01) |
| `retry_exhaust` | `flaky`: exactly 3 SAFs, attempts 1..3, `in_activation`. `retry_at_ms - ts_ms` is exactly **50 / 100 / 200 ms**. Each retry runs at or after the journaled `retry_at`. Then `StepCompleted(Err transient)`. The run is `failed`, and the parsed `failure.message` contains the step error. (WF-02) |
| `retry_recover_hot` | `fail_times 2`: 2 SAFs, then `StepCompleted(Ok 3)` and result `attempts:3`. One grant (`attempt == 1`) and exactly one executor job. |
| `retry_non_retryable` | 0 SAFs, `StepCompleted(Err fatal)`, run failed. |
| `retry_restart_between_attempts` | `exhaust_slow`: after attempt 1's SAF, `TestServer::restart()`. Exactly 2 requeued SAFs (attempts 1, 2) and one final `StepCompleted(Err)`, so **3 attempts in total** (Pitfall 8). `before` is replayed, not re-run. |
| `replay_after_eviction` | `engine.hot_wait = 1 ms`: sdk-basic is evicted on its sleeps, giving 2 or more jobs and 2 or more grants. `greet` and `add` are each completed once. One `run_started`, one `run_completed`, and timers fired. |
| `native_determinism` | sdk-native completes. Restart the server, read the journal from the partition, and re-execute it on a **fresh `Runtime`** (new engine, empty caches, different token key) with `testkit::replay_journal`. The output bytes are **identical** to the stored result payload, and 0 events are appended. Both maps have 50 keys. GET after the restart serves the same result. (WF-07, Rust) |
| `lease_zombie` | `lease_ttl 500 ms`, heartbeat 125 ms, 1 slot. Attempt 1 has its heartbeat suppressed and its appends gated. The run completes once with the correct result. Attempt 1's job is `Evicted` and revoked or self-fenced, and a later attempt completes it. After that, an append with attempt 1's token returns **`AppendError::Fenced`**. Releasing the gate writes nothing. History has no `zombie` step, `greet` and `add` once each, one `run_completed`, and it ends with `run_completed`. (DISP-07) |
| `meta_stale_cas_rejected` | A `Put` on `Active(sdk-basic)` with `expected_version` of current-1 or current+1 returns `StaleVersion{current}`. A create on the existing key returns `Conflict`. REST activation with a stale `expected_version` returns 412. `GET /v1/workflows` is byte-for-byte unchanged. (META-03) |

- **`tests/common/mod.rs`** (additive): `TestServer::start_with(opts, tweak)` (config knobs), `handle_of`, `journal`, `result_bytes`, `jobs_of`.

### Task 3: CI wiring + gate

- `scripts/ci-local.sh` has three new steps before `cargo test`. The SDK steps run without `--locked` because the SDK lock is not committed.
  - `sdk workspace fmt + clippy`: fmt check, clippy `--all-targets` on the host, and clippy for `wasm32-wasip2`, all `-D warnings`.
  - `sdk workspace test + wasm build`: host tests including trybuild, then the wasm32-wasip2 build.
  - `guests build (wasm32-wasip2)`: the same build `guest_wasm()` runs, so a broken guest fails with compiler output.
- `.github/workflows/ci.yml`: SDK fmt and clippy in the `lint` job. SDK test, wasm build and guests build on the `linux-x86_64-gnu` leg. YAML validation passes, and all actions are still SHA-pinned.
- `04-VALIDATION.md`: every per-task row is `✅ green`, `nyquist_compliant: true`, `wave_0_complete: true`, and the Wave 0 checklist is ticked. The "feedback latency < 60 s" item stays unticked by design.

## Task Commits

1. **Task 1: binary-level e2e through spawned `ostro dev`**: `3eec439` (test)
2. **Task 2: full-server SDK e2e**: `254e2a0` (test)
3. **Task 3: ci-local + CI SDK/guests steps**: `b0b4e58` (chore). VALIDATION is in the docs commit.

## CI (phase gate)

The full `bash scripts/ci-local.sh`, musl legs included, with `CARGO_TARGET_DIR=target/wt-04-20`, passed every step at HEAD `b0b4e58` (all three task commits in; the later `f1b8f4f` is a Phase 9 docs-only commit by a planning agent):

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | |
| cargo fmt --check | PASS | |
| cargo clippy -D warnings | PASS | |
| cargo build (warning-free) | PASS | |
| sdk workspace fmt + clippy (new) | PASS | |
| sdk workspace test + wasm build (new) | PASS | |
| guests build (wasm32-wasip2) (new) | PASS | |
| cargo test | PASS | 391 s |
| failpoint suites | PASS | 358 s |
| cargo deny check | PASS | |
| check-vendored.sh | PASS | |
| check-deny-fixtures.sh | PASS | |
| no chronik/tokio-uring/prometheus | PASS | |
| workflow validation | PASS | |
| zig + cargo-zigbuild present | PASS | |
| musl release (static, `allocator=mimalloc target=x86_64-unknown-linux-musl`) | PASS | |
| musl zigbuild test | PASS | 407 s |

- Inside `cargo test`: e2e_dev 3/3 (7.4 s), sdk_e2e 9/9 (9.6 s), dev_startup 4 (+1 ignored). The musl zigbuild test leg ran the same suites against the musl test build, and they were green: e2e_dev 3/3, sdk_e2e 9/9, dev_startup 4.
- No `Node::open timed out` flake in either full run.
- Short step times reflect a warm target dir, seeded by copying `target/wt-04-19`.
- Repeats before the gate: `sdk_e2e` and `e2e_dev`, three back-to-back runs each, all green.
- The first full run failed only `cargo test`, on `dev_startup::warm_restart_under_1s` (7.5 s under disk writeback; see Finding 2). All 16 other steps passed.

## Findings for the verifier / Phase 5

1. **Logical time inside a step does not advance to `retry_at` on a requeued retry.** `ctx.now()` returns the timestamp of the last journaled command (`ReplayCursor::logical_now_nanos`). In a requeued attempt, the last command is the previous `StepAttemptFailed`.
   - Effect: sdk-retry `long_backoff` measures `ctx.now() - t0` inside `slow` and needs 2 s. Attempt 2 (wall clock about 3 s later) sees about 0 ms and fails again. Attempt 3 sees attempt 2's SAF time (3016 ms) and succeeds.
   - The result is deterministic and replay-safe, but it contradicts the guest's doc comment ("one Requeue backoff, then success").
   - Whether in-step `now()` should be at least `retry_at` (or wall clock, since the step result is journaled anyway) is a semantic question for the Phase 5 freeze. It was not changed here (engine semantics, Rule 4). The e2e asserts the actual behaviour, 2 SAFs, with a comment.
2. **Transient warm-restart budget failure under I/O load.** In the first full ci-local run, `dev_startup::warm_restart_under_1s` measured **7.5 s** while writeback of a 55 GB target-dir copy (my private `CARGO_TARGET_DIR` seeding) was still draining. Because `cargo test` stops at the first failing binary, the step failed after 24 s. Every other step passed, including the musl zigbuild test of the whole workspace with both new suites.
   - Re-running `dev_startup` alone three times gave warm restarts of 451, 453 and 272 ms, all green.
   - The second full run is the gate result above.
   - No server code changed in this plan. The 1 s assertion is sensitive to fsync stalls on WSL2, so `OSTRO_STARTUP_BUDGET_MS` exists for loaded machines.
3. **Requirements left Pending on purpose:**
   - **WF-07**: Rust is proven through the server. The requirement text also names Python `time.time()` and TS `Date.now()`/`Math.random()`, which arrive with those SDKs.
   - **META-01**: a single voter runs on the vendored driver, but the transport is still `NullOutbox` (04-17 note). "Ostro's own transport" is not delivered.
   - The orchestrator or verifier decides whether a single node is enough for either one.
   - **Marked Complete: SDK-01, API-01.**
     - SDK-01: `#[workflow]`, `#[step]`, ctx, `join!`/`select!` and the effect ops (durable sleeps) run end to end from a plain cargo wasm32-wasip2 build. The SDK's HTTP-effect call exists, but the host effects bridge is Phase 6. Revisit if the verifier reads "effects" as host execution.
     - API-01: one port serves REST and gRPC with identical semantics (04-11 parity suite, plus this binary e2e), and the OpenAPI document is served.
4. **Success criterion 5 wording:** "modules, workflows, queues, tokens persist". Modules, workflow versions, active versions and the token-key record are proven to survive restart: here through SIGKILL and the binary, and in 04-17's `metadata_survives_restart`. Queue records exist in the meta model but have no API in Phase 4, so no e2e can create one.
5. **Phase 5 gaps were not hit by these tests:**
   - signals/children/cancel do not wake a suspended run;
   - the attempt-failure count is held in memory only;
   - concurrency keys are not enforced.
   - Note that `retry_restart_between_attempts` counts **step** attempts, which are journaled and durable. It does not count the in-memory run-level attempt failures.
6. **`attempt`** is asserted `== 1` only in `retry_recover_hot`, where the run has no sleeps and must be a single grant. Everywhere else it is `>= 1` or `>= 2`, per the 04-18 hand-off.

## Deviations from Plan

### Auto-fixed / adjusted

**1. [Rule 1 - Test expectation] Restart test expects 2 `slow` SAFs, not 1.** See Finding 1. The plan's assertions ("exactly one StepCompleted for `before` and for `slow`") hold unchanged. **Commit:** 3eec439

**2. [Rule 1 - Test design] `replay_after_eviction` uses `hot_wait = 1 ms`, not 100 ms.** sdk-basic's awaited sleeps are 10, 20 and 5 ms (the 1 h sleep loses the select and is cancelled), so a 100 ms window never evicts. 1 ms forces eviction and resume on each sleep. **Commit:** 254e2a0

**3. [Rule 3 - Blocking] Cargo.lock** gained one line for the `sha2` dev-dep of `ostro`, which was not listed in `files_modified`. **Commit:** 3eec439

**4. TDD RED not observable.** Every behaviour these suites test was already implemented by 04-06..04-19, so both suites passed on their first green run, as 04-19 also reported. The one real failure, the `slow` SAF count, was an expectation error, not a missing feature.

### Design choices within the plan's latitude

- **Native determinism method:** a server-level re-execution of the recorded journal on a fresh `Runtime` (the plan's preferred option) instead of comparing two independent runs. Two runs have different seeds, so their outputs differ by design.
- **The `meta_stale` test** also covers create-on-existing (`Conflict`) and the REST `expected_version` path (412).
- **No separate serial `dev_startup` step:** see key-decisions.
- **The binary e2e uses P=4** (`OSTRO_NODE__PARTITIONS=4`) and `OSTRO_LOG__LEVEL=warn` for speed. The P=16 multi-partition behaviour is covered by 04-18's `runs_api`.

## Self-Check: PASSED

- FOUND: crates/ostro/tests/e2e_dev.rs, crates/ostro-server/tests/sdk_e2e.rs, both tests/common/mod.rs, scripts/ci-local.sh (`sdks/rust`), .github/workflows/ci.yml
- FOUND: the `start_with_hooks` path used by sdk_e2e (via TestServer::boot); `CARGO_BIN_EXE_ostro` used by e2e_dev (via common::bin)
- FOUND: commits 3eec439, 254e2a0, b0b4e58
