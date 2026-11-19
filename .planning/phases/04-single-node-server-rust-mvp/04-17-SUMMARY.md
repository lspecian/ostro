---
phase: 04-single-node-server-rust-mvp
plan: 17
subsystem: server-bootstrap
tags: [server, bootstrap, meta, leadership, ephemeral, control-plane, modules, workflows]
requires:
  - phase: 04-01
    provides: MetaHandle (single voter, immediate campaign), versioned CAS records, InitCluster, token key
  - phase: 04-02
    provides: ServerConfig (node/api/engine/executor/log sections)
  - phase: 04-11
    provides: ostro_api::router (REST + gRPC on one port), ControlPlane trait
  - phase: 04-16
    provides: EngineExecutor, ExecutorConfig, ExecutorHooks, tests/common TestNode harness
provides:
  - ostro_server::{Server, start, ServerHandle, ServerError, StartupTimings}
  - ostro_server::server::start_with_hooks (doc(hidden), executor hooks for full-server tests)
  - ostro_server::leadership::MetaLeadership (LeadershipSource over meta partition epochs)
  - ostro_server::control::ServerControlPlane (real ControlPlane; modules/workflows; runs stubbed)
  - tests/common TestServer harness (real server on 127.0.0.1:0, tmpfs dir, restart keeping the dir)
affects: [04-18, 04-19, 04-20, 06-effects-bridge, 08-triggers]
tech-stack:
  added: [ostro-meta, ostro-api, axum, sha2, uuid in ostro-server; dev: reqwest, tonic, ostro-proto]
  patterns:
    - "Bind the listener before any disk work (busy port fails fast); serve only after the node is open"
    - "Module record is the upload commit point: workflow + active records first, module record last"
    - "Checks (sha, validation, describe, write-once bindings) before anything is stored"
key-files:
  created:
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/leadership.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/control/modules.rs
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/tests/bootstrap.rs
    - crates/ostro-server/tests/modules_api.rs
  modified:
    - crates/ostro-server/Cargo.toml
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/tests/common/mod.rs
    - Cargo.lock
key-decisions:
  - "Bootstrap: bind -> meta -> InitCluster (uuid v7 cluster id + getrandom token key on first boot; stored key read back every boot) -> Runtime(token key, <data>/cache/aot) -> Node(MetaLeadership, dispatcher EngineExecutor) -> SetPartitionLeader per Ready partition -> serve"
  - "Partition-count mismatch is checked against the stored cluster record before and after InitCluster and reported as ServerError::PartitionCountMismatch{stored, configured}"
  - "Any non-Ok SetPartitionLeader at startup is fatal (MetaLeadership makes a stale term impossible)"
  - "ExecutorConfig from [executor]: heartbeat = effective_heartbeat(), lease_ttl shared with the dispatcher, self_fence_margin = ttl/10, max_attempt_failures, retry_backoff"
  - "Upload order: sha checks -> existing module = no-op (runtime untouched) -> \\0asm magic check -> prepare_cached + describe -> write-once binding check -> blob put -> create-only Workflow records -> create-only Active for names with none (lowest version of this module) -> create-only Module record (commit point)"
  - "record_version = version of the Active(name) record (0 while none); activation with expected_version is an exact CAS (412 on mismatch), without it read-and-CAS with up to 8 retries"
  - "Readiness = serving flag && meta.is_leader() && every partition slot Ready and machine health not Failed; failed partition slots keep the server up but /readyz 503"
requirements-completed: [API-02]
duration: 35min
completed: 2026-11-19
---

# Phase 4 Plan 17: Server bootstrap + modules/workflows ControlPlane Summary

**`ostro_server::Server::start(ServerConfig)` now boots the whole single-node server in one process: meta, cluster identity and token key, engine runtime, the partition node with the dispatcher running `EngineExecutor`, and the one-port REST + gRPC API. A real `ControlPlane` handles module upload by sha256 (validated, AOT-compiled, write-once workflow bindings, first version auto-activated) and CAS activation. Restarts keep identity and metadata and always start a higher partition epoch; `--ephemeral` runs on a tmpfs temp dir that is removed on shutdown.**

## Performance

- **Duration:** about 35 min (02:53 - 03:28 UTC, excluding the ci-local run)
- **Tasks:** 2 (Task 2 TDD: RED commit, then GREEN)
- **Files:** 7 created, 4 modified

## Accomplishments

- **server.rs**
  - `Server::start` / `start` / `start_with_hooks(cfg, Arc<ExecutorHooks>)` follow the research startup order (see the module docs).
  - `ServerHandle`: `local_addr()`, `data_dir()`, `is_ephemeral()`, `cluster_id()`, `token_key()`, `timings()`, `ready()`, `control()`, `meta()`, `runtime()`, `node()`, `dispatcher_stats()`, `wait()`, `shutdown()`.
  - `StartupTimings { prepare, meta_leader, init_cluster, runtime, node_open, record_leaders, total }`, logged at info as `ostro server started`. On tmpfs with P=4 (debug build), total was about 37 ms (meta 1.6 ms, runtime 6 ms, node open 24 ms). `/readyz` returned 200 within 2 ms of `start` returning.
  - `ServerError` one-line messages: `DataDir`, `Bind{addr}`, `Config`, `Meta`, `PartitionCountMismatch{stored, configured}`, `Runtime`, `Node`, `Invariant`.
  - Shutdown order: `/readyz` 503, then `axum` graceful drain (5 s cap), then `Node::shutdown`, then meta stop, then the ephemeral `TempDir` is closed.
- **leadership.rs**: `MetaLeadership::epoch_floor(p)` returns the stored meta epoch (0 when absent).
- **control/mod.rs**: `ServerControlPlane` with helpers for 04-18: `partition(p)`, `partition_count()`, `blobs()`, `meta()`, `runtime()`, `config()`, `now_ms()`. `ready()` is the readiness rule above.
- **control/modules.rs**: upload, get, list and activate as described in `key-decisions`. Error shapes match `FakeControlPlane`: `actual_sha256` details on a mismatch, `existing_module_sha256` on a 409, `record_version` on a 412.
- **control/runs.rs**: the four run methods return `Unavailable("runs API lands in 04-18")`, via the const `PENDING`.
- **tests/bootstrap.rs** (5 tests):
  - ready plus `/healthz` plus gRPC health SERVING on one port, and every partition epoch in meta equals the handle epoch
  - a restart keeps the cluster id and token key, and every partition epoch strictly increases
  - P=4 then P=8 gives `PartitionCountMismatch{4, 8}` with both numbers in the message, and the same P restarts fine
  - an ephemeral dir lives under `/dev/shm`, the configured dir is untouched, and the dir is removed after shutdown
  - a lower-epoch `SetPartitionLeader` is `StaleVersion{current}` and meta is unchanged
- **tests/modules_api.rs** (3 tests):
  - REST: 201 with `[sdk-basic v1, sdk-echo v1]`, active 1, blob stored and L2 warm. A re-upload is 200 with `created=false` and identical `cache_stats()` and L2 size. These cases are 400: a wrong sha (with `actual_sha256`), a 20 MB body with a wrong sha, a non-hex sha, junk bytes, a component preamble followed by garbage, and a core module (with the engine message). A different module that describes `(sdk-basic, 1)` is 409 naming the v1 sha, and nothing is recorded. Uploading v2 is 201 and the active version stays 1. Activation: stale 412, current 200, omitted 200, unknown version or name 404. Runs return 503.
  - gRPC: the same scenario on the same port. Codes are InvalidArgument, AlreadyExists (with details), FailedPrecondition and NotFound.
  - Restart: module, workflow, active and token records keep their versions, and the workflow listing is identical. A re-upload after the restart is still a no-op.

## Task Commits

1. **Task 1: bootstrap, MetaLeadership, readiness, ephemeral, shutdown, TestServer, bootstrap suite**: `3cae3da` (feat)
2. **Task 2 RED: API-02 suite**: `4a4f17f` (test)
3. **Task 2 GREEN: modules/workflows ControlPlane**: `3e6de42` (feat)

## Verification

- `cargo test -p ostro-server --locked`: lib 18, bootstrap 5, config 16, executor 8, leases 4, modules_api 3, sink 15. All green.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings` and `cargo fmt --all --check`: clean.
- RED was confirmed before GREEN: modules_api failed 3/3 against the stub (503).
- `bash scripts/ci-local.sh --no-musl` with the private `CARGO_TARGET_DIR=target/wt-04-17` at 3e6de42. Results:
  - **PASS:** toolchain, fmt, clippy, warning-free build, `cargo test` (396 s), failpoint suites (473 s), deny, check-vendored, deny-fixtures, forbidden deps, workflow validation.
  - **SKIP:** the musl legs.
  - No `Node::open timed out` flake. FAILPOINT_CRATES is unchanged (no new failpoint crates).

## Deviations from Plan

### Design choices within the plan's latitude

**1. Store only after the checks.** The plan lists `blobs().put` before `prepare_cached`. Here the blob is stored only after validation, describe and the write-once binding check pass, so an invalid or conflicting upload leaves nothing in the blob store.

**2. The listener is bound first.** The plan binds it in step 5. Binding at step 0 makes a busy port fail before meta and the node touch the disk. Early connections wait in the backlog and are answered as soon as serving starts.

**3. Module record written last.** It is the upload's commit point. An interrupted upload is completed by repeating it, because existing workflow and active records with the same sha are accepted. The plan wrote Module first.

**4. `\0asm` magic check, and AOT compile errors mapped to 400.** Without a cache dir, `prepare_cached` reports invalid bytes as `EngineError::Validation`. With one, the AOT cache reports them as `EngineError::Cache("compile failed: ...")`. Both map to `400 invalid Wasm component: ...`. Other cache errors are 500.

**5. `get_module` with a malformed sha is 400.** `FakeControlPlane` returns 404. The parity suite never sends a malformed sha.

**6. No `engine.aot_cache` switch.** `EngineSection` has no such field (04-02). The AOT cache is always on, under `<data>/cache/aot`.

**7. A failed partition slot does not abort startup.** The server serves with `/readyz` at 503 and logs the slot error. The plan's readiness rule is unchanged.

None of these change an interface the plan names.

## Known gaps / notes

- **META-01 stays Pending.** Meta now runs as a single voter inside the server, but the transport is still `NullOutbox`, as 04-01 noted. Whether this closes META-01 is for the orchestrator or verifier to decide.
- **CLI-01 stays Pending.** The ephemeral strategy is proven at library level only. The `ostro dev --ephemeral` binary and its < 1 s timing are 04-19.
- If a connection outlives the 5 s drain, the control plane is still referenced at shutdown. In that case the meta driver stops when the last reference drops (a `spawn_blocking` drop) and a warning is logged. This never happened in the tests.

## Hand-off: 04-18 (runs ControlPlane)

- Replace the four bodies in `control/runs.rs` and delete `PENDING`. The verifier greps for "runs API lands in 04-18".
- Add `pub mod history; pub mod list;` to `control/mod.rs`. The trait impl already delegates to `runs::{start_run, get_run, run_history, list_runs}(cp, req)`; change the targets as you like.
- Helpers on `ServerControlPlane`:
  - `partition(p) -> Result<&PartitionHandle<Rf1Log>, ApiError>` (503 for a failed slot)
  - `partition_count()`, `blobs()`, `now_ms()`, `config()`, `runtime()`
  - `meta().view().read(|s| s.workflow(name, v) / s.active(name))`
- `WorkflowVersion` carries `module_sha256`, `default_queue` and `run_timeout_ms` from `describe()`.
- `meta_unavailable(MetaError) -> ApiError` is private in `control/modules.rs`. Move it to `mod.rs` if runs need it.
- Tests: use `common::{TestServer, ServerOpts, guest_wasm}`. `TestServer::start(ServerOpts::default())` gives P=4 on tmpfs. Then call `wait_ready`, use `http` (reqwest) or `channel()` (tonic), call `restart()` to keep the dir, and end with `shutdown()`. To deploy, `PUT /v1/modules/{sha}`; see `put_module` in tests/modules_api.rs.
- StartRun wiring from the 04-16 hand-off still applies (module_sha, requested_at_ms = now, 32-byte rng_seed, run_timeout_ms, application/json input).

## Hand-off: 04-19 (binary)

- `ostro_server::Server::start(cfg).await -> Result<ServerHandle, ServerError>`. `ServerError`'s Display is a one-line message suitable for stderr: a busy port reads `cannot listen on ADDR: ...`, and a P mismatch names both values.
- Set `cfg.node.ephemeral = true` for `--ephemeral`. The server creates the tmpfs dir and `handle.data_dir()` reports it. `handle.is_ephemeral()` is true, and `handle.shutdown().await` removes the dir.
- For the ready line, read `handle.local_addr()` (the real port when listening on :0) and `handle.timings().total`. The server is ready when `start` returns, provided no partition failed.
- Signals: `tokio::select! { _ = ctrl_c => {}, _ = sigterm => {}, _ = handle.wait() => {} }`, then `handle.shutdown().await`.
- Startup cost is dominated by `Node::open`. P=16 on disk will be slower than the P=4 tmpfs number above; measure it in the spawned-binary test.

## Files both 04-18 and 04-19 might touch

- **Cargo.lock**: 04-19 adds deps to `crates/ostro`. 04-18 should need no new deps; if it adds any, rebase the lock.
- **.planning/STATE.md, ROADMAP.md, REQUIREMENTS.md**: both update them.
- **Source files**: no overlap. 04-18 owns `crates/ostro-server/src/control/*` and `crates/ostro-server/tests/*`. 04-19 owns `crates/ostro/*`. Neither needs `server.rs`, `lib.rs` or `crates/ostro-server/Cargo.toml`. If 04-19 needs another `ServerHandle` accessor, that edit lands in `server.rs`, which 04-18 does not touch.
- Phase 8's services/* stubs and Phase 6's `effects.rs` / `EffectHooks` were not created. `server.rs` builds the router in a single place, step 5 of `start_with_hooks`, so extra services can be merged there.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/server.rs (`pub async fn start`)
- FOUND: crates/ostro-server/src/leadership.rs (`impl LeadershipSource for MetaLeadership`)
- FOUND: crates/ostro-server/src/control/modules.rs (`prepare_cached`)
- FOUND: crates/ostro-server/tests/bootstrap.rs, tests/modules_api.rs
- FOUND: commits 3cae3da, 4a4f17f, 3e6de42
