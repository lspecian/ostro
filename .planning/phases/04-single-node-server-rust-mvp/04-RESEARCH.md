# Phase 4: Single-Node Server & Rust MVP - Research

**Researched:** 2026-11-18
**Domain:** Integration of the durable partition log (Phase 2) with the deterministic Wasm engine (Phase 3), plus metadata Raft, dispatcher/leases, one-port HTTP+gRPC API, `ostro dev`/`server` CLI and the Rust guest SDK
**Confidence:** HIGH for code seams (read from the tree at `e4a744c`); HIGH for measured startup numbers (probes run on this machine); MEDIUM for library choices (registry-verified versions, docs partly verified); MEDIUM for new designs (retry, run lifecycle), which need the planner to adopt them explicitly

## Summary

Phase 4 is mostly integration work, but the Phase 2 and Phase 3 contracts have **five real gaps** that the planner has to close before runs can go end to end. The gaps are listed in "Contract Gaps" below, and closing them is the critical path.

1. Nothing writes the `RunStarted` journal event, and `StartRun` does not record which module a run is pinned to.
2. Apply does not model a run that is waiting between activations. A `Running` run that gets evicted can never be woken.
3. Inputs such as `TimerFired` go into an `inbox` table that nothing consumes. The engine expects inputs to be journal events at `next_seq`.
4. Step retry (`retry-now`, the `retry-policy` in `step-options`) has no journal representation, and step `timeout-ms` is not enforced.
5. `StartRun` and `RunRecord` lack tags, tenant, traceparent and module sha.

All of these can still change additively, because the format freeze is at the end of Phase 5.

Most of the ingredients exist.
- `PartitionHandle::{submit, grant_attempt, prepare_payload, fetch_payload, reader}` is the write and read API.
- `MachineHandle::subscribe_actions()` delivers `EnqueueReady`, `ScheduleTimer` and the other actions. The dispatcher uses it as its wake-up, the same pattern the 02-14 timer service uses.
- `Runtime::{prepare_cached, describe, activate}` plus the `ActivationSink` trait (`append`, `fetch_blob`, `done`) is the engine API.
- `Activate.inputs` / `cancel` are the hot-wait and self-fence hooks.
- The vendored `RaftDriver` already gives a single-voter state machine with "proposal returns the apply result, visible locally when it resolves" semantics.
- The `ostro-api` skeleton already merges `tonic::service::Routes::into_axum_router()` into an axum router. `axum::serve` uses `hyper_util`'s auto builder, so HTTP/1.1, h2c and gRPC share one port.

**Measured on this machine (WSL2, 12 cores):**
- **Raft election.** Raft single-voter election waits for the election timeout (about 2.85 s per start, from the existing `single_voter` test: 5.7 s for two starts). Calling `RaftHandle::campaign()` right after `start` gives a leader in **25–60 ms**.
- **Partition open.** `Node::open` with P=16 on a fresh directory takes **1.53–1.69 s on disk**, about 2,000 fsyncs at about 6.8 ms each under WSL. A warm reopen takes **260–300 ms**, and a fresh open on `/dev/shm` takes **44 ms**.
- **Engine.** `Runtime::new` takes 4–5 ms.

So `ostro dev --ephemeral` on tmpfs meets the < 1 s budget comfortably. A warm `ostro dev` meets it. A **first-ever `ostro dev` on a slow-fsync disk does not** meet it without mitigation.

**Primary recommendation:** Build a new `ostro-server` glue crate that owns the real `ActivationSink`, the mapping from journal events to Derived intents, the executor, and the bootstrap order (meta → partitions → dispatcher → API).
- Put a generic dispatcher in `ostro-partition` as a fourth leader service behind an `Executor` trait.
- Keep `ostro-api` as thin HTTP/gRPC adapters over a `ControlPlane` trait.
- Close the contract gaps with additive changes to `ostro-types::command` and apply, before any end-to-end plan.

## User Constraints

No `04-CONTEXT.md` exists. `/gsd:discuss-phase` was not run for Phase 4. These locked constraints come from STATE.md, PROJECT.md and the orchestrator prompt, and the planner must honour them:

### Locked Decisions (project-level)
- Rust 1.100.0 pinned. Builds must be warning-free, and the per-crate `unused_dependencies` lint is active: a dependency wired before its consuming plan is anchored with `use x as _;`. `scripts/ci-local.sh` is the gate.
- **No `chrono`** (banned in deny.toml; use `jiff`). **No `bincode`.** cargo-deny bans duplicate `tonic`, `tonic-prost`, `prost`, `prost-types`, `hyper`, `http`, `axum`, `opentelemetry`, `opentelemetry_sdk` and `wasmtime`. rustls with aws-lc-rs only (no openssl or native-tls).
- tonic/prost 0.14.9 / 0.14.6, axum 0.8.9 on one port through `Routes::into_axum_router()`. `protox` codegen, no `protoc`.
- Fixed partition count, default P=16 single node.
- One replication code path, including RF=1 (D5): single node runs `Rf1Log` behind `ReplicatedLog`. **The ephemeral mode must use the same code path, not a separate in-memory store.**
- Wasmtime `=50.0.2`. Host imports stay thin.
- WIT `ostro:runtime@0.1.0`, the command set and the on-disk format can change **additively** until they freeze at the end of Phase 5. Golden vectors (`tests/golden/commands.txt`, `journal_v1.txt`, the det-basic snapshot) are never blindly re-blessed.
- The command tags 1..=7 and the apply outcome codes are frozen. Command tags 8..=63 are reserved for additions.
- The step handle is a `resource step`, and the engine must not rely on `HostStep::drop`.
- Rust guests build with `cargo build --target wasm32-wasip2` and `wit-bindgen =0.63.0`. Do not use `cargo-component`.
- The `guests/` workspace is separate from the root workspace and its `Cargo.lock` is gitignored. Tests get guest wasm through `tests/common::guest_wasm(name)`.
- Apply never reads clocks: every time value comes from the command. `clippy.toml` in ostro-partition bans clocks and HashMap/HashSet.

### Claude's Discretion (all Phase 4 design choices not listed above)
Crate placement, the retry mechanics, API shapes, the config crate, the SDK macro design and the ephemeral strategy. The recommendations for each are below.

### Deferred Ideas (OUT OF SCOPE for Phase 4)
- Timers, sleep and signals as user-facing features (WF-03/04/05/08/09) are Phase 5.
- Signal, cancel, terminate and retry over the API, long-poll and token completion (API-05..07) are Phase 5.
- The effects bridge (FX-*) is Phase 6. Workers and flow-control enforcement (DISP-03..05, WORK-*) are Phase 7.
- The UI is Phase 11; Phase 4 ships only a placeholder "UI shell" page. `ostro build/deploy/init` is Phase 10.
- Auth enforcement (SEC-02) is Phase 14. Phase 4 only stores token records.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| META-01 | Metadata SM on vendored Raft driver, own transport, single voter | `RaftDriver::start(dir, DriverConfig, &[1], sm, NullOutbox)` + **`handle.campaign()` immediately** (measured 25–60 ms vs ~2.85 s). `NullOutbox` is correct for 1 voter; the tonic transport is Phase 13. See "Metadata State Machine" |
| META-02 | Metadata holds membership, partition map, modules, workflows+active versions, queues, schedules, tokens, tenants, sealed secrets, grants, connections, egress policies | One `MetaState` of `BTreeMap`s, a postcard snapshot, every record type defined now. Phase 4 *exercises* modules, workflows, queues, tokens, membership and partition map; the rest are defined and persisted only |
| META-03 | Every record carries a version; stale never overwrites newer | `Versioned<T>{version, updated_index}` plus CAS at apply (`expected_version`). The partition map uses epoch as its causality token (Chronik RP lesson). Table-driven "stale rejected" test over every record kind |
| DISP-01 | Per-partition leader dispatcher, in-memory ready queues rebuilt from state on leadership, no polling | New leader service `leader::dispatcher` (same spawn contract as timers/shuffler): rebuild from the `Ready` table plus in-flight runs, wake on `Action::EnqueueReady` via `subscribe_actions`, `sleep_until` for future `ready_at`, rescan only on `Lagged` |
| DISP-02 | Ready entries carry a concurrency key | Already stored: `ReadyValue{concurrency_key}` and `Action::EnqueueReady{concurrency_key}`. Carry it into the in-memory `ReadyEntry` (not enforced until Phase 7) |
| DISP-07 | Soft leases, re-dispatch after `lease_ttl`, zombies fenced by `(epoch, attempt)`, self-fence | `AttemptGranted` (logged, monotonic) gives the fence. Apply already rejects a stale token as `Fenced`, and the engine maps `AppendError::Fenced` to `ActivationOutcome::Fenced`. The lease table is in-memory; self-fence cancels `Activate.cancel` |
| API-01 | One port: HTTP/1.1, HTTP/2, gRPC; identical semantics; OpenAPI | `axum::serve` (hyper-util auto, h2c) + `Routes::into_axum_router()`. **utoipa 6 + utoipa-axum 0.3** for OpenAPI at `/openapi.json`. One `ControlPlane` trait; the REST and gRPC adapters are thin; a parity test suite |
| API-02 | Upload module by sha256 (idempotent), activate a workflow version | `PUT /v1/modules/{sha256}`: verify sha, `BlobStore::put`, `Runtime::prepare_cached` (validate + AOT) + `describe()`, propose `PutModule`. Re-upload of a known sha is a no-op. Activate is a CAS on the workflow record |
| API-03 | Start run with queue, delay, priority, tags, version, traceparent, tenant, idempotency key + conflict policy | `StartRun` already has queue/priority/ready_at/idempotency_key/conflict_policy. **Add** tags/tenant/traceparent/module_sha (additive tags 14+). Partition = `partition_for_key(workflow, key)` when keyed. Outcome mapping: Accepted→201, Duplicate{existing}→200, IdempotencyConflict→409 |
| API-04 | Get run status (incl. published status), history, list/filter | Read `RunRecord` + decode the `Events` table with the journal codec; latest `StatusPublished` = published status. List = bounded k-way merge across P partitions with a scan budget and page token |
| CLI-01 | `ostro dev` zero config, < 1 s; `--ephemeral` in memory | Ephemeral = TempDir on `/dev/shm` (tmpfs, fsync ≈ free; 44 ms open measured), deleted on exit. Same code path. A first-run disk open is 1.5 s on WSL; see "Startup Budget" |
| CLI-02 | `ostro server` zero-config defaults; TOML + env overrides | clap 4.6 subcommands + **`config` 0.15** (TOML file + `OSTRO_` env with `__` nesting) deserialised into one `#[serde(default)]` struct; clap flags as top-priority overrides |
| SDK-01 | Rust SDK: `#[workflow]`, `#[step]`, ctx, effects, `join!`/`select!`, `cargo build --target wasm32-wasip2` + wit-bindgen | `sdks/rust/{ostro-sdk, ostro-sdk-macros}` (own workspace). `wit_bindgen::generate!{pub_export_macro: true, default_bindings_module: "ostro_sdk::bindings"}`; explicit `ostro_sdk::export!(wf1, wf2)` registration (no ctor magic) |
| WF-01 | Workflow with stable name + version, typed JSON in/out | `#[workflow(name, version)]` + serde_json with content-type `application/json`; describe() exposes name/version; the API stores workflow records keyed (name, version) → module sha |
| WF-02 | Steps with retry policy (max attempts, intervals, backoff, non-retryable kinds), timeout, fuel; replay returns recorded results | **New design required** (see "Step Retry Design"): host-evaluated policy, a new `StepAttemptFailed` journal event, `retry-now` for short backoff, evict + `EnqueueReady{ready_at}` for long backoff. Step timeout via epoch deadline. Fuel already per step (ENG-05) |
| WF-07 | Native time/random/HashMap deterministic under replay | Already true in the engine (WASI shim; det-basic proves SystemTime, HashMap, rand). Phase 4 proves it **through the server across a restart** |
</phase_requirements>

## Dependency Status on Phase 2 (verified in git, 2026-11-18)

| Plan | Status | Phase 4 dependency |
|------|--------|--------------------|
| 02-01..02-12 | Done | Node, PartitionHandle, apply, acks, router, leader skeleton: **hard deps, present** |
| 02-13 shuffler | **Done** (6bcabec, e4a744c; summary written, uncommitted) | Not needed functionally in Phase 4 (no cross-partition features). Its `tests/common` helpers (`inbox`, `start_and_grant`, …) assert the inbox behaviour that Gap 3 changes |
| 02-14 timers + retention | **Done** (9c1d28b, 9561c89, 93303c9) | Needed only if Phase 4 materialises `TimerFired` as a journal input (Gap 3). Retention is inert by default |
| 02-15 crash harness | Pending | Touches `handle.rs`, `tests/rebuild_digest.rs`, new `ostro-crashtest`, `ci-local.sh`. **Conflict risk** with Phase 4 edits to `handle.rs` and apply |
| 02-16 measurements/docs | Pending | Touches `store/config.rs`, `docs/storage-format.md`, `docs/03-architecture.md`. Phase 4 format additions must update `docs/storage-format.md` **after** 02-16 lands |

**Planning rule:** Phase 4 plans that edit `crates/ostro-partition/src/apply/*`, `handle.rs`, `store/values.rs` or `ostro-types/src/command.rs` must be sequenced **after 02-15/02-16 complete**. Plans in new crates or files (ostro-meta, ostro-api adapters, SDK, CLI config) can start right away.

## Contract Gaps

These are the most important part of this research. Each one blocks a success criterion.

### Gap 1: `RunStarted` is never journaled; the module pin is missing
- `apply::run::start_run` creates the `RunRecord` with `next_seq: 0` and writes **no** journal event. The engine requires that `Activate.journal[0]` is a committed `RunStarted`, with workflow, version, input, rng_seed, accepted_at, deadline, module_hash and tenant.
- `StartRun` and `RunRecord` have **no `module_sha`**. Runs must be pinned to a module sha (docs/03 §7.4, WF-08).
- **Fix (recommended):**
  - Add `StartRun.module_sha` (bytes, tag 14), `tags` (repeated string, 15), `tenant` (16), `traceparent` (17) and `run_timeout_ms` (18). Store them in `RunRecord`.
  - On the **first activation**, the executor builds `RunStarted` from `RunRecord`, the input payload and the workflow meta (its `run_timeout_ms` gives the deadline), and submits **one envelope** `[AttemptGranted{n}, EventsAppended{from_seq:0, events:[RunStarted]}]`.
  - `PartitionHandle::submit` takes a `Vec<Command>` in one envelope, so this costs a single fsync.
  - Apply cannot write `RunStarted` itself, because the input may be a blob and apply does no I/O.

### Gap 2: No "waiting" state between activations
- `AttemptGranted` sets `Running` and removes the ready row. `make_ready` (used by `TimerFired` and inbound messages) only moves `Scheduled → Ready`.
- Result: an activation evicted with `WaitUnsatisfied` leaves the run `Running` forever, and no input can wake it.
- **Fix:** add `Derived::Suspend{}` (oneof tag 6). Apply sets the run's status to `Scheduled` and removes the ready row.
  - The executor submits `EventsAppended{from_seq: <expected next_seq>, events: [], derived:[Suspend]}` when an activation ends `Evicted{WaitUnsatisfied}`.
  - **The `from_seq` check acts as a CAS.** If an input was journaled in the meantime (Gap 3), `next_seq` has moved, so `Suspend` is rejected with `SeqMismatch`. The executor then re-activates immediately. No lost wake-up race.
  - `events_contiguous_from` already accepts an empty list.
- Also needed: an index of in-flight runs, so a new leader can find them without scanning all `Runs`. Recommended: a small `Running` keyspace, or keep scanning `Runs` and filter on `Running`. The scan is acceptable before retention exists, but it is O(all runs). See Open Questions.

### Gap 3: Inputs go into `inbox`, but the engine expects journal events
- `apply_timer_fired` (and the inbound signal, child and cancel paths) inserts `InboxMsg` rows and bumps `inbox_next`. Nothing consumes them, and there is no Derived intent to delete them.
- The engine's model is different. Its testkit and 03-11 notes say: "the partition leader commits inputs and forwards them to `Activate.inputs`". `ReplayState::deliver_inputs` takes `Event`s with seqs, and the FakeSink commits inputs at `seq = journal length`.
- **Fix (recommended):** apply writes timer inputs straight into `Events` at `rec.next_seq`.
  - Encode them with `ostro_types::journal::encode_event`. It lives in the same crate, so no new dependency.
  - Use `ts_nanos = fired_at_ms * 1_000_000` and bump `next_seq`.
  - If the run is `Scheduled`, make it ready. If it is `Running`, emit a new `Action::InputAppended{run, seq}`, which the dispatcher forwards to the hot activation's `inputs` channel.
  - Signals stay in `inbox` until Phase 5, because a signal must wait for a matching `wait-signal` op.
  - This is a behaviour change to 02-10/02-14 apply, so the Phase 2 tests that assert inbox rows for timers (`tests/timers.rs`, `tests/common::inbox`) change with it.
- **Minimal alternative:** if Phase 4 skips timers entirely, Gap 3 can wait for Phase 5. Long step backoff then uses `EnqueueReady{ready_at}` (no timer needed). Hot wait still works in-activation, but every unsatisfied timer wait evicts and is never woken. The det-basic guest (5 s sleep) would then not run through the server. **Recommendation: do Gap 3 for timers in Phase 4.** It is small, it unblocks the hot-wait input path and the existing determinism corpus, and Phase 5 needs it anyway.

### Gap 4: No step-retry representation; step timeout is not enforced
- `complete_step_impl` always returns `CompletionAck::Recorded`. Its comment says "retry-now is never returned in Phase 3: … WF-02 (Phase 4)".
- `StepOptions.retry` and `timeout_ms` are ignored. The step's command hash covers only the name.
- See "Step Retry Design" for the fix.

### Gap 5: No run-level failure path for engine-classified failures
- `ActivationOutcome::AttemptFailed(AttemptFailure)` covers nondeterminism, traps, fuel, timeout and memory. No guest `RunFailed` event exists for these, so an executor-written `RunFailed` would be replayed as a guest command and diverge.
- **Fix:**
  - For a retryable class (`AttemptFailure::retryable()`), re-dispatch with a new attempt and backoff, up to a node-configured `executor.max_attempt_failures` (default 3).
  - Then, or immediately for `NonDeterminism` and `ProtocolViolation`: submit `EventsAppended{events: [], derived: [CompleteRun{Failed, result = JSON {reason_code, detail}}]}`. Do not write a journal event.
  - The DLQ table and `on_failure` are OPS-04 (Phase 11). Phase 4 stores the reason as the result payload, so the API can show "failed: nondeterminism at seq N…".

### Also missing (small)
- `Runtime::prepare_cached(&wasm)` needs the bytes on every call, because it hashes them to find the L2 entry. Add `Runtime::prepared(sha) -> Option<Arc<Prepared>>`, an L2 lookup by sha, so activations do not read and hash the module from disk each time.
- `TerminalStatus` has no `ContinuedAsNew`. Server-side continue-as-new is not a Phase 4 requirement, so defer it to Phase 5. If a guest returns continue-as-new in Phase 4, treat it as `Completed` and log a warning, or fail it explicitly as `unsupported`.
- Effects (http/sql/object), child workflows and await tokens have no server-side executor in Phase 4. **Recommendation:** when an appended `EffectScheduled` or `ChildStarted` is applied, the executor immediately commits a `NonRetryable{kind:"unsupported", message:"…arrives in Phase 6/9"}` completion input, so runs fail fast instead of hanging. The alternative is to reject such modules at deploy, but the validation linker would accept them anyway.

## Standard Stack

### Already in the workspace (reuse; do not add alternatives)
| Library | Version | Purpose |
|---------|---------|---------|
| tokio | 1.54 | runtime, signals (`tokio::signal`), `sleep_until` |
| axum | 0.8.9 (`http2`, `json`, `macros`) | REST + router host; `axum::serve` = hyper-util auto (h1 + h2c) |
| tonic / tonic-prost / prost | 0.14.9 / 0.14.9 / 0.14.6 (`router` feature already on) | gRPC; `Routes::into_axum_router()` |
| protox + tonic-prost-build | 0.10.0 / 0.14.9 | proto codegen (existing `ostro-proto/build.rs` pattern) |
| clap | 4.6.8 (`derive`, `env`) | subcommands |
| serde / postcard | 1 / 1.1.3 | meta records and snapshot encoding (postcard, not bincode) |
| ostro-raft-driver | vendored | metadata Raft |
| tracing / metrics | 0.1 / 0.24.6 | logging facade, metrics facade |
| uuid (v7) | 1.x | run ids (already in `RunId::new_v7`) |
| tempfile | 3 | `--ephemeral` TempDir (move from dev-dep to normal dep in the binary/server crate) |

### New dependencies to add
| Library | Version | Purpose | Why |
|---------|---------|---------|-----|
| utoipa | **6.0.0** (2026-09-22; MSRV 1.88; `macros` feature) | OpenAPI document from handler annotations | Current, active, axum-agnostic core. aide 0.15.1 (2026-04) still pins schemars **0.9** while schemars is at 1.2.3, so it lags |
| utoipa-axum | **0.3.0** (depends on `axum ^0.8.4`, `utoipa ^6`) | `OpenApiRouter` + `routes!` gives the router and spec from one source | Keeps the spec and routes from drifting |
| serde_json | 1.0.154 (already in lockfile transitively) | JSON bodies, typed workflow payloads | standard |
| config | **0.15.27** (2026-09-30; `default-features=false, features=["toml"]`, pulls toml 1.x) | Layered config: defaults → TOML file → `OSTRO_*` env | Actively maintained. **figment 0.10.19 is stale** (last release 2024-05, toml 0.8 transitive, no commits since 2024-09 per aggregators, LOW-confidence source) |
| jiff | 1.0.x (1.0.1 released 2026-11-18) | RFC 3339 timestamps in API JSON; "friendly" duration strings in config (`"2s"`) | chrono is banned. Jiff 1.0 is one day old: pin it `=1.0.1` |
| tracing-subscriber | 0.3.x (`env-filter`, `fmt`) | `RUST_LOG`/config log output for dev/server | standard (verify the version at add time) |
| tower-http | 0.7.1 (`limit`, `trace`) | Request body limits per route, request tracing | the axum 0.8 companion; STACK.md pins it |
| wit-bindgen (guest SDK only) | =0.63.0 | SDK bindings | already pinned in `guests/` |
| syn / quote / proc-macro2 (SDK macros only) | syn 2.x | `#[workflow]`/`#[step]` proc macros | standard |
| trybuild (dev, SDK macros) | 1.x | compile-pass/fail tests for macro diagnostics | standard |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| utoipa 6 | aide 0.15 | aide derives from axum extractors (less annotation), but lags on schemars 0.9 vs 1.x, and its maintainer cadence is slower |
| utoipa 6 | hand-written OpenAPI JSON | No macros, but drifts from the code. Acceptable only with a strong snapshot test |
| config 0.15 | toml 1.x + ~80 LOC env overlay | Fewer deps, full control over env parsing. Choose this if `config`'s env coercion (`try_parsing`) misbehaves on u64 and duration fields in tests |
| config 0.15 | figment 0.10 | Nicest provenance in errors, but stale and pulls toml 0.8 |
| explicit `export!(wf…)` | `inventory` 0.3.25 ctor registration | Static-constructor registration on `wasm32-wasip2` components relies on `__wasm_call_ctors` behaviour. Not worth the risk; explicit lists are deterministic |
| Separate REST DTOs + protobuf | prost types with serde derives (pbjson) for JSON | One schema, but oneofs, i32 enums and bytes give unidiomatic JSON, and utoipa derives on generated code are awkward |

**Installation (root workspace additions):**
```toml
# [workspace.dependencies]
utoipa       = { version = "6.0.0", features = ["macros"] }
utoipa-axum  = "0.3.0"
serde_json   = "1"
config       = { version = "0.15.27", default-features = false, features = ["toml"] }
jiff         = { version = "=1.0.1", features = ["serde"] }
tracing-subscriber = { version = "0.3", features = ["env-filter", "fmt"] }
tower-http   = { version = "0.7.1", features = ["limit", "trace"] }
```
Run `cargo deny check` after each addition. The duplicate bans on http/hyper/axum must stay green, and utoipa-axum uses `axum ^0.8.4`, which unifies with 0.8.9.

## Architecture Patterns

### Recommended crate layout
```
crates/
├── ostro-meta/            # MetaState, MetaCommand/MetaResult, MetaSm (impl StateMachine), MetaHandle (propose/read/watch), record types (Versioned<T>)
├── ostro-partition/       # + apply additions (Gaps 1–5), + leader/dispatcher.rs (generic over trait Executor), + lease table
├── ostro-engine/          # + StepAttemptFailed handling, retry policy, step timeout, Runtime::prepared(sha)
├── ostro-types/           # + StartRun fields 14..18, Derived::Suspend (tag 6), EventKind::StepAttemptFailed (29), Action::InputAppended
├── ostro-proto/           # + proto/ostro/api/v1/{modules,workflows,runs}.proto
├── ostro-api/             # trait ControlPlane + REST handlers (utoipa) + gRPC impls + router()/openapi(); no storage deps
├── ostro-server/  (NEW)   # glue: PartitionSink (impl ActivationSink), derive.rs (journal Event → Derived), EngineExecutor (impl Executor),
│                          #       RunStarted synthesis, ControlPlane impl, bootstrap (meta → Node → dispatcher → API), config structs
└── ostro/                 # binary: clap (dev | server | build-info | openapi), signal handling, tracing init
sdks/rust/                 # separate cargo workspace (like guests/)
├── ostro-sdk/             # bindings (wit-bindgen), Ctx, step runner, effects/op handles, join!/select!, export!
└── ostro-sdk-macros/      # #[workflow], #[step]
guests/sdk-*/              # SDK-built test guests (path-dep on ../../sdks/rust/ostro-sdk)
```
Why a new `ostro-server` crate: `ostro-partition` must not depend on `ostro-engine`, `ostro-api` should stay testable with a fake `ControlPlane`, and the binary should stay a thin clap shell. FOUND-01's crate list is not exclusive; 02-15 adds `ostro-crashtest` the same way.

### Startup order (`ostro dev` / `ostro server`)
1. Parse config (defaults → TOML → env → CLI flags). Resolve the data dir: either `--ephemeral`, giving a TempDir on `/dev/shm` if it exists and is writable, else `std::env::temp_dir()`; or the configured dir.
2. **Meta:**
   - `RaftDriver::start(<data>/meta, cfg, &[node_id], MetaSm, NullOutbox)`, then `handle.campaign()`, then `wait_for_leader()`.
   - On first boot, propose `InitCluster{cluster_id, node, partitions: P}`. Idempotent: if it already exists with the same P, accept; if P differs, that is a fatal mismatch.
   - Load or create the await-token key and store it in meta as a sealed record: `RuntimeOptions.token_key`. The engine's default per-process key does not verify after a restart.
3. `Runtime::new(engine_cfg, RuntimeOptions{clock, token_key, cache_dir: <data>/cache/aot, ..})`.
4. `Node::open(NodeConfig{leadership: Arc<MetaLeadership>, leader: LeaderConfig{dispatcher: Some(executor), ..}, ..})`. `MetaLeadership::epoch_floor(p)` reads the partition map. After a term starts, propose `PartitionLeader{p, leader, epoch, isr:[self]}`; a stale epoch is rejected (META-03).
5. Build `ControlPlane` → `ostro_api::router(cp)` → `axum::serve(listener, app).with_graceful_shutdown(signal)`. Report ready (`/readyz` 200) only after meta has a leader and every partition slot is `Ready`.
6. Shutdown, in reverse: stop accepting → cancel activations (`Activate.cancel`, which evicts) → `Node::shutdown()` → `RaftDriver::stop()` → drop the TempDir.

### Run lifecycle across the seam
```
API StartRun ──► apply: RunRecord{Scheduled→Ready}, ready row, Action::EnqueueReady
dispatcher (wakes on action) ──► submit [AttemptGranted{e,n}, EventsAppended{seq0 RunStarted}] (first attempt only)
                             ──► read journal (Events prefix scan, decode_event) ──► Runtime::activate(prepared, Activate{inputs, cancel}, sink)
engine flush ──► PartitionSink::append(run, attempt, from_seq, events)
             ──► derive intents (derive.rs) ──► submit EventsAppended{token, from_seq, events (encode_event → JournalEvent, prepare_payload), derived}
             ──► Accepted → Ok(next) | Rejected(Fenced|StaleEpoch) → AppendError::Fenced | Rejected(SeqMismatch{expected}) → Conflict{expected}
                 | PartitionError::{Failed,NotLeader,...} → Unavailable
done(outcome) ──► Completed/Failed: terminal already appended with CompleteRun → release lease
              ──► Evicted{WaitUnsatisfied} → submit Suspend (CAS via from_seq); Conflict → re-activate now
              ──► Evicted{HotWaitElapsed|BudgetPressure|AppendConflict|External(not self-fence)} → re-activate (same token; lease kept)
              ──► AttemptFailed(f) → retryable? new attempt after backoff : CompleteRun{Failed, reason}
              ──► Fenced → drop (a newer attempt owns the run)
```

### Mapping journal events to Derived intents (`ostro-server::derive`, pure, unit-tested)
| Journal event (appended by engine) | Derived intent(s) |
|---|---|
| `RunStarted` (seq 0, written by executor) | none |
| `StepCompleted`, `PatchMarker`, `StatusPublished`, `WaitResolved` | none |
| `StepAttemptFailed{retry_at}` (new, Gap 4) | `EnqueueReady{ready_at_ms = ceil(retry_at/1e6)}` **only when the activation evicts for backoff**; none for in-activation retry |
| `TimerStarted{fire_at_nanos}` | `ScheduleTimer{op: seq, due_at_ms: ceil(fire_at/1e6)}` |
| `OpCancelled{op}` where op is a timer | `CancelTimer{op, due_at_ms}` (look up the TimerStarted in the journal) |
| `EffectScheduled`, `ChildStarted`, `AwaitTokenCreated`, `SignalWaitStarted` | Phase 4: none, plus the "unsupported" completion input (see "Also missing"). Signal/token timeouts become `ScheduleTimer` in Phase 5 |
| `RunCompleted{output}` | `CompleteRun{Completed, result: prepare_payload(output), completed_at_ms}` |
| `RunFailed{failure}` | `CompleteRun{Failed, result: encode failure JSON}` |
| `RunContinuedAsNew` | Phase 5 (see Gap notes) |

Rules:
- `CompleteRun` must be the last intent; apply rejects any intent after it with `BadIntent`.
- Times are ms in commands and nanos in journal events. Round consistently, with ceil for due times.
- Each `JournalEvent.payload` goes through `prepare_payload`. Events over 256 KiB go to the blob store **before** the append.

### Dispatcher (DISP-01/02/07)
- **Location:** `ostro-partition/src/leader/dispatcher.rs`. Spawned by `spawn_all` when `LeaderConfig.dispatcher: Option<Arc<dyn Executor>>` is `Some`. That keeps the "services start after barrier + router" invariant, and Phase 2 tests run with `None`.
- **In-memory state:**
  - `BTreeMap<String /*queue*/, BTreeSet<(Reverse<i32> /*priority*/, u64 /*ready_at*/, RunId)>>` holding `ReadyEntry{concurrency_key}`.
  - A delay set for entries with future `ready_at`.
  - `leases: BTreeMap<RunId, Lease{token, expires_at, cancel: CancellationToken, inputs: UnboundedSender<Event>}>`.
  - A node-wide `Semaphore` of executor slots, from config or derived from the engine pool size.
- **Rebuild on leadership:**
  - Scan the `Ready` table (all queues) into the queues.
  - Re-dispatch every run that was `Running` under an older epoch. Its token is stale after the restart, so it is re-granted immediately; no `lease_ttl` wait is needed, because the old process is gone.
  - Read the in-flight runs from the Gap 2 index.
- **Wake-ups, with no polling:**
  - `subscribe_actions()` (`EnqueueReady`, `InputAppended`, `RunCompleted`). On `RecvError::Lagged`, do a full rescan.
  - `sleep_until(next ready_at or next lease expiry)`, shutdown, slot available.
  - Copy the timer service's structure (02-14). It already handles lagged actions and stale re-inserts.
- **Leases (soft, not logged):** granting writes `AttemptGranted` (logged) and records `Lease{expires_at = now + lease_ttl}`.
  - The executor renews on every successful `append` and on a heartbeat tick (`lease_ttl/3`).
  - **Expiry:** cancel the old activation's token, grant `n+1`, and re-dispatch. A zombie's late `append` then carries the old token, so apply rejects it with `Fenced{current}`, the sink returns `AppendError::Fenced`, and the outcome is `Fenced`.
  - **Self-fence:** the executor wrapper also arms a local deadline (`lease_ttl - margin`, measured from its last successful renewal). When it fires, it cancels `Activate.cancel` itself. Pitfall 14: a fence on the journal does not stop I/O. That matters from Phase 6; build the mechanism now.
- **Hot-wait inputs:** on `Action::InputAppended{run, seq}` for a leased run, read the event from `Events`, `decode_event`, and send it to `lease.inputs`.
- **Defaults:** `lease_ttl` 30 s, `hot_wait` 2 s (the engine default). The `lease_ttl` must exceed `engine.activation_timeout` (60 s) or be renewed during activation. **Recommendation:** renew on a heartbeat while the activation task is alive (embedded mode), so `lease_ttl` only catches stuck or zombie executors. Tests use 200–500 ms.

### Metadata state machine (META-01/02/03)
```rust
// ostro-meta
pub struct Versioned<T> { pub version: u64, pub updated_index: u64, pub value: T }
pub struct MetaState {
    pub cluster: Option<Versioned<ClusterInfo>>,                 // id, P, format
    pub members: BTreeMap<u64, Versioned<Member>>,               // membership
    pub partitions: BTreeMap<u16, Versioned<PartitionAssignment>>, // leader, epoch, isr (version = epoch-ordered)
    pub modules: BTreeMap<[u8;32], Versioned<ModuleRecord>>,     // sha, size, language, workflows (describe())
    pub workflows: BTreeMap<(String, u32), Versioned<WorkflowVersion>>, // (name,version) -> module sha, default_queue, run_timeout
    pub active: BTreeMap<String, Versioned<ActiveVersion>>,      // name -> version
    pub queues: BTreeMap<String, Versioned<QueueConfig>>,
    pub schedules: BTreeMap<String, Versioned<Schedule>>,        // defined, unused until Phase 8
    pub tokens: BTreeMap<String, Versioned<ApiToken>>,           // hash + scopes (enforced Phase 14)
    pub tenants, sealed_secrets, grants, connections, egress: BTreeMap<..>, // defined, unused until 6/14
}
pub enum MetaCommand { InitCluster{..}, Put{key: MetaKey, expected_version: Option<u64>, record: MetaRecord}, Delete{key, expected_version}, SetPartitionLeader{p, epoch, leader, isr} }
pub enum MetaResult { Ok{version}, StaleVersion{current}, Conflict{current}, NotFound, Invalid(String) }
```
- **Encoding:** `[format=1] ‖ postcard(MetaCommand)`. The snapshot is `postcard(MetaState)`; BTreeMaps make it deterministic. Use postcard, not bincode.
- **Determinism:** proposers supply every timestamp. `apply` reads no clock and uses no HashMap.
- **Versioning rule (META-03):**
  - `Put` with `expected_version = Some(v)` succeeds only if `current.version == v`, and the new version becomes `v + 1`.
  - `None` means "create only": it fails if a record exists.
  - The partition map uses `epoch` as its version: apply accepts `epoch >= current` (equal re-asserts are idempotent) and rejects `epoch < current`. That is the Chronik RP-5 rule.
  - Retrying an `OutcomeUnknown` proposal is safe because the CAS makes it idempotent.
- **Reads:** wrap `MetaState` in `Arc<RwLock<MetaState>>`, shared by the SM on the driver thread and API readers, plus a `watch<u64>` of the applied index. `propose` resolves only after local apply (driver contract), so read-your-writes holds on one node.
- **Snapshots:** the driver default `snapshot_every = 10_000` entries is fine at metadata scale.
- **Raft log fsync:** `LogStoreOptions.sync` stays `true`, even for ephemeral (tmpfs makes it cheap).

### Step Retry Design (WF-02)

This design is recommended and must be adopted by the planner.

**Policy evaluation is host-side**, in one implementation for all three languages. The WIT already defines `step-options.retry` and `completion-ack::retry-now`.

1. `begin-step(name, opts)` stores `opts` in the `StepRep`. The replay core's `begin_step` is extended so that, when replaying, it **consumes a chain of `StepAttemptFailed` events with the same step hash**, followed by either:
   - `StepCompleted`, which is returned as `Replayed`; or
   - the end of the journal, which goes live as attempt `k+1` with a new seq.

   If the last `StepAttemptFailed.retry_at` is still in the future in live mode, re-evict with `StepBackoff` and the same `EnqueueReady`.
2. `complete(Err(f))` on a live step works as follows.
   - `f` is retryable if it is `Retryable(e)` with `e.kind ∉ non_retryable_kinds`, or `Timeout`. `NonRetryable` and `Cancelled` are never retried.
   - With `k` = this attempt number, retry if `f` is retryable and `k < max_attempts`.
   - If it retries: compute `delay = min(initial_ms * coeff^(k-1), max_ms)`. Clamp `coeff < 1` and NaN to 1, and use saturating math. No jitter in v1.
   - Then journal `StepAttemptFailed{meta: same step hash, name, attempt: k, failure: f, retry_at_nanos: logical_now + delay}` at the reserved seq.
     - If `delay <= engine.hot_wait`: flush, sleep `delay` in the host, reserve the next seq for the same step, update the `StepRep`, and return **`retry-now`**. The SDK re-runs the body.
     - Otherwise: flush, then trap `Evict{StepBackoff}` (new `EvictReason`). The sink's derive step emits `EnqueueReady{ready_at}` for that append.
   - If it does not retry, journal `StepCompleted(Err(f))` and return `recorded`.
3. `max_attempts` counts total attempts, including the first. 0 and 1 both mean "no retry". Document this; it differs from Temporal, where 0 means unlimited.
4. **Step timeout:**
   - At live `begin-step` with `timeout_ms`, lower the store's epoch deadline to `now + timeout`.
   - If the epoch interrupt fires while that step is open, the driver records a `Timeout` failure for the open step after the trap unwinds. That gives `StepAttemptFailed`, or `StepCompleted(Err(Timeout))` once attempts are exhausted. The driver then flushes and evicts for re-dispatch.
   - The same path covers a per-step fuel breach, as `NonRetryable{kind:"fuel_exhausted"}`.
   - This needs `[method]step.complete` (currently shadowed with sync `func_wrap`) to become **`func_wrap_async`** so it can sleep. Wasmtime supports `func_wrap_async` on `LinkerInstance` with the `async` feature; the 03-07 plan originally specified it.
5. **New `EventKind::StepAttemptFailed = 29`** (the 16–63 command range). This is additive in postcard, because existing variant indices are unchanged. The STATE.md rule says "any mirror-type field/variant change requires a codec version bump", so the planner must either bump `EVENT_CODEC_VER` deliberately with new golden vectors, or record an explicit decision that appending a variant is additive. Do not silently re-bless `journal_v1.txt`.

**Fallback if the above is too large:** an SDK-side retry loop. Each attempt is a separate journaled step named `name#k`, with `ctx.sleep(backoff)` between attempts. It needs no engine change, but it needs Gap 3 timers and has to be reimplemented in the Python and TS SDKs (Phase 9). Not recommended.

### API surface (API-01..04)
| Operation | REST (JSON) | gRPC (`ostro.api.v1`) |
|---|---|---|
| Upload module | `PUT /v1/modules/{sha256}` body = wasm bytes (`application/wasm`), header `Ostro-Language` optional; 201 created / 200 exists | `Modules.Upload(UploadModuleRequest{sha256, wasm, language})` (unary; raise `max_decoding_message_size` to the configured limit) |
| Get module | `GET /v1/modules/{sha256}` | `Modules.Get` |
| Activate version | `PUT /v1/workflows/{name}/active` `{version, module_sha?}` (CAS via `If-Match`/`expected_version`) | `Workflows.Activate` |
| List workflows | `GET /v1/workflows` | `Workflows.List` |
| Start run | `POST /v1/workflows/{name}/runs` `{input: <json>, version?, queue?, delay_ms?, priority?, tags?, traceparent?, tenant?, idempotency_key?, conflict_policy?: "return_existing" \| "reject", concurrency_key?}` → 201 `{run_id, created: true}` / 200 `{run_id, created: false}` / 409 | `Runs.Start` |
| Get run | `GET /v1/runs/{run_id}` → status, workflow, version, module_sha, attempt, created/completed, published status, result | `Runs.Get` |
| History | `GET /v1/runs/{run_id}/history?from_seq=&limit=` | `Runs.History` |
| List runs | `GET /v1/runs?workflow=&status=&queue=&tag=&created_after=&created_before=&limit=&page_token=` | `Runs.List` |
| OpenAPI | `GET /openapi.json` (also `ostro openapi` prints it) | — |
| Health | `GET /healthz`, `GET /readyz`; gRPC health already exists | `ostro.health.v1` |

- **Module upload semantics:**
  - Verify `sha256(body) == path sha`; a mismatch is 400.
  - If meta already has the module, return 200 with no recompile. That is the "re-upload is a no-op" criterion; assert it via `Runtime::cache_stats()` (0 new compiles).
  - Otherwise: blob put (fsync + rename), `prepare_cached` (validate against the production linker, then AOT behind compile permits), `describe()`, then propose `Put Module`.
  - For each described (name, version), create a `WorkflowVersion` record. **Refuse a known (name, version) bound to a different sha** with 409 (PITFALLS UX: immutable versions). The first upload of a workflow name auto-activates; later activations are explicit.
  - Body limit: `DefaultBodyLimit::max(api.max_module_bytes)` (default 64 MiB) on this route only. Python components are about 18 MB.
- **Start run:**
  - Resolve the version: explicit, else active. Get `module_sha` from meta.
  - Partition: `partition_for_key(workflow, key, P)` if `idempotency_key`, else `partition_for_new_run(uuid)`. Run id: `RunId::new(p, uuid_v7)`. `rng_seed` is 32 random bytes from the OS RNG. Input is `prepare_payload(serde_json bytes)`.
  - Submit `StartRun` to `node.handle(p)` and map the outcome: `Accepted` → 201; `Duplicate{existing: Some(id)}` → 200 with that id; `Rejected(IdempotencyConflict{existing})` → 409 with `existing_run_id`; `Rejected(PayloadTooLarge)` → 413; `PartitionError::NotLeader/Failed` → 503.
  - `delay_ms` → `ready_at_ms = requested_at + delay`. The start-delay timer already exists.
- **Identical semantics:** both transports call the same `ControlPlane` methods with the same domain request/response and error types (`ApiError{code, message, details}` → HTTP status / `tonic::Code`). Use one mapping table, tested both ways.
- **List runs:** run ids are `partition ‖ uuidv7`, so each partition's `Runs` keyspace is time-ordered.
  - Range-scan every partition from its cursor in the page token and k-way merge by uuid time.
  - Filter in memory, with a per-request scan budget (for example 10,000 rows) that returns a continuation token even when fewer rows match. Cap `limit` at 1000.
  - Tag search with an index is OPS-05 (Phase 11).
- **History:** prefix-scan `Events` by run, then `decode_event(seq, kind, bytes)`, after `fetch_payload` if the payload is a blob. Render `{seq, kind, ts (RFC 3339 via jiff), summary, details}`. Recorded results are rendered as JSON when the content type is JSON, else base64.
- **Published status:** the latest `StatusPublished.json` in the run's journal. Phase 4 scans backwards, or the executor caches it. Add a `RunRecord.status_seq` pointer if scans prove slow.

### CLI and config (CLI-01/02)
```rust
#[derive(Subcommand)]
enum Command {
    /// Server + embedded executor + UI shell, zero config.
    Dev { #[arg(long)] ephemeral: bool, #[arg(long, env = "OSTRO_LISTEN", default_value = "127.0.0.1:7700")] listen: SocketAddr, #[arg(long)] data_dir: Option<PathBuf> },
    /// Single-node server; TOML + env overrides.
    Server { #[arg(long, short, env = "OSTRO_CONFIG")] config: Option<PathBuf>, #[arg(long)] print_config: bool, /* listen/data_dir overrides */ },
    Openapi,
    BuildInfo,
}
```
- **`ServerConfig`:** a `#[serde(default, deny_unknown_fields)]` struct with sections `node` (`data_dir`, `partitions`, `node_id`), `api` (`listen`, `max_module_bytes`), `engine` (maps onto `EngineConfig`), `executor` (`slots`, `lease_ttl`, `max_attempt_failures`), `log` (`level`, `format`).
- **Layering:** `Config::builder().add_source(File::from(path).required(true/false)).add_source(Environment::with_prefix("OSTRO").prefix_separator("_").separator("__").try_parsing(true))`, then `try_deserialize`, then apply clap overrides. Durations are friendly strings (`"30s"`) parsed with jiff.
- **`--print-config`** prints the effective config, the testable proof for CLI-02.
- **Dev defaults:** listen `127.0.0.1:7700`; data dir `./.ostro` (persistent) unless `--ephemeral`; P=16; pretty logs.
- **Server defaults:** listen `0.0.0.0:7700`; data dir `./ostro-data`, or as configured. Do not default to `/var/lib/ostro` (needs root).
- **"UI shell":** `GET /` serves an embedded `include_str!` HTML placeholder (version, link to `/openapi.json`, run counts). The real UI is Phase 11.

### Rust SDK (SDK-01)
- `ostro-sdk/src/bindings.rs`:
  ```rust
  wit_bindgen::generate!({
      path: "../../../wit/ostro-runtime-0.1.0",   // repo WIT; vendor a copy before crates.io publish (Phase 10/14)
      world: "workflow",
      pub_export_macro: true,
      export_macro_name: "__export_world",
      default_bindings_module: "ostro_sdk::bindings",
  });
  ```
- **User-facing surface:**
  ```rust
  use ostro_sdk::prelude::*;

  #[derive(Serialize, Deserialize)] struct Order { id: String, amount: u64 }

  #[step(retry(max_attempts = 5, initial = "200ms", max = "10s", backoff = 2.0, non_retryable = ["card_declined"]), timeout = "5s")]
  fn charge(order: &Order) -> Result<Receipt, StepError> { /* body runs at most once to a recorded completion */ }

  #[workflow(name = "order", version = 1)]
  fn order(ctx: &Ctx, input: Order) -> Result<Done, WorkflowError> {
      let r = charge(ctx, &input)?;        // macro injects ctx param handling
      ctx.set_status(&json!({"charged": r.id}));
      Ok(Done { receipt: r.id })
  }

  ostro_sdk::export!(order);              // generates describe() + run(name) dispatch + bindings export
  ```
- **What `#[step]` expands to:**
  - `begin_step(name, StepOptions{retry, timeout_ms, fuel})`.
  - `Replayed(Ok(p))` → `serde_json::from_slice`. `Replayed(Err(f))` → `Err(StepError::from(f))`.
  - `Execute(h)` → `loop { let out = body(); match h.complete(encode(out)) { Recorded => return out, RetryNow => continue } }`.
  - `StepError` carries `kind` + `message` and has `retryable()` / `non_retryable()` constructors.
- **What `#[workflow]` expands to:** a unit struct implementing `ostro_sdk::Workflow { const NAME, const VERSION, const DEFAULT_QUEUE, const RUN_TIMEOUT_MS; fn run(Payload) -> Result<RunResult, Failure> }` with serde_json in and out (content type `application/json`).
- **`join!(a, b, …)` / `select!`:** macros over `effects::wait(&[ops], All|Any)` that return typed outputs. Op handles: `ctx.sleep(Duration) -> Op<()>`. The http/sql/object handles exist but complete "unsupported" in Phase 4.
- **Determinism notes:**
  - Native `SystemTime`, `HashMap` and `rand` already route through the WASI shim (WF-07).
  - Document that `std::time::Instant` is monotonic logical time since `accepted_at`.
  - Logging goes through `ctx.log` (suppressed during replay).
- **Testing:**
  - Macro expansion and diagnostics with `trybuild` on the host. wit-bindgen import stubs compile natively but are unreachable at runtime (MEDIUM confidence; verify).
  - Behaviour through `guests/sdk-*` built for wasm32-wasip2, run on the engine testkit (`drive_to_completion`) and end to end through the server.

### Anti-Patterns to Avoid
- **The dispatcher polling the `Ready` table on an interval.** DISP-01 says no polling. Use the actions plus `sleep_until`, and rescan only on `Lagged`.
- **Writing `Events` or run state anywhere except apply.** The API, executor and dispatcher only *submit* commands (ARCHITECTURE rule 1).
- **Calling `Runtime::prepare_cached(bytes)` on every activation**, which reads and hashes the module each time. Use an L2 lookup by sha.
- **An executor-written `RunFailed` journal event** for engine failures. Replay would treat it as a guest command and diverge. Use `CompleteRun` with no event (Gap 5).
- **Treating `ProposeError::OutcomeUnknown` as failure.** It must be retried; the CAS makes that safe.
- **A real in-memory store for `--ephemeral`.** That violates D5. Use a tmpfs TempDir on the same code path.
- **Holding std `RwLock` guards across `.await`** in API handlers reading `MetaState`. Clone what you need, then drop the guard.
- **Mapping prost-generated types straight to JSON.** You get i32 enums and base64 everywhere. Keep separate serde DTOs.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| HTTP/1 + h2c + gRPC on one socket | Custom protocol sniffing | `axum::serve` (hyper-util auto) + `Routes::into_axum_router()` | Already proven in `ostro-api` skeleton |
| OpenAPI document | Hand-maintained JSON | utoipa 6 + utoipa-axum `OpenApiRouter::split_for_parts()` | Spec and routes from one source; snapshot it |
| Layered config / env nesting | Custom env parser | `config` 0.15 (toml + Environment) | Env separators, typed coercion, file layering |
| Raft single voter | Hand-written WAL for meta | vendored `RaftDriver` + `RaftLogStore` | Durable, fsynced, snapshotting, already tested |
| Run-id routing / idempotent partition choice | New hash | `ostro_types::{partition_for_key, partition_for_new_run}`, `RunId::new_v7` | Frozen, golden-pinned |
| Idempotent start / conflict policy | API-side dedupe map | `StartRun{idempotency_key, conflict_policy}` + apply `Dedupe` table | Already exactly-once at apply, survives crashes |
| Fencing zombies | Lease checks in the API | logged `AttemptGranted` + apply token check + `AppendError::Fenced` | Already deterministic and tested (02-09/02-12) |
| Blob offload | Size checks in handlers | `PartitionHandle::prepare_payload` / `fetch_payload` | Durable put before append, sha-verified reads |
| Journal encoding | serde_json of events | `ostro_types::journal::{encode_event, decode_event, encode_result, decode_result}` | Golden-pinned canonical bytes (ENG-04) |
| Module validation/compile/cache | Custom import checks | `Runtime::prepare_cached` + `describe` | Validation, HMAC'd AOT cache, compile permits |
| Timestamps | chrono / manual RFC 3339 | jiff `Timestamp` | chrono banned; jiff handles RFC 3339 + friendly durations |
| Tempdir lifecycle | manual `rm -rf` | `tempfile::TempDir` (drop = delete) | Correct cleanup on graceful exit |
| Macro diagnostics tests | ad-hoc compile scripts | `trybuild` | Standard compile-fail harness |

**Key insight:** Phase 2 and Phase 3 already solved the hard correctness problems: fencing, dedupe, CRC, determinism. Phase 4 bugs will come from **glue that bypasses them**. Examples: writing state outside apply, inventing a second fencing check, or caching the journal outside the log.

## Common Pitfalls

### Pitfall 1: Single-voter Raft waits for its election timeout
**What goes wrong:** Every startup stalls for 2–4 s before meta has a leader. With `election_tick 20 × 100 ms` plus randomisation, the measured cost is about 2.85 s per start.
**How to avoid:** Call `handle.campaign()` right after `RaftDriver::start` when the voter set is `{self}`. Measured: leader in 25–60 ms, first proposal applied in about 40–75 ms.
**Warning signs:** `ostro dev` startup test above 2 s. The existing `single_voter` test takes 5.7 s.

### Pitfall 2: First-run startup dominated by fsyncs
**What goes wrong:** A fresh `Node::open` with P=16 does about 2,000 fsyncs, because each fjall database creates 13 keyspaces. On WSL2 that is 1.5–1.7 s. On tmpfs it is 44 ms; a warm reopen is 260–300 ms.
**How to avoid:**
- `--ephemeral` uses `/dev/shm`.
- The startup-time test measures `--ephemeral` and warm `ostro dev`, and records first-run cold disk time as a separate, documented number.
- Mitigation options, if the cold criterion is enforced: P=4 for `dev`, which changes the data dir format, so it is fixed per directory at creation; or open partitions lazily.
**Warning signs:** The CI startup test is flaky on slow disks. Profile with `strace -f -c -e trace=fsync`.

### Pitfall 3: A lost wake-up between eviction and input
**What goes wrong:** A timer fires while the run is `Running`. The executor then marks the run waiting, and the run sleeps forever.
**How to avoid:** Inputs bump `next_seq` (Gap 3), and `Suspend` travels in an `EventsAppended` whose `from_seq` must match, so the race shows up as `SeqMismatch` and triggers re-activation. Test it: inject the input between the activation's last flush and the `Suspend`.

### Pitfall 4: A zombie's late completion accepted, or attempt numbers reused
**What goes wrong:** A re-dispatched attempt and a zombie both write.
**How to avoid:**
- Always grant through the logged `AttemptGranted`. Never reuse a token or compute `n` from in-memory state; `grant_attempt` reads the run record.
- A restart is a new epoch (`LeaderAnnounced`), so every old token is fenced.
- The test pauses an executor past `lease_ttl`, lets the re-dispatch complete, then releases the zombie. Assert `Fenced`, assert exactly one `RunCompleted`, and assert the zombie's events are absent from `Events`.

### Pitfall 5: Stale metadata overwrites newer state (Chronik RP-5)
**What goes wrong:** A replayed or retried proposal with an old version wins.
**How to avoid:** CAS on `expected_version` for every record kind, and epoch ordering for the partition map. Use a table-driven test that iterates every `MetaKey` kind.

### Pitfall 6: Module immutability violated
**What goes wrong:** "Version 3" is redeployed with different code, and in-flight runs diverge silently.
**How to avoid:** A (name, version) → sha binding is write-once. Uploading a different sha for an existing (name, version) is 409. Runs pin `module_sha` at start (Gap 1).

### Pitfall 7: Request size limits
**What goes wrong:** axum's default extractor body limit is 2 MiB and tonic's default decode limit is 4 MiB. Module uploads fail with an opaque 413 or `OutOfRange`.
**How to avoid:** Set `DefaultBodyLimit::max(cfg.api.max_module_bytes)` only on the upload route and `max_decoding_message_size(cfg.api.max_module_bytes)` on the Modules gRPC server. Test with a 20 MB fake module: it must be rejected for sha mismatch, not size.

### Pitfall 8: Retry accounting that is not durable
**What goes wrong:** In-memory attempt counts reset on eviction or crash, so retries become unbounded.
**How to avoid:** Journal every failed attempt (`StepAttemptFailed`) and compute `k` from the journal. Test it: restart between attempts, then assert the total attempts equal `max_attempts` exactly.

### Pitfall 9: Concurrent Phase 2 edits
**What goes wrong:** Phase 4 plans modify `apply/*` or `handle.rs` while 02-15/02-16 executors edit the same files. 03-07 and 03-08 already hit this, which forced scratch-worktree verification.
**How to avoid:** Sequence the plans that touch apply after 02-16. Verify in a clean worktree when Phase 2 work is uncommitted.

### Pitfall 10: The token key changes per process
**What goes wrong:** `RuntimeOptions::default()` derives `token_key` from time and pid, so await tokens fail to verify after a restart.
**How to avoid:** Generate the key once and persist it in meta as a sealed-secret record, created on `InitCluster`. Pass it to `RuntimeOptions`.

## Code Examples

### Single-voter meta start with immediate campaign (verified by probe)
```rust
// Source: vendor/ostro-raft-driver/src/driver.rs API; latency measured 2026-11-18
let driver = RaftDriver::start(data.join("meta"), DriverConfig::new(node_id), &[node_id], MetaSm::new(shared.clone()), NullOutbox)?;
let h = driver.handle();
h.campaign();                                       // single voter: leader in ~25-60 ms instead of ~2.85 s
tokio::time::timeout(Duration::from_secs(5), h.wait_for_leader()).await??;
let applied = h.propose(encode(&MetaCommand::InitCluster { .. })).await?; // visible locally on return
```

### One port, REST + gRPC + OpenAPI
```rust
// Sources: crates/ostro-api/src/lib.rs (existing merge), utoipa-axum docs (split_for_parts)
let (rest, openapi) = OpenApiRouter::with_openapi(ApiDoc::openapi())
    .routes(routes!(upload_module))
    .routes(routes!(start_run))
    .routes(routes!(get_run, list_runs))
    .with_state(cp.clone())
    .split_for_parts();
let grpc = tonic::service::Routes::new(HealthServer::new(HealthSvc))
    .add_service(ModulesServer::new(GrpcModules(cp.clone())).max_decoding_message_size(max_module))
    .add_service(RunsServer::new(GrpcRuns(cp.clone())))
    .into_axum_router();
let app = axum::Router::new()
    .merge(rest)
    .route("/openapi.json", get(move || async move { Json(openapi) }))
    .merge(grpc);
axum::serve(listener, app).with_graceful_shutdown(shutdown).await?; // h1 + h2c (gRPC) on one socket
```

### PartitionSink::append (sketch)
```rust
// Seams: ostro_engine::sink::ActivationSink, ostro_partition::handle::PartitionHandle
async fn append(&self, run: &RunRef, attempt: u32, from_seq: u64, events: Vec<Event>) -> Result<u64, AppendError> {
    let token = self.lease_token(run, attempt)?;                 // AttemptToken{epoch, n} granted for this activation
    let derived = derive::intents(&events, self.clock_ms())?;    // pure mapping table above
    let mut jevs = Vec::with_capacity(events.len());
    for ev in &events {
        let (kind, bytes) = ostro_types::journal::encode_event(ev);
        let payload = self.handle.prepare_payload(bytes.into()).await.map_err(unavailable)?;
        jevs.push(JournalEvent { seq: ev.seq, kind, payload: Some(payload) });
    }
    let n = jevs.len() as u64;
    match self.handle.submit_one(Command::events_appended(run_id, token, from_seq, jevs, derived)).await {
        Ok(ApplyOutcome::Accepted) => { self.renew_lease(run); Ok(from_seq + n) }
        Ok(ApplyOutcome::Rejected(RejectReason::Fenced { .. } | RejectReason::StaleEpoch { .. })) => Err(AppendError::Fenced),
        Ok(ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected })) => Err(AppendError::Conflict { expected }),
        Ok(other) => Err(AppendError::Unavailable(format!("{other:?}"))),
        Err(e) => Err(AppendError::Unavailable(e.to_string())),
    }
}
```

### Layered config
```rust
let cfg: ServerConfig = config::Config::builder()
    .add_source(config::File::from(path).format(config::FileFormat::Toml).required(explicit))
    .add_source(config::Environment::with_prefix("OSTRO").prefix_separator("_").separator("__").try_parsing(true))
    .build()?
    .try_deserialize()?;                  // OSTRO_EXECUTOR__LEASE_TTL=5s -> executor.lease_ttl
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `cargo-component` for Rust guests | `cargo build --target wasm32-wasip2` + wit-bindgen 0.63 | 2025 (cargo-component stale since 2025-07) | SDK uses plain cargo |
| utoipa 5 / OpenAPI 3.1 | utoipa 6 adds OpenAPI 3.2 (`Version32`), MSRV 1.88 | 2026-09-23 | Use 6.0.0; default 3.1 output is fine |
| figment as the go-to layered config | `config` 0.15 (active) or plain toml + overlay | figment idle since 2024 | Pick `config` |
| chrono | jiff 1.0 | jiff 1.0.0/1.0.1 Nov 2026; chrono banned in this repo | Pin `=1.0.1` |
| tonic owning the server (`Server::builder`) | tonic `Routes` merged into axum, served by `axum::serve` | tonic 0.12+ | Already in the skeleton |

**Deprecated/outdated:** docs/03 §2.2 says "a new leader bumps attempts of in-flight runs" as soft state. Superseded: the epoch component of the token plus logged `AttemptGranted` do this durably. docs/03's `ActivationSink` sketch (`resolve_secret`, `u32` seqs) is superseded by `crates/ostro-engine/src/sink.rs`.

## Open Questions

1. **Index of in-flight runs on leadership rebuild (Gap 2).**
   - What we know: `AttemptGranted` removes the ready row. After a restart, `Running` runs are found only by scanning `Runs` (O(all runs, terminal included) until Phase 5 retention).
   - What's unclear: whether to add a 14th keyspace (`Running`), keep the ready row while leased, or accept the scan.
   - Recommendation: add a `Running` keyspace (run → token). It changes `TABLES` and the digest, which is acceptable before the freeze. Coordinate with 02-16's `docs/storage-format.md`.
2. **Cold first-run `ostro dev` < 1 s.** Measured 1.5–1.7 s on WSL2 disk. Needs a decision: accept "warm and ephemeral < 1 s" as the criterion, or reduce fsyncs or partitions for dev. Measure on native Linux and macOS before deciding.
3. **Journal codec version for `StepAttemptFailed`.** Additive postcard variant vs `EVENT_CODEC_VER` bump. Decide and record in STATE.md.
4. **Phase 4 behaviour for effects, children and tokens.** Recommendation: commit an immediate "unsupported" NonRetryable completion input. Alternative: reject at deploy. This needs a decision, because it affects which guests the e2e suite can use (wit-surface uses effects).
5. **Step timeout mechanics.** Lowering the store epoch deadline per step and attributing the interrupt to the open step needs a small driver change. Verify that the Wasmtime 50 epoch-deadline callback API (`StoreHookState`, changed in 50) supports per-step adjustment from inside a host call.
6. **wit-bindgen on host targets for trybuild.** Expected to compile with unreachable import stubs. Verify early; if it fails, run macro tests only through wasm32-wasip2 builds.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | `cargo test` (Rust 1.100.0) + `insta` 1 (snapshots) + `proptest` 1.12 + `trybuild` (SDK macros); guests via `tests/common::guest_wasm` nested wasm32-wasip2 build |
| Config file | none beyond `Cargo.toml`; SDK has its own workspace `sdks/rust/Cargo.toml` (Wave 0) |
| Quick run command | `cargo test -p ostro-meta -p ostro-api -p ostro-server --locked` (< 60 s warm) |
| Full suite command | `bash scripts/ci-local.sh --no-musl` (fmt, clippy -D warnings, warning-free build, workspace tests, failpoints, deny, vendored) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| META-01 | Single voter elects < 200 ms with campaign; proposals persist across restart; replay into fresh SM | integration | `cargo test -p ostro-meta --test meta_persist` | ❌ Wave 0 (only `single_voter.rs` smoke exists) |
| META-02 | Every record kind round-trips through propose → snapshot → restore (equal state) | integration + proptest | `cargo test -p ostro-meta --test meta_records` | ❌ Wave 0 |
| META-03 | Stale `expected_version` / lower epoch rejected for **every** record kind; random interleavings end at max version | unit (table-driven) + proptest | `cargo test -p ostro-meta --test meta_versions` | ❌ Wave 0 |
| DISP-01 | Ready runs rebuilt after restart and dispatched; wake via actions (no periodic scan: assert scan counter stays 0 while idle) | integration | `cargo test -p ostro-partition --test dispatcher` | ❌ Wave 0 |
| DISP-02 | ReadyEntry carries concurrency key from StartRun through rebuild | unit | `cargo test -p ostro-partition --test dispatcher -- concurrency_key` | ❌ Wave 0 |
| DISP-07 | Stalled executor → re-dispatch after lease_ttl (attempt n+1); zombie append → Fenced; exactly one RunCompleted; self-fence cancels activation | integration | `cargo test -p ostro-server --test leases` | ❌ Wave 0 |
| API-01 | Same scenario over REST and gRPC on one port gives equal results; `/openapi.json` served; OpenAPI snapshot | integration + insta | `cargo test -p ostro-api --test parity` and `-- openapi_snapshot` | ❌ Wave 0 |
| API-02 | Upload by sha (mismatch 400), re-upload no-op (0 new compiles via `cache_stats`), conflicting (name,version) 409, activate CAS | integration | `cargo test -p ostro-server --test modules_api` | ❌ Wave 0 |
| API-03 | Start with all options persisted; idempotency return_existing → same id, reject → 409; delay → Scheduled until ready_at | integration | `cargo test -p ostro-server --test start_run_api` | ❌ Wave 0 |
| API-04 | Get status incl. published status; history decode; list filters + pagination over P=16 | integration | `cargo test -p ostro-server --test runs_api` | ❌ Wave 0 |
| CLI-01 | `ostro dev --ephemeral` → `/readyz` 200 in < 1 s; data dir removed after SIGINT; warm `ostro dev` < 1 s | e2e (spawn binary) | `cargo test -p ostro --test dev_startup --release` | ❌ Wave 0 |
| CLI-02 | `ostro server --config x.toml --print-config` reflects TOML; `OSTRO_EXECUTOR__LEASE_TTL` overrides TOML; zero-config defaults | e2e (spawn binary) | `cargo test -p ostro --test server_config` | ❌ Wave 0 |
| SDK-01 | Macros expand/diagnose (trybuild); SDK guest builds for wasm32-wasip2 and validates against production linker; join!/select! over sleeps on engine testkit | unit + integration | `cargo test --manifest-path sdks/rust/Cargo.toml` and `cargo test -p ostro-server --test sdk_e2e` | ❌ Wave 0 |
| WF-01 | Typed JSON input/output round trip through API with name+version | e2e | `cargo test -p ostro-server --test sdk_e2e -- typed_json` | ❌ Wave 0 |
| WF-02 | Retry: exactly max_attempts attempts, backoff gaps match policy (journaled retry_at), non-retryable kind stops at 1, restart between attempts keeps count; completed steps not re-executed after restart (StepCompleted count == 1) | integration (engine) + e2e | `cargo test -p ostro-engine --test step_retry` and `cargo test -p ostro-server --test sdk_e2e -- retry` | ❌ Wave 0 |
| WF-07 | Guest using SystemTime/HashMap/rand: output identical across forced restart + replay through server | e2e | `cargo test -p ostro-server --test sdk_e2e -- native_determinism` | ❌ Wave 0 |

Manual-only: none required. The cold first-run disk startup number (Open Question 2) is recorded by a `--ignored` measurement test, not asserted.

### Sampling Rate
- **Per task commit:** `cargo test -p <touched crate> --locked` plus `cargo clippy -p <crate> --all-targets -- -D warnings`.
- **Per wave merge:** `cargo test --workspace --locked` plus the SDK workspace tests.
- **Phase gate:** `bash scripts/ci-local.sh` (including the musl leg) green before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `crates/ostro-server/` crate skeleton (Cargo.toml, lib.rs with `use x as _;` anchors) plus `tests/common/mod.rs`: start an in-process server on a tempdir or `/dev/shm`, with an ephemeral port and a guest-building helper that reuses `guest_wasm`.
- [ ] `crates/ostro-meta/tests/{meta_persist,meta_records,meta_versions}.rs`
- [ ] `crates/ostro-partition/tests/dispatcher.rs` with a fake `Executor`
- [ ] `crates/ostro-api/tests/parity.rs` with a fake `ControlPlane`, plus an insta snapshot of `openapi.json`
- [ ] `crates/ostro/tests/{dev_startup,server_config}.rs` (spawn the `CARGO_BIN_EXE_ostro` binary, poll `/readyz`)
- [ ] `sdks/rust/` workspace (ostro-sdk, ostro-sdk-macros, trybuild tests), plus `guests/sdk-basic`, `guests/sdk-retry` and `guests/sdk-native` using the SDK
- [ ] `crates/ostro-engine/tests/step_retry.rs` (FakeSink-driven)
- [ ] Add `--test-threads` and `--release` notes in ci-local for the timing-sensitive `dev_startup` test
- [ ] Framework installs: `trybuild` dev-dep in the SDK macros crate; `reqwest` (rustls, already the Phase 6 pick) **or** hyper-util client as a dev-dep for HTTP tests; a tonic client for gRPC (already a dependency)

## Sources

### Primary (HIGH confidence)
- Repository code at `e4a744c`, read directly:
  - `crates/ostro-engine/src/{sink,runtime,activation,host/journal,host/state,replay/cursor}.rs`
  - `crates/ostro-partition/src/{node,handle,leadership,leader/mod,leader/timers,apply/run,apply/timer,store/{values,keys,mod}}.rs`
  - `crates/ostro-types/src/{command,journal,routing,ids}.rs`
  - `vendor/ostro-raft-driver/src/{driver,log_store}.rs`
  - `crates/ostro-api/src/lib.rs`, `wit/ostro-runtime-0.1.0/runtime.wit`, `deny.toml`, `scripts/ci-local.sh`
- Planning docs: Phase 2/3 summaries 02-06, 02-09, 02-11, 02-12, 02-13, 03-07, 03-08, 03-11, 03-VERIFICATION; STATE.md; PROJECT.md; research SUMMARY/STACK/ARCHITECTURE/PITFALLS; docs/03-architecture.md.
- Local probes run 2026-11-18 (scratch crates, since deleted):
  - Raft campaign latency: 25–60 ms.
  - `Node::open` P=16: 1.53–1.69 s fresh on disk, 260–300 ms warm, 44 ms on `/dev/shm`; about 2,000 fsyncs per fresh open (strace).
  - `Runtime::new`: 4–5 ms.
  - Existing `single_voter` test: 5.7 s for two starts.
- Local registry sources: `axum-0.8.9/src/serve/mod.rs` (hyper-util auto Builder), `tonic-0.14.9/src/service/router.rs` (`Routes::into_axum_router`), `fjall-3.1.13/src/builder.rs` (`temporary(true)` only; no in-memory mode).
- crates.io API, queried 2026-11-18: utoipa 6.0.0, utoipa-axum 0.3.0 (axum ^0.8.4), aide 0.15.1 (schemars ^0.9), schemars 1.2.3, figment 0.10.19 (2024-05-17, toml ^0.8), config 0.15.27 (toml ^1.0.6), jiff 1.0.1, wit-bindgen 0.63.0, tower-http 0.7.1, inventory 0.3.25.

### Secondary (MEDIUM confidence)
- [utoipa CHANGELOG](https://github.com/juhaku/utoipa/blob/master/utoipa/CHANGELOG.md): 6.0.0 on 2026-09-23, OpenAPI 3.2, MSRV 1.88.
- [utoipa-axum docs](https://docs.rs/utoipa-axum/latest/utoipa_axum/): `OpenApiRouter`, `routes!`, `split_for_parts`.
- [wit-bindgen 0.63 `generate!` docs](https://docs.rs/wit-bindgen/0.63.0/wit_bindgen/macro.generate.html): `pub_export_macro`, `export_macro_name`, `default_bindings_module`, `runtime_path`.
- [config 0.15.27 `Environment` docs](https://docs.rs/config/0.15.27/config/struct.Environment.html): `with_prefix`, `prefix_separator`, `separator`, `try_parsing`.

### Tertiary (LOW confidence, flagged)
- Figment maintenance status, from aggregator and distro pages via web search: [rust-digger figment](https://rust-digger.code-maven.com/crates/figment), [Debian tracker](https://tracker.debian.org/rust-figment), [meterian](https://www.meterian.io/components/rust/figment). "No commits since 2024-09" is not verified on GitHub.
- wit-bindgen host-target import stubs compiling for trybuild: from training knowledge, so verify early (Open Question 6).
- `inventory` behaviour on wasm32-wasip2 components: not verified, which is why explicit registration is recommended.

## Metadata

**Confidence breakdown:**
- Code seams and gaps: HIGH. Read from source; Gaps 1–3 and 5 confirmed in apply code.
- Standard stack: MEDIUM-HIGH. Registry-verified versions; utoipa-axum and config APIs partially doc-verified.
- Architecture (dispatcher, leases, meta, API): MEDIUM-HIGH. Follows existing patterns (timer service, apply CAS), but these are new designs.
- Step retry design: MEDIUM. A new design touching the replay core; it needs a planner decision and an engine test-first plan.
- Startup numbers: HIGH for this machine (WSL2). Native Linux and macOS are unmeasured.

**Research date:** 2026-11-18
**Valid until:** 2026-12-18. Wasmtime 51 is due around 2026-12-20, and jiff 1.0 is days old; re-check before planning if later than that.
