---
phase: 11-run-operations-embedded-ui
plan: 04
subsystem: api contract / server config / server seam
tags: [contract, seam, config, stubs, literal-fixups]
requires:
  - 11-02 (RunLimits in EngineConfig, Cmd 45, StartRun 40/41, JournalEvent.4)
  - 11-03 (ostro-ui crate, UiDeps/UiConfig/AllowAll)
provides:
  - ostro_api::phase11 (OpsApi, InventoryApi, every DTO)
  - ApiExtensions.ops / .inventory, ApiConfig.max_payload_bytes / .root_redirect
  - "[limits] [ui] [bulk] [dlq] config sections (LimitsSection, UiSection, BulkSection, DlqSection)"
  - ostro_server::p11 (P11Ctx, P11Feature, p11_feature_enabled, P11Controllers, extensions, ui_config, wire) - final
  - control/p11_{search,inventory,rerun,dlq,bulk}.rs, services/{dlq_hook,bulk}.rs, rerun.rs, onfailure.rs stubs
  - tests/common/p11.rs (P11Server + REST helpers)
affects: [11-06, 11-07, 11-09, 11-10, 11-11, 11-12, 11-13, 11-15, 11-16, 11-17, 11-18, Phase 13 (feature gate body)]
tech-stack:
  added: []
  patterns:
    - "control plane holds a Weak<P11Ctx> (P11Ctx -> ControlCtx -> control plane would otherwise be a cycle)"
    - "Phase 11 services started late through Services::attach_p11 and joined by Services::shutdown"
key-files:
  created:
    - crates/ostro-api/src/phase11.rs
    - crates/ostro-server/src/p11.rs
    - crates/ostro-server/src/rerun.rs
    - crates/ostro-server/src/onfailure.rs
    - crates/ostro-server/src/control/p11_search.rs
    - crates/ostro-server/src/control/p11_inventory.rs
    - crates/ostro-server/src/control/p11_rerun.rs
    - crates/ostro-server/src/control/p11_dlq.rs
    - crates/ostro-server/src/control/p11_bulk.rs
    - crates/ostro-server/src/services/dlq_hook.rs
    - crates/ostro-server/src/services/bulk.rs
    - crates/ostro-server/tests/common/p11.rs
    - crates/ostro-server/tests/p11_contract.rs
  modified:
    - crates/ostro-api/src/{lib,phase8}.rs
    - crates/ostro-api/tests/{lifecycle_surface,parity,rest}.rs
    - crates/ostro-server/src/{lib,server,config}.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/services/mod.rs
decisions:
  - "ServerControlPlane.p11 is Arc<OnceLock<Weak<P11Ctx>>> (read via ServerControlPlane::p11() -> Option<Arc<P11Ctx>>), not Arc<OnceLock<P11Ctx>>: P11Ctx.cc holds the control plane strongly, so a strong slot leaks the control plane and meta past shutdown"
  - "P11Controllers holds Arc<P11Ctx> (the strong owner, with the Phase 11 service tasks); wire takes &Services instead of &ServiceCtx"
  - "wire builds the API router itself (router_with) because the ext slots and ApiConfig fields must be filled first; server.rs passes axum::Router::new() as the base"
  - "ApiConfig loses Copy (root_redirect: Option<String>); DTO status fields use rest::RunStatusDto (domain RunStatus has no serde)"
  - "[ui] enabled is Option<bool>: None (both profiles) = on exactly when the API listener is loopback; the dev profile's listener is loopback, so dev serves the UI; dev on a non-loopback listener needs an explicit enabled = true (TOML cannot express None, so a profile-specific Some(true) broke the --print-config round trip)"
  - "ostro-server was already in FAILPOINT_CRATES (failpoints_phase5, children_failpoints targets); ci files unchanged"
metrics:
  duration: "~1h30m"
  completed: 2026-11-22
---

# Phase 11 Plan 04: Contract c (API contract, config, final p11 seam) Summary

The Phase 11 API contract (`ostro_api::phase11`), the `[limits]`/`[ui]`/`[bulk]`/`[dlq]` config sections and the final server seam `p11.rs` are in. `server.rs` calls `p11::wire` once. That call builds the `P11Ctx`, publishes it to the control plane and the services seam, fills the new extension slots and ApiConfig fields, and merges the `ostro_ui` router onto the one port. Every later Phase 11 server plan now has its own stub file with a fixed signature.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 6f0d19f | `feat(11-04)`: Phase 11 API contract and extension slots |
| 2 | faac8ab | `feat(11-04)`: Phase 11 config sections, final p11 seam and stubs |

## Exact names

- **ApiExtensions** (`ostro_api::phase8`): `pub ops: Option<Arc<dyn ostro_api::phase11::OpsApi>>` and `pub inventory: Option<Arc<dyn ostro_api::phase11::InventoryApi>>`, both `None` by default. `Debug` prints them as booleans.
- **ApiConfig** (`ostro_api`): `pub max_payload_bytes: usize` (default 4 MiB) and `pub root_redirect: Option<String>` (default `None`). ApiConfig is now `Clone + Debug + PartialEq + Eq`; it is no longer `Copy`.
- **Traits**: `OpsApi` has `rerun_run`, `analyze_rerun`, `list_dlq`, `requeue_dlq`, `dismiss_dlq`, `create_bulk`, `list_bulk`, `get_bulk`, `cancel_bulk`, `limits`, `get_failure_policy` and `set_failure_policy`. `InventoryApi` has `list_modules`, `list_queues` and `list_partitions`. Every default body returns `Unimplemented` with the message "`<method>` lands in 11-NN" and `details.method`.
- **DTOs**: exactly as in the plan's `<interfaces>`, with serde and `utoipa::ToSchema` derives. Enums are externally tagged in snake_case: `{"seq":7}`, `{"step":{"name":"x","occurrence":1}}` (occurrence defaults to 1), `"active"`/`"source"`/`{"version":3}`, `"auto"`/`{"rerun":{"from":..}}`, and `{"terminate":{"reason":..}}`. `RerunVersion` defaults to `Active` and `RequeueMode` defaults to `Auto`. `status` fields use `ostro_api::rest::RunStatusDto`. `RunPolicy` is untouched (B2). No routes were added, so the OpenAPI snapshot is unchanged.
- **Config** (`ostro_server::config`):
  - `LimitsSection {max_history_events: u64 = 50000, max_children_per_run: u64 = 10000, max_payload_bytes: ByteSize = 4MiB}` with `to_run_limits()`.
  - `UiSection {enabled: Option<bool>, allowed_hosts: Vec<String>}` with `effective_enabled(listen)`.
  - `BulkSection {max_jobs 4, max_matches 100000, default_rate 20, max_rate 500, burst 10, max_in_flight 4, backlog_high 10000, checkpoint_every 1s, checkpoint_ops 500}`.
  - `DlqSection {hook_catchup 24h, hooks_enabled true}`.
  - All four sections are `deny_unknown_fields` and added to `SECTIONS`, so env overlay and `--print-config` cover them. `UI_ALLOWED_HOSTS_ENV = "OSTRO_UI_ALLOWED_HOSTS"` is a comma-separated overlay.
  - `ServerConfig::to_engine_config()` returns `[engine]` with `run_limits` taken from `[limits]`. `ServerConfig::validate_phase11()` is called by `load_layered`. It rejects zero values and `default_rate > max_rate`, and rejects `limits.max_payload_bytes` above `api.max_request_bytes` with an error naming both keys.
- **Seam** (`ostro_server::p11`):
  - `P11Ctx {cc, runtime, limits, bulk, cfg}`.
  - `P11Feature {DlqUpdate, RerunPrefix, JournalAttempt, FailurePolicyExt}` plus `ALL`, and `p11_feature_enabled(f) -> true`.
  - `P11Controllers(pub Arc<P11Ctx>)`, which implements `OpsApi + InventoryApi` and delegates to `control::p11_*`.
  - `extensions(cc, &Arc<P11Ctx>) -> ApiExtensions`, which is the Phase 8 slots plus ops/inventory.
  - `ui_config(&ServerConfig) -> ostro_ui::UiConfig`.
  - `wire(cc: &ControlCtx, runtime: Arc<Runtime>, services: &Services, cp_p11: &Arc<OnceLock<Weak<P11Ctx>>>, api_cfg: &mut ApiConfig, router: axum::Router, cfg: &Arc<ServerConfig>) -> axum::Router`.
- **Control plane**: `ServerControlPlane.p11: Arc<OnceLock<Weak<P11Ctx>>>` is `pub(crate)` and read through `pub fn p11(&self) -> Option<Arc<P11Ctx>>`. `list_runs` tries `p11_search::list_runs_indexed(&ctx, &req).await` first; its stub returns `None`, so the existing scan still runs.
- **Stubs**: in each stub, the owning plan is named in the doc comment and in `details.lands_in`.
  - `p11_rerun::{rerun_run, analyze_rerun}` (11-10)
  - `p11_dlq::{list_dlq, requeue_dlq, dismiss_dlq, get_failure_policy, set_failure_policy}` (11-12)
  - `p11_dlq::is_dlq_member(ctx, &RunId, &RunRecord) -> Result<bool, ApiError>` (stub: Failed or TimedOut)
  - `p11_bulk::{create_bulk, list_bulk, get_bulk, cancel_bulk}` (11-13)
  - `p11_inventory::{limits, list_modules, list_queues, list_partitions}` (11-11)
  - `p11_search::list_runs_indexed` (11-11)
  - `services::bulk::{BulkRegistry::new(data_dir, BulkSection, Arc<dyn Clock>) -> Arc<Self>, spawn}` (11-13)
  - `services::dlq_hook::spawn` (11-12)
  - `rerun.rs` (11-07)
  - `onfailure.rs` (11-12)

## How the Runtime and BulkRegistry reach P11Ctx and the services seam

- **Runtime**: `server.rs` already had `runtime: Arc<Runtime>` (the one the executor uses) at the ApiExtensions site. It is passed as `Arc::clone(&runtime)` in the same hunk as the wire call.
- **BulkRegistry**: `wire` creates one instance with `BulkRegistry::new(cc.data_dir, cfg.bulk, cc.clock)` and stores it as `P11Ctx.bulk`. It then calls `Services::attach_p11(&Arc<P11Ctx>)`. `Services::start` now keeps a clone of its `ServiceCtx` (idle services keep none), and `attach_p11` calls `dlq_hook::spawn(&svc, &p11)` and `bulk::spawn(&svc, &p11)`. Both services therefore see the same `Arc<BulkRegistry>` through `p11.bulk`. Their tasks go into a separate list that `Services::shutdown` joins with the other tasks, so they release their node and meta clones before `Node::shutdown`. `running()` lists them too.

## ApiConfig / ApiExtensions literal sites fixed

At execution HEAD, `git grep -nE '\b(ApiConfig|ApiExtensions) \{'` found these sites, each fixed with `..Default::default()` or by adding the field to the struct's own definition:

- `crates/ostro-api/src/lib.rs` (Default impl: both new fields)
- `crates/ostro-api/src/phase8.rs` (struct, Default, Debug, and the `FakePhase8::extensions` literal)
- `crates/ostro-api/tests/lifecycle_surface.rs`
- `crates/ostro-api/tests/parity.rs`
- `crates/ostro-api/tests/rest.rs` (2 literals)
- `crates/ostro-server/src/server.rs`
- `crates/ostro-server/src/control/mod.rs` (`phase8_extensions`)

`crates/ostro-api/tests/phase8_surface.rs` already used `..p8.extensions()`, so it needed no change.

## FAILPOINT_CRATES

ostro-server was already in the list as `ostro-server:failpoints_phase5` and `ostro-server:children_failpoints`, and ci.yml's "Failpoint suites" step lists the same commands. `scripts/ci-local.sh` and `.github/workflows/ci.yml` are therefore unchanged. Phase 11 failpoint test targets (`rerun::`, `dlq::`, `bulk::`) will need their own `ostro-server:<target>` entries in both lists when they land (11-18 or the owning plans).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Weak P11Ctx slot instead of a strong `Arc<OnceLock<P11Ctx>>`**
- **Issue:** `P11Ctx.cc` is a `ControlCtx`, which holds the control plane strongly through `ControlCtx::partitions`. A strong `OnceLock<P11Ctx>` inside `ServerControlPlane` creates a reference cycle. Shutdown's `Arc::try_unwrap(control)` would then fail, and meta would never stop, even though the data-dir lock is released.
- **Fix:** The field is `Arc<OnceLock<Weak<P11Ctx>>>` and is read through `ServerControlPlane::p11()`. The strong owners are `P11Controllers(pub Arc<P11Ctx>)` in the router and the Phase 11 service tasks. The key_link pattern `OnceLock<.*P11Ctx` still matches. `p11_contract` asserts that a `Weak` of the control plane is dead after both `restart()` and `shutdown()`.

**2. [Rule 3 - Blocking] `wire(services: &Services, ..)` instead of `svc: &ServiceCtx`**
- **Issue:** `server.rs` moves its `ServiceCtx` into `Services::start`, and idle servers have none, so no `&ServiceCtx` exists at the wire site.
- **Fix:** `Services` keeps the context and gains `attach_p11`. As a result `services/mod.rs` changed by more than the planned "two lines": the field, `attach_p11`, and the joins in `shutdown` and `running`.

**3. [Rule 3 - Blocking] `wire` builds the API router**
- **Issue:** The extension slots and ApiConfig fields have to exist before `ostro_api::router_with` runs.
- **Fix:** `wire` calls `router_with` itself and merges the result onto the `router` argument; `server.rs` passes `axum::Router::new()`. `server.rs` has exactly one hunk with `p11::wire`. Separately, it has two small hunks: the `phase8_extensions` import is gone, and the runtime is built from `cfg.to_engine_config()` so that `[limits]` reaches `EngineConfig.run_limits`.

**4. Signature details**
- `p11_search::list_runs_indexed` is `async`, which lets 11-11 await the index.
- DTO `status` fields are `RunStatusDto`.
- `ApiConfig` is no longer `Copy`.
- `ostro-api/src/phase8.rs` was edited (outside `files_modified`) because `ApiExtensions` is defined there.

**5. [Rule 1 - Bug] Dev profile leaves `[ui] enabled` unset**
- **Found during:** Task 2 (`tests/config.rs` round-trip tests)
- **Issue:** Setting `Some(true)` in `dev_defaults` broke `effective_config_round_trips_through_toml` and `phase7_sections_round_trip_through_toml_and_print`. A server-profile print omits `None`, and reloading it on the dev base gave `Some(true)`.
- **Fix:** Both profiles leave the field unset, and the loopback rule decides. Dev's 127.0.0.1 listener means the UI is on. Dev on a non-loopback listener now needs an explicit `enabled = true`, which is stricter than D-P11-17's "dev: true".

**6. Payload versus request-limit validation runs in `load_layered`**
- This covers `ostro server`/`ostro dev` startup. A server started programmatically with a hand-built `ServerConfig` skips it, which is the existing pattern for every other value check.

**7. Workflow per the executor brief**
- I used scratch worktree `wt-11-04`, slot-2, and CAS `update-ref` on main, instead of the plan's `/tmp/ostro-wt-11-04` and private index. Task 1 landed on main before Task 2 was built.

### Not done
- The UI WARN (explicit or dev `enabled` on a non-loopback listener; the message names Phase 14) is emitted in `wire`, but no test captures it. The enable/disable logic is tested.

## Tests

- `cargo test -p ostro-api --locked`: all green, including the new `phase11::tests` (default bodies name the plan; enum wire shapes). OpenAPI snapshot unchanged.
- `cargo test -p ostro-server --test p11_contract --locked`: 10 passed. Covered:
  - defaults and engine mapping
  - TOML parse and `--print-config` round trip
  - unknown keys rejected in each of the four sections
  - bad duration/size/rate/zero values name their key
  - payload above request limit names both keys
  - env overlay, including `OSTRO_UI_ALLOWED_HOSTS`
  - UI enable defaults
  - feature gate
  - a live server: P11Ctx set, ext slots Some, stub delegation, `/v1/runs`, `/metrics`, `/` and `/healthz` unchanged, no cycle across restart and shutdown
  - a server started with all overrides
- `cargo test -p ostro-server -p ostro-ui -p ostro-api -p ostro-cli --locked --no-fail-fast`: 660 passed. The 2 failures were in `tests/config.rs` (the round-trip tests, deviation 5). After the fix, `cargo test -p ostro-server --lib --test config --test p11_contract --test engine_class` gave 134 + 38 + 10 + 5 passed.
- `cargo test -p ostro --test server_config --test cli`: 5 + 4 passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. `cargo clippy -p ostro-server -p ostro --features failpoints --all-targets -- -D warnings`: clean. `cargo fmt --all --check`: clean.
- CLOCK_GATE_SCOPED: no new `SystemTime::now`, `SystemClock` or `Instant::now` in the plan's server source files.
- Not run: the full `bash scripts/ci-local.sh --no-musl` (shared machine with 4–5 concurrent agents; the baseline run took more than 2h). The steps this plan affects were run individually.
