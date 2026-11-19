---
phase: 05-workflow-semantics-core-hardening
plan: 24
subsystem: api
tags: [d23, tenancy, qualified-names, module-name, phase-14-f1, phase-14-f4, phase-14-f5, api-05]

requires:
  - phase: 05-04
    provides: "Phase 5 API surface; hand-off: D23 grammar belongs in the ControlPlane, signal names are not resource names"
  - phase: 05-07
    provides: "WorkflowVersion.module (META_FORMAT 2) and the D23 grammar checked at meta apply (ostro-meta validate.rs)"
  - phase: 04-17
    provides: "ServerControlPlane module upload / workflow registration"
  - phase: 04-18
    provides: "ServerControlPlane start path (dedupe, routing, StartRun)"
provides:
  - "control/names.rs: validate_tenant, validate_name(field, name), request_tenant, qualify, split_qualified, qualify_request_name"
  - "ostro_meta::{is_resource_name, is_tenant_id, is_qualified_name} public (the single D23 grammar for API and apply)"
  - "Tenant-qualified start path: StartRun.workflow and queue in storage form; tenant(workflow) == tenant enforced at the API"
  - "UploadModule.tenant / UploadModule.name: REST ?tenant=&name= on PUT /v1/modules/{sha256}, UploadModuleRequest fields 4 and 5"
  - "WorkflowVersion.module = qualify(tenant, name); versions keyed <tenant>/<name>; cross-tenant re-upload registers that tenant's versions"
  - "FakeControlPlane::upload_naming(sha) test helper"
  - "tests/tenancy_api.rs (two-tenant collision, grammar 400s over REST and gRPC, upload naming)"
affects: [05-11, 05-13, 05-14, 05-17, phase-6, phase-8, phase-14]

tech-stack:
  added: []
  patterns:
    - "Grammar lives once in ostro-meta; the API validates (clean 400 with details.field) before meta apply would reject"
    - "Names may arrive bare or already qualified; a qualified name must carry the request's own tenant (qualify_request_name)"
    - "Re-upload of an existing sha rebuilds the describe() view from the earlier WorkflowVersion records: no runtime, no blob write"

key-files:
  created:
    - crates/ostro-server/src/control/names.rs
    - crates/ostro-server/tests/tenancy_api.rs
  modified:
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/control/runs.rs
    - crates/ostro-server/src/control/modules.rs
    - crates/ostro-server/tests/modules_api.rs
    - crates/ostro-server/tests/start_run_api.rs
    - crates/ostro-proto/proto/ostro/api/v1/modules.proto
    - crates/ostro-api/src/domain.rs
    - crates/ostro-api/src/rest.rs
    - crates/ostro-api/src/grpc.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-meta/src/validate.rs
    - crates/ostro-meta/src/lib.rs

key-decisions:
  - "The D23 grammar helpers in ostro-meta became pub and are re-exported, not mirrored: API and apply share one implementation"
  - "Signal names are NOT validated with the D23 grammar (05-04 hand-off and orchestrator instruction override the plan text): only their 1..=256-byte limit applies"
  - "An absent or empty tenant is the default tenant; `_` is rejected with 400 (reserved, details.field = tenant)"
  - "Start accepts the workflow/queue bare or already qualified with the request's tenant; another tenant's prefix (or a prefix without a request tenant) is 400"
  - "Queues are always qualified, including the describe() default and the built-in \"default\" (acme's default queue is acme/default)"
  - "No fallback: a tenant start for a workflow uploaded only in the default tenant is 404"
  - "GET run shows the stored (qualified) workflow and queue plus the separate tenant field"
  - "WorkflowVersion.default_queue stays module-local; qualification happens at start time"
  - "Upload validates describe() workflow names AND default queues with the resource grammar (400)"
  - "An existing sha is never recompiled or rewritten; another tenant's re-upload registers and auto-activates its versions and merges its qualified pairs into the name-less ModuleRecord by CAS"
  - "Bindings stay write-once: an explicit module name that differs from an existing binding's module is 409 (details.existing_module_name); a re-upload without a name keeps the existing bindings"
  - "OpenAPI change is strictly additive (two query parameters; response descriptions untouched)"

requirements-completed: []

duration: ~70min
completed: 2026-11-19
---

# Phase 5 Plan 24: D23 tenant-qualified names and the deployed module name

**The API now validates the D23 grammar and submits tenant-qualified `StartRun.workflow`/`queue`, so dedupe, routing and meta keys are tenant-scoped with no format change. Upload takes `tenant` and `name` and writes `WorkflowVersion.module`. Two tenants using the same workflow name and idempotency key get two distinct runs.**

## Performance

- **Duration:** about 70 min
- **Started:** 2026-11-19T07:56Z
- **Tasks:** 2
- **Files:** 2 created, 14 modified

## Accomplishments

### Task 1: grammar and start-path qualification (`0892b93`)

- **`control/names.rs`:**
  - `validate_tenant`: `_` is reserved, else `^[a-z0-9][a-z0-9-]{0,62}$`.
  - `validate_name(field, name)`: `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`.
  - `request_tenant`: `None`/`""` is the default tenant.
  - `qualify`, `split_qualified`.
  - `qualify_request_name(field, tenant, raw)`: accepts a bare or self-qualified name, rejects a foreign prefix, validates the local part, and returns the storage form.
  - Every error is 400 InvalidArgument with `details.field`.
  - Five unit tests cover the boundary cases: 63/64-char tenants, 128/129-char names, `_`, `-a`, `Acme`, `a/b`, `x:y`, `__z`, `_z`, `https://h`, non-ASCII input, and the round trip with `ostro_meta::is_qualified_name`.
- **Grammar source:** the helpers come from `ostro-meta`. `is_resource_name`, `is_tenant_id` and `is_qualified_name` went from `pub(crate)` to `pub` and are re-exported from `lib.rs`. This was a sanctioned minimal edit, so the API and meta apply share one grammar.
- **`start_run`:**
  - Validates the tenant and the workflow and queue names before resolve, payload or routing.
  - Qualifies the workflow, plus the explicit queue or else the `describe()` default or `"default"`.
  - Resolves the version through the qualified meta key.
  - Computes `partition_for_key` on the qualified workflow.
  - Submits `StartRun{workflow, queue}` in storage form with `tenant` set to the validated id. So `tenant(StartRun.workflow) == StartRun.tenant` holds by construction.

### Task 2: upload tenant and name, cross-tenant registration, collision test (`aed4ff6`)

- **API (additive only):**
  - `domain::UploadModule{tenant, name}`.
  - REST `UploadModuleQuery{tenant, name}` (`PUT /v1/modules/{sha}?tenant=acme&name=mod-a`).
  - proto `UploadModuleRequest.tenant = 4` and `name = 5` (`optional string`).
  - The OpenAPI snapshot gains exactly two query parameters.
- **Fake and parity:** the fake records `(tenant, name)` per upload (`upload_naming`). The parity scenario uploads m1 as `acme`/`mod-a` over both transports and compares what reached the control plane.
- **`control/modules.rs`:**
  - Validates the tenant, the module name, and the `describe()` workflow names and default queues.
  - Workflow versions are keyed `qualify(tenant, name)` and carry `module = qualify(tenant, module_name)`, which is empty without a name.
  - Write-once checks run on the qualified keys.
  - An existing sha skips compile, describe and the blob write. Its workflows are rebuilt from the earlier `WorkflowVersion` records (`described_from_meta`), and the missing ones are registered and auto-activated for the uploading tenant.
  - The name-less `ModuleRecord.workflows` gains the new qualified pairs through a CAS merge (`merge_module_workflows`). It does the same when a concurrent create wins.
  - A same-tenant re-upload issues no proposals.
- **`tests/tenancy_api.rs`** (3 tests):
  - **Upload naming:**
    - grammar 400s (`_`, `Acme`, `a/b` as tenant; `a/b`, `x:y`, `__z` as name);
    - the default tenant's `module = "mod-a"`;
    - an `acme` re-upload: `created=false`, no AOT cache change, `acme/sdk-*` registered with `module = "acme/mod-a"` and active, and the module record lists the pairs of both tenants;
    - the same name is a no-op and a different name is 409;
    - `beta` over gRPC with no name gets an empty module;
    - gRPC grammar errors return InvalidArgument;
    - the records survive a restart.
  - **Collision:**
    - `acme`, `beta` and the default tenant each start `sdk-basic` with key `k` and get three distinct run ids;
    - each tenant's second start returns its own run;
    - a gRPC start with workflow `acme/sdk-basic` and tenant `acme` dedupes to acme's run;
    - the records show `acme/sdk-basic`, `acme/default` and tenant `acme`;
    - the dedupe row `dedupe_key("<tenant>/sdk-basic", "k")` exists on each run's partition;
    - an explicit queue becomes `acme/q1`.
  - **Start rejects:**
    - over REST: tenant `_`, `Acme`, `a:b`; queues `a/b`, `x:y`, `__z`, and the foreign-qualified `beta/q`; workflows `x:y`, `__z` (path segment);
    - over gRPC: workflows `a/b`, `x:y`, `__z`, `acme/sdk-basic` with no tenant or tenant `beta`, and tenant `_`;
    - a tenant start for a default-only workflow returns 404 (no fallback);
    - a default-tenant start with `tenant: ""` completes with workflow `sdk-basic`, queue `default` and no tenant (behaviour unchanged).

## Verification

- `cargo test -p ostro-server --lib names`: 5 passed.
- `cargo test -p ostro-server --test tenancy_api --test start_run_api --test modules_api`: 3 + 6 + 3 passed.
- `cargo test -p ostro-api`: all suites green, including parity and the openapi snapshot.
- `cargo clippy -p ostro-server -p ostro-api -p ostro-proto -p ostro-meta --all-targets -- -D warnings`: clean. `cargo fmt --check`: clean.
- `bash scripts/ci-local.sh --no-musl` (detached worktree at `aed4ff6`, `CARGO_TARGET_DIR=target/wt-05-24`): see CI below.

## CI

`bash scripts/ci-local.sh --no-musl` ran in a detached worktree at `aed4ff6` while other executors were busy (load average 23-29).

- **Passed:** toolchain, fmt, clippy `-D warnings`, the warning-free build, SDK workspace fmt/clippy/test, the guests build, cargo-deny, check-vendored, the deny fixtures, the forbidden-crates check and workflow validation.
- **`cargo test` failed:**
  - `ostro dev_startup warm_restart_under_1s` took 1.099 s against a 1 s budget. This is a known load flake.
  - `default_data_dir_is_dot_ostro_and_persists` failed with "metadata raft did not elect a leader in time". It passed when rerun alone.
  - The plain `cargo test` stops at the first failing binary. A follow-up `cargo test --workspace --no-fail-fast` ran all 109 test binaries. Its only failure was `warm_restart_under_1s` again (load), and every other test passed, including all ostro-server and ostro-api suites.
- **Failpoint suites failed:** `ostro-log` `fatal_io` failed. `append_failure_is_never_reported_as_success` and `enospc_partial_write_is_never_reported_as_success` both found the role still `Leader` when `Offline` was expected, right after health turned Failed.
  - Rerun alone, 1 of 3 still failed. In a full `--features failpoints --no-fail-fast` rerun, ostro-log, ostro-partition and ostro-crashtest all passed.
  - ostro-log does not depend on any crate this plan touches (`cargo tree`), so this is a timing flake under load. It is **not** on the known-flake list: worth a look by the ostro-log owner (a race between the health and role watch updates).

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] `crates/ostro-server/tests/start_run_api.rs` edited (outside files_modified; owned later by 05-13, wave 5, not running)**
- **Found during:** Task 1.
- **Issue:** two completion-path tests started a *default-tenant* upload with `tenant: "acme"` or `"t-g"` and waited for `completed`. Under D23 such a start resolves `acme/sdk-basic`, which returns 404. A tenant upload would not help either: until 05-11 strips the prefix, the guest is invoked with the qualified name and the run fails. The plan's own verify command runs this test.
- **Fix:** the two option blocks no longer send a tenant, and they now assert that the tenant is absent. Tenant round-trip coverage moved to `tenancy_api.rs`, which checks the tenant field on GET.
- **Commit:** `0892b93`.

**2. [Sanctioned] `ostro-meta/src/validate.rs` and `lib.rs`:** three helpers changed from `pub(crate)` to `pub` and are re-exported (orchestrator option "make them pub"). There was no behaviour change.

**3. [Plan text vs hand-off] Signal-name grammar not applied.** The plan says signal names use the same grammar. The 05-04 summary and the orchestrator say signal names are not resource names, and that guidance was followed. No signal API was touched.

**4. [Rule 2] Extra correctness beyond the plan text:**
- the `describe()` `default_queue` is validated at upload;
- an explicit module name that differs from an existing binding is 409 rather than silently ignored;
- the cross-tenant re-upload also merges the new pairs into `ModuleRecord.workflows`, so the Phase 14 F4 visibility rule ("a tenant sees a module iff a pair carries its prefix") works.

## Known limitations

- **Tenant runs fail at execution until 05-11:** `RunStarted.workflow` still carries `acme/w`, so the guest's `run(name)` sees the qualified name. Default-tenant runs are unaffected.
- **Activation of a non-default tenant's workflow:** `PUT /v1/workflows/{name}/active` takes the name as one path segment, so `acme/w` cannot be addressed there. The first upload auto-activates, which is enough for now. This is left to 05-13/Phase 14 (tenant param or `%2F`).
- **Listing:** `GET /v1/workflows` and run listing show storage-form names, and the `workflow=` filter matches the stored form. Phase 14 makes them tenant-aware.

## Hand-off notes

### 05-13 (server ControlPlane: policies, signals, lifecycle, start fields)
- **Policy routes:** apply D23 by calling `names::request_tenant(tenant)` and then `names::qualify_request_name("workflow" | "queue", tenant, &name)`. It returns the storage key, or a 400 with `details.field`. Use that key for `MetaKey::WorkflowPolicy` / `QueuePolicy` lookups.
- **Signals:** do **not** use `validate_name` for signal names. Only the 1..=256-byte limit applies (05-04).
- **New start fields** (`schedule_to_start`, `retention`, `dry_run`): add them after the D23 block at the top of `runs::start_run`. Keep `workflow`/`queue` as the qualified locals; `req.workflow`/`req.queue` are consumed there. A dry-run start must run the same validation so it returns the same 400s.
- **Retry restart:** copy `RunRecord.workflow` as stored (already qualified) together with the predecessor's tenant. Do not re-qualify it.
- **`start_run_api.rs`:** in 05-24 the two completion-path tests stopped sending a tenant (Deviation 1). Once 05-11 strips the journal name, the tenant can go back in those tests, provided the module is also uploaded with `?tenant=` and the asserted workflow/queue become qualified.
- **`tenancy_api.rs` must stay green.**

### 05-11
- `run_started.rs` must strip a valid `<tenant>/` prefix: `names::split_qualified` is the rule, or `ostro_meta::is_tenant_id` on the prefix. `names` is `pub mod` under `control`.
- After that, a tenancy test can drive an `acme` run to `completed`.

### 05-14 (CLI)
- `ostro deploy --name/--tenant` maps to `PUT /v1/modules/{sha}?name=<stem>&tenant=<t>`.
- A re-deploy under a different `--name` for already-registered versions returns 409 with `details.existing_module_name`.

### Phase 6
- Resolve the module name as `(RunRecord.workflow, workflow_version) → WorkflowVersion.module`. The value is tenant-qualified, and empty when the uploader gave no name.

## STATE notes (for the orchestrator; STATE.md not edited)

- 05-24 complete (2/2 tasks). Decisions:
  - grammar shared from ostro-meta;
  - signal names exempt;
  - queues always qualified;
  - no default-tenant fallback;
  - module name write-once (409).
- Requirement API-05 is not marked: it is shared with 05-04/05-13 and is proven by 05-13.
